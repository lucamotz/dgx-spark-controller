# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""SSH transport that observes and controls named deployments on Sparks.

The orchestrator calls :class:`ManagedSSHTransport`. A single-node profile runs
as one named ``systemd --user`` unit owned by :class:`SystemdUserRunner`; a
shared vLLM profile runs through its pinned :class:`ClusterScriptRecipe`. All
remote scripts are fixed templates whose values come from validated
node/profile configuration, never from browser-supplied shell text.
"""

from __future__ import annotations

import contextlib
import json
import re
import shlex
from collections.abc import Mapping, Sequence

from controller.operations import DeploymentSpec, NodeAssignment
from controller.runners.systemd_user import SystemdUserRunner
from controller.telemetry import NodeTelemetry
from controller.transport import cluster_scripts
from controller.transport.base import (
    CanaryRequest,
    CanaryResult,
    CommandResult,
    LogChunk,
    NodeObservation,
    PreflightCheck,
    UnitState,
    evaluate_canary_response,
)
from controller.transport.managed_config import (
    DEPLOYMENT_ID_RE,
    HEAD_SCRIPT_LAUNCH,
    PER_NODE_LAUNCH,
    ClusterScriptRecipe,
    ManagedSSHNode,
    required_mapping,
    required_string,
)
from controller.transport.observation import (
    cluster_observe_script,
    combine_observations,
    parse_cluster_observation,
    parse_unit_observation,
    unit_observe_script,
)
from controller.transport.preflight_checks import cluster_preflight, unit_preflight
from controller.transport.script_output import decode_b64, parse_fields
from controller.transport.ssh import SSHExecution, SSHExecutionError, SSHExecutor
from controller.transport.telemetry import sample_ssh_telemetry

# Kept importable from this module for existing callers.
from controller.transport.vllm_identity import (  # noqa: F401
    command_has_exact_tokens as _command_has_exact_tokens,
)

__all__ = [
    "HEAD_SCRIPT_LAUNCH",
    "PER_NODE_LAUNCH",
    "ClusterScriptRecipe",
    "ManagedSSHNode",
    "ManagedSSHTransport",
]

# A completion canary reads back at most this much of the response body; a
# bounded max_tokens keeps a valid answer far below it.
CANARY_BODY_LIMIT_BYTES = 64 * 1024


class ManagedSSHTransport:
    """Observe and control named script deployments on configured SSH nodes."""

    def __init__(
        self,
        executor: SSHExecutor,
        runner: SystemdUserRunner,
        nodes: Mapping[str, ManagedSSHNode],
        cluster_recipes: Sequence[ClusterScriptRecipe] = (),
    ) -> None:
        self._executor = executor
        self._runner = runner
        self._nodes = dict(nodes)
        if not self._nodes or any(key != node.id for key, node in self._nodes.items()):
            raise ValueError(
                "managed node registry must be non-empty and keyed by node id"
            )
        self._cluster_recipes = tuple(cluster_recipes)
        self._cluster_by_deployment = {
            recipe.deployment_id: recipe for recipe in self._cluster_recipes
        }
        if len(self._cluster_by_deployment) != len(self._cluster_recipes):
            raise ValueError("cluster deployment ids must be unique")

    def _node(self, node_id: str) -> ManagedSSHNode:
        try:
            return self._nodes[node_id]
        except KeyError as exc:
            raise KeyError(f"unknown managed node: {node_id}") from exc

    @staticmethod
    def unit_name(deployment_id: str) -> str:
        if not DEPLOYMENT_ID_RE.fullmatch(deployment_id):
            raise ValueError(
                "deployment id must use lowercase letters, digits, and hyphens "
                "(max 53 characters)"
            )
        return f"dgx-model-{deployment_id}"

    def sample_telemetry(self, node_id: str) -> NodeTelemetry:
        self._node(node_id)
        return sample_ssh_telemetry(self._executor, node_id)

    def observe(self, node_id: str) -> NodeObservation:
        node = self._node(node_id)
        own_recipes = [
            recipe for recipe in self._cluster_recipes if node_id in recipe.node_ids
        ]
        try:
            result = self._executor.run_script(
                node_id,
                unit_observe_script(node, own_recipes),
                timeout_seconds=15,
                check=True,
            )
        except SSHExecutionError as exc:
            return NodeObservation(
                node_id=node_id,
                reachable=False,
                free_memory_bytes=0,
                message=str(exc),
            )
        unit = parse_unit_observation(node, result.stdout)
        clusters = [
            observation
            for recipe in own_recipes
            if (observation := self._observe_cluster(node_id, recipe)) is not None
        ]
        return combine_observations(unit, clusters)

    def _observe_cluster(
        self,
        node_id: str,
        recipe: ClusterScriptRecipe,
    ) -> NodeObservation | None:
        is_head = node_id == recipe.control_node_id
        # Per-node recipes start a launcher unit on every rank, so every node
        # reports its own launcher outcome; head-script recipes only on the head.
        reads_launcher = is_head or recipe.per_node
        try:
            result = self._executor.run_script(
                node_id,
                cluster_observe_script(
                    recipe, is_head=is_head, reads_launcher=reads_launcher
                ),
                timeout_seconds=15,
                check=True,
            )
        except SSHExecutionError as exc:
            return NodeObservation(
                node_id=node_id,
                reachable=False,
                free_memory_bytes=0,
                message=str(exc),
                facts={"cluster_probe_failed": True},
            )
        return parse_cluster_observation(
            node_id,
            recipe,
            result.stdout,
            is_head=is_head,
            reads_launcher=reads_launcher,
        )

    def preflight(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> Sequence[PreflightCheck]:
        if spec.profile.get("deployment_mode") == "shared-vllm":
            recipe = self._cluster_by_deployment.get(spec.deployment_id)
            if recipe is None:
                return (
                    PreflightCheck(
                        "cluster_recipe",
                        False,
                        "cluster recipe is not registered",
                        "Restart with the reviewed profile registry.",
                    ),
                )
            return cluster_preflight(
                self._executor, self.observe, node_id, spec, assignment, recipe
            )
        self._node(node_id)
        return unit_preflight(self._executor, self.observe, node_id, spec)

    def drain(self, node_id: str, deployment_id: str) -> CommandResult:
        self._node(node_id)
        return CommandResult(
            True, f"deployment {deployment_id} has no configured proxy drain"
        )

    def start(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> CommandResult:
        if spec.profile.get("deployment_mode") == "shared-vllm":
            return self._start_cluster(node_id, spec, assignment)
        runner = required_mapping(spec.profile, "runner")
        recipe_id = required_string(runner, "script_id")
        return self._runner.start_unit(
            node_id, self.unit_name(spec.deployment_id), recipe_id
        )

    def _start_cluster(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> CommandResult:
        recipe = self._cluster_by_deployment.get(spec.deployment_id)
        if recipe is None:
            return CommandResult(
                False, "cluster recipe is not registered", "cluster_recipe_missing"
            )
        if assignment.role == "worker" and not recipe.per_node:
            return CommandResult(
                True, "worker is started first by the reviewed head launcher"
            )
        target = node_id if recipe.per_node else recipe.control_node_id
        rendered_environment = self._render_cluster_environment(recipe, target)
        if not rendered_environment.ok:
            return rendered_environment
        marker = self._write_cluster_markers(
            recipe, (target,) if recipe.per_node else None
        )
        if not marker.ok:
            return marker
        try:
            result = self._executor.run_script(
                target,
                cluster_scripts.launch_script(
                    recipe, target, spec.stop_timeout_seconds
                ),
                timeout_seconds=max(60, spec.startup_timeout_seconds + 60),
                check=False,
            )
        except SSHExecutionError as exc:
            self._stop_cluster_recipe(recipe, spec.stop_timeout_seconds, node_id=target)
            return CommandResult(False, str(exc), "cluster_start_failed")
        if not result.ok:
            self._stop_cluster_recipe(recipe, spec.stop_timeout_seconds, node_id=target)
            return _failure(result, "cluster_start_failed")
        if recipe.per_node:
            return CommandResult(
                True,
                f"pinned launcher dispatched for the {assignment.role} rank "
                f"on {target}",
            )
        return CommandResult(
            True,
            "durable cluster launcher dispatched and both ranks are controller-owned",
        )

    def stop(
        self,
        node_id: str,
        deployment_id: str,
        *,
        timeout_seconds: float,
        force: bool = False,
    ) -> CommandResult:
        recipe = self._cluster_by_deployment.get(deployment_id)
        if recipe is not None:
            if recipe.per_node:
                return self._stop_cluster_recipe(
                    recipe, timeout_seconds, node_id=node_id
                )
            if node_id != recipe.control_node_id:
                return CommandResult(
                    True, "worker was stopped by the reviewed head stop script"
                )
            return self._stop_cluster_recipe(recipe, timeout_seconds)
        return self._runner.stop_unit(
            node_id,
            self.unit_name(deployment_id),
            timeout_seconds=timeout_seconds,
            force=force,
        )

    def recover(self, spec: DeploymentSpec) -> CommandResult:
        """Stop only a recognized reviewed cluster recipe, marker or not."""
        recipe = self._cluster_by_deployment.get(spec.deployment_id)
        if recipe is None or spec.profile.get("deployment_mode") != "shared-vllm":
            return CommandResult(
                False,
                "recovery is not supported for this profile",
                "recovery_not_supported",
            )
        for node_id in recipe.node_ids:
            # Recovery is destructive, so a failed identity probe must never be
            # treated like a successful observation of an absent container.
            observation = self._observe_cluster(node_id, recipe)
            if observation is None:
                continue
            if not observation.reachable:
                return CommandResult(
                    False,
                    f"runtime identity could not be proven on {node_id}",
                    "node_unreachable",
                )
            exact_stopped_container = (
                observation.unit_state is UnitState.STOPPED
                and not observation.is_process_running
                and not observation.endpoint_healthy
                and bool(observation.facts.get("static_identity_matches"))
            )
            if (
                not bool(observation.facts.get("identity_matches"))
                and not exact_stopped_container
            ):
                return CommandResult(
                    False,
                    f"recognized container identity does not match on {node_id}",
                    "recovery_identity_mismatch",
                )
        if recipe.per_node:
            ordered = sorted(
                recipe.node_ids, key=lambda item: (item != recipe.control_node_id, item)
            )
            for node_id in ordered:
                result = self._stop_cluster_recipe(
                    recipe, spec.stop_timeout_seconds, node_id=node_id
                )
                if not result.ok:
                    return result
            return CommandResult(True, "pinned stop script completed on every rank")
        return self._stop_cluster_recipe(recipe, spec.stop_timeout_seconds)

    def completion_canary(
        self, node_id: str, spec: DeploymentSpec, request: CanaryRequest
    ) -> CanaryResult:
        """Send one bounded chat completion from the serving node to its endpoint.

        The URL root is the one readiness observation uses: loopback for a
        shared vLLM rank, the configured management address for an independent
        unit. The body is built from reviewed spec values only; the response
        is judged here and never returned or stored.
        """
        node = self._node(node_id)
        recipe = self._cluster_by_deployment.get(spec.deployment_id)
        root = (
            f"http://127.0.0.1:{recipe.endpoint_port}"
            if recipe is not None
            else node.endpoint_root
        )
        payload = json.dumps(
            request.payload(spec.expected_alias), separators=(",", ":")
        )
        try:
            result = self._executor.run_script(
                node_id,
                _canary_script(root, request, payload),
                timeout_seconds=request.timeout_seconds + 30,
                check=False,
            )
        except (SSHExecutionError, ValueError) as exc:
            return CanaryResult(False, "completion_canary_failed", str(exc))
        if not result.ok:
            # Never echo the script's output: it may hold response text.
            return CanaryResult(
                False,
                "completion_canary_failed",
                f"completion canary script exited with status {result.returncode}",
            )
        fields = parse_fields(result.stdout)
        status_text, _, seconds_text = fields.get("RESULT", "").partition(" ")
        status = (
            int(status_text) if re.fullmatch(r"[1-5][0-9]{2}", status_text) else None
        )
        try:
            latency_ms: int | None = max(0, round(float(seconds_text) * 1000))
        except (ValueError, OverflowError):
            latency_ms = None
        return evaluate_canary_response(
            status, decode_b64(fields.get("BODY_B64", "")), latency_ms
        )

    def logs(
        self,
        node_id: str,
        deployment_id: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> LogChunk:
        recipe = self._cluster_by_deployment.get(deployment_id)
        if recipe is None:
            return self._runner.unit_logs(
                node_id, self.unit_name(deployment_id), cursor=cursor, limit=limit
            )
        try:
            result = self._executor.run_script(
                node_id,
                cluster_scripts.container_logs_script(
                    recipe, max(1, min(int(limit), 500))
                ),
                timeout_seconds=15,
                check=False,
            )
        except SSHExecutionError as exc:
            return LogChunk((f"log retrieval failed: {exc}",), None)
        return LogChunk(tuple(result.stdout.splitlines()), None)

    def _write_cluster_markers(
        self, recipe: ClusterScriptRecipe, node_ids: Sequence[str] | None = None
    ) -> CommandResult:
        script = cluster_scripts.write_marker_script(recipe)
        for node_id in node_ids or recipe.node_ids:
            try:
                result = self._executor.run_script(
                    node_id, script, timeout_seconds=15, check=False
                )
            except SSHExecutionError as exc:
                return CommandResult(False, str(exc), "cluster_ownership_marker_failed")
            if not result.ok:
                return CommandResult(
                    False,
                    result.stderr.strip() or "ownership marker failed",
                    "cluster_ownership_marker_failed",
                )
        return CommandResult(True, "ownership markers written")

    def _render_cluster_environment(
        self, recipe: ClusterScriptRecipe, node_id: str | None = None
    ) -> CommandResult:
        """Copy the operator environment and pin only profile-reviewed values."""
        target = node_id or recipe.control_node_id
        try:
            result = self._executor.run_script(
                target,
                cluster_scripts.environment_overlay_script(recipe, target),
                timeout_seconds=15,
                check=False,
            )
        except SSHExecutionError as exc:
            return CommandResult(False, str(exc), "cluster_environment_failed")
        if not result.ok:
            return _failure(
                result,
                "cluster_environment_failed",
                fallback="cluster environment render failed",
            )
        return CommandResult(True, "controller environment overlay rendered")

    def _stop_cluster_recipe(
        self,
        recipe: ClusterScriptRecipe,
        timeout_seconds: float,
        *,
        node_id: str | None = None,
    ) -> CommandResult:
        """Run the pinned stop script on the control node, or on one per-node rank."""
        target = node_id or recipe.control_node_id
        try:
            self._executor.run_script(
                target,
                cluster_scripts.launcher_reset_script(recipe),
                timeout_seconds=max(30, timeout_seconds),
                check=False,
            )
            result = self._executor.run_script(
                target,
                cluster_scripts.stop_script(recipe),
                timeout_seconds=max(30, timeout_seconds + 30),
                check=False,
            )
        except SSHExecutionError as exc:
            return CommandResult(False, str(exc), "cluster_stop_failed")
        if not result.ok:
            return _failure(result, "cluster_stop_failed")
        self._remove_cluster_markers(recipe, (target,) if recipe.per_node else None)
        if recipe.per_node:
            return CommandResult(True, f"pinned stop script completed on {target}")
        return CommandResult(True, "cluster stop script completed")

    def _remove_cluster_markers(
        self, recipe: ClusterScriptRecipe, node_ids: Sequence[str] | None = None
    ) -> None:
        script = cluster_scripts.remove_marker_script(recipe)
        for node_id in node_ids or recipe.node_ids:
            with contextlib.suppress(SSHExecutionError):
                self._executor.run_script(
                    node_id, script, timeout_seconds=15, check=False
                )


def _failure(result: SSHExecution, code: str, *, fallback: str = "") -> CommandResult:
    """A failed remote command, described by its own stderr or stdout."""
    return CommandResult(
        False, result.stderr.strip() or result.stdout.strip() or fallback, code
    )


def _canary_script(root: str, request: CanaryRequest, payload: str) -> str:
    return f"""set -u
body_file=$(mktemp)
cleanup() {{
  test ! -e "$body_file" || unlink "$body_file"
}}
trap cleanup EXIT HUP INT TERM
result=$(printf '%s' {shlex.quote(payload)} | curl -sS -o "$body_file" \\
  -w '%{{http_code}} %{{time_total}}' --max-time {request.timeout_seconds} \\
  -H 'Content-Type: application/json' --data-binary @- \\
  {shlex.quote(root + request.completion_path)} 2>/dev/null || true)
printf '%s\\n' "RESULT=$result" \\
  "BODY_B64=$(head -c {CANARY_BODY_LIMIT_BYTES} "$body_file" | base64 -w0)"
"""
