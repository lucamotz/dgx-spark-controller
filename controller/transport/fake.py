# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Deterministic in-memory ``NodeTransport`` for lifecycle and failure tests.

It models enough node reality to prove the controller invariants: a model consumes
memory, an endpoint can be healthy with the wrong alias, and an unmanaged process
is visible but never eligible for a generic stop.  No timers or subprocesses are
used; delayed readiness advances on successive observations.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from controller.operations import DeploymentSpec, NodeAssignment, utc_now
from controller.telemetry import MemorySample, NodeTelemetry
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


@dataclass
class FakeNode:
    """Mutable fake-node fixture with explicit, opt-in fault injection fields."""

    node_id: str
    baseline_free_memory_bytes: int = 64 * 1024 * 1024 * 1024
    reachable: bool = True
    alias_override: str | None = None
    start_delay_observations: int = 0
    never_ready: bool = False
    release_memory_on_stop: bool = True
    fail_drain: bool = False
    fail_start: bool = False
    fail_stop: bool = False
    canary_http_status: int = 200
    canary_finish_reason: str | None = "stop"
    canary_completion_tokens: int = 1
    preflight_failures: dict[str, str] = field(default_factory=dict)
    active_deployment_id: str | None = None
    active_alias: str | None = None
    active_memory_bytes: int = 0
    managed: bool = False
    unit_state: UnitState = UnitState.STOPPED
    endpoint_healthy: bool = False
    _pending_observations: int = 0
    _logs: list[str] = field(default_factory=list)

    @property
    def free_memory_bytes(self) -> int:
        return self.baseline_free_memory_bytes - self.active_memory_bytes


class FakeNodeTransport:
    """A transport fixture whose public fault fields are intentionally test-friendly."""

    def __init__(self, nodes: Mapping[str, FakeNode] | Sequence[str]) -> None:
        if isinstance(nodes, Mapping):
            self.nodes = dict(nodes)
        else:
            self.nodes = {node_id: FakeNode(node_id=node_id) for node_id in nodes}
        if not self.nodes:
            raise ValueError("FakeNodeTransport requires at least one node")
        self.calls: list[tuple[str, str, str]] = []

    def node(self, node_id: str) -> FakeNode:
        """Return a fixture node for deliberate test setup."""
        try:
            return self.nodes[node_id]
        except KeyError as error:
            raise KeyError(f"unknown fake node {node_id}") from error

    def seed_deployment(
        self,
        node_id: str,
        *,
        deployment_id: str,
        alias: str,
        memory_bytes: int = 0,
        managed: bool = True,
        endpoint_healthy: bool = True,
    ) -> None:
        """Place a running process on a fake node without going through start."""
        node = self.node(node_id)
        node.active_deployment_id = deployment_id
        node.active_alias = alias
        node.active_memory_bytes = memory_bytes
        node.managed = managed
        node.unit_state = UnitState.RUNNING
        node.endpoint_healthy = endpoint_healthy
        node._pending_observations = 0
        node._logs.append(f"seeded deployment {deployment_id}")

    def sample_telemetry(self, node_id: str) -> NodeTelemetry:
        node = self.node(node_id)
        self.calls.append(("telemetry", node_id, ""))
        if not node.reachable:
            return NodeTelemetry(node_id, simulated=True)
        return NodeTelemetry(
            node_id=node_id,
            sampled_at=utc_now(),
            status="ok",
            memory=MemorySample(
                node.baseline_free_memory_bytes, node.free_memory_bytes, 0
            ),
            gpu_utilization_percent=35.0 if node.active_deployment_id else 0.0,
            message="Simulated metrics.",
            simulated=True,
        )

    def observe(self, node_id: str) -> NodeObservation:
        node = self.node(node_id)
        self.calls.append(("observe", node_id, node.active_deployment_id or ""))
        if (
            node.reachable
            and node.unit_state == UnitState.STARTING
            and node._pending_observations > 0
        ):
            node._pending_observations -= 1
            if node._pending_observations == 0:
                node.unit_state = UnitState.RUNNING
                if not node.never_ready:
                    node.endpoint_healthy = True
        aliases: tuple[str, ...] = ()
        if node.endpoint_healthy and node.active_alias:
            aliases = (node.alias_override or node.active_alias,)
        return NodeObservation(
            node_id=node_id,
            reachable=node.reachable,
            free_memory_bytes=node.free_memory_bytes,
            unit_state=node.unit_state if node.reachable else UnitState.UNKNOWN,
            deployment_id=node.active_deployment_id,
            active_alias=node.active_alias,
            served_aliases=aliases,
            endpoint_healthy=node.endpoint_healthy if node.reachable else False,
            managed=node.managed,
            message="" if node.reachable else "simulated SSH loss",
            facts={"fake": True},
        )

    def preflight(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> Sequence[PreflightCheck]:
        node = self.node(node_id)
        self.calls.append(("preflight", node_id, spec.deployment_id))
        required_memory = int(spec.profile.get("required_memory_bytes", 0))
        required_free = max(spec.minimum_free_memory_bytes, required_memory)
        # A managed deployment selected for replacement is intentionally still
        # consuming memory during static preflight.  The orchestrator validates
        # actual free memory only after its targeted stop has been proven.
        deferred_for_managed_conflict = bool(node.active_deployment_id and node.managed)
        checks = [
            PreflightCheck(
                "reachable",
                node.reachable,
                "node reachable" if node.reachable else "simulated node is unreachable",
                "Restore node transport connectivity before retrying.",
            ),
            PreflightCheck(
                "memory",
                deferred_for_managed_conflict
                or node.free_memory_bytes >= required_free,
                "memory check deferred until managed conflict is stopped"
                if deferred_for_managed_conflict
                else f"free memory is {node.free_memory_bytes} bytes",
                "Free memory or select a profile with lower requirements.",
            ),
        ]
        checks.extend(
            PreflightCheck(
                name, False, message, "Resolve this preflight gate before retrying."
            )
            for name, message in sorted(node.preflight_failures.items())
        )
        return checks

    def drain(self, node_id: str, deployment_id: str) -> CommandResult:
        node = self.node(node_id)
        self.calls.append(("drain", node_id, deployment_id))
        if not node.reachable:
            return _unreachable()
        if node.fail_drain:
            return CommandResult(False, "simulated drain failure", "drain_failed")
        node._logs.append(f"draining {deployment_id}")
        return CommandResult(True, "drain complete")

    def start(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> CommandResult:
        node = self.node(node_id)
        self.calls.append(("start", node_id, spec.deployment_id))
        if not node.reachable:
            return _unreachable()
        if node.fail_start:
            node._logs.append(f"failed to start {spec.deployment_id}")
            return CommandResult(False, "simulated start failure", "start_failed")
        if (
            node.active_deployment_id
            and node.active_deployment_id != spec.deployment_id
        ):
            return CommandResult(
                False, "another deployment is still active", "conflicting_deployment"
            )
        required_memory = int(spec.profile.get("required_memory_bytes", 0))
        if node.free_memory_bytes < required_memory:
            return CommandResult(
                False, "insufficient free memory", "insufficient_memory"
            )
        node.active_deployment_id = spec.deployment_id
        node.active_alias = spec.expected_alias
        node.active_memory_bytes = required_memory
        node.managed = True
        node.unit_state = (
            UnitState.STARTING if node.start_delay_observations else UnitState.RUNNING
        )
        node.endpoint_healthy = (
            not node.start_delay_observations and not node.never_ready
        )
        node._pending_observations = node.start_delay_observations
        node._logs.append(f"started {assignment.role} for {spec.deployment_id}")
        return CommandResult(True, "start accepted")

    def stop(
        self,
        node_id: str,
        deployment_id: str,
        *,
        timeout_seconds: float,
        force: bool = False,
    ) -> CommandResult:
        node = self.node(node_id)
        self.calls.append(("stop-force" if force else "stop", node_id, deployment_id))
        if not node.reachable:
            return _unreachable()
        if node.active_deployment_id not in {None, deployment_id}:
            return CommandResult(
                False, "deployment identity does not match", "ownership_mismatch"
            )
        if node.fail_stop:
            return CommandResult(False, "simulated stop failure", "stop_failed")
        if node.active_deployment_id is None:
            return CommandResult(True, "deployment already stopped")
        node.unit_state = UnitState.STOPPED
        node.endpoint_healthy = False
        node.active_deployment_id = None
        node.active_alias = None
        node.managed = False
        node._pending_observations = 0
        if node.release_memory_on_stop:
            node.active_memory_bytes = 0
        node._logs.append(f"stopped {deployment_id}{' forcefully' if force else ''}")
        return CommandResult(True, "stop complete")

    def recover(self, spec: DeploymentSpec) -> CommandResult:
        self.calls.append(("recover", spec.assignments[0].node_id, spec.deployment_id))
        for assignment in spec.assignments:
            node = self.node(assignment.node_id)
            if not node.reachable:
                return _unreachable()
            if node.active_deployment_id not in {None, spec.deployment_id}:
                return CommandResult(
                    False,
                    "runtime identity does not match",
                    "recovery_identity_mismatch",
                )
            if (
                node.active_deployment_id == spec.deployment_id
                and node.active_alias != spec.expected_alias
            ):
                return CommandResult(
                    False, "runtime alias does not match", "recovery_identity_mismatch"
                )
        for assignment in spec.assignments:
            result = self.stop(
                assignment.node_id,
                spec.deployment_id,
                timeout_seconds=spec.stop_timeout_seconds,
                force=False,
            )
            if not result.ok:
                return result
        return CommandResult(True, "reviewed placement reset to baseline")

    def completion_canary(
        self, node_id: str, spec: DeploymentSpec, request: CanaryRequest
    ) -> CanaryResult:
        node = self.node(node_id)
        self.calls.append(("canary", node_id, spec.deployment_id))
        if not node.reachable:
            return CanaryResult(False, "completion_canary_failed", "simulated SSH loss")
        serving = (
            node.endpoint_healthy
            and node.active_deployment_id == spec.deployment_id
            and (node.alias_override or node.active_alias) == spec.expected_alias
        )
        status = node.canary_http_status if serving else 404
        body = {
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": ""},
                    "finish_reason": node.canary_finish_reason,
                }
            ],
            "usage": {
                "completion_tokens": min(
                    node.canary_completion_tokens, request.max_tokens
                )
            },
        }
        node._logs.append(f"completion canary for {spec.deployment_id}: {status}")
        return evaluate_canary_response(status, json.dumps(body), latency_ms=1)

    def logs(
        self,
        node_id: str,
        deployment_id: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> LogChunk:
        node = self.node(node_id)
        start = int(cursor or "0")
        bounded_limit = max(0, min(limit, 1_000))
        lines = tuple(node._logs[start : start + bounded_limit])
        next_index = start + len(lines)
        return LogChunk(
            lines=lines,
            next_cursor=str(next_index) if next_index < len(node._logs) else None,
        )


def _unreachable() -> CommandResult:
    return CommandResult(False, "simulated SSH loss", "node_unreachable")
