# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Concrete single-node SSH transport for allowlisted script deployments.

The generic orchestrator calls this class; this class delegates exact process
ownership to :class:`SystemdUserRunner`. All remote scripts are fixed templates
whose values come from validated node/profile configuration, never from browser
supplied shell text.
"""

from __future__ import annotations

import contextlib
import json
import re
import shlex
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from controller.operations import DeploymentSpec, NodeAssignment
from controller.runners.systemd_user import SystemdUserRunner
from controller.telemetry import NodeTelemetry
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
from controller.transport.script_output import (
    decode_b64,
    parse_fields,
    parse_int,
    parse_key_values,
)
from controller.transport.ssh import SSHExecutionError, SSHExecutor
from controller.transport.telemetry import sample_ssh_telemetry

DEPLOYMENT_ID_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,52}\Z")
HEAD_SCRIPT_LAUNCH = "head-script"
PER_NODE_LAUNCH = "per-node"
# "degraded" only means some unrelated user unit failed (for example a desktop
# snap notifier); the manager can still start and track transient units.
USER_MANAGER_CHECK = (
    'case "$(systemctl --user is-system-running 2>/dev/null)" in '
    "running|degraded) true ;; *) false ;; esac"
)
PROJECT_NAME_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}\Z")
# A completion canary reads back at most this much of the response body; a
# bounded max_tokens keeps a valid answer far below it.
CANARY_BODY_LIMIT_BYTES = 64 * 1024
IDENTITY_ENVIRONMENT_KEYS = (
    "MAX_MODEL_LEN",
    "MAX_NUM_SEQS",
    "MAX_NUM_BATCHED_TOKENS",
    "LONG_PREFILL_TOKEN_THRESHOLD",
    "GPU_MEMORY_UTILIZATION",
    "KV_CACHE_MEMORY_BYTES",
    "KV_CACHE_DTYPE",
    "ATTENTION_BACKEND",
    "MTP_NUM_TOKENS",
    "ENABLE_LEGACY_DSV4_TOKENIZER_SHIM",
    "FLASHINFER_WORKSPACE_BASE",
    "VLLM_MEMORY_GUARD_EMERGENCY_AVAILABLE_MIB",
    "VLLM_MEMORY_GUARD_PRESSURE_AVAILABLE_MIB",
    "VLLM_MEMORY_GUARD_FULL_PSI_AVG10",
    "VLLM_MEMORY_GUARD_SWAP_GROWTH_MIB",
    "VLLM_MEMORY_GUARD_RAPID_DROP_MIB_PER_SECOND",
    "VLLM_MEMORY_GUARD_POLL_SECONDS",
    "VLLM_MEMORY_GUARD_TERMINATE_GRACE_SECONDS",
    "MOE_BACKEND",
    "ENFORCE_EAGER",
    "FLASHINFER_AUTOTUNE",
)

# Docker Compose consumes these recipe variables while rendering Config.Cmd in
# the retained DeepSeek recipe.  They therefore may be absent from Config.Env
# even though their effective values remain fixed in the container identity.
# Only fall back to the rendered command when the variable is absent; an
# explicitly conflicting environment value must still fail closed.
COMMAND_SUBSTITUTED_ENVIRONMENT_FLAGS = {
    "MAX_MODEL_LEN": "--max-model-len",
    "MAX_NUM_SEQS": "--max-num-seqs",
    "MAX_NUM_BATCHED_TOKENS": "--max-num-batched-tokens",
    "LONG_PREFILL_TOKEN_THRESHOLD": "--long-prefill-token-threshold",
    "GPU_MEMORY_UTILIZATION": "--gpu-memory-utilization",
    "KV_CACHE_MEMORY_BYTES": "--kv-cache-memory-bytes",
}


@dataclass(frozen=True)
class ClusterScriptRecipe:
    """Pinned two-node recipe whose head launcher owns both Docker ranks."""

    profile_id: str
    deployment_id: str
    node_ids: tuple[str, ...]
    control_node_id: str
    start_script_path: str
    stop_script_path: str
    start_sha256: str
    stop_sha256: str
    working_directory: str
    environment_path: str
    required_environment: Mapping[str, str]
    expected_recipe_commit: str
    image_digest: str
    container_name: str
    endpoint_port: int
    health_path: str
    models_path: str
    expected_alias: str
    required_command_fragments: tuple[str, ...]
    launch_mode: str = HEAD_SCRIPT_LAUNCH
    node_environment: Mapping[str, Mapping[str, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not DEPLOYMENT_ID_RE.fullmatch(self.deployment_id):
            raise ValueError("invalid fixed cluster deployment id")
        if self.launch_mode not in {HEAD_SCRIPT_LAUNCH, PER_NODE_LAUNCH}:
            raise ValueError("unsupported cluster launch mode")
        minimum_nodes = 1 if self.per_node else 2
        if (
            self.control_node_id not in self.node_ids
            or len(self.node_ids) < minimum_nodes
        ):
            raise ValueError(
                "cluster recipe requires its control node and enough nodes "
                "for its launch mode"
            )
        if self.per_node and set(self.node_environment) != set(self.node_ids):
            raise ValueError("per-node recipes need one environment overlay per node")
        if not PROJECT_NAME_RE.fullmatch(self.container_name):
            raise ValueError("invalid cluster container name")

    @property
    def per_node(self) -> bool:
        """Whether the controller starts every rank itself (generated recipes)."""
        return self.launch_mode == PER_NODE_LAUNCH

    def environment_for(self, node_id: str) -> dict[str, str]:
        """Profile-wide values plus this node's rank overlay, as rendered remotely."""
        values = dict(self.required_environment)
        values.update(self.node_environment.get(node_id, {}))
        return values

    @property
    def marker_name(self) -> str:
        return f"{self.deployment_id}.owner"

    @property
    def managed_environment_path(self) -> str:
        """Private recipe environment rendered from the reviewed profile."""
        return f"{self.working_directory}/.env.dgx-controller-{self.profile_id}"

    @property
    def launcher_unit_name(self) -> str:
        """Stable remote unit that owns one in-flight cluster launcher."""
        return f"dgx-cluster-{self.deployment_id}.service"


@dataclass(frozen=True)
class ManagedSSHNode:
    """Observed-service settings for one configured Spark."""

    id: str
    management_address: str
    endpoint_port: int = 11434
    health_path: str = "/health"
    models_path: str = "/v1/models"
    legacy_process_pattern: str = "build/bin/llama-server -m"
    # Listening ports in this inclusive range are asked for /v1/models so an
    # inference server the controller did not start is named, not just counted.
    probe_port_range: tuple[int, int] | None = None

    def __post_init__(self) -> None:
        if not self.id or not self.management_address:
            raise ValueError("managed node id and management address are required")
        if not 1 <= self.endpoint_port <= 65535:
            raise ValueError("endpoint port must be between 1 and 65535")
        if not self.health_path.startswith("/") or not self.models_path.startswith("/"):
            raise ValueError("endpoint paths must be absolute URL paths")
        if not self.legacy_process_pattern.strip():
            raise ValueError("legacy process pattern must not be empty")
        if self.probe_port_range is not None:
            start, end = self.probe_port_range
            if not 1 <= start <= end <= 65535:
                raise ValueError("probe port range must be ordered within 1-65535")

    @property
    def endpoint_root(self) -> str:
        return f"http://{self.management_address}:{self.endpoint_port}"


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
        root = node.endpoint_root
        own_recipes = [
            recipe for recipe in self._cluster_recipes if node_id in recipe.node_ids
        ]
        known_containers = " ".join(recipe.container_name for recipe in own_recipes)
        known_ports = " ".join(
            str(port)
            for port in sorted(
                {node.endpoint_port, *(recipe.endpoint_port for recipe in own_recipes)}
            )
        )
        probe_low, probe_high = node.probe_port_range or (0, 0)
        script = f"""set -u
unit=$(systemctl --user list-units 'dgx-model-*' --type=service --all \\
  --no-legend --plain 2>/dev/null | awk '$3 != "inactive" {{print $1; exit}}')
active=inactive
sub=dead
pid=0
if [ -n "$unit" ]; then
  active=$(systemctl --user show "$unit" -p ActiveState --value 2>/dev/null \\
    || printf inactive)
  sub=$(systemctl --user show "$unit" -p SubState --value 2>/dev/null || printf dead)
  pid=$(systemctl --user show "$unit" -p MainPID --value 2>/dev/null || printf 0)
fi
legacy_pid=0
if [ "$pid" = 0 ]; then
  legacy_pid=$(pgrep -f {shlex.quote(node.legacy_process_pattern)} 2>/dev/null \\
    | head -1 || true)
  legacy_pid=${{legacy_pid:-0}}
fi
observed_pid=$pid
if [ "$observed_pid" = 0 ]; then observed_pid=$legacy_pid; fi
cmdline=
if [ "$observed_pid" != 0 ] && [ -r "/proc/$observed_pid/cmdline" ]; then
  cmdline=$(tr '\\000' ' ' < "/proc/$observed_pid/cmdline" | base64 -w0)
fi
health=$(curl -sS -o /dev/null -w '%{{http_code}}' --max-time 3 \\
  {shlex.quote(root + node.health_path)} 2>/dev/null || true)
models=$(curl -fsS --max-time 3 {shlex.quote(root + node.models_path)} \\
  2>/dev/null || true)
models_b64=$(printf '%s' "$models" | base64 -w0)
# Foreign GPU workloads: containers with GPU access that no configured recipe
# owns, and host GPU processes outside any container when no unit is observed.
known_containers={shlex.quote(known_containers)}
gpu_format='{{{{.Name}}}}|{{{{json .HostConfig.DeviceRequests}}}}'
gpu_format="$gpu_format|{{{{json .HostConfig.Devices}}}}"
foreign_containers=
for cid in $(docker ps -q 2>/dev/null | head -32); do
  info=$(docker inspect --format "$gpu_format" "$cid" 2>/dev/null) || continue
  cname=${{info%%|*}}
  cname=${{cname#/}}
  case " $known_containers " in *" $cname "*) continue ;; esac
  case "$info" in
    *'"gpu"'*|*nvidia.com/gpu*|*/dev/nvidia*)
      foreign_containers="$foreign_containers $cname" ;;
  esac
done
host_gpu=
if [ "$observed_pid" = 0 ] && command -v nvidia-smi >/dev/null 2>&1; then
  for gpid in $(nvidia-smi --query-compute-apps=pid --format=csv,noheader \\
      2>/dev/null | head -16); do
    case "$gpid" in ''|*[!0-9]*) continue ;; esac
    if grep -qsE 'docker|containerd|libpod' "/proc/$gpid/cgroup"; then continue; fi
    comm=$(tr -cd 'A-Za-z0-9._:-' < "/proc/$gpid/comm" 2>/dev/null || true)
    host_gpu="$host_gpu ${{comm:-process}}:$gpid"
  done
fi
foreign_endpoints=
# Observation has a 15 second SSH budget: probe at most six ports, one second
# each, so ports that accept but never answer cannot time the whole call out.
if [ {probe_low} -gt 0 ]; then
  for port in $(ss -ltnH 2>/dev/null | awk -v lo={probe_low} -v hi={probe_high} \\
      '{{n = split($4, a, ":"); p = a[n] + 0; if (p >= lo && p <= hi) print p}}' \\
      | sort -nu | head -6); do
    case " {known_ports} " in *" $port "*) continue ;; esac
    body=$(curl -fsS --connect-timeout 1 --max-time 1 \\
      "http://127.0.0.1:$port/v1/models" 2>/dev/null || true)
    if [ -n "$body" ]; then
      foreign_endpoints="$foreign_endpoints $port=$(printf '%s' "$body" | base64 -w0)"
    fi
  done
fi
mem_total=$(awk '/^MemTotal:/ {{print $2 * 1024}}' /proc/meminfo)
mem_available=$(awk '/^MemAvailable:/ {{print $2 * 1024}}' /proc/meminfo)
swap_total=$(awk '/^SwapTotal:/ {{print $2 * 1024}}' /proc/meminfo)
swap_free=$(awk '/^SwapFree:/ {{print $2 * 1024}}' /proc/meminfo)
printf '%s\\n' \\
  "HOSTNAME=$(hostname)" "UNIT=$unit" "ACTIVE=$active" "SUB=$sub" \\
  "PID=$pid" "LEGACY_PID=$legacy_pid" "CMDLINE_B64=$cmdline" \\
  "HEALTH_CODE=$health" "MODELS_B64=$models_b64" \\
  "MEM_TOTAL=$mem_total" "MEM_AVAILABLE=$mem_available" \\
  "SWAP_TOTAL=$swap_total" "SWAP_FREE=$swap_free" \\
  "FOREIGN_CONTAINERS=$foreign_containers" "HOST_GPU_PROCESSES=$host_gpu" \\
  "FOREIGN_ENDPOINTS=$foreign_endpoints" \\
  "UPTIME_SECONDS=$(cut -d. -f1 /proc/uptime)"
"""
        try:
            result = self._executor.run_script(
                node_id, script, timeout_seconds=15, check=True
            )
        except SSHExecutionError as exc:
            return NodeObservation(
                node_id=node_id,
                reachable=False,
                free_memory_bytes=0,
                message=str(exc),
            )
        fields = parse_fields(result.stdout)
        unit = fields.get("UNIT", "")
        deployment_id = _deployment_from_unit(unit)
        active = fields.get("ACTIVE", "inactive")
        sub = fields.get("SUB", "dead")
        pid = parse_int(fields.get("PID"))
        legacy_pid = parse_int(fields.get("LEGACY_PID"))
        if active == "active" and sub == "running":
            state = UnitState.RUNNING
        elif active == "activating":
            state = UnitState.STARTING
        elif active == "deactivating":
            # A stopping process remains live until systemd reports it inactive.
            state = UnitState.RUNNING
        elif active == "failed":
            state = UnitState.FAILED
        elif legacy_pid:
            state = UnitState.RUNNING
        else:
            state = UnitState.STOPPED
        aliases = _model_aliases(decode_b64(fields.get("MODELS_B64", "")))
        cmdline = decode_b64(fields.get("CMDLINE_B64", ""))
        managed = bool(
            unit
            and deployment_id
            and pid
            and state in {UnitState.STARTING, UnitState.RUNNING}
        )
        free_memory = parse_int(fields.get("MEM_AVAILABLE"))
        swap_total = parse_int(fields.get("SWAP_TOTAL"))
        swap_free = parse_int(fields.get("SWAP_FREE"))
        foreign_containers = tuple(fields.get("FOREIGN_CONTAINERS", "").split())
        host_gpu_processes = tuple(
            entry.rpartition(":")[0] or entry
            for entry in fields.get("HOST_GPU_PROCESSES", "").split()
        )
        foreign_endpoints = _foreign_endpoints(fields.get("FOREIGN_ENDPOINTS", ""))
        foreign_workload = not managed and bool(
            foreign_containers or host_gpu_processes
        )
        if foreign_workload and state is UnitState.STOPPED:
            # A GPU workload this controller did not start occupies the node.
            # Report it as a running unmanaged process so every lifecycle gate
            # refuses to start beside it or to stop it.
            state = UnitState.RUNNING
        if legacy_pid and not managed:
            message = "unmanaged model process observed"
        elif foreign_workload:
            message = _foreign_workload_message(
                foreign_containers, host_gpu_processes, foreign_endpoints
            )
        else:
            message = ""
        base_observation = NodeObservation(
            node_id=node_id,
            reachable=True,
            free_memory_bytes=free_memory,
            unit_state=state,
            deployment_id=deployment_id if managed else None,
            active_alias=aliases[0] if aliases else None,
            served_aliases=aliases,
            endpoint_healthy=fields.get("HEALTH_CODE") == "200",
            managed=managed,
            message=message,
            facts={
                "hostname": fields.get("HOSTNAME", ""),
                "foreign_gpu_containers": foreign_containers,
                "foreign_gpu_processes": host_gpu_processes,
                "foreign_endpoints": {
                    str(port): list(aliases)
                    for port, aliases in foreign_endpoints.items()
                },
                "unit": unit or None,
                "pid": pid or legacy_pid or None,
                "legacy_pid": legacy_pid or None,
                "process_command": cmdline or None,
                "memory_total_bytes": parse_int(fields.get("MEM_TOTAL")),
                "memory_available_bytes": free_memory,
                "swap_total_bytes": swap_total,
                "swap_used_bytes": max(0, swap_total - swap_free),
                "uptime_seconds": parse_int(fields.get("UPTIME_SECONDS")),
                "endpoint": root,
            },
        )
        cluster_observations = [
            observation
            for recipe in self._cluster_recipes
            if node_id in recipe.node_ids
            for observation in [self._observe_cluster(node_id, recipe)]
            if observation is not None
        ]
        if not cluster_observations:
            return base_observation
        cluster = cluster_observations[0]
        if not cluster.reachable:
            return cluster
        if base_observation.is_process_running:
            return NodeObservation(
                node_id=node_id,
                reachable=True,
                free_memory_bytes=min(
                    base_observation.free_memory_bytes, cluster.free_memory_bytes
                ),
                unit_state=UnitState.RUNNING,
                endpoint_healthy=base_observation.endpoint_healthy
                or cluster.endpoint_healthy,
                managed=False,
                message="multiple model runtimes are active; ownership is ambiguous",
                facts={"systemd": base_observation.facts, "cluster": cluster.facts},
            )
        return cluster

    def _observe_cluster(
        self,
        node_id: str,
        recipe: ClusterScriptRecipe,
    ) -> NodeObservation | None:
        is_head = node_id == recipe.control_node_id
        # Per-node recipes start a launcher unit on every rank, so every node
        # reports its own launcher outcome; head-script recipes only on the head.
        reads_launcher = is_head or recipe.per_node
        health_url = f"http://127.0.0.1:{recipe.endpoint_port}{recipe.health_path}"
        models_url = f"http://127.0.0.1:{recipe.endpoint_port}{recipe.models_path}"
        script = f"""set -u
container={shlex.quote(recipe.container_name)}
marker_root=${{XDG_STATE_HOME:-$HOME/.local/state}}/dgx-controller
owner=$(cat "$marker_root/{recipe.marker_name}" 2>/dev/null || true)
if [ ! -e "$marker_root/{recipe.marker_name}" ]; then
  legacy_root=${{XDG_RUNTIME_DIR:-/run/user/$(id -u)}}/dgx-controller
  owner=$(cat "$legacy_root/{recipe.marker_name}" 2>/dev/null || true)
fi
launcher_active=inactive
launcher_sub=dead
launcher_result=
if [ {"1" if reads_launcher else "0"} = 1 ]; then
  launcher_active=$(systemctl --user show {shlex.quote(recipe.launcher_unit_name)} \
    -p ActiveState --value 2>/dev/null || printf inactive)
  launcher_sub=$(systemctl --user show {shlex.quote(recipe.launcher_unit_name)} \
    -p SubState --value 2>/dev/null || printf dead)
  launcher_result=$(systemctl --user show {shlex.quote(recipe.launcher_unit_name)} \
    -p Result --value 2>/dev/null || true)
fi
if ! docker inspect "$container" >/dev/null 2>&1; then
  printf '%s\n' 'CONTAINER_PRESENT=0' "OWNER=$owner" \
    "LAUNCHER_ACTIVE=$launcher_active" "LAUNCHER_SUB=$launcher_sub" \
    "LAUNCHER_RESULT=$launcher_result"
  exit 0
fi
running=$(docker inspect -f '{{{{.State.Running}}}}' "$container" 2>/dev/null \
  || printf false)
container_status=$(docker inspect -f '{{{{.State.Status}}}}' "$container" \
  2>/dev/null || true)
container_exit_code=$(docker inspect -f '{{{{.State.ExitCode}}}}' "$container" \
  2>/dev/null || true)
echo "CONTAINER_STATUS=$container_status"
echo "CONTAINER_EXIT_CODE=$container_exit_code"
image=$(docker inspect -f '{{{{.Config.Image}}}}' "$container" 2>/dev/null || true)
image_id=$(docker inspect -f '{{{{.Image}}}}' "$container" 2>/dev/null || true)
cmd=$(docker inspect -f '{{{{json .Config.Cmd}}}}' "$container" 2>/dev/null \
  | base64 -w0 || true)
container_env=$(docker inspect -f '{{{{json .Config.Env}}}}' "$container" \
  2>/dev/null | base64 -w0 || true)
health=000
models=
if [ {"1" if is_head else "0"} = 1 ]; then
  health=$(curl -sS -o /dev/null -w '%{{http_code}}' --max-time 3 \
    {shlex.quote(health_url)} 2>/dev/null || true)
  models=$(curl -fsS --max-time 3 {shlex.quote(models_url)} 2>/dev/null || true)
fi
printf '%s\n' 'CONTAINER_PRESENT=1' "RUNNING=$running" "IMAGE=$image" \
  "IMAGE_ID=$image_id" "CMD_B64=$cmd" "OWNER=$owner" "HEALTH_CODE=$health" \
  "ENV_B64=$container_env" \
  "LAUNCHER_ACTIVE=$launcher_active" "LAUNCHER_SUB=$launcher_sub" \
  "LAUNCHER_RESULT=$launcher_result" \
  "MODELS_B64=$(printf '%s' \"$models\" | base64 -w0)" \
  "MEM_AVAILABLE=$(awk '/^MemAvailable:/ {{print $2 * 1024}}' /proc/meminfo)" \
  "SWAP_USED=$(awk '/^SwapTotal:/ {{total=$2}} \
    /^SwapFree:/ {{print (total-$2) * 1024}}' /proc/meminfo)"
"""
        try:
            result = self._executor.run_script(
                node_id, script, timeout_seconds=15, check=True
            )
        except SSHExecutionError as exc:
            return NodeObservation(
                node_id=node_id,
                reachable=False,
                free_memory_bytes=0,
                message=str(exc),
                facts={"cluster_probe_failed": True},
            )
        fields = parse_fields(result.stdout)
        launcher_failed = reads_launcher and (
            fields.get("LAUNCHER_ACTIVE") == "failed"
            or fields.get("LAUNCHER_RESULT", "") not in {"", "success"}
        )
        launcher_starting = (
            reads_launcher
            and fields.get("LAUNCHER_ACTIVE") == "active"
            and fields.get("LAUNCHER_SUB") != "exited"
        )
        owner = fields.get("OWNER")
        if fields.get("CONTAINER_PRESENT") != "1":
            if owner == recipe.deployment_id and (launcher_failed or launcher_starting):
                return NodeObservation(
                    node_id=node_id,
                    reachable=True,
                    free_memory_bytes=0,
                    unit_state=UnitState.FAILED
                    if launcher_failed
                    else UnitState.STARTING,
                    deployment_id=recipe.deployment_id,
                    managed=True,
                    message="cluster launcher failed before containers became ready"
                    if launcher_failed
                    else "cluster launcher is starting",
                    facts={
                        "launcher_failed": launcher_failed,
                        "launcher_unit": recipe.launcher_unit_name,
                    },
                )
            return None
        running = fields.get("RUNNING") == "true"
        managed = running and owner == recipe.deployment_id
        aliases = _model_aliases(decode_b64(fields.get("MODELS_B64", "")))
        command = decode_b64(fields.get("CMD_B64", ""))
        container_environment = _container_environment(
            decode_b64(fields.get("ENV_B64", ""))
        )
        expected_environment = {
            key: recipe.required_environment[key]
            for key in IDENTITY_ENVIRONMENT_KEYS
            if key in recipe.required_environment
        }
        command_substituted_matches = tuple(
            sorted(
                key
                for key, value in expected_environment.items()
                if key not in container_environment
                and (flag := COMMAND_SUBSTITUTED_ENVIRONMENT_FLAGS.get(key)) is not None
                and _command_has_flag_value(command, flag, value)
            )
        )
        environment_mismatches = tuple(
            sorted(
                key
                for key, value in expected_environment.items()
                if container_environment.get(key) != value
                and key not in command_substituted_matches
            )
        )
        command_flag_mismatches = tuple(
            sorted(
                key
                for key, value in expected_environment.items()
                if (flag := COMMAND_SUBSTITUTED_ENVIRONMENT_FLAGS.get(key)) is not None
                and not _command_has_flag_value(command, flag, value)
                and not (
                    container_environment.get(key) == value
                    and _command_has_flag_environment_reference(command, flag, key)
                )
            )
        )
        expected_image_id = (
            recipe.required_environment.get("VLLM_HEAD_IMAGE_ID")
            if is_head
            else recipe.required_environment.get("WORKER_VLLM_IMAGE_ID")
        )
        static_identity_matches = (
            fields.get("IMAGE") == recipe.image_digest
            and (not expected_image_id or fields.get("IMAGE_ID") == expected_image_id)
            and _command_has_exact_tokens(command, recipe.required_command_fragments)
        )
        identity_matches = (
            static_identity_matches
            and not environment_mismatches
            and not command_flag_mismatches
        )
        # A successful oneshot launcher can outlive its detached container.
        # Preserve STOPPED/unmanaged semantics; only readiness consumes this
        # exact-owner terminal evidence. Absence does not prove an exit.
        exited_deployment_id = (
            recipe.deployment_id
            if reads_launcher
            and fields.get("LAUNCHER_ACTIVE") == "active"
            and fields.get("LAUNCHER_SUB") == "exited"
            and fields.get("LAUNCHER_RESULT") == "success"
            and fields.get("RUNNING") == "false"
            and fields.get("CONTAINER_STATUS") in {"exited", "dead"}
            and owner == recipe.deployment_id
            and identity_matches
            else None
        )
        unit_state = (
            UnitState.FAILED
            if launcher_failed
            else UnitState.STARTING
            if launcher_starting
            else UnitState.RUNNING
            if running
            else UnitState.STOPPED
        )
        return NodeObservation(
            node_id=node_id,
            reachable=True,
            free_memory_bytes=parse_int(fields.get("MEM_AVAILABLE")),
            unit_state=unit_state,
            deployment_id=recipe.deployment_id
            if managed and identity_matches
            else None,
            active_alias=aliases[0]
            if aliases
            else (recipe.expected_alias if running and not is_head else None),
            served_aliases=aliases,
            endpoint_healthy=fields.get("HEALTH_CODE") == "200",
            managed=managed and identity_matches,
            message=(
                ""
                if managed and identity_matches
                else (
                    "reviewed cluster identity matches but the controller "
                    "ownership marker is missing or mismatched"
                )
                if running and identity_matches
                else (
                    "recognized cluster container ownership or identity does not "
                    "match the reviewed recipe"
                )
            ),
            facts={
                "container": recipe.container_name,
                "container_status": fields.get("CONTAINER_STATUS"),
                "container_exit_code": parse_int(fields["CONTAINER_EXIT_CODE"])
                if fields.get("CONTAINER_EXIT_CODE")
                else None,
                "exited_deployment_id": exited_deployment_id,
                "image": fields.get("IMAGE"),
                "image_id": fields.get("IMAGE_ID"),
                "expected_image_id": expected_image_id,
                "environment_identity_matches": not environment_mismatches,
                "environment_mismatch_keys": environment_mismatches,
                "command_substituted_environment_keys": command_substituted_matches,
                "command_flag_mismatch_keys": command_flag_mismatches,
                "static_identity_matches": static_identity_matches,
                "process_command": command,
                "identity_matches": identity_matches,
                "ownership_matches": owner == recipe.deployment_id,
                "launcher_failed": launcher_failed,
                "launcher_unit": recipe.launcher_unit_name if reads_launcher else None,
                "swap_used_bytes": parse_int(fields.get("SWAP_USED")),
            },
        )

    def preflight(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> Sequence[PreflightCheck]:
        if spec.profile.get("deployment_mode") == "shared-vllm":
            return self._preflight_cluster(node_id, spec, assignment)
        del assignment
        self._node(node_id)
        profile = dict(spec.profile)
        runner = _mapping(profile, "runner")
        artifact = _mapping(profile, "artifact")
        runtime = _mapping(profile, "runtime")
        safety = _mapping(profile, "safety")
        script_path = _required_string(runner, "script_path")
        expected_hash = _required_string(runner, "expected_sha256")
        expected_mode = str(runner.get("expected_mode") or "0644").lstrip("0") or "0"
        expected_size = int(runner.get("expected_size_bytes") or 0)
        runtime_path = _required_string(runtime, "executable_path")
        primary_path = _required_string(artifact, "primary_path")
        primary_size = int(artifact.get("primary_size_bytes") or 0)
        required_files = tuple(
            str(item) for item in artifact.get("required_files") or ()
        )
        build_revision = str(runtime.get("build_revision") or "")
        build_commit = str(runtime.get("build_commit") or "")
        max_swap = int(safety.get("maximum_swap_used_bytes") or 0)
        if not max_swap and safety.get("maximum_swap_used_mib") is not None:
            max_swap = int(safety["maximum_swap_used_mib"]) * 1024 * 1024
        checks: list[tuple[str, str]] = [
            ("script_readable", f"test -r {shlex.quote(script_path)}"),
            ("script_syntax", f"bash -n {shlex.quote(script_path)}"),
            (
                "script_hash",
                f'test "$(sha256sum {shlex.quote(script_path)} | '
                f"awk '{{print $1}}')\" = {shlex.quote(expected_hash)}",
            ),
            (
                "script_mode",
                f'test "$(stat -c %a {shlex.quote(script_path)})" = '
                f"{shlex.quote(expected_mode)}",
            ),
            ("runtime_executable", f"test -x {shlex.quote(runtime_path)}"),
            ("primary_model", f"test -r {shlex.quote(primary_path)}"),
            ("user_manager", USER_MANAGER_CHECK),
        ]
        if expected_size:
            checks.append(
                (
                    "script_size",
                    f'test "$(stat -c %s {shlex.quote(script_path)})" = '
                    f"{expected_size}",
                )
            )
        if primary_size:
            checks.append(
                (
                    "primary_model_size",
                    f'test "$(stat -c %s {shlex.quote(primary_path)})" = '
                    f"{primary_size}",
                )
            )
        for index, path in enumerate(required_files):
            checks.append((f"model_file_{index + 1}", f"test -r {shlex.quote(path)}"))
        remote_lines = [
            "set +e",
            f"runtime_version=$({shlex.quote(runtime_path)} --version 2>&1 | head -3)",
        ]
        for name, command in checks:
            remote_lines.extend(
                [
                    f"if {command}; then",
                    f"  printf '%s\\n' {shlex.quote('CHECK_' + name + '=ok')}",
                    "else",
                    f"  printf '%s\\n' {shlex.quote('CHECK_' + name + '=failed')}",
                    "fi",
                ]
            )
        version_b64_expression = "$(printf '%s' \"$runtime_version\" | base64 -w0)"
        remote_lines.append(
            f"printf '%s\\n' \"RUNTIME_VERSION_B64={version_b64_expression}\""
        )
        remote_lines.append(
            "awk '/^MemAvailable:/ {print \"MEM_AVAILABLE=\" $2 * 1024}' /proc/meminfo"
        )
        remote_lines.append(
            "awk '/^SwapTotal:/ {total=$2} "
            '/^SwapFree:/ {print "SWAP_USED=" (total-$2) * 1024}\' /proc/meminfo'
        )
        try:
            result = self._executor.run_script(
                node_id, "\n".join(remote_lines) + "\n", timeout_seconds=30, check=True
            )
        except SSHExecutionError as exc:
            return (
                PreflightCheck(
                    "ssh_reachable",
                    False,
                    str(exc),
                    "Restore SSH connectivity before changing the active model.",
                ),
            )
        fields = parse_fields(result.stdout)
        result_checks: list[PreflightCheck] = []
        for name, _command in checks:
            passed = fields.get("CHECK_" + name) == "ok"
            result_checks.append(
                PreflightCheck(
                    name,
                    passed,
                    f"{name.replace('_', ' ')} {'verified' if passed else 'failed'}",
                    "Correct the reviewed profile or remote installation "
                    "before retrying.",
                )
            )
        version_text = decode_b64(fields.get("RUNTIME_VERSION_B64", ""))
        if build_revision:
            result_checks.append(
                PreflightCheck(
                    "runtime_revision",
                    build_revision in version_text,
                    f"runtime reports {version_text or 'no version'}",
                    "Install the profile-pinned llama.cpp revision or "
                    "intentionally revise the profile.",
                )
            )
        if build_commit:
            result_checks.append(
                PreflightCheck(
                    "runtime_commit",
                    build_commit in version_text,
                    f"runtime reports {version_text or 'no version'}",
                    "Install the profile-pinned llama.cpp commit or "
                    "intentionally revise the profile.",
                )
            )
        observation = self.observe(node_id)
        memory_deferred = observation.is_process_running and (
            observation.managed or bool(observation.active_alias)
        )
        required_free = spec.minimum_free_memory_bytes
        result_checks.append(
            PreflightCheck(
                "memory",
                memory_deferred or observation.free_memory_bytes >= required_free,
                "memory check deferred until resident deployment stops"
                if memory_deferred
                else f"available memory is {observation.free_memory_bytes} bytes",
                "Stop the resident deployment and verify memory recovery "
                "before launch.",
            )
        )
        swap_used = parse_int(fields.get("SWAP_USED"))
        if max_swap:
            result_checks.append(
                PreflightCheck(
                    "swap_bound",
                    swap_used <= max_swap,
                    f"swap used is {swap_used} bytes",
                    "Allow the node to recover or reboot it before loading "
                    "another large model.",
                )
            )
        return tuple(result_checks)

    def _preflight_cluster(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> Sequence[PreflightCheck]:
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
        artifact = _mapping(spec.profile, "artifact")
        safety = _mapping(spec.profile, "safety")
        recipe_revision_path = (
            f"{recipe.working_directory}/.dgx-controller-recipe-commit"
        )
        recipe_revision = (
            f"revision=$(git -C {shlex.quote(recipe.working_directory)} "
            "rev-parse HEAD 2>/dev/null || true); "
            'if [ -z "$revision" ] && '
            f"[ -r {shlex.quote(recipe_revision_path)} ]; then "
            f"revision=$(cat {shlex.quote(recipe_revision_path)}); fi; "
            f'test "$revision" = {shlex.quote(recipe.expected_recipe_commit)}'
        )
        checks: list[tuple[str, str]] = [
            ("recipe_commit", recipe_revision),
            (
                "image_digest",
                f"docker image inspect {shlex.quote(recipe.image_digest)} >/dev/null",
            ),
        ]
        for index, path in enumerate(artifact.get("required_files") or ()):
            checks.append(
                (f"model_file_{index + 1}", f"test -r {shlex.quote(str(path))}")
            )
        manifest_sha256 = str(artifact.get("manifest_sha256") or "")
        required_files = tuple(
            str(path) for path in artifact.get("required_files") or ()
        )
        if manifest_sha256 and required_files:
            manifest_path = required_files[-1]
            checks.append(
                (
                    "manifest_hash",
                    f'test "$(sha256sum {shlex.quote(manifest_path)} | '
                    f"awk '{{print $1}}')\" = {shlex.quote(manifest_sha256)}",
                )
            )
        if assignment.role == "head" or recipe.per_node:
            checks.extend(
                [
                    (
                        "start_script_readable",
                        f"test -r {shlex.quote(recipe.start_script_path)}",
                    ),
                    (
                        "start_script_syntax",
                        f"bash -n {shlex.quote(recipe.start_script_path)}",
                    ),
                    (
                        "start_script_hash",
                        f'test "$(sha256sum {shlex.quote(recipe.start_script_path)} | '
                        f"awk '{{print $1}}')\" = {shlex.quote(recipe.start_sha256)}",
                    ),
                    (
                        "stop_script_readable",
                        f"test -r {shlex.quote(recipe.stop_script_path)}",
                    ),
                    (
                        "stop_script_syntax",
                        f"bash -n {shlex.quote(recipe.stop_script_path)}",
                    ),
                    (
                        "stop_script_hash",
                        f'test "$(sha256sum {shlex.quote(recipe.stop_script_path)} | '
                        f"awk '{{print $1}}')\" = {shlex.quote(recipe.stop_sha256)}",
                    ),
                    (
                        "environment_source",
                        f"test -r {shlex.quote(recipe.environment_path)}",
                    ),
                    (
                        "environment_overlay_directory",
                        f"test -w {shlex.quote(recipe.working_directory)}",
                    ),
                    ("user_manager", USER_MANAGER_CHECK),
                ]
            )
        remote_lines = ["set +e"]
        for name, command in checks:
            remote_lines.extend(
                [
                    f"if {command}; then",
                    f"  printf '%s\\n' {shlex.quote('CHECK_' + name + '=ok')}",
                    "else",
                    f"  printf '%s\\n' {shlex.quote('CHECK_' + name + '=failed')}",
                    "fi",
                ]
            )
        remote_lines.append(
            "printf 'RECIPE_COMMIT_B64=%s\\n' "
            "\"$(printf '%s' \"${revision:-}\" | base64 | tr -d '\\n')\""
        )
        remote_lines.append(
            "awk '/^SwapTotal:/ {total=$2} "
            '/^SwapFree:/ {print "SWAP_USED=" (total-$2) * 1024}\' /proc/meminfo'
        )
        try:
            result = self._executor.run_script(
                node_id, "\n".join(remote_lines) + "\n", timeout_seconds=45, check=True
            )
        except SSHExecutionError as exc:
            return (
                PreflightCheck(
                    "ssh_reachable",
                    False,
                    str(exc),
                    "Restore SSH connectivity before changing the active model.",
                ),
            )
        fields = parse_fields(result.stdout)
        observed_recipe_commit = (
            decode_b64(fields.get("RECIPE_COMMIT_B64", "")) or "unavailable"
        )
        result_checks = []
        for name, _command in checks:
            passed = fields.get("CHECK_" + name) == "ok"
            message = f"{name.replace('_', ' ')} {'verified' if passed else 'failed'}"
            if name == "recipe_commit":
                message = (
                    f"recipe commit observed {observed_recipe_commit}; "
                    f"expected {recipe.expected_recipe_commit}"
                )
            result_checks.append(
                PreflightCheck(
                    name,
                    passed,
                    message,
                    "Restore the exact reviewed recipe, model files, environment, "
                    "or image pin before retrying.",
                )
            )
        observation = self.observe(node_id)
        memory_deferred = observation.is_process_running and (
            observation.managed or bool(observation.active_alias)
        )
        result_checks.append(
            PreflightCheck(
                "memory",
                memory_deferred
                or observation.free_memory_bytes >= spec.minimum_free_memory_bytes,
                "memory check deferred until resident deployment stops"
                if memory_deferred
                else f"available memory is {observation.free_memory_bytes} bytes",
                "Recover the profile-required memory on both nodes before launch.",
            )
        )
        max_swap = int(safety.get("maximum_swap_used_bytes") or 0)
        result_checks.append(
            PreflightCheck(
                "swap_bound",
                parse_int(fields.get("SWAP_USED")) <= max_swap,
                f"swap used is {parse_int(fields.get('SWAP_USED'))} bytes",
                "Allow swap to recover before launch.",
            )
        )
        return tuple(result_checks)

    def drain(self, node_id: str, deployment_id: str) -> CommandResult:
        self._node(node_id)
        return CommandResult(
            True, f"deployment {deployment_id} has no configured proxy drain"
        )

    def start(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> CommandResult:
        if spec.profile.get("deployment_mode") == "shared-vllm":
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
            environment_checks = "\n".join(
                (
                    f"grep -Fqx -- {shlex.quote(f'{key}={value}')} "
                    f"{shlex.quote(recipe.managed_environment_path)}"
                )
                for key, value in sorted(recipe.environment_for(target).items())
            )
            launch_script = f"""set -eu
script_path={shlex.quote(recipe.start_script_path)}
expected_hash={shlex.quote(recipe.start_sha256)}
test -r "$script_path"
bash -n "$script_path"
test "$(sha256sum "$script_path" | awk '{{print $1}}')" = "$expected_hash"
{environment_checks}
cd {shlex.quote(recipe.working_directory)}
unit={shlex.quote(recipe.launcher_unit_name)}
systemctl --user stop "$unit" >/dev/null 2>&1 || true
systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true
systemd-run --user --unit "$unit" \
  --property=RemainAfterExit=yes \
  --property=TimeoutStopSec={max(30, int(spec.stop_timeout_seconds))} \
  --setenv=ENV_FILE={shlex.quote(recipe.managed_environment_path)} \
  /bin/bash "$script_path"
"""
            try:
                result = self._executor.run_script(
                    target,
                    launch_script,
                    timeout_seconds=max(60, spec.startup_timeout_seconds + 60),
                    check=False,
                )
            except SSHExecutionError as exc:
                self._stop_after_failed_start(recipe, target, spec.stop_timeout_seconds)
                return CommandResult(False, str(exc), "cluster_start_failed")
            if not result.ok:
                self._stop_after_failed_start(recipe, target, spec.stop_timeout_seconds)
                return CommandResult(
                    False,
                    result.stderr.strip() or result.stdout.strip(),
                    "cluster_start_failed",
                )
            if recipe.per_node:
                return CommandResult(
                    True,
                    f"pinned launcher dispatched for the {assignment.role} rank "
                    f"on {target}",
                )
            return CommandResult(
                True,
                "durable cluster launcher dispatched and both ranks are "
                "controller-owned",
            )
        del assignment
        runner = _mapping(spec.profile, "runner")
        recipe_id = _required_string(runner, "script_id")
        return self._runner.start_unit(
            node_id, self.unit_name(spec.deployment_id), recipe_id
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
        script = f"""set -u
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
        try:
            result = self._executor.run_script(
                node_id,
                script,
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
        if recipe is not None:
            bounded_limit = max(1, min(int(limit), 500))
            try:
                result = self._executor.run_script(
                    node_id,
                    "set -eu\n"
                    f"docker logs --tail {bounded_limit} "
                    f"{shlex.quote(recipe.container_name)} 2>&1 || true\n",
                    timeout_seconds=15,
                    check=False,
                )
            except SSHExecutionError as exc:
                return LogChunk((f"log retrieval failed: {exc}",), None)
            return LogChunk(tuple(result.stdout.splitlines()), None)
        return self._runner.unit_logs(
            node_id, self.unit_name(deployment_id), cursor=cursor, limit=limit
        )

    def _write_cluster_markers(
        self, recipe: ClusterScriptRecipe, node_ids: Sequence[str] | None = None
    ) -> CommandResult:
        script = f"""set -eu
umask 077
root=${{XDG_STATE_HOME:-$HOME/.local/state}}/dgx-controller
install -d -m 700 "$root"
printf '%s\n' {shlex.quote(recipe.deployment_id)} > "$root/{recipe.marker_name}"
chmod 600 "$root/{recipe.marker_name}"
"""
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
        replacements: list[str] = []
        for key, value in sorted(recipe.environment_for(target).items()):
            replacements.extend(
                [
                    'next="${tmp}.next"',
                    f"awk -F= -v key={shlex.quote(key)} '$1 != key {{ print }}' "
                    '"$tmp" > "$next"',
                    f"printf '%s=%s\\n' {shlex.quote(key)} {shlex.quote(value)} "
                    '>> "$next"',
                    'mv "$next" "$tmp"',
                ]
            )
        replacement_script = "\n".join(replacements)
        script = f"""set -eu
umask 077
source_path={shlex.quote(recipe.environment_path)}
managed_path={shlex.quote(recipe.managed_environment_path)}
test -r "$source_path"
tmp=$(mktemp "${{managed_path}}.tmp.XXXXXX")
cleanup() {{
  test ! -e "$tmp" || unlink "$tmp"
  test ! -e "${{tmp}}.next" || unlink "${{tmp}}.next"
}}
trap cleanup EXIT HUP INT TERM
cp "$source_path" "$tmp"
{replacement_script}
chmod 600 "$tmp"
mv "$tmp" "$managed_path"
trap - EXIT HUP INT TERM
"""
        try:
            result = self._executor.run_script(
                target,
                script,
                timeout_seconds=15,
                check=False,
            )
        except SSHExecutionError as exc:
            return CommandResult(False, str(exc), "cluster_environment_failed")
        if not result.ok:
            return CommandResult(
                False,
                result.stderr.strip()
                or result.stdout.strip()
                or "cluster environment render failed",
                "cluster_environment_failed",
            )
        return CommandResult(True, "controller environment overlay rendered")

    def _stop_after_failed_start(
        self, recipe: ClusterScriptRecipe, node_id: str, timeout_seconds: float
    ) -> None:
        if recipe.per_node:
            self._stop_cluster_recipe(recipe, timeout_seconds, node_id=node_id)
        else:
            self._stop_cluster_recipe(recipe, timeout_seconds)

    def _stop_cluster_recipe(
        self,
        recipe: ClusterScriptRecipe,
        timeout_seconds: float,
        *,
        node_id: str | None = None,
    ) -> CommandResult:
        """Run the pinned stop script on the control node, or on one per-node rank."""
        target = node_id or recipe.control_node_id
        if recipe.per_node:
            # A rank whose start failed before its overlay was rendered still has
            # the installed base environment, which names the exact container.
            environment_lines = (
                f"env_file={shlex.quote(recipe.managed_environment_path)}\n"
                'test -r "$env_file" || '
                f"env_file={shlex.quote(recipe.environment_path)}\n"
                'test -r "$env_file"\n'
                f"cd {shlex.quote(recipe.working_directory)}\n"
                'ENV_FILE="$env_file" /bin/bash "$script_path"\n'
            )
        else:
            environment_lines = (
                f"test -r {shlex.quote(recipe.managed_environment_path)}\n"
                f"cd {shlex.quote(recipe.working_directory)}\n"
                f"ENV_FILE={shlex.quote(recipe.managed_environment_path)} "
                '/bin/bash "$script_path"\n'
            )
        stop_script = f"""set -eu
script_path={shlex.quote(recipe.stop_script_path)}
expected_hash={shlex.quote(recipe.stop_sha256)}
test -r "$script_path"
bash -n "$script_path"
test "$(sha256sum "$script_path" | awk '{{print $1}}')" = "$expected_hash"
{environment_lines}"""
        try:
            self._executor.run_script(
                target,
                (
                    "set -u\n"
                    f"unit={shlex.quote(recipe.launcher_unit_name)}\n"
                    'systemctl --user stop "$unit" >/dev/null 2>&1 || true\n'
                    'systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true\n'
                ),
                timeout_seconds=max(30, timeout_seconds),
                check=False,
            )
            result = self._executor.run_script(
                target,
                stop_script,
                timeout_seconds=max(30, timeout_seconds + 30),
                check=False,
            )
        except SSHExecutionError as exc:
            return CommandResult(False, str(exc), "cluster_stop_failed")
        if not result.ok:
            return CommandResult(
                False,
                result.stderr.strip() or result.stdout.strip(),
                "cluster_stop_failed",
            )
        self._remove_cluster_markers(recipe, (target,) if recipe.per_node else None)
        if recipe.per_node:
            return CommandResult(True, f"pinned stop script completed on {target}")
        return CommandResult(True, "cluster stop script completed")

    def _remove_cluster_markers(
        self, recipe: ClusterScriptRecipe, node_ids: Sequence[str] | None = None
    ) -> None:
        script = f"""set -u
root=${{XDG_STATE_HOME:-$HOME/.local/state}}/dgx-controller
test ! -f "$root/{recipe.marker_name}" || unlink "$root/{recipe.marker_name}"
legacy_root=${{XDG_RUNTIME_DIR:-/run/user/$(id -u)}}/dgx-controller
test ! -f "$legacy_root/{recipe.marker_name}" \\
  || unlink "$legacy_root/{recipe.marker_name}"
"""
        for node_id in node_ids or recipe.node_ids:
            with contextlib.suppress(SSHExecutionError):
                self._executor.run_script(
                    node_id, script, timeout_seconds=15, check=False
                )


def _model_aliases(payload: str) -> tuple[str, ...]:
    if not payload:
        return ()
    try:
        value = json.loads(payload)
    except json.JSONDecodeError:
        return ()
    rows = value.get("data", []) if isinstance(value, dict) else []
    aliases = []
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("id"), str):
            aliases.append(row["id"])
    return tuple(aliases)


def _foreign_endpoints(raw: str) -> dict[int, tuple[str, ...]]:
    """Parse ``port=<base64 /v1/models body>`` pairs from the observe script."""
    endpoints: dict[int, tuple[str, ...]] = {}
    for item in raw.split():
        port, _, payload = item.partition("=")
        aliases = _model_aliases(decode_b64(payload))
        if port.isdigit() and aliases:
            endpoints[int(port)] = aliases
    return endpoints


def _foreign_workload_message(
    containers: Sequence[str],
    processes: Sequence[str],
    endpoints: Mapping[int, Sequence[str]],
) -> str:
    """Name a GPU workload the controller does not own, without command text."""
    details = []
    if containers:
        details.append("container " + ", ".join(containers[:3]))
    if processes:
        details.append("host process " + ", ".join(processes[:3]))
    details.extend(
        f"serving {', '.join(aliases[:3])} on port {port}"
        for port, aliases in sorted(endpoints.items())[:3]
    )
    return "unmanaged GPU workload observed: " + " · ".join(details)


def _container_environment(payload: str) -> dict[str, str]:
    """Decode Docker's JSON environment list without retaining it in observations."""
    if not payload:
        return {}
    try:
        values = json.loads(payload)
    except json.JSONDecodeError:
        return {}
    if not isinstance(values, list):
        return {}
    return parse_key_values(item for item in values if isinstance(item, str))


def _effective_vllm_arguments(payload: str) -> list[str] | None:
    """Return tokens after the one reviewed absolute vLLM executable."""
    if not payload:
        return None
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        decoded = payload
    parts = decoded if isinstance(decoded, list) else [decoded]
    tokens: list[str] = []
    for part in parts:
        if not isinstance(part, str):
            continue
        try:
            lexer = shlex.shlex(part, posix=True, punctuation_chars=";&|")
            lexer.whitespace_split = True
            lexer.commenters = "#"
            part_tokens = list(lexer)
        except ValueError:
            return None
        tokens.extend(part_tokens)

    # Ignore setup commands, diagnostics, and comments before the final
    # executable. The reviewed recipes invoke vLLM by absolute path.
    executable = "/usr/local/bin/vllm"
    executable_positions = [
        index for index, token in enumerate(tokens) if token == executable
    ]
    if len(executable_positions) != 1:
        return None
    arguments = tokens[executable_positions[0] + 1 :]
    shell_controls = {";", "&", "&&", "|", "||"}
    for index, token in enumerate(arguments):
        if token in shell_controls:
            return arguments[:index]
    return arguments


def _command_has_exact_tokens(payload: str, required_tokens: tuple[str, ...]) -> bool:
    """Require every identity fragment as a complete effective argv token."""
    arguments = _effective_vllm_arguments(payload)
    if arguments is None:
        return False
    executable = "/usr/local/bin/vllm"
    required = [token for token in required_tokens if token != executable]
    cursor = 0
    index = 0
    while index < len(required):
        token = required[index]
        try:
            position = arguments.index(token, cursor)
        except ValueError:
            return False
        if (
            token.startswith("--")
            and index + 1 < len(required)
            and not required[index + 1].startswith("--")
        ):
            value = required[index + 1]
            if position + 1 >= len(arguments) or arguments[position + 1] != value:
                return False
            if not _command_has_flag_value(payload, token, value):
                return False
            cursor = position + 2
            index += 2
            continue
        cursor = position + 1
        index += 1
    return True


def _command_has_flag_value(payload: str, flag: str, value: str) -> bool:
    """Match one exact option on the effective rendered vLLM invocation."""
    if not flag.startswith("--") or not value:
        return False
    arguments = _effective_vllm_arguments(payload)
    if arguments is None:
        return False
    matches: list[str] = []
    for index, argument in enumerate(arguments):
        if argument == flag:
            if index + 1 >= len(arguments):
                return False
            matches.append(arguments[index + 1])
        elif argument.startswith(flag + "="):
            matches.append(argument[len(flag) + 1 :])
    return matches == [value]


def _command_has_flag_environment_reference(payload: str, flag: str, key: str) -> bool:
    """Match one option whose value is supplied by an exact container env key."""
    if not flag.startswith("--") or not key:
        return False
    arguments = _effective_vllm_arguments(payload)
    if arguments is None:
        return False
    references = {f"${key}", f"${{{key}}}"}
    matches: list[str] = []
    for index, argument in enumerate(arguments):
        if argument == flag:
            if index + 1 >= len(arguments):
                return False
            matches.append(arguments[index + 1])
        elif argument.startswith(flag + "="):
            matches.append(argument[len(flag) + 1 :])
    return len(matches) == 1 and matches[0] in references


def _deployment_from_unit(unit: str) -> str | None:
    prefix = "dgx-model-"
    suffix = ".service"
    if not unit.startswith(prefix) or not unit.endswith(suffix):
        return None
    value = unit[len(prefix) : -len(suffix)]
    return value if DEPLOYMENT_ID_RE.fullmatch(value) else None


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    result = value.get(key)
    if not isinstance(result, Mapping):
        raise ValueError(f"deployment profile is missing object {key!r}")
    return result


def _required_string(value: Mapping[str, Any], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ValueError(f"deployment profile is missing string {key!r}")
    return result
