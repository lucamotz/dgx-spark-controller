# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Fixed observation probes of managed nodes and the parsers of their output.

The unit probe reports the controller's ``dgx-model-*`` unit, a legacy model
process, endpoint health and aliases, memory, and every GPU workload that no
configured recipe owns. The cluster probe reports one recognized recipe
container, its ownership marker and its durable launcher unit. A probe only
reads; ownership is never inferred from a port or a process name alone.
"""

from __future__ import annotations

import json
import shlex
from collections.abc import Mapping, Sequence

from controller.transport.base import NodeObservation, UnitState
from controller.transport.managed_config import (
    DEPLOYMENT_ID_RE,
    ClusterScriptRecipe,
    ManagedSSHNode,
)
from controller.transport.script_output import decode_b64, parse_fields, parse_int
from controller.transport.vllm_identity import (
    command_has_exact_tokens,
    container_environment,
    environment_identity,
)


def unit_observe_script(
    node: ManagedSSHNode, recipes: Sequence[ClusterScriptRecipe]
) -> str:
    """Render the unit probe; ``recipes`` are the cluster recipes using ``node``."""
    root = node.endpoint_root
    known_containers = " ".join(recipe.container_name for recipe in recipes)
    known_ports = " ".join(
        str(port)
        for port in sorted(
            {node.endpoint_port, *(recipe.endpoint_port for recipe in recipes)}
        )
    )
    probe_low, probe_high = node.probe_port_range or (0, 0)
    return f"""set -u
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


def parse_unit_observation(node: ManagedSSHNode, output: str) -> NodeObservation:
    """Build the observation of the unit probe; only a live named unit is managed."""
    node_id = node.id
    root = node.endpoint_root
    fields = parse_fields(output)
    unit = fields.get("UNIT", "")
    deployment_id = _deployment_from_unit(unit)
    active = fields.get("ACTIVE", "inactive")
    sub = fields.get("SUB", "dead")
    pid = parse_int(fields.get("PID"))
    legacy_pid = parse_int(fields.get("LEGACY_PID"))
    state = _unit_state(active, sub, legacy_pid)
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
    foreign_workload = not managed and bool(foreign_containers or host_gpu_processes)
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
    return NodeObservation(
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
                str(port): list(aliases) for port, aliases in foreign_endpoints.items()
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


def combine_observations(
    unit: NodeObservation, clusters: Sequence[NodeObservation]
) -> NodeObservation:
    """Prefer a recognized cluster container, refusing two live runtimes."""
    if not clusters:
        return unit
    cluster = clusters[0]
    if not cluster.reachable:
        return cluster
    if unit.is_process_running:
        return NodeObservation(
            node_id=unit.node_id,
            reachable=True,
            free_memory_bytes=min(unit.free_memory_bytes, cluster.free_memory_bytes),
            unit_state=UnitState.RUNNING,
            endpoint_healthy=unit.endpoint_healthy or cluster.endpoint_healthy,
            managed=False,
            message="multiple model runtimes are active; ownership is ambiguous",
            facts={"systemd": unit.facts, "cluster": cluster.facts},
        )
    return cluster


def cluster_observe_script(
    recipe: ClusterScriptRecipe, *, is_head: bool, reads_launcher: bool
) -> str:
    """Render the probe of one recipe container and, optionally, its launcher."""
    health_url = f"http://127.0.0.1:{recipe.endpoint_port}{recipe.health_path}"
    models_url = f"http://127.0.0.1:{recipe.endpoint_port}{recipe.models_path}"
    return f"""set -u
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


def parse_cluster_observation(
    node_id: str,
    recipe: ClusterScriptRecipe,
    output: str,
    *,
    is_head: bool,
    reads_launcher: bool,
) -> NodeObservation | None:
    """Build the observation of a cluster probe; ``None`` when nothing is there."""
    fields = parse_fields(output)
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
                unit_state=UnitState.FAILED if launcher_failed else UnitState.STARTING,
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
    identity = environment_identity(
        recipe.required_environment,
        container_environment(decode_b64(fields.get("ENV_B64", ""))),
        command,
    )
    expected_image_id = (
        recipe.required_environment.get("VLLM_HEAD_IMAGE_ID")
        if is_head
        else recipe.required_environment.get("WORKER_VLLM_IMAGE_ID")
    )
    static_identity_matches = (
        fields.get("IMAGE") == recipe.image_digest
        and (not expected_image_id or fields.get("IMAGE_ID") == expected_image_id)
        and command_has_exact_tokens(command, recipe.required_command_fragments)
    )
    identity_matches = static_identity_matches and identity.matches
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
        deployment_id=recipe.deployment_id if managed and identity_matches else None,
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
            "environment_identity_matches": not identity.mismatch_keys,
            "environment_mismatch_keys": identity.mismatch_keys,
            "command_substituted_environment_keys": (identity.command_substituted_keys),
            "command_flag_mismatch_keys": identity.command_flag_mismatch_keys,
            "static_identity_matches": static_identity_matches,
            "process_command": command,
            "identity_matches": identity_matches,
            "ownership_matches": owner == recipe.deployment_id,
            "launcher_failed": launcher_failed,
            "launcher_unit": recipe.launcher_unit_name if reads_launcher else None,
            "swap_used_bytes": parse_int(fields.get("SWAP_USED")),
        },
    )


def _unit_state(active: str, sub: str, legacy_pid: int) -> UnitState:
    if active == "active" and sub == "running":
        return UnitState.RUNNING
    if active == "activating":
        return UnitState.STARTING
    if active == "deactivating":
        # A stopping process remains live until systemd reports it inactive.
        return UnitState.RUNNING
    if active == "failed":
        return UnitState.FAILED
    if legacy_pid:
        return UnitState.RUNNING
    return UnitState.STOPPED


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


def _deployment_from_unit(unit: str) -> str | None:
    prefix = "dgx-model-"
    suffix = ".service"
    if not unit.startswith(prefix) or not unit.endswith(suffix):
        return None
    value = unit[len(prefix) : -len(suffix)]
    return value if DEPLOYMENT_ID_RE.fullmatch(value) else None
