# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Fixed, short-lived SSH metrics query; no model, container or CUDA work."""

from __future__ import annotations

import math

from controller.operations import utc_now
from controller.telemetry import MemorySample, NodeTelemetry
from controller.transport.ssh import SSHExecutor

TELEMETRY_SCRIPT = """set -eu
export LC_ALL=C
available_kib=0
while read -r key value unit; do
  case "$key" in
    MemTotal:|MemAvailable:|SwapTotal:|SwapFree:) printf '%s %s\\n' "$key" "$value" ;;
  esac
  if [ "$key" = MemAvailable: ]; then available_kib=$value; fi
done < /proc/meminfo
printf 'GPU\\n'
if [ "$available_kib" -ge 1048576 ]; then
  timeout --signal=KILL 3s nvidia-smi --id=0 --query-gpu=utilization.gpu \\
    --format=csv,noheader,nounits 2>/dev/null || true
else
  printf 'deferred\\n'
fi
"""


def sample_ssh_telemetry(executor: SSHExecutor, node_id: str) -> NodeTelemetry:
    result = executor.run_script(
        node_id, TELEMETRY_SCRIPT, timeout_seconds=8, check=False
    )
    if not result.ok:
        return NodeTelemetry(node_id)
    return parse_telemetry(node_id, result.stdout)


def parse_telemetry(node_id: str, output: str) -> NodeTelemetry:
    memory_text, separator, gpu_text = output.partition("GPU\n")
    fields = {}
    for line in memory_text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit():
            fields[parts[0].rstrip(":")] = int(parts[1]) * 1024
    total = fields.get("MemTotal", 0)
    available = fields.get("MemAvailable")
    memory = None
    if total > 0 and available is not None and 0 <= available <= total:
        swap_total, swap_free = fields.get("SwapTotal"), fields.get("SwapFree")
        swap_used = (
            swap_total - swap_free
            if swap_total is not None
            and swap_free is not None
            and 0 <= swap_free <= swap_total
            else None
        )
        memory = MemorySample(total, available, swap_used)
    utilization = None
    if separator:
        try:
            value = float(gpu_text.strip())
            if math.isfinite(value) and 0 <= value <= 100:
                utilization = value
        except ValueError:
            pass
    if memory is None and utilization is None:
        return NodeTelemetry(node_id)
    complete = memory is not None and utilization is not None
    message = (
        ""
        if complete
        else (
            "GPU utilization is unavailable on this node."
            if utilization is None
            else "System memory metrics are unavailable on this node."
        )
    )
    if memory is not None and gpu_text.strip() == "deferred":
        message = "GPU query paused while available memory is below 1 GiB."
    return NodeTelemetry(
        node_id,
        utc_now(),
        "ok" if complete else "partial",
        memory,
        utilization,
        message,
    )
