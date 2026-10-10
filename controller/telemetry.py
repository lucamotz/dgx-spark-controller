# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Read-only capacity samples, cached independently of lifecycle observations."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass

TELEMETRY_INTERVAL_SECONDS = 10


@dataclass(frozen=True)
class MemorySample:
    total_bytes: int
    available_bytes: int
    swap_used_bytes: int | None

    def to_dict(self) -> dict:
        used = self.total_bytes - self.available_bytes
        return {
            **asdict(self),
            "used_bytes": used,
            "used_percent": round(100 * used / self.total_bytes, 1),
        }


@dataclass(frozen=True)
class NodeTelemetry:
    node_id: str
    sampled_at: str | None = None
    status: str = "unavailable"
    memory: MemorySample | None = None
    gpu_utilization_percent: float | None = None
    message: str = "Node metrics are unavailable."
    simulated: bool = False

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "sampled_at": self.sampled_at,
            "status": self.status,
            "memory": self.memory.to_dict() if self.memory else None,
            "gpu": {"utilization_percent": self.gpu_utilization_percent},
            "message": self.message,
            "simulated": self.simulated,
        }


class TelemetryCache:
    """Share bounded samples across browsers; idle dashboards cause no remote work."""

    def __init__(
        self,
        node_ids: Sequence[str],
        sample: Callable[[str], NodeTelemetry],
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._node_ids = tuple(node_ids)
        self._sample = sample
        self._monotonic = monotonic
        self._cache: dict[str, tuple[float, NodeTelemetry]] = {}
        self._lock = threading.Lock()

    def snapshot(self) -> dict:
        # One collector across every requesting tab, including slow/failing SSH.
        with self._lock:
            items = []
            for node_id in self._node_ids:
                cached = self._cache.get(node_id)
                if (
                    cached is None
                    or self._monotonic() - cached[0] >= TELEMETRY_INTERVAL_SECONDS
                ):
                    try:
                        sample = self._sample(node_id)
                    except Exception:
                        # Never expose SSH stderr, paths, or connection details.
                        sample = NodeTelemetry(node_id)
                    self._cache[node_id] = (self._monotonic(), sample)
                items.append(self._cache[node_id][1].to_dict())
            return {"items": items, "interval_seconds": TELEMETRY_INTERVAL_SECONDS}
