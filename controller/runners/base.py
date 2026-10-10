# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Runner protocol used by SSH transports to own stable remote units."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from controller.transport.base import CommandResult, LogChunk, UnitState


@dataclass(frozen=True)
class UnitStatus:
    """Identity and state of one named runner-owned remote unit."""

    unit_name: str
    state: UnitState
    process_identity: str | None = None
    message: str = ""


class NodeRunner(Protocol):
    """Narrow runner interface.

    Implementations must never use broad process matching.
    """

    def start_unit(self, node_id: str, unit_name: str, recipe_id: str) -> CommandResult:
        """Start one allowlisted runner unit."""

    def stop_unit(
        self,
        node_id: str,
        unit_name: str,
        *,
        timeout_seconds: float,
        force: bool = False,
    ) -> CommandResult:
        """Gracefully stop, then optionally targeted-kill, one named unit."""

    def unit_status(self, node_id: str, unit_name: str) -> UnitStatus:
        """Return current unit identity and process state."""

    def unit_logs(
        self, node_id: str, unit_name: str, cursor: str | None = None, limit: int = 100
    ) -> LogChunk:
        """Read a bounded log chunk for one named unit."""
