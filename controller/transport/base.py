# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Transport boundary for all remote node observations and lifecycle commands.

The orchestrator only calls this protocol.  SSH, a future node agent, and the
deterministic fake transport therefore share one safety contract: observations
must identify ownership and readiness must include the expected model alias.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from controller.operations import DeploymentSpec, NodeAssignment
from controller.telemetry import NodeTelemetry


class UnitState(str, Enum):
    """Portable unit/process state reported by a node runner."""

    UNKNOWN = "unknown"
    STARTING = "starting"
    RUNNING = "running"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True)
class NodeObservation:
    """Fresh facts used to make a lifecycle decision for one node.

    ``deployment_id`` must be supplied only when the transport can prove the
    running unit belongs to a known controller deployment.  A process with
    ``managed=False`` is intentionally never stopped by the generic engine.
    ``served_aliases`` comes from the endpoint's model-list response, not merely
    a configured port, so a healthy wrong model is never accepted as ready.
    """

    node_id: str
    reachable: bool
    free_memory_bytes: int
    unit_state: UnitState = UnitState.UNKNOWN
    deployment_id: str | None = None
    active_alias: str | None = None
    served_aliases: tuple[str, ...] = ()
    endpoint_healthy: bool = False
    managed: bool = False
    message: str = ""
    facts: Mapping[str, Any] = field(default_factory=dict)

    @property
    def is_process_running(self) -> bool:
        return self.unit_state in {UnitState.STARTING, UnitState.RUNNING}

    def ready_for(self, deployment_id: str, expected_alias: str) -> bool:
        """Require identity, process state, health, and exact expected alias."""
        return (
            self.reachable
            and self.managed
            and self.deployment_id == deployment_id
            and self.unit_state == UnitState.RUNNING
            and self.endpoint_healthy
            and self.served_aliases == (expected_alias,)
        )

    def stopped_for(self, deployment_id: str, minimum_free_memory_bytes: int) -> bool:
        """Require unit/endpoint/alias absence.

        Post-stop memory recovery must also be verified.
        """
        owns_target = self.deployment_id == deployment_id
        return (
            self.reachable
            and not owns_target
            and not self.is_process_running
            and not self.endpoint_healthy
            and not self.active_alias
            and not self.served_aliases
            and self.free_memory_bytes >= minimum_free_memory_bytes
        )


@dataclass(frozen=True)
class PreflightCheck:
    """One named non-destructive launch gate for a node."""

    name: str
    passed: bool
    message: str = ""
    remediation: str = ""


@dataclass(frozen=True)
class CommandResult:
    """Redacted result of a narrow transport command or runner action."""

    ok: bool
    message: str = ""
    code: str = "ok"
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LogChunk:
    """Bounded, cursor-addressed node log data suitable for operation events."""

    lines: tuple[str, ...]
    next_cursor: str | None


class NodeTransport(Protocol):
    """Synchronous remote-node contract consumed by :class:`OperationOrchestrator`.

    Implementations must not accept arbitrary commands from callers.  ``spec``
    contains an allowlisted, serialisable profile projection and ``assignment``
    identifies the role on one node.  A concrete SSH transport commonly delegates
    ``start``/``stop`` to a ``NodeRunner``.
    """

    def sample_telemetry(self, node_id: str) -> NodeTelemetry:
        """Read resource counters without observing or changing model lifecycle."""

    def observe(self, node_id: str) -> NodeObservation:
        """Return a current observation; unreachable nodes use ``reachable=False``."""

    def preflight(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> Sequence[PreflightCheck]:
        """Perform read-only profile/node checks before any deployment is stopped."""

    def drain(self, node_id: str, deployment_id: str) -> CommandResult:
        """Optionally reject new traffic before the deployment is stopped."""

    def start(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> CommandResult:
        """Start exactly the named deployment role using its allowlisted profile."""

    def stop(
        self,
        node_id: str,
        deployment_id: str,
        *,
        timeout_seconds: float,
        force: bool = False,
    ) -> CommandResult:
        """Stop exactly one managed deployment role; force is targeted escalation."""

    def recover(self, spec: DeploymentSpec) -> CommandResult:
        """Reset a reviewed placement after exact runtime identity has been proven.

        Recovery is intentionally profile-scoped. Implementations must reject
        unknown processes and may only invoke the profile's reviewed stop path.
        """

    def logs(
        self,
        node_id: str,
        deployment_id: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> LogChunk:
        """Return a bounded redacted log tail for one named deployment."""
