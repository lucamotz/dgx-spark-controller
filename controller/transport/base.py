# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Transport boundary for all remote node observations and lifecycle commands.

The orchestrator only calls this protocol.  SSH, a future node agent, and the
deterministic fake transport therefore share one safety contract: observations
must identify ownership and readiness must include the expected model alias.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from controller.operations import DeploymentSpec, NodeAssignment
from controller.telemetry import NodeTelemetry

CANARY_FINISH_REASONS = frozenset({"stop", "length"})
_COMPLETION_PATH_RE = re.compile(r"/[A-Za-z0-9/._~-]{0,255}\Z")
_FINISH_REASON_RE = re.compile(r"[a-z_]{1,32}\Z")


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

    @property
    def shows_workload(self) -> bool:
        """Any trace of a deployment: owner, live process, endpoint, or alias."""
        return (
            self.deployment_id is not None
            or self.is_process_running
            or self.endpoint_healthy
            or bool(self.active_alias)
            or bool(self.served_aliases)
        )

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

    def ready_for_assignment(
        self, assignment: NodeAssignment, spec: DeploymentSpec
    ) -> bool:
        """Readiness of one rank; a worker needs only its running owned unit."""
        if assignment.is_worker:
            return (
                self.reachable
                and self.managed
                and self.deployment_id == spec.deployment_id
                and self.unit_state == UnitState.RUNNING
            )
        return self.ready_for(spec.deployment_id, spec.expected_alias)

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


@dataclass(frozen=True)
class CanaryRequest:
    """The reviewed, bounded completion canary projected into a deployment spec."""

    timeout_seconds: int
    max_tokens: int
    prompt: str
    completion_path: str = "/v1/chat/completions"

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0 or self.max_tokens <= 0:
            raise ValueError("completion canary timeout and tokens must be positive")
        if not self.prompt.strip():
            raise ValueError("completion canary prompt must not be empty")
        if not _COMPLETION_PATH_RE.fullmatch(self.completion_path):
            raise ValueError("completion canary path must be a plain absolute URL path")

    def payload(self, alias: str) -> dict[str, Any]:
        """Return the fixed OpenAI-compatible request body for one exact alias."""
        return {
            "model": alias,
            "messages": [{"role": "user", "content": self.prompt}],
            "max_tokens": self.max_tokens,
            "temperature": 0,
        }


def canary_request_for(spec: DeploymentSpec) -> CanaryRequest | None:
    """Return the enabled completion canary of a spec, or ``None``.

    Specs persisted before the canary was projected carry no settings and are
    treated as disabled, so an older deployment's behaviour never changes.

    Raises:
        ValueError: The projected settings are malformed.

    """
    settings = spec.profile.get("completion_canary")
    if not isinstance(settings, Mapping) or settings.get("enabled") is not True:
        return None
    timeout = settings.get("timeout_s")
    max_tokens = settings.get("max_tokens")
    prompt = settings.get("prompt")
    path = settings.get("completion_path", "/v1/chat/completions")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or isinstance(max_tokens, bool)
        or not isinstance(max_tokens, int)
        or not isinstance(prompt, str)
        or not isinstance(path, str)
    ):
        raise ValueError("completion canary settings are malformed")
    return CanaryRequest(timeout, max_tokens, prompt, path)


@dataclass(frozen=True)
class CanaryResult:
    """Bounded evidence from one completion canary.

    It never holds the prompt or any generated text: only the HTTP status, the
    latency, the completion token count, and an allowlisted finish reason.
    """

    passed: bool
    code: str = "ok"
    message: str = ""
    http_status: int | None = None
    latency_ms: int | None = None
    completion_tokens: int | None = None
    finish_reason: str | None = None

    def evidence(self) -> dict[str, Any]:
        """Return the journal-safe facts of this canary."""
        return {
            "passed": self.passed,
            "http_status": self.http_status,
            "latency_ms": self.latency_ms,
            "completion_tokens": self.completion_tokens,
            "finish_reason": self.finish_reason,
        }


def evaluate_canary_response(
    http_status: int | None, body: str, latency_ms: int | None
) -> CanaryResult:
    """Decide whether one chat completion proves the endpoint can generate.

    A pass needs HTTP 200, a first choice whose ``finish_reason`` is ``stop`` or
    ``length``, and ``usage.completion_tokens > 0``. A reasoning model may spend
    every token on reasoning and return empty ``content``; that still passes.

    Args:
        http_status: Status reported by the HTTP client, if any.
        body: Response body; it is parsed and then discarded.
        latency_ms: Total request time in milliseconds, if measured.

    Returns:
        A result holding only bounded, text-free evidence.

    """
    finish_reason: str | None = None
    completion_tokens: int | None = None
    try:
        document = json.loads(body) if body else None
    except (ValueError, RecursionError):
        document = None
    if isinstance(document, Mapping):
        choices = document.get("choices")
        first = choices[0] if isinstance(choices, list) and choices else None
        if isinstance(first, Mapping):
            reason = first.get("finish_reason")
            if isinstance(reason, str):
                finish_reason = (
                    reason if _FINISH_REASON_RE.fullmatch(reason) else "other"
                )
        usage = document.get("usage")
        tokens = usage.get("completion_tokens") if isinstance(usage, Mapping) else None
        if isinstance(tokens, int) and not isinstance(tokens, bool):
            completion_tokens = tokens
    passed = (
        http_status == 200
        and finish_reason in CANARY_FINISH_REASONS
        and completion_tokens is not None
        and completion_tokens > 0
    )
    if passed:
        message = "completion canary passed"
    elif http_status != 200:
        message = f"completion canary returned HTTP {http_status or 'no response'}"
    elif finish_reason not in CANARY_FINISH_REASONS:
        message = f"completion canary finished with {finish_reason or 'no choice'}"
    else:
        message = "completion canary generated no completion tokens"
    return CanaryResult(
        passed=passed,
        code="ok" if passed else "completion_canary_failed",
        message=message,
        http_status=http_status,
        latency_ms=latency_ms,
        completion_tokens=completion_tokens,
        finish_reason=finish_reason,
    )


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

    def completion_canary(
        self, node_id: str, spec: DeploymentSpec, request: CanaryRequest
    ) -> CanaryResult:
        """Send one bounded chat completion to the serving endpoint on a node.

        The request reaches the endpoint the same way readiness observation does
        and carries only reviewed spec values. Implementations judge the
        response with :func:`evaluate_canary_response` and return no text.
        """

    def logs(
        self,
        node_id: str,
        deployment_id: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> LogChunk:
        """Return a bounded redacted log tail for one named deployment."""
