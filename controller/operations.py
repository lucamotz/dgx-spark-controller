# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Stable operation records and state-machine validation.

The controller deliberately keeps operation state separate from any HTTP or runtime
implementation.  Records in this module can therefore be serialised into the
SQLite journal and safely reconstructed during restart reconciliation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


def utc_now() -> str:
    """Return a sortable, timezone-aware timestamp suitable for journal records."""
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class OperationAction(str, Enum):
    """Mutations that are represented by a durable operation."""

    LOAD = "load"
    UNLOAD = "unload"
    RECOVER = "recover"
    RECONCILE = "reconcile"
    PREPARE = "prepare"
    DELETE_RECIPE = "delete-recipe"


# Actions owned and resolved by the recipe service's own worker. Lifecycle
# reconciliation and startup cleanup never resolve them; the recipe service
# reattaches to or settles them itself after a controller restart.
RECIPE_ACTIONS = frozenset({OperationAction.PREPARE, OperationAction.DELETE_RECIPE})


class OperationState(str, Enum):
    """Terminal and non-terminal operation lifecycle states."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NEEDS_ATTENTION = "needs-attention"

    @property
    def terminal(self) -> bool:
        """Whether no ordinary worker may mutate this operation further."""
        return self in {
            OperationState.SUCCEEDED,
            OperationState.FAILED,
            OperationState.CANCELLED,
            OperationState.NEEDS_ATTENTION,
        }


class OperationPhase(str, Enum):
    """Observable checkpoints shared by load, unload, and reconciliation work."""

    QUEUED = "queued"
    VALIDATING = "validating"
    LOCKING = "locking"
    REFRESHING = "refreshing"
    PREFLIGHTING = "preflighting"
    SNAPSHOTTING = "snapshotting"
    DRAINING = "draining"
    STOPPING_CONFLICTS = "stopping-conflicts"
    VERIFYING_STOPPED = "verifying-stopped"
    STARTING_WORKERS = "starting-workers"
    STARTING_HEAD = "starting-head"
    LOADING = "loading"
    VERIFYING_READY = "verifying-ready"
    COMMITTING = "committing"
    ROLLING_BACK = "rolling-back"
    RECONCILING = "reconciling"
    PREPARING = "preparing"
    DELETING = "deleting"
    COMPLETE = "complete"


# A phase may stay the same (for a log/progress event) but can never move
# backwards.  Explicit edges make accidental, misleading progress impossible.
LEGAL_PHASE_TRANSITIONS: dict[OperationPhase, frozenset[OperationPhase]] = {
    OperationPhase.QUEUED: frozenset(
        {
            OperationPhase.VALIDATING,
            OperationPhase.LOCKING,
            OperationPhase.RECONCILING,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.VALIDATING: frozenset(
        {
            OperationPhase.LOCKING,
            OperationPhase.PREPARING,
            OperationPhase.DELETING,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.LOCKING: frozenset(
        {OperationPhase.REFRESHING, OperationPhase.COMPLETE}
    ),
    OperationPhase.REFRESHING: frozenset(
        {
            OperationPhase.PREFLIGHTING,
            OperationPhase.DRAINING,
            OperationPhase.RECONCILING,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.PREFLIGHTING: frozenset(
        {
            OperationPhase.SNAPSHOTTING,
            OperationPhase.COMMITTING,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.SNAPSHOTTING: frozenset(
        {
            OperationPhase.DRAINING,
            OperationPhase.STOPPING_CONFLICTS,
            OperationPhase.VERIFYING_STOPPED,
            OperationPhase.STARTING_WORKERS,
            OperationPhase.STARTING_HEAD,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.DRAINING: frozenset(
        {
            OperationPhase.STOPPING_CONFLICTS,
            OperationPhase.VERIFYING_STOPPED,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.STOPPING_CONFLICTS: frozenset(
        {
            OperationPhase.VERIFYING_STOPPED,
            OperationPhase.ROLLING_BACK,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.VERIFYING_STOPPED: frozenset(
        {
            OperationPhase.STARTING_WORKERS,
            OperationPhase.STARTING_HEAD,
            OperationPhase.ROLLING_BACK,
            OperationPhase.COMMITTING,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.STARTING_WORKERS: frozenset(
        {
            OperationPhase.STARTING_HEAD,
            OperationPhase.LOADING,
            OperationPhase.ROLLING_BACK,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.STARTING_HEAD: frozenset(
        {
            OperationPhase.LOADING,
            OperationPhase.VERIFYING_READY,
            OperationPhase.ROLLING_BACK,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.LOADING: frozenset(
        {
            OperationPhase.VERIFYING_READY,
            OperationPhase.ROLLING_BACK,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.VERIFYING_READY: frozenset(
        {
            OperationPhase.COMMITTING,
            OperationPhase.ROLLING_BACK,
            OperationPhase.COMPLETE,
        }
    ),
    OperationPhase.COMMITTING: frozenset({OperationPhase.COMPLETE}),
    OperationPhase.ROLLING_BACK: frozenset({OperationPhase.COMPLETE}),
    OperationPhase.RECONCILING: frozenset(
        {OperationPhase.COMMITTING, OperationPhase.COMPLETE}
    ),
    # Recipe preparation downloads runtime images and weights without touching
    # any model lifecycle, so it never enters the locking/start phases.
    OperationPhase.PREPARING: frozenset(
        {OperationPhase.COMMITTING, OperationPhase.COMPLETE}
    ),
    # Deleting a generated recipe removes controller-owned files only; it never
    # starts or stops a model, so it has no lifecycle phases either.
    OperationPhase.DELETING: frozenset({OperationPhase.COMPLETE}),
    OperationPhase.COMPLETE: frozenset(),
}

# Terminal states have no outgoing edge: a finished record is immutable.
LEGAL_STATE_TRANSITIONS: dict[OperationState, frozenset[OperationState]] = {
    OperationState.QUEUED: frozenset(
        {OperationState.RUNNING, OperationState.CANCELLED}
    ),
    OperationState.RUNNING: frozenset(
        {
            OperationState.SUCCEEDED,
            OperationState.FAILED,
            OperationState.CANCELLED,
            OperationState.NEEDS_ATTENTION,
        }
    ),
    OperationState.SUCCEEDED: frozenset(),
    OperationState.FAILED: frozenset(),
    OperationState.CANCELLED: frozenset(),
    OperationState.NEEDS_ATTENTION: frozenset(),
}

PROGRESS_STATES = frozenset({"running", "succeeded", "failed", "absent", "unknown"})
ARTIFACT_KINDS = frozenset({"weights", "image", "cache-volume"})
ARTIFACT_STATES = frozenset({"removed", "absent", "in-use", "refused", "failed"})
_PROGRESS_STEP_RE = re.compile(r"[a-z][a-z-]{0,23}\Z")
_NODE_ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_MAX_PROGRESS_BYTES = 2**53


def bounded_node_progress(
    value: object, node_ids: Sequence[str]
) -> dict[str, dict[str, object]]:
    """Return per-node prepare progress restricted to known nodes and fields.

    Progress is journaled by the recipe service and shown in the browser, so
    it is re-validated on the way out: only the operation's own node ids, a
    fixed state vocabulary, a short lowercase step name, and byte counts.
    Nothing here can carry a path or free-form remote text.

    Args:
        value: The ``progress_by_node`` mapping from an operation event.
        node_ids: The operation's node ids, in placement order.

    Returns:
        A new mapping in placement order; empty when ``value`` is unusable.

    """
    if not isinstance(value, Mapping):
        return {}
    progress: dict[str, dict[str, object]] = {}
    for node_id in node_ids:
        item = value.get(node_id)
        if not isinstance(item, Mapping):
            continue
        state = item.get("state")
        step = item.get("step")
        progress[node_id] = {
            "state": state
            if isinstance(state, str) and state in PROGRESS_STATES
            else "unknown",
            "step": step
            if isinstance(step, str) and _PROGRESS_STEP_RE.fullmatch(step)
            else "unknown",
            "downloaded_bytes": _progress_bytes(item.get("downloaded_bytes")),
            "total_bytes": _progress_bytes(item.get("total_bytes")),
        }
    return progress


def bounded_artifact_report(value: object) -> list[dict[str, str]]:
    """Return a recipe deletion's artifact report with known values only."""
    if not isinstance(value, (list, tuple)):
        return []
    report: list[dict[str, str]] = []
    for item in value[:64]:
        if not isinstance(item, Mapping):
            continue
        node_id, kind, state = item.get("node_id"), item.get("kind"), item.get("state")
        if (
            isinstance(node_id, str)
            and _NODE_ID_RE.fullmatch(node_id)
            and isinstance(kind, str)
            and kind in ARTIFACT_KINDS
            and isinstance(state, str)
            and state in ARTIFACT_STATES
        ):
            report.append({"node_id": node_id, "kind": kind, "state": state})
    return report


def _progress_bytes(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 <= value < _MAX_PROGRESS_BYTES else None


class OperationError(RuntimeError):
    """Base error with a stable code and operator-oriented remediation text."""

    def __init__(self, code: str, message: str, remediation: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = remediation


class IllegalTransition(OperationError):
    """Raised when an operation would misrepresent progress in its journal."""

    def __init__(self, current: OperationPhase, requested: OperationPhase) -> None:
        super().__init__(
            "illegal_phase_transition",
            f"Operation phase cannot move from {current.value!r} to "
            f"{requested.value!r}.",
            "Inspect the operation events and resume through reconciliation if it was "
            "interrupted.",
        )


class LockConflict(OperationError):
    """Raised when a live lease owned by another operation overlaps a mutation."""

    def __init__(self, resource: str, owner_operation_id: str) -> None:
        super().__init__(
            "operation_locked",
            f"Resource {resource!r} is locked by operation {owner_operation_id}.",
            "Wait for the active operation to finish, then retry with the same "
            "idempotency key.",
        )
        self.resource = resource
        self.owner_operation_id = owner_operation_id


# Ranks that serve no endpoint of their own: they are ready once their
# controller-owned unit runs, start before and stop after the serving rank.
WORKER_ROLES = frozenset({"worker", "rpc-worker"})


@dataclass(frozen=True)
class NodeAssignment:
    """A node's role within one deployment placement."""

    node_id: str
    role: str = "single"

    @property
    def is_worker(self) -> bool:
        return self.role in WORKER_ROLES

    def to_dict(self) -> dict[str, str]:
        return {"node_id": self.node_id, "role": self.role}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> NodeAssignment:
        return cls(node_id=str(value["node_id"]), role=str(value.get("role", "single")))


@dataclass(frozen=True)
class DeploymentSpec:
    """Minimal deployment contract consumed by the generic orchestrator.

    ``profile`` is intentionally a JSON-compatible mapping.  The eventual domain
    model may carry richer typed records; the application adapter only needs to
    project it into this durable, allowlisted execution contract.  Recognised
    optional profile keys include ``required_memory_bytes``, ``script_valid``,
    ``model_present`` and runtime-specific preflight fields used by transports.
    """

    deployment_id: str
    profile_id: str
    expected_alias: str
    assignments: tuple[NodeAssignment, ...]
    profile: Mapping[str, Any] = field(default_factory=dict)
    endpoint: str | None = None
    startup_timeout_seconds: float = 30.0
    stop_timeout_seconds: float = 15.0
    minimum_free_memory_bytes: int = 0
    post_stop_free_memory_bytes: int = 0

    def __post_init__(self) -> None:
        if not self.deployment_id or not self.profile_id or not self.expected_alias:
            raise ValueError(
                "deployment_id, profile_id, and expected_alias are required"
            )
        if not self.assignments:
            raise ValueError("a deployment requires at least one node assignment")
        node_ids = [assignment.node_id for assignment in self.assignments]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("a deployment cannot assign a node more than once")
        if self.startup_timeout_seconds < 0 or self.stop_timeout_seconds < 0:
            raise ValueError("operation timeouts must be non-negative")

    @property
    def node_ids(self) -> tuple[str, ...]:
        return tuple(assignment.node_id for assignment in self.assignments)

    @property
    def stopped_free_memory_bytes(self) -> int:
        """Free memory a node must report before this deployment counts as stopped.

        It is the stricter of the pre-start and post-stop minimums, so a stop is
        never proven while the next start would still be refused.
        """
        return max(self.minimum_free_memory_bytes, self.post_stop_free_memory_bytes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_id": self.deployment_id,
            "profile_id": self.profile_id,
            "expected_alias": self.expected_alias,
            "assignments": [assignment.to_dict() for assignment in self.assignments],
            "profile": dict(self.profile),
            "endpoint": self.endpoint,
            "startup_timeout_seconds": self.startup_timeout_seconds,
            "stop_timeout_seconds": self.stop_timeout_seconds,
            "minimum_free_memory_bytes": self.minimum_free_memory_bytes,
            "post_stop_free_memory_bytes": self.post_stop_free_memory_bytes,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> DeploymentSpec:
        return cls(
            deployment_id=str(value["deployment_id"]),
            profile_id=str(value["profile_id"]),
            expected_alias=str(value["expected_alias"]),
            assignments=tuple(
                NodeAssignment.from_dict(item) for item in value["assignments"]
            ),
            profile=dict(value.get("profile") or {}),
            endpoint=value.get("endpoint"),
            startup_timeout_seconds=float(value.get("startup_timeout_seconds", 30.0)),
            stop_timeout_seconds=float(value.get("stop_timeout_seconds", 15.0)),
            minimum_free_memory_bytes=int(value.get("minimum_free_memory_bytes", 0)),
            post_stop_free_memory_bytes=int(
                value.get("post_stop_free_memory_bytes", 0)
            ),
        )


@dataclass(frozen=True)
class OperationEvent:
    """An immutable event in an operation's ordered audit trail."""

    operation_id: str
    sequence: int
    created_at: str
    code: str
    phase: OperationPhase
    message: str
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OperationRecord:
    """Current materialised operation state plus enough data for recovery."""

    id: str
    idempotency_key: str
    action: OperationAction
    state: OperationState
    phase: OperationPhase
    request: Mapping[str, Any]
    requester: str | None
    node_ids: tuple[str, ...]
    created_at: str
    updated_at: str
    previous_placement: Mapping[str, Any] | None = None
    result: Mapping[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    remediation: str | None = None


@dataclass(frozen=True)
class DeploymentRecord:
    """Persisted desired/observed deployment used for conflicts and rollback."""

    id: str
    profile_id: str
    expected_alias: str
    node_ids: tuple[str, ...]
    desired_state: str
    observed_state: str
    spec: Mapping[str, Any]
    endpoint: str | None
    owning_operation_id: str | None
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "profile_id": self.profile_id,
            "expected_alias": self.expected_alias,
            "node_ids": list(self.node_ids),
            "desired_state": self.desired_state,
            "observed_state": self.observed_state,
            "spec": dict(self.spec),
            "endpoint": self.endpoint,
            "owning_operation_id": self.owning_operation_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


def ensure_phase_transition(current: OperationPhase, requested: OperationPhase) -> None:
    """Reject a phase edge that is not part of the explicit state graph."""
    if requested == current:
        return
    if requested not in LEGAL_PHASE_TRANSITIONS[current]:
        raise IllegalTransition(current, requested)


def ensure_state_transition(current: OperationState, requested: OperationState) -> None:
    """Validate operation-state edges independently from detailed phase progress."""
    if requested != current and requested not in LEGAL_STATE_TRANSITIONS[current]:
        raise OperationError(
            "illegal_state_transition",
            f"Operation state cannot move from {current.value!r} to "
            f"{requested.value!r}.",
            "Create a new operation; completed journal records are immutable.",
        )


def latest_node_progress(
    events: Sequence[OperationEvent], node_ids: Sequence[str]
) -> dict[str, dict[str, object]]:
    """Return the newest bounded ``progress_by_node`` journaled in ``events``."""
    for event in reversed(events):
        details = event.details if isinstance(event.details, Mapping) else {}
        if "progress_by_node" in details:
            return bounded_node_progress(details["progress_by_node"], node_ids)
    return {}
