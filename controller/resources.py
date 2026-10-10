# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Browser-safe JSON resources for the local controller API.

Every function here is pure: it shapes reviewed configuration, journal records,
and cached observations into the documented response objects without reading
the journal or calling a transport. Free text reaches the browser only through
:func:`safe_text`, and operation results only through the allowlist in
:func:`safe_result`. Remote paths and executable recipe details never appear.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from typing import Any

from controller.domain import ModelProfile, NodeConfig
from controller.operations import (
    DeploymentRecord,
    OperationAction,
    OperationEvent,
    OperationPhase,
    OperationRecord,
    OperationState,
    bounded_artifact_report,
    latest_node_progress,
)
from controller.transport.base import NodeObservation, UnitState

PUBLIC_ID_RE = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}\Z")
_ABSOLUTE_PATH_RE = re.compile(r"(?<![\w])/(?=[^\s'\"`]*[A-Za-z])[^\s'\"`]+")
_SENSITIVE_VALUE_RE = re.compile(
    r"(?i)\b(token|secret|password|authorization|private[_ -]?key|api[_ -]?key)\b"
    r"\s*(?:=|:)\s*\S+"
)
_COMMAND_LINE_RE = re.compile(
    r"(?i)^\s*(?:ssh|bash|sh|systemctl|systemd-run|journalctl|curl)\b"
)
_MAX_EVENTS = 100
_PROFILE_PROVENANCE_KEYS = (
    "hf_repo",
    "hf_revision",
    "vllm_release",
    "vllm_version",
    "draft_id",
)
_CANARY_EVIDENCE_KEYS = (
    "node_id",
    "passed",
    "http_status",
    "latency_ms",
    "completion_tokens",
    "finish_reason",
)
_SCALAR_RESULT_KEYS = frozenset(
    {
        "deployment_id",
        "expected_alias",
        "already_ready",
        "already_stopped",
        "rollback",
        "cancelled",
        "recovered",
        "profile_id",
        "draft_id",
    }
)
_LIST_RESULT_KEYS = frozenset(
    {
        "ready_deployments",
        "stopped_deployments",
        "uncertain_deployments",
        "unmanaged_nodes",
    }
)
_PHASE_PROGRESS = {
    OperationPhase.QUEUED: 0,
    OperationPhase.VALIDATING: 5,
    OperationPhase.LOCKING: 10,
    OperationPhase.REFRESHING: 18,
    OperationPhase.PREFLIGHTING: 28,
    OperationPhase.SNAPSHOTTING: 35,
    OperationPhase.DRAINING: 45,
    OperationPhase.STOPPING_CONFLICTS: 55,
    OperationPhase.VERIFYING_STOPPED: 65,
    OperationPhase.STARTING_WORKERS: 72,
    OperationPhase.STARTING_HEAD: 78,
    OperationPhase.LOADING: 84,
    OperationPhase.VERIFYING_READY: 92,
    OperationPhase.RECONCILING: 80,
    OperationPhase.PREPARING: 50,
    OperationPhase.DELETING: 60,
    OperationPhase.COMMITTING: 98,
    OperationPhase.ROLLING_BACK: 90,
    OperationPhase.COMPLETE: 100,
}


def safe_text(
    value: object, *, fallback: str = "No additional safe detail is available."
) -> str:
    """Bound error/log text before it crosses the local browser boundary."""
    text = str(value or "").strip()
    if not text:
        return fallback
    if _COMMAND_LINE_RE.search(text):
        return "Remote command detail redacted."
    text = _SENSITIVE_VALUE_RE.sub(r"\1=[redacted]", text)
    text = _ABSOLUTE_PATH_RE.sub("[redacted path]", text)
    # Remote command output can contain shell fragments without starting with a
    # command.  Avoid presenting those verbatim in the browser.
    if " --" in text or ";" in text or "$(" in text:
        return "Remote command detail redacted."
    return text[:1_000]


def model_resource(
    profile: ModelProfile, compatible_node_ids: Sequence[str]
) -> dict[str, Any]:
    """Informative artifact facts for one profile, never paths or recipes."""
    artifact = profile.artifact
    return {
        "id": artifact.artifact_id,
        "display_name": artifact.display_name,
        "alias": profile.api_alias,
        "format": artifact.format.value,
        "quantization": artifact.quantization,
        "context_length": _context_length(profile),
        "size_bytes": _artifact_bytes(profile),
        "expected_memory_bytes": _artifact_bytes(profile),
        "validation_state": profile.origin.value,
        "origin": profile.origin.value,
        "presence": {
            node_id: {"state": "unverified"} for node_id in compatible_node_ids
        },
        "nodes": [
            {"id": node_id, "state": "unverified"} for node_id in compatible_node_ids
        ],
    }


def profile_resource(profile: ModelProfile) -> dict[str, Any]:
    """Reviewed serving facts and provenance of one profile, never its recipe."""
    return {
        "id": profile.profile_id,
        "display_name": profile.display_name,
        "model_id": profile.artifact.artifact_id,
        "api_alias": profile.api_alias,
        "backend": profile.backend.value,
        "deployment_mode": profile.deployment_mode.value,
        "node_count": profile.node_count,
        "startup_timeout_seconds": profile.readiness.startup_timeout_s,
        "expected_memory_bytes": _artifact_bytes(profile),
        "weights_bytes": _artifact_bytes(profile),
        "gpu_memory_utilization": _gpu_memory_utilization(profile),
        "reserved_memory_bytes": _reserved_memory_bytes(profile),
        "validation_state": profile.origin.value,
        "origin": profile.origin.value,
        "qualified": profile.origin.value == "reviewed",
        "provenance": {
            key: profile.provenance[key]
            for key in _PROFILE_PROVENANCE_KEYS
            if key in profile.provenance
        },
        "node_ids": sorted(profile.node_selector.allowed_node_ids),
        "context_length": _context_length(profile),
        "endpoint_port": profile.endpoint.port,
        "invariants": sorted(profile.safety.required_invariant_ids),
    }


def _artifact_bytes(profile: ModelProfile) -> int | None:
    return profile.artifact.size_bytes or profile.artifact.primary_size_bytes


def _flag_value(profile: ModelProfile, flags: Collection[str]) -> str | None:
    """The value after the first of ``flags`` in the process identity argv."""
    fragments = profile.readiness.process.required_command_fragments
    for index, fragment in enumerate(fragments[:-1]):
        if fragment in flags:
            return fragments[index + 1]
    return None


def _context_length(profile: ModelProfile) -> int | None:
    value = _flag_value(profile, {"-c", "--ctx-size", "--max-model-len"})
    try:
        return None if value is None else int(value)
    except ValueError:
        return None


def _gpu_memory_utilization(profile: ModelProfile) -> float | None:
    # Only generated profiles carry the exact launched argv as identity
    # fragments; reviewed profiles list evidence fragments, not argv.
    if profile.origin.value != "generated":
        return None
    value = _flag_value(profile, {"--gpu-memory-utilization"})
    try:
        utilization = None if value is None else float(value)
    except ValueError:
        return None
    return utilization if utilization is not None and 0 < utilization <= 1 else None


def _reserved_memory_bytes(profile: ModelProfile) -> int | None:
    """Unified memory a generated recipe reserves on each node.

    The recipe renderer pins the start gate to gpu_memory_utilization times
    the smallest node memory of the placement, so the gate is the
    reservation. Reviewed profiles use the gate as a plain safety margin.
    """
    if _gpu_memory_utilization(profile) is None:
        return None
    return profile.safety.minimum_free_memory_mib_before_start * 1024 * 1024


def node_resource(
    node: NodeConfig,
    observation: NodeObservation,
    *,
    observed_at: str | None,
    deployment: DeploymentRecord | None,
    endpoint: str | None,
    active_profile: ModelProfile | None,
) -> dict[str, Any]:
    """One node's observed state; ``observed_at`` is None before any refresh."""
    facts = observation.facts
    total = _integer_fact(facts, "memory_total_bytes")
    free = max(0, observation.free_memory_bytes)
    used = max(0, total - free) if total else None
    state = (
        _node_state(observation, deployment is not None)
        if observed_at is not None
        else "unknown"
    )
    runtime_version = (
        active_profile.runtime.name
        if active_profile is not None
        else "llama.cpp"
        if "llama.cpp" in node.capabilities
        else None
    )
    return {
        "id": node.node_id,
        "display_name": node.display_name,
        "management_address": node.management_address,
        "state": state,
        "last_refresh_at": observed_at,
        "endpoint": endpoint,
        "active_model_alias": observation.active_alias or _foreign_alias(facts),
        "runtime_version": runtime_version,
        "memory": {
            "total_bytes": total or None,
            "used_bytes": used,
            "used_percent": round((used / total) * 100, 1)
            if total and used is not None
            else None,
            "free_bytes": free if observation.reachable else None,
            "swap_used_bytes": _integer_fact(facts, "swap_used_bytes") or None,
        },
        "gpu": {
            "name": "GB10" if "gb10" in node.labels else None,
            "utilization_percent": None,
        },
        "warnings": [safe_text(observation.message)] if observation.message else [],
        "deployment": deployment_resource(deployment) if deployment else None,
    }


def _node_state(observation: NodeObservation, has_known_deployment: bool) -> str:
    if not observation.reachable:
        return "offline"
    if not observation.managed and (
        observation.is_process_running
        or observation.endpoint_healthy
        or observation.active_alias
        or observation.served_aliases
    ):
        # Something the controller cannot prove it started occupies the node.
        return "unmanaged"
    if (
        observation.endpoint_healthy
        and observation.active_alias
        and has_known_deployment
    ):
        return "ready"
    if observation.is_process_running:
        return "busy"
    if observation.unit_state is UnitState.FAILED:
        return "degraded"
    return "idle"


def _foreign_alias(facts: Mapping[str, Any]) -> str | None:
    """Display-only alias of an unmanaged endpoint; never used for gating."""
    endpoints = facts.get("foreign_endpoints")
    if not isinstance(endpoints, Mapping):
        return None
    for aliases in endpoints.values():
        if isinstance(aliases, list) and aliases and isinstance(aliases[0], str):
            return aliases[0]
    return None


def _integer_fact(facts: Mapping[str, Any], name: str) -> int:
    value = facts.get(name, 0)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def deployment_resource(record: DeploymentRecord) -> dict[str, Any]:
    """Desired and observed state of one persisted deployment."""
    return {
        "id": record.id,
        "profile_id": record.profile_id,
        "desired_state": record.desired_state,
        "state": record.observed_state,
        "node_ids": list(record.node_ids),
        "endpoint": record.endpoint,
        "alias": record.expected_alias,
        "owning_operation_id": record.owning_operation_id,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def operation_resource(
    record: OperationRecord, events: Sequence[OperationEvent]
) -> dict[str, Any]:
    """One operation with its newest events; ``events`` is the full trail."""
    # ``updated_at`` changes at every checkpoint and terminal transition.
    # The immutable claim event is the only durable fact for when execution
    # actually started, including after a controller restart.
    started_at = next(
        (event.created_at for event in events if event.code == "operation_started"),
        None,
    )
    recent = events[-_MAX_EVENTS:]
    latest = recent[-1].message if recent else "Operation accepted and queued."
    item: dict[str, Any] = {
        "id": record.id,
        "action": _operation_action(record),
        "state": record.state.value,
        "phase": record.phase.value,
        "progress_percent": _progress_percent(record.phase, record.state),
        "message": safe_text(record.error_message or latest),
        "profile_id": _operation_profile_id(record),
        "deployment_id": operation_deployment_id(record),
        "node_ids": list(record.node_ids),
        "requested_at": record.created_at,
        "started_at": started_at,
        "updated_at": record.updated_at,
        "events": [
            {
                "sequence": event.sequence,
                "timestamp": event.created_at,
                "phase": event.phase.value,
                "code": event.code,
                "message": safe_text(event.message),
            }
            for event in recent
        ],
    }
    if record.error_code:
        item["error"] = {
            "code": record.error_code,
            "message": safe_text(record.error_message),
            "remediation": safe_text(record.remediation),
        }
    if record.action is OperationAction.PREPARE:
        progress = latest_node_progress(events, record.node_ids)
        if progress:
            item["progress_by_node"] = progress
    if record.result:
        item["result"] = safe_result(record.result)
    return item


def operation_deployment_id(record: OperationRecord) -> str | None:
    """The deployment an operation targets, read from its durable request."""
    if record.action is OperationAction.UNLOAD:
        value = record.request.get("deployment_id")
        return value if isinstance(value, str) else None
    deployment = record.request.get("deployment")
    if isinstance(deployment, Mapping):
        value = deployment.get("deployment_id")
        return value if isinstance(value, str) else None
    return None


def _operation_profile_id(record: OperationRecord) -> str | None:
    if record.action is OperationAction.PREPARE and record.result:
        value = record.result.get("profile_id")
        return value if isinstance(value, str) else None
    deployment = record.request.get("deployment")
    if isinstance(deployment, Mapping):
        value = deployment.get("profile_id")
        return value if isinstance(value, str) else None
    return None


def _operation_action(record: OperationRecord) -> str:
    kind = record.request.get("kind")
    return kind if isinstance(kind, str) else record.action.value


def _progress_percent(phase: OperationPhase, state: OperationState) -> int:
    if state.terminal:
        return 100
    return _PHASE_PROGRESS.get(phase, 0)


def safe_result(result: Mapping[str, Any]) -> dict[str, Any]:
    """Allowlisted operation result fields; anything unknown is dropped."""
    safe: dict[str, Any] = {}
    for key, value in result.items():
        if key == "completion_canary" and isinstance(value, Mapping):
            # Text-free canary evidence only; never prompt or response text.
            safe[key] = {
                field: value.get(field)
                for field in _CANARY_EVIDENCE_KEYS
                if field in value
            }
            continue
        if key in _SCALAR_RESULT_KEYS:
            safe[key] = value
        elif key in _LIST_RESULT_KEYS:
            safe[key] = list(value) if isinstance(value, (tuple, list)) else []
        elif key == "deleted_profiles":
            safe[key] = [
                item
                for item in (value if isinstance(value, (tuple, list)) else [])
                if isinstance(item, str) and PUBLIC_ID_RE.fullmatch(item)
            ]
        elif key == "artifacts":
            safe[key] = bounded_artifact_report(value)
    return safe
