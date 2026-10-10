# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Local application service for the controller HTTP boundary.

This module deliberately contains no HTTP parsing and no remote command
construction.  It validates small identifier-only requests, projects reviewed
configuration into :mod:`controller.operations`, queues the synchronous engine
on one local worker, and shapes safe resource representations for the server.
"""

from __future__ import annotations

import re
import threading
import uuid
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from controller.domain import ModelProfile, NodeConfig
from controller.operations import (
    OperationAction,
    OperationPhase,
    OperationRecord,
    OperationState,
    utc_now,
)
from controller.orchestrator import OperationOrchestrator
from controller.persistence import OperationJournal
from controller.projection import ProjectionError, project_deployment
from controller.reconcile import Reconciler
from controller.telemetry import TelemetryCache
from controller.transport.base import (
    LogChunk,
    NodeObservation,
    NodeTransport,
    UnitState,
)

VERSION = "0.2.0"
SCHEMA_VERSION = 1
_IDEMPOTENCY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}\Z")
_PUBLIC_ID_RE = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}\Z")
_ABSOLUTE_PATH_RE = re.compile(r"(?<![\w])/(?=[^\s'\"`]*[A-Za-z])[^\s'\"`]+")
_SENSITIVE_VALUE_RE = re.compile(
    r"(?i)\b(token|secret|password|authorization|private[_ -]?key|api[_ -]?key)\b"
    r"\s*(?:=|:)\s*\S+"
)
_COMMAND_LINE_RE = re.compile(
    r"(?i)^\s*(?:ssh|bash|sh|systemctl|systemd-run|journalctl|curl)\b"
)


class ApplicationError(ValueError):
    """A safe, status-aware error suitable for the stable API envelope."""

    def __init__(
        self,
        code: str,
        message: str,
        remediation: str,
        *,
        status: int = 400,
        retryable: bool = False,
        operation_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = remediation
        self.status = status
        self.retryable = retryable
        self.operation_id = operation_id

    def envelope(self) -> dict[str, Any]:
        error: dict[str, Any] = {
            "code": self.code,
            "message": safe_text(self.message),
            "retryable": self.retryable,
            "remediation": safe_text(self.remediation),
        }
        if self.operation_id:
            error["operation_id"] = self.operation_id
        return {"error": error}


@dataclass(frozen=True)
class CachedObservation:
    """A fresh observation and the time the local controller recorded it."""

    observation: NodeObservation
    observed_at: str


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


class ControllerApplication:
    """Durable local controller service with a single background operation worker."""

    def __init__(
        self,
        *,
        nodes: Sequence[NodeConfig],
        profiles: Sequence[ModelProfile],
        journal: OperationJournal,
        transport: NodeTransport,
        start_reconciliation: bool = True,
        live_operations: Callable[[], Collection[str]] = frozenset,
        owns_journal: bool = True,
    ) -> None:
        if not nodes:
            raise ValueError(
                "ControllerApplication requires at least one configured node"
            )
        # A freshly onboarded cluster has nodes but no profile until a recipe is
        # prepared; the catalog is then simply empty.
        self.nodes = {node.node_id: node for node in nodes}
        self.profiles = {profile.profile_id: profile for profile in profiles}
        if len(self.nodes) != len(nodes) or len(self.profiles) != len(profiles):
            raise ValueError("configured node and profile identifiers must be unique")
        self.journal = journal
        self.transport = transport
        self.telemetry = TelemetryCache(tuple(self.nodes), transport.sample_telemetry)
        self.orchestrator = OperationOrchestrator(journal, transport)
        self.reconciler = Reconciler(
            journal, transport, live_operations=live_operations
        )
        self._owns_journal = owns_journal
        self._observations: dict[str, CachedObservation] = {}
        self._warnings: list[str] = []
        self._mutex = threading.RLock()
        self._closed = False
        self._workers = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="dgx-controller-operation"
        )
        if start_reconciliation:
            self._startup_reconcile()

    def close(self, *, wait: bool = True) -> None:
        """Stop accepting local work without ever unloading a remote model."""
        with self._mutex:
            if self._closed:
                return
            self._closed = True
        self._workers.shutdown(wait=wait, cancel_futures=False)
        if self._owns_journal:
            self.journal.close()

    def has_active_lifecycle_operation(self) -> bool:
        """Whether a load/unload/recover/reconcile is queued or running here."""
        lifecycle = {
            OperationAction.LOAD,
            OperationAction.UNLOAD,
            OperationAction.RECOVER,
            OperationAction.RECONCILE,
        }
        return any(
            record.action in lifecycle
            for record in self.journal.list_operations(
                states=[OperationState.QUEUED, OperationState.RUNNING]
            )
        )

    def health(self) -> dict[str, Any]:
        return {"status": "ok", "version": VERSION, "schema_version": SCHEMA_VERSION}

    def bootstrap(self) -> dict[str, Any]:
        return {
            "nodes": self.list_nodes(),
            "models": self.list_models(),
            "profiles": self.list_profiles(),
            "deployments": self.list_deployments(),
            "operations": self.list_operations(limit=50),
            "warnings": self.warnings(),
            "health": self.health(),
        }

    def warnings(self) -> list[str]:
        with self._mutex:
            return list(self._warnings)

    def list_nodes(self) -> list[dict[str, Any]]:
        return [self._node_resource(node) for node in self.nodes.values()]

    def list_telemetry(self) -> dict[str, Any]:
        return self.telemetry.snapshot()

    def get_node(self, node_id: str) -> dict[str, Any]:
        return self._node_resource(self._node(node_id))

    def refresh_node(self, node_id: str) -> dict[str, Any]:
        self._node(node_id)
        self.refresh_observations((node_id,))
        return self.get_node(node_id)

    def refresh_observations(self, node_ids: Iterable[str] | None = None) -> None:
        """Update the in-memory observed view; failures become safe offline facts."""
        selected = tuple(node_ids or self.nodes.keys())
        for node_id in selected:
            self._node(node_id)
            try:
                observation = self.transport.observe(node_id)
            except (
                Exception
            ) as error:  # transport failures are an observed offline state
                observation = NodeObservation(
                    node_id=node_id,
                    reachable=False,
                    free_memory_bytes=0,
                    unit_state=UnitState.UNKNOWN,
                    message=safe_text(error, fallback="Node observation failed."),
                )
            with self._mutex:
                self._observations[node_id] = CachedObservation(observation, utc_now())

    def list_models(self) -> list[dict[str, Any]]:
        """Expose informative artifacts, never remote paths or executable recipes."""
        items: list[dict[str, Any]] = []
        for profile in self.profiles.values():
            artifact = profile.artifact
            compatible = [
                node_id
                for node_id, node in self.nodes.items()
                if self._node_matches_profile(node, profile)
            ]
            presence = {node_id: {"state": "unverified"} for node_id in compatible}
            items.append(
                {
                    "id": artifact.artifact_id,
                    "display_name": artifact.display_name,
                    "alias": profile.api_alias,
                    "format": artifact.format.value,
                    "quantization": artifact.quantization,
                    "context_length": self._context_length(profile),
                    "size_bytes": artifact.size_bytes or artifact.primary_size_bytes,
                    "expected_memory_bytes": artifact.size_bytes
                    or artifact.primary_size_bytes,
                    "validation_state": profile.origin.value,
                    "origin": profile.origin.value,
                    "presence": presence,
                    "nodes": [
                        {"id": node_id, "state": "unverified"} for node_id in compatible
                    ],
                }
            )
        return items

    def list_profiles(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for profile in self.profiles.values():
            items.append(
                {
                    "id": profile.profile_id,
                    "display_name": profile.display_name,
                    "model_id": profile.artifact.artifact_id,
                    "api_alias": profile.api_alias,
                    "backend": profile.backend.value,
                    "deployment_mode": profile.deployment_mode.value,
                    "node_count": profile.node_count,
                    "startup_timeout_seconds": profile.readiness.startup_timeout_s,
                    "expected_memory_bytes": profile.artifact.size_bytes
                    or profile.artifact.primary_size_bytes,
                    "weights_bytes": profile.artifact.size_bytes
                    or profile.artifact.primary_size_bytes,
                    "gpu_memory_utilization": self._gpu_memory_utilization(profile),
                    "reserved_memory_bytes": self._reserved_memory_bytes(profile),
                    "validation_state": profile.origin.value,
                    "origin": profile.origin.value,
                    "qualified": profile.origin.value == "reviewed",
                    "provenance": {
                        key: profile.provenance[key]
                        for key in (
                            "hf_repo",
                            "hf_revision",
                            "vllm_release",
                            "vllm_version",
                            "draft_id",
                        )
                        if key in profile.provenance
                    },
                    "node_ids": sorted(profile.node_selector.allowed_node_ids),
                    "context_length": self._context_length(profile),
                    "endpoint_port": profile.endpoint.port,
                    "invariants": sorted(profile.safety.required_invariant_ids),
                }
            )
        return items

    def list_deployments(self) -> list[dict[str, Any]]:
        return [
            self._deployment_resource(record)
            for record in self.journal.list_deployments()
        ]

    def list_operations(self, *, limit: int = 50) -> list[dict[str, Any]]:
        bounded = max(1, min(limit, 100))
        return [
            self._operation_resource(record)
            for record in self.journal.list_operations()[:bounded]
        ]

    def get_operation(self, operation_id: str) -> dict[str, Any]:
        record = self.journal.get_operation(operation_id)
        if record is None:
            raise ApplicationError(
                "operation_not_found",
                "The requested operation does not exist.",
                "Refresh operation history and choose an existing operation.",
                status=404,
            )
        return self._operation_resource(record)

    def get_logs(
        self,
        operation_id: str,
        *,
        node_id: str | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        record = self._operation(operation_id)
        selected_node = node_id or (
            record.node_ids[0] if len(record.node_ids) == 1 else None
        )
        if not selected_node or selected_node not in record.node_ids:
            raise ApplicationError(
                "node_not_in_operation",
                "Select a node that belongs to the requested operation.",
                "Refresh the operation and select one of its assigned nodes.",
                status=400,
            )
        if cursor is not None and (not cursor.isdecimal() or len(cursor) > 10):
            raise ApplicationError(
                "invalid_log_cursor",
                "The log cursor is invalid.",
                "Refresh the operation log from its current cursor.",
            )
        deployment_id = self._operation_deployment_id(record)
        if not deployment_id:
            return {
                "operation_id": record.id,
                "node_id": selected_node,
                "cursor": cursor or "0",
                "next_cursor": None,
                "logs": [],
            }
        try:
            chunk = self.transport.logs(
                selected_node, deployment_id, cursor=cursor, limit=100
            )
        except Exception:
            chunk = LogChunk(("Log retrieval is currently unavailable.",), None)
        return {
            "operation_id": record.id,
            "node_id": selected_node,
            "cursor": cursor or "0",
            "next_cursor": chunk.next_cursor,
            "logs": [
                {"timestamp": utc_now(), "level": "info", "message": safe_text(line)}
                for line in chunk.lines
            ],
        }

    def submit_load(
        self, body: Mapping[str, Any], *, idempotency_key: str
    ) -> dict[str, Any]:
        self._require_idempotency_key(idempotency_key)
        self._require_body_keys(body, {"profile_id", "node_ids", "confirmation"})
        profile_id = body.get("profile_id")
        node_ids = body.get("node_ids")
        if not isinstance(profile_id, str) or not profile_id:
            raise ApplicationError(
                "invalid_request",
                "A reviewed profile_id is required.",
                "Choose a profile from the controller catalog.",
            )
        if (
            not isinstance(node_ids, list)
            or not node_ids
            or not all(isinstance(item, str) and item for item in node_ids)
        ):
            raise ApplicationError(
                "invalid_request",
                "A non-empty node_ids array is required.",
                "Choose one or more configured nodes for the reviewed profile.",
            )
        if len(node_ids) > len(self.nodes):
            raise ApplicationError(
                "invalid_request",
                "The requested placement contains too many nodes.",
                "Choose only configured nodes.",
            )
        try:
            profile = self.profiles[profile_id]
        except KeyError as error:
            raise ApplicationError(
                "profile_not_found",
                "The requested profile is not registered.",
                "Refresh profiles and choose a reviewed entry.",
                status=404,
            ) from error
        selected_nodes = tuple(self._node(node_id) for node_id in node_ids)
        try:
            spec = project_deployment(
                profile, selected_nodes, deployment_id=self._new_deployment_id(profile)
            )
        except ProjectionError as error:
            raise ApplicationError(
                error.code, error.message, error.remediation
            ) from error
        try:
            operation, created = self.orchestrator.submit_load(
                spec, idempotency_key=idempotency_key
            )
        except ValueError as error:
            raise self._journal_error(error) from error
        if created:
            self._schedule(operation.id, self.orchestrator.run)
        return self._operation_resource(
            self.journal.get_operation(operation.id) or operation
        )

    def submit_unload(
        self, deployment_id: str, *, idempotency_key: str
    ) -> dict[str, Any]:
        self._require_idempotency_key(idempotency_key)
        self._require_public_id(deployment_id, "deployment")
        try:
            operation, created = self.orchestrator.submit_unload(
                deployment_id, idempotency_key=idempotency_key
            )
        except KeyError as error:
            raise ApplicationError(
                "deployment_not_found",
                "The requested deployment does not exist.",
                "Refresh deployments and choose an existing deployment.",
                status=404,
            ) from error
        except ValueError as error:
            raise self._journal_error(error) from error
        if created:
            self._schedule(operation.id, self.orchestrator.run)
        return self._operation_resource(
            self.journal.get_operation(operation.id) or operation
        )

    def submit_recover(
        self, body: Mapping[str, Any], *, idempotency_key: str
    ) -> dict[str, Any]:
        """Reset a reviewed shared placement without accepting runtime input."""
        self._require_idempotency_key(idempotency_key)
        self._require_body_keys(body, {"profile_id", "confirmation"})
        profile_id = body.get("profile_id")
        if not isinstance(profile_id, str) or not profile_id:
            raise ApplicationError(
                "invalid_request",
                "A reviewed profile_id is required for recovery.",
                "Choose the reviewed profile whose placement should be reset.",
            )
        try:
            profile = self.profiles[profile_id]
        except KeyError as error:
            raise ApplicationError(
                "profile_not_found",
                "The requested recovery profile is not registered.",
                "Refresh profiles and choose a reviewed entry.",
                status=404,
            ) from error
        if profile.deployment_mode.value != "shared-vllm":
            raise ApplicationError(
                "recovery_not_supported",
                "Baseline recovery is currently available only for reviewed "
                "shared-vLLM placements.",
                "Use ordinary unload for a managed single-node deployment.",
                status=409,
            )
        selected_nodes = tuple(
            node
            for node in self.nodes.values()
            if not profile.node_selector.allowed_node_ids
            or node.node_id in profile.node_selector.allowed_node_ids
        )
        try:
            spec = project_deployment(
                profile, selected_nodes, deployment_id=self._new_deployment_id(profile)
            )
        except ProjectionError as error:
            raise ApplicationError(
                error.code, error.message, error.remediation
            ) from error
        try:
            operation, created = self.orchestrator.submit_recover(
                spec, idempotency_key=idempotency_key
            )
        except ValueError as error:
            raise self._journal_error(error) from error
        if created:
            self._schedule(operation.id, self.orchestrator.run)
        return self._operation_resource(
            self.journal.get_operation(operation.id) or operation
        )

    def submit_reconcile(self, *, idempotency_key: str) -> dict[str, Any]:
        """Persist and run a read-only reconciliation pass for all configured nodes."""
        self._require_idempotency_key(idempotency_key)
        try:
            operation, created = self.journal.create_or_get_operation(
                action=OperationAction.RECONCILE,
                idempotency_key=idempotency_key,
                request={"kind": "reconcile"},
                node_ids=tuple(self.nodes),
            )
        except ValueError as error:
            raise self._journal_error(error) from error
        if created:
            self._schedule(operation.id, self._run_reconcile_operation)
        return self._operation_resource(
            self.journal.get_operation(operation.id) or operation
        )

    def cancel_operation(
        self, operation_id: str, *, idempotency_key: str
    ) -> dict[str, Any]:
        self._require_idempotency_key(idempotency_key)
        record = self._operation(operation_id)
        if record.state.terminal:
            return self._operation_resource(record)
        if record.state is not OperationState.QUEUED:
            raise ApplicationError(
                "operation_not_cancellable",
                "This operation has already started and cannot be safely cancelled.",
                "Wait for it to finish, then inspect its journal events or reconcile "
                "the affected nodes.",
                status=409,
                operation_id=record.id,
            )
        cancelled = self.journal.finish(
            record.id,
            state=OperationState.CANCELLED,
            code="operation_cancelled",
            message="Operation cancelled before any remote lifecycle work started.",
            result={"cancelled": True},
        )
        return self._operation_resource(cancelled)

    def _schedule(self, operation_id: str, runner: Any) -> None:
        with self._mutex:
            if self._closed:
                raise ApplicationError(
                    "controller_shutting_down",
                    "The local controller is shutting down and cannot accept a new "
                    "operation.",
                    "Wait for the controller to restart, then retry with the same "
                    "idempotency key.",
                    status=503,
                    retryable=True,
                    operation_id=operation_id,
                )
            self._workers.submit(self._run_scheduled, operation_id, runner)

    def _run_scheduled(self, operation_id: str, runner: Any) -> None:
        try:
            runner(operation_id)
        except (
            Exception
        ):  # preserve a durable, safe terminal result if a service bug escapes
            record = self.journal.get_operation(operation_id)
            if record is not None and not record.state.terminal:
                try:
                    if record.state is OperationState.QUEUED:
                        record, _ = self.journal.claim_operation(operation_id)
                    if not record.state.terminal:
                        self.journal.finish(
                            operation_id,
                            state=OperationState.FAILED,
                            code="application_worker_failed",
                            message="The local operation worker stopped unexpectedly.",
                            error_code="application_worker_failed",
                            remediation=(
                                "Inspect the operation journal and run "
                                "reconciliation before another mutation."
                            ),
                        )
                except Exception:
                    pass
        finally:
            try:
                record = self.journal.get_operation(operation_id)
                if record is not None:
                    self.refresh_observations(record.node_ids)
            except Exception:
                pass

    def _run_reconcile_operation(self, operation_id: str) -> OperationRecord:
        # ``Reconciler`` terminally resolves every *running* operation it sees.
        # Keep this own observation-only request queued until that pass is done,
        # otherwise the reconciler would correctly-but-unhelpfully mark itself
        # ``needs-attention`` as an interrupted action.
        queued = self.journal.get_operation(operation_id)
        if queued is None:
            raise KeyError(operation_id)
        if queued.state is not OperationState.QUEUED:
            return queued
        try:
            self.refresh_observations(queued.node_ids)
            report = self.reconciler.reconcile(queued.node_ids)
            record, claimed = self.journal.claim_operation(operation_id)
            if not claimed:
                return record
            self.journal.advance(
                operation_id,
                phase=OperationPhase.LOCKING,
                code="reconcile_preparing",
                message="Preparing a read-only observation pass.",
            )
            self.journal.advance(
                operation_id,
                phase=OperationPhase.REFRESHING,
                code="reconcile_refreshing",
                message="Refreshing configured node observations.",
            )
            self.refresh_observations(record.node_ids)
            self.journal.advance(
                operation_id,
                phase=OperationPhase.RECONCILING,
                code="reconcile_started",
                message="Reconciling persisted deployments against fresh observations.",
            )
            finished = self.journal.finish(
                operation_id,
                state=OperationState.SUCCEEDED,
                code="reconcile_succeeded",
                message="Observation reconciliation completed.",
                result={
                    "ready_deployments": list(report.ready_deployments),
                    "stopped_deployments": list(report.stopped_deployments),
                    "uncertain_deployments": list(report.uncertain_deployments),
                    "unmanaged_nodes": list(report.unmanaged_nodes),
                },
            )
            self.refresh_observations(record.node_ids)
            return finished
        except Exception:
            current = self.journal.get_operation(operation_id)
            if current is not None and not current.state.terminal:
                if current.state is OperationState.QUEUED:
                    current, _ = self.journal.claim_operation(operation_id)
                return self.journal.finish(
                    operation_id,
                    state=OperationState.FAILED,
                    code="reconcile_failed",
                    message="Reconciliation could not complete.",
                    error_code="reconcile_failed",
                    remediation=(
                        "Inspect node observations and retry the read-only refresh."
                    ),
                )
            return current or queued

    def _startup_reconcile(self) -> None:
        try:
            self.reconciler.reconcile(tuple(self.nodes))
            for queued in self.journal.list_operations(states=[OperationState.QUEUED]):
                if queued.action is OperationAction.PREPARE:
                    continue
                self.journal.finish(
                    queued.id,
                    state=OperationState.CANCELLED,
                    code="queued_operation_interrupted",
                    message="Controller restart cancelled work that had not started.",
                    result={"cancelled": True},
                )
            self.refresh_observations()
        except Exception:
            with self._mutex:
                self._warnings.append(
                    "Startup reconciliation could not complete; refresh node "
                    "observations before a mutation."
                )

    def _node_resource(self, node: NodeConfig) -> dict[str, Any]:
        cached = self._observation(node.node_id)
        observation = (
            cached.observation
            if cached
            else NodeObservation(node.node_id, False, 0, message="Not observed yet.")
        )
        deployment = self._deployment_for_node(node.node_id, observation.deployment_id)
        facts = observation.facts
        total = self._integer_fact(facts, "memory_total_bytes")
        free = max(0, observation.free_memory_bytes)
        used = max(0, total - free) if total else None
        state = (
            self._node_state(observation, deployment is not None)
            if cached
            else "unknown"
        )
        endpoint = (
            deployment.endpoint if deployment else self._configured_endpoint(node)
        )
        warnings = [safe_text(observation.message)] if observation.message else []
        active_profile = (
            self.profiles.get(deployment.profile_id) if deployment else None
        )
        if active_profile is None and observation.active_alias:
            alias_matches = [
                profile
                for profile in self.profiles.values()
                if profile.api_alias == observation.active_alias
            ]
            active_profile = alias_matches[0] if len(alias_matches) == 1 else None
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
            "last_refresh_at": cached.observed_at if cached else None,
            "endpoint": endpoint,
            "active_model_alias": observation.active_alias
            or self._foreign_alias(facts),
            "runtime_version": runtime_version,
            "memory": {
                "total_bytes": total or None,
                "used_bytes": used,
                "used_percent": round((used / total) * 100, 1)
                if total and used is not None
                else None,
                "free_bytes": free if observation.reachable else None,
                "swap_used_bytes": self._integer_fact(facts, "swap_used_bytes") or None,
            },
            "gpu": {
                "name": "GB10" if "gb10" in node.labels else None,
                "utilization_percent": None,
            },
            "warnings": warnings,
            "deployment": self._deployment_resource(deployment) if deployment else None,
        }

    def _deployment_resource(self, record: Any) -> dict[str, Any]:
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

    def _operation_resource(self, record: OperationRecord) -> dict[str, Any]:
        deployment_id = self._operation_deployment_id(record)
        profile_id = self._operation_profile_id(record)
        # ``updated_at`` changes at every checkpoint and terminal transition.
        # The immutable claim event is the only durable fact for when execution
        # actually started, including after a controller restart.
        all_events = self.journal.events(record.id)
        started_at = next(
            (
                event.created_at
                for event in all_events
                if event.code == "operation_started"
            ),
            None,
        )
        events = all_events[-100:]
        item: dict[str, Any] = {
            "id": record.id,
            "action": self._operation_action(record),
            "state": record.state.value,
            "phase": record.phase.value,
            "progress_percent": self._progress(record.phase, record.state),
            "message": safe_text(record.error_message or self._latest_message(events)),
            "profile_id": profile_id,
            "deployment_id": deployment_id,
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
                for event in events
            ],
        }
        if record.error_code:
            item["error"] = {
                "code": record.error_code,
                "message": safe_text(record.error_message),
                "remediation": safe_text(record.remediation),
            }
        if record.result:
            item["result"] = self._safe_result(record.result)
        return item

    def _operation(self, operation_id: str) -> OperationRecord:
        self._require_public_id(operation_id, "operation")
        record = self.journal.get_operation(operation_id)
        if record is None:
            raise ApplicationError(
                "operation_not_found",
                "The requested operation does not exist.",
                "Refresh operation history and choose an existing operation.",
                status=404,
            )
        return record

    def _node(self, node_id: str) -> NodeConfig:
        if not isinstance(node_id, str) or not _PUBLIC_ID_RE.fullmatch(node_id):
            raise ApplicationError(
                "invalid_node_id",
                "The node identifier is invalid.",
                "Choose a node from the controller dashboard.",
            )
        try:
            return self.nodes[node_id]
        except KeyError as error:
            raise ApplicationError(
                "node_not_found",
                "The requested node is not configured.",
                "Refresh nodes and choose a configured node.",
                status=404,
            ) from error

    def _observation(self, node_id: str) -> CachedObservation | None:
        with self._mutex:
            return self._observations.get(node_id)

    def _deployment_for_node(
        self, node_id: str, deployment_id: str | None
    ) -> Any | None:
        if deployment_id:
            record = self.journal.get_deployment(deployment_id)
            if (
                record is not None
                and node_id in record.node_ids
                and record.observed_state != "stopped"
            ):
                return record
        return None

    def _configured_endpoint(self, node: NodeConfig) -> str | None:
        # Endpoint details are derived from reviewed profiles, never submitted
        # by a browser.
        profiles = sorted(
            self.profiles.values(),
            key=lambda profile: profile.deployment_mode.value != "independent",
        )
        for profile in profiles:
            if self._node_matches_profile(node, profile):
                return f"http://{node.management_address}:{profile.endpoint.port}"
        return None

    @staticmethod
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

    @staticmethod
    def _foreign_alias(facts: Mapping[str, Any]) -> str | None:
        """Display-only alias of an unmanaged endpoint; never used for gating."""
        endpoints = facts.get("foreign_endpoints")
        if not isinstance(endpoints, Mapping):
            return None
        for aliases in endpoints.values():
            if isinstance(aliases, list) and aliases and isinstance(aliases[0], str):
                return aliases[0]
        return None

    @staticmethod
    def _integer_fact(facts: Mapping[str, Any], name: str) -> int:
        value = facts.get(name, 0)
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _latest_message(events: Sequence[Any]) -> str:
        return events[-1].message if events else "Operation accepted and queued."

    @staticmethod
    def _progress(phase: OperationPhase, state: OperationState) -> int:
        if state.terminal:
            return 100
        values = {
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
            OperationPhase.COMMITTING: 98,
            OperationPhase.ROLLING_BACK: 90,
            OperationPhase.COMPLETE: 100,
        }
        return values.get(phase, 0)

    @staticmethod
    def _operation_deployment_id(record: OperationRecord) -> str | None:
        if record.action is OperationAction.UNLOAD:
            value = record.request.get("deployment_id")
            return value if isinstance(value, str) else None
        deployment = record.request.get("deployment")
        if isinstance(deployment, Mapping):
            value = deployment.get("deployment_id")
            return value if isinstance(value, str) else None
        return None

    @staticmethod
    def _operation_profile_id(record: OperationRecord) -> str | None:
        if record.action is OperationAction.PREPARE and record.result:
            value = record.result.get("profile_id")
            return value if isinstance(value, str) else None
        deployment = record.request.get("deployment")
        if isinstance(deployment, Mapping):
            value = deployment.get("profile_id")
            return value if isinstance(value, str) else None
        deployment_id = record.request.get("deployment_id")
        return None if not isinstance(deployment_id, str) else None

    @staticmethod
    def _operation_action(record: OperationRecord) -> str:
        kind = record.request.get("kind")
        return kind if isinstance(kind, str) else record.action.value

    @staticmethod
    def _safe_result(result: Mapping[str, Any]) -> dict[str, Any]:
        safe: dict[str, Any] = {}
        for key, value in result.items():
            if key in {
                "deployment_id",
                "expected_alias",
                "already_ready",
                "already_stopped",
                "rollback",
                "cancelled",
                "recovered",
                "profile_id",
                "draft_id",
            }:
                safe[key] = value
            elif key in {
                "ready_deployments",
                "stopped_deployments",
                "uncertain_deployments",
                "unmanaged_nodes",
            }:
                safe[key] = list(value) if isinstance(value, (tuple, list)) else []
        return safe

    @staticmethod
    def _context_length(profile: ModelProfile) -> int | None:
        fragments = profile.readiness.process.required_command_fragments
        for index, fragment in enumerate(fragments[:-1]):
            if fragment in {"-c", "--ctx-size", "--max-model-len"}:
                try:
                    return int(fragments[index + 1])
                except ValueError:
                    return None
        return None

    @staticmethod
    def _gpu_memory_utilization(profile: ModelProfile) -> float | None:
        # Only generated profiles carry the exact launched argv as identity
        # fragments; reviewed profiles list evidence fragments, not argv.
        if profile.origin.value != "generated":
            return None
        fragments = profile.readiness.process.required_command_fragments
        for index, fragment in enumerate(fragments[:-1]):
            if fragment == "--gpu-memory-utilization":
                try:
                    value = float(fragments[index + 1])
                except ValueError:
                    return None
                return value if 0 < value <= 1 else None
        return None

    @classmethod
    def _reserved_memory_bytes(cls, profile: ModelProfile) -> int | None:
        """Unified memory a generated recipe reserves on each node.

        The recipe renderer pins the start gate to gpu_memory_utilization times
        the smallest node memory of the placement, so the gate is the
        reservation. Reviewed profiles use the gate as a plain safety margin.
        """
        if cls._gpu_memory_utilization(profile) is None:
            return None
        return profile.safety.minimum_free_memory_mib_before_start * 1024 * 1024

    @staticmethod
    def _node_matches_profile(node: NodeConfig, profile: ModelProfile) -> bool:
        selector = profile.node_selector
        return (
            (not selector.allowed_node_ids or node.node_id in selector.allowed_node_ids)
            and selector.required_labels.issubset(node.labels)
            and selector.required_capabilities.issubset(node.capabilities)
            and node.endpoint_ports.start
            <= profile.endpoint.port
            <= node.endpoint_ports.end
        )

    @staticmethod
    def _new_deployment_id(profile: ModelProfile) -> str:
        if profile.deployment_mode.value == "shared-vllm":
            return "dep-" + profile.profile_id
        return "dep-" + uuid.uuid4().hex

    @staticmethod
    def _require_body_keys(body: Mapping[str, Any], allowed: set[str]) -> None:
        unknown = set(body).difference(allowed)
        if unknown:
            raise ApplicationError(
                "invalid_request",
                "The request contains unsupported fields.",
                "Submit only reviewed profile and configured node identifiers.",
            )

    @staticmethod
    def _require_public_id(value: str, kind: str) -> None:
        if not _PUBLIC_ID_RE.fullmatch(value):
            raise ApplicationError(
                f"invalid_{kind}_id",
                f"The {kind} identifier is invalid.",
                f"Choose a {kind} from the controller resources.",
            )

    @staticmethod
    def _require_idempotency_key(value: str) -> None:
        if not isinstance(value, str) or not _IDEMPOTENCY_RE.fullmatch(value):
            raise ApplicationError(
                "invalid_idempotency_key",
                "A bounded Idempotency-Key header is required for this operation.",
                "Generate a new idempotency key and retry the same request.",
            )

    @staticmethod
    def _journal_error(error: ValueError) -> ApplicationError:
        return ApplicationError(
            "idempotency_conflict",
            "This idempotency key was already used for a different operation.",
            "Retry the original request with the existing key, or use a new key for a "
            "new action.",
            status=409,
        )
