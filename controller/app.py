# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Local application service for the controller HTTP boundary.

This module deliberately contains no HTTP parsing and no remote command
construction.  It validates small identifier-only requests, projects reviewed
configuration into :mod:`controller.operations`, and queues the synchronous
engine on one local worker. :mod:`controller.resources` shapes the safe
resource representations it returns.
"""

from __future__ import annotations

import re
import threading
import uuid
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from controller.domain import (
    DeploymentMode,
    ModelProfile,
    NodeConfig,
    profile_matches_node,
)
from controller.operations import (
    RECIPE_ACTIONS,
    DeploymentRecord,
    DeploymentSpec,
    OperationAction,
    OperationPhase,
    OperationRecord,
    OperationState,
    utc_now,
)
from controller.orchestrator import OperationOrchestrator
from controller.persistence import OperationJournal
from controller.projection import ProjectionError, endpoint_url, project_deployment
from controller.reconcile import Reconciler
from controller.resources import (
    PUBLIC_ID_RE,
    deployment_resource,
    model_resource,
    node_resource,
    operation_deployment_id,
    operation_resource,
    profile_resource,
    safe_text,
)
from controller.telemetry import TelemetryCache
from controller.transport.base import (
    LogChunk,
    NodeObservation,
    NodeTransport,
    UnitState,
)
from controller.updates.engine import ClusterUpdateEngine
from controller.updates.nodeops import UpdateNodeOps
from controller.updates.relay import UpdateRelay
from controller.updates.settings import UpdateSettings

VERSION = "0.2.0"
SCHEMA_VERSION = 1
_IDEMPOTENCY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}\Z")

OperationRunner = Callable[[str], object]


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


def require_idempotency_key(value: str) -> None:
    """Reject a missing or unbounded ``Idempotency-Key`` header value."""
    if not isinstance(value, str) or not _IDEMPOTENCY_RE.fullmatch(value):
        raise ApplicationError(
            "invalid_idempotency_key",
            "A bounded Idempotency-Key header is required for this operation.",
            "Generate a new idempotency key and retry the same request.",
        )


def _operation_not_found() -> ApplicationError:
    return ApplicationError(
        "operation_not_found",
        "The requested operation does not exist.",
        "Refresh operation history and choose an existing operation.",
        status=404,
    )


def _idempotency_conflict() -> ApplicationError:
    return ApplicationError(
        "idempotency_conflict",
        "This idempotency key was already used for a different operation.",
        "Retry the original request with the existing key, or use a new key for a "
        "new action.",
        status=409,
    )


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
        update_ops: UpdateNodeOps | None = None,
        update_relay: UpdateRelay | None = None,
        update_settings: Callable[[], UpdateSettings] = UpdateSettings,
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
        self.update_engine = (
            ClusterUpdateEngine(
                journal,
                self.orchestrator,
                transport,
                update_ops,
                update_relay,
                update_settings,
                live_operations,
            )
            if update_ops is not None and update_relay is not None
            else None
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
        return any(
            record.action not in RECIPE_ACTIONS
            for record in self.journal.list_operations(
                states=[OperationState.QUEUED, OperationState.RUNNING]
            )
        )

    def bootstrap(self) -> dict[str, Any]:
        return {
            "nodes": self.list_nodes(),
            "models": self.list_models(),
            "profiles": self.list_profiles(),
            "deployments": self.list_deployments(),
            "operations": self.list_operations(limit=50),
            "warnings": self.warnings(),
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
        self.refresh_observations((node_id,))
        return self.get_node(node_id)

    def refresh_observations(self, node_ids: Iterable[str] | None = None) -> None:
        """Update the in-memory observed view; failures become safe offline facts."""
        selected = tuple(node_ids or self.nodes.keys())
        for node_id in selected:
            self._node(node_id)
            try:
                observation = self.transport.observe(node_id)
            except Exception as error:
                # An unreachable or failing node is an observed offline state.
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
        return [
            model_resource(
                profile,
                [
                    node_id
                    for node_id, node in self.nodes.items()
                    if profile_matches_node(profile, node)
                ],
            )
            for profile in self.profiles.values()
        ]

    def list_profiles(self) -> list[dict[str, Any]]:
        return [profile_resource(profile) for profile in self.profiles.values()]

    def list_deployments(self) -> list[dict[str, Any]]:
        return [
            deployment_resource(record) for record in self.journal.list_deployments()
        ]

    def list_operations(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return [
            self._operation_resource(record)
            for record in self.journal.list_operations()[:limit]
        ]

    def get_operation(self, operation_id: str) -> dict[str, Any]:
        record = self.journal.get_operation(operation_id)
        if record is None:
            raise _operation_not_found()
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
        deployment_id = operation_deployment_id(record)
        chunk = LogChunk((), None)
        if deployment_id:
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
        require_idempotency_key(idempotency_key)
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
        profile = self._profile(profile_id, "The requested profile is not registered.")
        spec = self._project(
            profile, tuple(self._node(node_id) for node_id in node_ids)
        )
        try:
            operation, created = self.orchestrator.submit_load(
                spec, idempotency_key=idempotency_key
            )
        except ValueError as error:
            raise _idempotency_conflict() from error
        return self._accepted(operation, created, self.orchestrator.run)

    def submit_unload(
        self, deployment_id: str, *, idempotency_key: str
    ) -> dict[str, Any]:
        require_idempotency_key(idempotency_key)
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
            raise _idempotency_conflict() from error
        return self._accepted(operation, created, self.orchestrator.run)

    def submit_recover(
        self, body: Mapping[str, Any], *, idempotency_key: str
    ) -> dict[str, Any]:
        """Reset a reviewed shared placement without accepting runtime input."""
        require_idempotency_key(idempotency_key)
        self._require_body_keys(body, {"profile_id", "confirmation"})
        profile_id = body.get("profile_id")
        if not isinstance(profile_id, str) or not profile_id:
            raise ApplicationError(
                "invalid_request",
                "A reviewed profile_id is required for recovery.",
                "Choose the reviewed profile whose placement should be reset.",
            )
        profile = self._profile(
            profile_id, "The requested recovery profile is not registered."
        )
        if profile.deployment_mode is not DeploymentMode.SHARED_VLLM:
            raise ApplicationError(
                "recovery_not_supported",
                "Baseline recovery is currently available only for reviewed "
                "shared-vLLM placements.",
                "Use ordinary unload for a managed single-node deployment.",
                status=409,
            )
        allowed = profile.node_selector.allowed_node_ids
        spec = self._project(
            profile,
            tuple(
                node
                for node in self.nodes.values()
                if not allowed or node.node_id in allowed
            ),
        )
        try:
            operation, created = self.orchestrator.submit_recover(
                spec, idempotency_key=idempotency_key
            )
        except ValueError as error:
            raise _idempotency_conflict() from error
        return self._accepted(operation, created, self.orchestrator.run)

    def submit_reconcile(self, *, idempotency_key: str) -> dict[str, Any]:
        """Persist and run a read-only reconciliation pass for all configured nodes."""
        require_idempotency_key(idempotency_key)
        try:
            operation, created = self.journal.create_or_get_operation(
                action=OperationAction.RECONCILE,
                idempotency_key=idempotency_key,
                request={},
                node_ids=tuple(self.nodes),
            )
        except ValueError as error:
            raise _idempotency_conflict() from error
        return self._accepted(operation, created, self._run_reconcile_operation)

    def submit_update(
        self,
        body: Mapping[str, Any],
        *,
        idempotency_key: str,
        requester: str | None = None,
    ) -> dict[str, Any]:
        """Queue a system update of every configured node."""
        require_idempotency_key(idempotency_key)
        self._require_body_keys(body, {"confirmation"})
        if self.update_engine is None:
            raise ApplicationError(
                "updates_unavailable",
                "System updates are not available in this controller mode.",
                "Run the controller in live or fake mode with configured nodes.",
                status=409,
            )
        active = [
            record
            for record in self.journal.list_operations(
                states=[OperationState.QUEUED, OperationState.RUNNING]
            )
            if record.action is OperationAction.UPDATE
            and record.idempotency_key != idempotency_key
        ]
        if active:
            raise ApplicationError(
                "update_in_progress",
                "A cluster update is already queued or running.",
                "Wait for it to finish.",
                status=409,
                operation_id=active[0].id,
            )
        with self._mutex:
            if self._closed:
                raise ApplicationError(
                    "controller_shutting_down",
                    "The local controller is shutting down.",
                    "Retry after it restarts.",
                    status=503,
                    retryable=True,
                )
        try:
            operation, created = self.journal.create_or_get_operation(
                action=OperationAction.UPDATE,
                idempotency_key=idempotency_key,
                request={"node_ids": list(self.nodes)},
                node_ids=tuple(self.nodes),
                requester=requester,
            )
        except ValueError as error:
            raise _idempotency_conflict() from error
        return self._accepted(operation, created, self.update_engine.run)

    def cancel_operation(
        self, operation_id: str, *, idempotency_key: str
    ) -> dict[str, Any]:
        require_idempotency_key(idempotency_key)
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

    def _profile(self, profile_id: str, missing_message: str) -> ModelProfile:
        try:
            return self.profiles[profile_id]
        except KeyError as error:
            raise ApplicationError(
                "profile_not_found",
                missing_message,
                "Refresh profiles and choose a reviewed entry.",
                status=404,
            ) from error

    def _project(
        self, profile: ModelProfile, nodes: Sequence[NodeConfig]
    ) -> DeploymentSpec:
        if profile.deployment_mode is DeploymentMode.SHARED_VLLM:
            deployment_id = "dep-" + profile.profile_id
        else:
            deployment_id = "dep-" + uuid.uuid4().hex
        try:
            return project_deployment(profile, nodes, deployment_id=deployment_id)
        except ProjectionError as error:
            raise ApplicationError(
                error.code, error.message, error.remediation
            ) from error

    def _accepted(
        self, operation: OperationRecord, created: bool, runner: OperationRunner
    ) -> dict[str, Any]:
        """Queue a newly journaled operation; a replay only reports its state."""
        if created:
            try:
                self._schedule(operation.id, runner)
            except ApplicationError:
                # Never leave work queued that no worker will run.
                self.journal.finish(
                    operation.id,
                    state=OperationState.CANCELLED,
                    code="queued_operation_interrupted",
                    message="The controller was shutting down, so nothing started.",
                    result={"cancelled": True},
                )
                raise
        return self._operation_resource(
            self.journal.get_operation(operation.id) or operation
        )

    def _schedule(self, operation_id: str, runner: OperationRunner) -> None:
        with self._mutex:
            if self._closed:
                raise ApplicationError(
                    "controller_shutting_down",
                    "The local controller is shutting down and cannot accept a new "
                    "operation.",
                    "Wait for the controller to restart, then retry with a new "
                    "idempotency key.",
                    status=503,
                    retryable=True,
                    operation_id=operation_id,
                )
            self._workers.submit(self._run_scheduled, operation_id, runner)

    def _run_scheduled(self, operation_id: str, runner: OperationRunner) -> None:
        # Exceptions escaping here end in the worker's discarded future.
        try:
            runner(operation_id)
        except Exception:
            # Preserve a durable, safe terminal result if a service bug escapes.
            record = self.journal.get_operation(operation_id)
            if record is not None and not record.state.terminal:
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
        finally:
            record = self.journal.get_operation(operation_id)
            if record is not None:
                self.refresh_observations(self._affected_node_ids(record))

    def _affected_node_ids(self, record: OperationRecord) -> tuple[str, ...]:
        """Requested nodes plus configured nodes a switch stopped outside them."""
        node_ids = list(record.node_ids)
        placement = record.previous_placement or {}
        observed = placement.get("observed_node_ids")
        if isinstance(observed, list):
            node_ids.extend(
                node_id
                for node_id in observed
                if isinstance(node_id, str)
                and node_id in self.nodes
                and node_id not in node_ids
            )
        return tuple(node_ids)

    def _run_reconcile_operation(self, operation_id: str) -> OperationRecord:
        # ``Reconciler`` terminally resolves every *running* operation it sees.
        # Keep this own observation-only request queued until that pass is done,
        # otherwise the reconciler would correctly-but-unhelpfully mark itself
        # ``needs-attention`` as an interrupted action.
        queued = self.journal.get_operation(operation_id)
        assert queued is not None
        if queued.state is not OperationState.QUEUED:
            return queued
        try:
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
                if queued.action in RECIPE_ACTIONS:
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
                    "Startup reconciliation could not complete. Refresh node "
                    "observations before a mutation."
                )

    def _node_resource(self, node: NodeConfig) -> dict[str, Any]:
        with self._mutex:
            cached = self._observations.get(node.node_id)
        observation = (
            cached.observation
            if cached
            else NodeObservation(node.node_id, False, 0, message="Not observed yet.")
        )
        deployment = self._deployment_for_node(node.node_id, observation.deployment_id)
        return node_resource(
            node,
            observation,
            observed_at=cached.observed_at if cached else None,
            deployment=deployment,
            endpoint=(
                deployment.endpoint if deployment else self._configured_endpoint(node)
            ),
            active_profile=self._active_profile(deployment, observation.active_alias),
        )

    def _operation_resource(self, record: OperationRecord) -> dict[str, Any]:
        return operation_resource(record, self.journal.events(record.id))

    def _operation(self, operation_id: str) -> OperationRecord:
        self._require_public_id(operation_id, "operation")
        record = self.journal.get_operation(operation_id)
        if record is None:
            raise _operation_not_found()
        return record

    def _node(self, node_id: str) -> NodeConfig:
        self._require_public_id(node_id, "node")
        try:
            return self.nodes[node_id]
        except KeyError as error:
            raise ApplicationError(
                "node_not_found",
                "The requested node is not configured.",
                "Refresh nodes and choose a configured node.",
                status=404,
            ) from error

    def _deployment_for_node(
        self, node_id: str, deployment_id: str | None
    ) -> DeploymentRecord | None:
        if deployment_id:
            record = self.journal.get_deployment(deployment_id)
            if (
                record is not None
                and node_id in record.node_ids
                and record.observed_state != "stopped"
            ):
                return record
        return None

    def _active_profile(
        self, deployment: DeploymentRecord | None, active_alias: str | None
    ) -> ModelProfile | None:
        """The deployment's profile, else the one profile serving ``active_alias``."""
        profile = self.profiles.get(deployment.profile_id) if deployment else None
        if profile is None and active_alias:
            matches = [
                candidate
                for candidate in self.profiles.values()
                if candidate.api_alias == active_alias
            ]
            profile = matches[0] if len(matches) == 1 else None
        return profile

    def _configured_endpoint(self, node: NodeConfig) -> str | None:
        # Endpoint details are derived from reviewed profiles, never submitted
        # by a browser.
        profiles = sorted(
            self.profiles.values(),
            key=lambda item: item.deployment_mode is not DeploymentMode.INDEPENDENT,
        )
        for profile in profiles:
            if profile_matches_node(profile, node):
                return endpoint_url(profile, node)
        return None

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
        if not PUBLIC_ID_RE.fullmatch(value):
            raise ApplicationError(
                f"invalid_{kind}_id",
                f"The {kind} identifier is invalid.",
                f"Choose a {kind} from the controller resources.",
            )
