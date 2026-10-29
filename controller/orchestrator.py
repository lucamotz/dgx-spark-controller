# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Transactional load/unload engine built on the durable operation journal.

The orchestrator has no SSH, shell, or HTTP-server knowledge.  It coordinates a
``NodeTransport`` through an explicit safety sequence: preflight all nodes, stop
and prove release of conflicting deployments on every node they use, start in
role order, prove process identity, health, and model alias, then optionally run
one bounded completion canary.  Failures after mutation clean up the partial new
deployment and best-effort restore the recorded prior placement.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from controller.operations import (
    DeploymentRecord,
    DeploymentSpec,
    NodeAssignment,
    OperationAction,
    OperationError,
    OperationPhase,
    OperationRecord,
    OperationState,
    utc_now,
)
from controller.persistence import OperationJournal
from controller.transport.base import (
    CanaryRequest,
    CommandResult,
    NodeObservation,
    NodeTransport,
    UnitState,
    canary_request_for,
)


class PreflightFailed(OperationError):
    """Raised when a non-destructive node/profile check blocks a mutation."""

    def __init__(self, failures: Sequence[Mapping[str, str]]) -> None:
        summary = "; ".join(f"{item['node_id']}:{item['name']}" for item in failures)
        recipe_detail = next(
            (
                item.get("message", "").rstrip(".")
                for item in failures
                if item.get("name") == "recipe_commit" and item.get("message")
            ),
            "",
        )
        message = f"Preflight failed ({summary})."
        if recipe_detail:
            message = f"{message} {recipe_detail}."
        super().__init__(
            "preflight_failed",
            message,
            failures[0].get(
                "remediation", "Resolve the failing preflight check before retrying."
            )
            if failures
            else "",
        )
        self.failures = tuple(failures)


class VerificationFailed(OperationError):
    """Raised when observation cannot prove a safety invariant within its timeout."""


class OwnershipConflict(OperationError):
    """Raised before stopping a process whose controller ownership is uncertain."""


class CompletionCanaryFailed(VerificationFailed):
    """Raised when a ready endpoint cannot complete one bounded request."""

    def __init__(self, message: str, evidence: Mapping[str, Any]) -> None:
        super().__init__(
            "completion_canary_failed",
            message,
            "The endpoint passed health and alias checks but could not generate. "
            "Inspect the bounded runtime logs. Rollback has been attempted.",
        )
        self.evidence = dict(evidence)


@dataclass(frozen=True)
class _LeaseScope:
    """Leases one operation holds; the lease keeper renews exactly these."""

    node_ids: tuple[str, ...]
    cluster: bool
    cluster_id: str


@dataclass
class _Progress:
    """Remote changes an operation has made, which decide its failure handling."""

    mutated: bool = False
    started: list[NodeAssignment] = field(default_factory=list)
    replaced: list[DeploymentRecord] = field(default_factory=list)


class OperationOrchestrator:
    """Run durable deployment operations against a transport implementation.

    The class is synchronous by design: the local app may execute ``run`` from a
    background worker, while tests can inspect every journal checkpoint without
    event-loop machinery.  ``submit_*`` only persists work; ``run`` claims and
    executes it exactly once.
    """

    def __init__(
        self,
        journal: OperationJournal,
        transport: NodeTransport,
        *,
        lock_lease_seconds: float = 120.0,
        poll_interval_seconds: float = 0.05,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if lock_lease_seconds <= 0:
            raise ValueError("lock_lease_seconds must be positive")
        self.journal = journal
        self.transport = transport
        self.lock_lease_seconds = lock_lease_seconds
        self.poll_interval_seconds = max(0.0, poll_interval_seconds)
        self._sleep = sleeper
        self._monotonic = monotonic
        self._lease_scopes: dict[str, _LeaseScope] = {}
        self._lease_mutex = threading.Lock()

    def submit_load(
        self,
        spec: DeploymentSpec,
        *,
        idempotency_key: str,
        requester: str | None = None,
    ) -> tuple[OperationRecord, bool]:
        """Persist a load/switch request without changing any remote node."""
        return self.journal.create_or_get_operation(
            action=OperationAction.LOAD,
            idempotency_key=idempotency_key,
            request={"deployment": spec.to_dict()},
            node_ids=spec.node_ids,
            requester=requester,
        )

    def submit_unload(
        self, deployment_id: str, *, idempotency_key: str, requester: str | None = None
    ) -> tuple[OperationRecord, bool]:
        """Persist a request to unload one known deployment."""
        deployment = self.journal.get_deployment(deployment_id)
        if deployment is None:
            raise KeyError(f"unknown deployment {deployment_id}")
        return self.journal.create_or_get_operation(
            action=OperationAction.UNLOAD,
            idempotency_key=idempotency_key,
            request={"deployment_id": deployment_id},
            node_ids=deployment.node_ids,
            requester=requester,
        )

    def submit_recover(
        self,
        spec: DeploymentSpec,
        *,
        idempotency_key: str,
        requester: str | None = None,
    ) -> tuple[OperationRecord, bool]:
        """Persist a profile-scoped baseline recovery request."""
        return self.journal.create_or_get_operation(
            action=OperationAction.RECOVER,
            idempotency_key=idempotency_key,
            request={"deployment": spec.to_dict(), "kind": "recover"},
            node_ids=spec.node_ids,
            requester=requester,
        )

    def execute_load(
        self,
        spec: DeploymentSpec,
        *,
        idempotency_key: str,
        requester: str | None = None,
    ) -> OperationRecord:
        """Convenience synchronous API: persist, then execute a newly queued load."""
        operation, created = self.submit_load(
            spec, idempotency_key=idempotency_key, requester=requester
        )
        return self.run(operation.id) if created else operation

    def execute_unload(
        self, deployment_id: str, *, idempotency_key: str, requester: str | None = None
    ) -> OperationRecord:
        """Convenience synchronous API: persist, then execute a newly queued unload."""
        operation, created = self.submit_unload(
            deployment_id, idempotency_key=idempotency_key, requester=requester
        )
        return self.run(operation.id) if created else operation

    def run(self, operation_id: str) -> OperationRecord:
        """Claim and execute a queued operation.

        Returns existing work unchanged otherwise.
        """
        operation, claimed = self.journal.claim_operation(operation_id)
        if not claimed:
            return operation
        try:
            if operation.action == OperationAction.LOAD:
                return self._run_load(
                    operation, DeploymentSpec.from_dict(operation.request["deployment"])
                )
            if operation.action == OperationAction.UNLOAD:
                return self._run_unload(
                    operation, str(operation.request["deployment_id"])
                )
            if operation.action == OperationAction.RECOVER:
                return self._run_recover(
                    operation, DeploymentSpec.from_dict(operation.request["deployment"])
                )
            raise OperationError(
                "unsupported_action", f"Cannot execute action {operation.action.value}."
            )
        except OperationError as error:
            return self._finish_error(operation.id, error)
        except Exception as error:  # pragma: no cover
            # Last resort: a claimed operation must never stay running.
            return self._finish_error(
                operation.id,
                OperationError(
                    "internal_error",
                    f"Unexpected operation engine error: {error}",
                    "Inspect the operation events and run reconciliation before "
                    "another mutation.",
                ),
            )

    def _run_load(
        self, operation: OperationRecord, spec: DeploymentSpec
    ) -> OperationRecord:
        progress = _Progress()
        with self._leases(operation.id, spec, "Acquiring placement leases."):
            try:
                return self._load(operation.id, spec, progress)
            except OperationError as error:
                if progress.mutated:
                    return self._rollback_load(
                        operation.id, spec, progress.started, progress.replaced, error
                    )
                return self._finish_error(operation.id, error)

    def _load(
        self, operation_id: str, spec: DeploymentSpec, progress: _Progress
    ) -> OperationRecord:
        observations, extra_node_ids = self._refresh_load_scope(operation_id, spec)
        self._advance(
            operation_id,
            OperationPhase.PREFLIGHTING,
            "preflight_started",
            "Running all read-only preflight checks.",
        )
        failures = self.preflight(spec)
        if failures:
            raise PreflightFailed(failures)
        canary = self._canary_request(spec)

        if self._placement_is_ready(spec, observations):
            self._advance(
                operation_id,
                OperationPhase.COMMITTING,
                "placement_already_ready",
                "Requested deployment is already ready.",
            )
            self._save_deployment(
                spec, operation_id, desired_state="ready", observed_state="ready"
            )
            return self.journal.finish(
                operation_id,
                state=OperationState.SUCCEEDED,
                code="operation_succeeded",
                message="Load request is already satisfied by observed state.",
                result={"deployment_id": spec.deployment_id, "already_ready": True},
            )

        progress.replaced = self._find_conflicts(spec, observations)
        self._advance(
            operation_id,
            OperationPhase.SNAPSHOTTING,
            "snapshotting_prior_placement",
            "Recording prior placement for rollback.",
        )
        self.journal.record_previous_placement(
            operation_id,
            {
                "deployments": [record.to_dict() for record in progress.replaced],
                "observed_node_ids": [*spec.node_ids, *extra_node_ids],
                "extra_node_ids": list(extra_node_ids),
            },
        )
        self._stop_replaced(operation_id, spec, progress)
        self._verify_released_before_start(operation_id, spec, progress.replaced)
        self._record_handover(operation_id, spec, progress.replaced)
        self._start_placement(operation_id, spec, progress)
        canary_evidence = self._verify_ready(operation_id, spec, canary)

        self._advance(
            operation_id,
            OperationPhase.COMMITTING,
            "committing_deployment",
            "Persisting ready deployment state.",
        )
        self._save_deployment(
            spec, operation_id, desired_state="ready", observed_state="ready"
        )
        outcome: dict[str, Any] = {
            "deployment_id": spec.deployment_id,
            "expected_alias": spec.expected_alias,
        }
        if canary_evidence is not None:
            outcome["completion_canary"] = canary_evidence
        return self.journal.finish(
            operation_id,
            state=OperationState.SUCCEEDED,
            code="operation_succeeded",
            message="Deployment is ready with the expected model alias.",
            result=outcome,
        )

    def _refresh_load_scope(
        self, operation_id: str, spec: DeploymentSpec
    ) -> tuple[dict[str, NodeObservation], tuple[str, ...]]:
        """Observe the placement plus every node a replaced deployment uses."""
        self._advance(
            operation_id,
            OperationPhase.REFRESHING,
            "refreshing_nodes",
            "Refreshing selected node facts.",
        )
        observations = self._observe_assignments(spec.assignments)
        self._require_reachable(observations)
        # A deployment cannot keep running on part of its placement, so a
        # switch replaces it on every node it uses, inside or outside the
        # requested placement. Lease and observe those nodes before deciding.
        extra_node_ids = self._replaced_nodes_outside(spec, observations)
        if extra_node_ids:
            self._advance(
                operation_id,
                OperationPhase.REFRESHING,
                "extending_switch_scope",
                "The replaced deployment also uses "
                f"{', '.join(extra_node_ids)}. Acquiring leases on those "
                "nodes and refreshing them.",
            )
            self._acquire(spec, operation_id, extra_node_ids)
            observations.update(self._observe_nodes_outside(extra_node_ids))
            self._require_reachable(observations)
        return observations, extra_node_ids

    def _stop_replaced(
        self, operation_id: str, spec: DeploymentSpec, progress: _Progress
    ) -> None:
        """Drain, then stop every rank of each replaced deployment."""
        replaced_assignments = [
            (record, assignment)
            for record in progress.replaced
            for assignment in DeploymentSpec.from_dict(record.spec).assignments
        ]
        if not replaced_assignments:
            return
        stopped_node_ids = sorted(
            {assignment.node_id for _, assignment in replaced_assignments}
        )
        self._advance(
            operation_id,
            OperationPhase.DRAINING,
            "draining_conflicts",
            "Draining conflicting managed deployments.",
        )
        for record, assignment in replaced_assignments:
            self._require_ok(
                self.transport.drain(assignment.node_id, record.id),
                "Resolve drain failure before stopping the deployment.",
            )
        self._advance(
            operation_id,
            OperationPhase.STOPPING_CONFLICTS,
            "stopping_conflicts",
            "Stopping conflicting managed deployments on "
            f"{', '.join(stopped_node_ids)}.",
        )
        for record, assignment in sorted(
            replaced_assignments, key=lambda pair: _serving_first(pair[1])
        ):
            result = self.transport.stop(
                assignment.node_id,
                record.id,
                timeout_seconds=spec.stop_timeout_seconds,
                force=False,
            )
            self._require_ok(
                result,
                "Verify the named unit and stop it safely before starting "
                "another model.",
            )
            progress.mutated = True
            self._heartbeat(operation_id)

    def _verify_released_before_start(
        self,
        operation_id: str,
        spec: DeploymentSpec,
        replaced: Sequence[DeploymentRecord],
    ) -> None:
        self._advance(
            operation_id,
            OperationPhase.VERIFYING_STOPPED,
            "verifying_memory_release",
            "Verifying processes, endpoints, aliases, and memory are released "
            "before start.",
        )
        self._wait_released(spec, operation_id)
        # Outside the new placement only the replaced deployment must be
        # gone, proven with the same release checks its unload would use.
        for record in replaced:
            replaced_spec = DeploymentSpec.from_dict(record.spec)
            self._wait_released(
                replaced_spec,
                operation_id,
                [
                    node_id
                    for node_id in replaced_spec.node_ids
                    if node_id not in spec.node_ids
                ],
            )

    def _record_handover(
        self,
        operation_id: str,
        spec: DeploymentSpec,
        replaced: Sequence[DeploymentRecord],
    ) -> None:
        # The replaced deployments are now proven stopped; record that before
        # starting, so the journal never shows two ready owners of one node.
        # A rollback restores them to ready.
        for record in replaced:
            if record.id != spec.deployment_id:
                self._save_deployment(
                    DeploymentSpec.from_dict(record.spec),
                    operation_id,
                    desired_state="stopped",
                    observed_state="stopped",
                )
        self._save_deployment(
            spec, operation_id, desired_state="ready", observed_state="starting"
        )

    def _start_placement(
        self, operation_id: str, spec: DeploymentSpec, progress: _Progress
    ) -> None:
        """Start worker roles before the serving head or single role."""
        workers = [
            assignment for assignment in spec.assignments if assignment.is_worker
        ]
        others = [
            assignment for assignment in spec.assignments if not assignment.is_worker
        ]
        progress.mutated = True
        if workers:
            self._advance(
                operation_id,
                OperationPhase.STARTING_WORKERS,
                "starting_workers",
                "Starting worker roles.",
            )
            self._start_assignments(spec, workers, progress.started, operation_id)
        if others:
            self._advance(
                operation_id,
                OperationPhase.STARTING_HEAD,
                "starting_serving_roles",
                "Starting serving head/single roles.",
            )
            self._start_assignments(spec, others, progress.started, operation_id)

    def _verify_ready(
        self,
        operation_id: str,
        spec: DeploymentSpec,
        canary: CanaryRequest | None,
    ) -> dict[str, Any] | None:
        """Prove readiness on every rank, then run the optional canary."""
        self._advance(
            operation_id,
            OperationPhase.LOADING,
            "waiting_for_runtime",
            "Waiting for runtime startup.",
        )
        self._advance(
            operation_id,
            OperationPhase.VERIFYING_READY,
            "verifying_readiness",
            "Verifying identity, health, and expected model alias.",
        )
        for assignment in spec.assignments:
            self._wait_ready(assignment, spec, operation_id)
        if canary is None:
            return None
        return self._run_completion_canary(spec, canary, operation_id)

    def _run_unload(
        self, operation: OperationRecord, deployment_id: str
    ) -> OperationRecord:
        deployment = self.journal.get_deployment(deployment_id)
        if deployment is None:
            return self._finish_error(
                operation.id,
                OperationError(
                    "unknown_deployment",
                    f"Deployment {deployment_id!r} does not exist.",
                    "Refresh deployments and retry.",
                ),
            )
        spec = DeploymentSpec.from_dict(deployment.spec)
        progress = _Progress()
        with self._leases(operation.id, spec, "Acquiring deployment leases."):
            try:
                return self._unload(operation.id, deployment_id, spec, progress)
            except OperationError as error:
                if not progress.mutated:
                    return self._finish_error(operation.id, error)
                self._mark_uncertain(spec, operation.id)
                error = OperationError(
                    error.code,
                    error.message,
                    error.remediation
                    or (
                        "The unload may be partial; inspect all nodes and reconcile "
                        "before another mutation."
                    ),
                )
                return self._finish_error(operation.id, error, needs_attention=True)

    def _unload(
        self,
        operation_id: str,
        deployment_id: str,
        spec: DeploymentSpec,
        progress: _Progress,
    ) -> OperationRecord:
        self._advance(
            operation_id,
            OperationPhase.REFRESHING,
            "refreshing_nodes",
            "Refreshing deployment ownership.",
        )
        observations = self._observe_assignments(spec.assignments)
        self._require_reachable(observations)
        self._require_unload_ownership(deployment_id, observations)
        if all(
            observation.deployment_id is None for observation in observations.values()
        ):
            # Missing ownership is not stop evidence. Even a genuinely absent
            # runtime must pass the same release checks as a completed stop.
            self._wait_released(spec, operation_id)
            self._save_deployment(
                spec,
                operation_id,
                desired_state="stopped",
                observed_state="stopped",
            )
            return self.journal.finish(
                operation_id,
                state=OperationState.SUCCEEDED,
                code="already_stopped",
                message="Deployment was already absent from every selected node.",
                result={"deployment_id": deployment_id, "already_stopped": True},
            )

        owned = [
            assignment
            for assignment in spec.assignments
            if observations[assignment.node_id].deployment_id == deployment_id
        ]
        self._advance(
            operation_id,
            OperationPhase.DRAINING,
            "draining_deployment",
            "Draining deployment before unload.",
        )
        for assignment in owned:
            self._require_ok(
                self.transport.drain(assignment.node_id, deployment_id),
                "Resolve drain failure before retrying unload.",
            )
        self._advance(
            operation_id,
            OperationPhase.STOPPING_CONFLICTS,
            "stopping_deployment",
            "Stopping serving roles before worker roles.",
        )
        for assignment in sorted(owned, key=_serving_first):
            # A transport error during stop is ambiguous: a remote unit can
            # have accepted part of the request before its connection failed.
            # Treat it as a potentially partial unload rather than claiming a
            # clean failure that would invite an unsafe immediate retry.
            progress.mutated = True
            self._require_ok(
                self._stop_escalating(
                    assignment.node_id, deployment_id, spec.stop_timeout_seconds
                ),
                "Inspect the named unit; do not use broad process matching.",
            )
            self._heartbeat(operation_id)
        self._advance(
            operation_id,
            OperationPhase.VERIFYING_STOPPED,
            "verifying_unload",
            "Verifying endpoint closure, alias removal, and memory recovery.",
        )
        self._wait_released(spec, operation_id)
        self._advance(
            operation_id,
            OperationPhase.COMMITTING,
            "committing_unload",
            "Persisting stopped deployment state.",
        )
        self._save_deployment(
            spec, operation_id, desired_state="stopped", observed_state="stopped"
        )
        return self.journal.finish(
            operation_id,
            state=OperationState.SUCCEEDED,
            code="operation_succeeded",
            message="Deployment is stopped and memory release is verified.",
            result={"deployment_id": deployment_id},
        )

    @staticmethod
    def _require_unload_ownership(
        deployment_id: str, observations: Mapping[str, NodeObservation]
    ) -> None:
        """Refuse an unload unless every observed workload is this deployment."""
        for node_id, observation in observations.items():
            if observation.deployment_id not in {None, deployment_id}:
                raise OwnershipConflict(
                    "ownership_mismatch",
                    f"Node {node_id} is now owned by "
                    f"{observation.deployment_id!r}, not {deployment_id!r}.",
                    "Do not stop it automatically; reconcile the deployment and "
                    "inspect the node.",
                )
            if observation.shows_workload and (
                not observation.managed or not observation.deployment_id
            ):
                raise OwnershipConflict(
                    "unmanaged_deployment",
                    f"Node {node_id} reports the deployment as unmanaged.",
                    "Use the documented migration/maintenance procedure; generic "
                    "unload will not kill it.",
                )

    def _run_recover(
        self, operation: OperationRecord, spec: DeploymentSpec
    ) -> OperationRecord:
        """Reset an exactly recognized reviewed placement to its idle baseline."""
        progress = _Progress()
        with self._leases(operation.id, spec, "Acquiring recovery leases."):
            try:
                return self._recover(operation.id, spec, progress)
            except OperationError as error:
                if progress.mutated:
                    self._mark_uncertain(spec, operation.id)
                return self._finish_error(
                    operation.id, error, needs_attention=progress.mutated
                )

    def _recover(
        self, operation_id: str, spec: DeploymentSpec, progress: _Progress
    ) -> OperationRecord:
        self._advance(
            operation_id,
            OperationPhase.REFRESHING,
            "refreshing_nodes",
            "Refreshing recovery targets.",
        )
        self._require_reachable(self._observe_assignments(spec.assignments))
        self._advance(
            operation_id,
            OperationPhase.PREFLIGHTING,
            "recovery_identity_check",
            "Proving reviewed runtime identity on every target.",
        )
        self._advance(
            operation_id,
            OperationPhase.SNAPSHOTTING,
            "recovery_scope_recorded",
            "Recording the reviewed recovery scope.",
        )
        self.journal.record_previous_placement(
            operation_id,
            {
                "recovery_profile_id": spec.profile_id,
                "observed_node_ids": list(spec.node_ids),
            },
        )
        self._advance(
            operation_id,
            OperationPhase.STOPPING_CONFLICTS,
            "recovering_baseline",
            "Stopping only the reviewed deployment recipe.",
        )
        progress.mutated = True
        self._require_ok(
            self.transport.recover(spec),
            "Inspect the reviewed recipe identity; recovery will not stop unknown "
            "processes.",
        )
        self._advance(
            operation_id,
            OperationPhase.VERIFYING_STOPPED,
            "verifying_recovery",
            "Verifying process, endpoint, alias, memory, and swap baseline.",
        )
        self._wait_released(spec, operation_id)
        self._save_deployment(
            spec, operation_id, desired_state="stopped", observed_state="stopped"
        )
        return self.journal.finish(
            operation_id,
            state=OperationState.SUCCEEDED,
            code="recovery_succeeded",
            message="Reviewed placement is stopped and the node baseline is verified.",
            result={"deployment_id": spec.deployment_id, "recovered": True},
        )

    def heartbeat(self, operation_id: str) -> None:
        """Renew the leases of a long maintenance step."""
        self._heartbeat(operation_id)

    def stop_for_maintenance(
        self, operation_id: str, records: Sequence[DeploymentRecord]
    ) -> None:
        """Drain and stop deployments, then prove their release on every node.

        The deployments stay desired ``ready``: a maintenance operation stops
        them only to restore them afterwards, so an interruption leaves the
        drift visible. A stop that cannot be proven marks them ``unknown``.
        """
        pairs = [
            (DeploymentSpec.from_dict(record.spec), assignment)
            for record in records
            for assignment in DeploymentSpec.from_dict(record.spec).assignments
        ]
        if not pairs:
            return
        self._advance(
            operation_id,
            OperationPhase.DRAINING,
            "draining_for_maintenance",
            "Draining the serving deployments before maintenance.",
        )
        for spec, assignment in pairs:
            self._require_ok(
                self.transport.drain(assignment.node_id, spec.deployment_id),
                "Resolve the drain failure before maintenance.",
            )
        self._advance(
            operation_id,
            OperationPhase.STOPPING_CONFLICTS,
            "stopping_for_maintenance",
            "Stopping serving roles before worker roles.",
        )
        try:
            for spec, assignment in sorted(
                pairs, key=lambda pair: _serving_first(pair[1])
            ):
                self._require_ok(
                    self._stop_escalating(
                        assignment.node_id,
                        spec.deployment_id,
                        spec.stop_timeout_seconds,
                    ),
                    "Inspect the named unit; do not use broad process matching.",
                )
                self._heartbeat(operation_id)
            self._advance(
                operation_id,
                OperationPhase.VERIFYING_STOPPED,
                "verifying_maintenance_stop",
                "Verifying endpoint closure, alias removal, and memory recovery.",
            )
            for record in records:
                self._wait_released(DeploymentSpec.from_dict(record.spec), operation_id)
        except OperationError:
            for record in records:
                self._save_deployment(
                    DeploymentSpec.from_dict(record.spec),
                    operation_id,
                    desired_state="ready",
                    observed_state="unknown",
                )
            raise
        for record in records:
            self._save_deployment(
                DeploymentSpec.from_dict(record.spec),
                operation_id,
                desired_state="ready",
                observed_state="stopped",
            )

    def restore_after_maintenance(
        self, operation_id: str, specs: Sequence[DeploymentSpec]
    ) -> list[dict[str, Any]]:
        """Preflight, start and prove the deployments a maintenance stopped.

        Runs each profile's preflight again (files, pinned hashes, images),
        starts workers before heads, waits for identity, health and the exact
        alias, and runs each enabled completion canary. On failure every
        started rank is stopped and its release verified before the error is
        raised; the deployments then stay desired ``ready``.

        Returns:
            Canary evidence per deployment that ran one.

        """
        if not specs:
            return []
        self.note(
            operation_id,
            "restore_preflight_started",
            "Running the restored profiles' preflight checks.",
        )
        failures = [failure for spec in specs for failure in self.preflight(spec)]
        if failures:
            raise PreflightFailed(failures)
        canaries = [(spec, self._canary_request(spec)) for spec in specs]
        started: list[tuple[DeploymentSpec, NodeAssignment]] = []
        try:
            for spec in specs:
                self._save_deployment(
                    spec, operation_id, desired_state="ready", observed_state="starting"
                )
            for workers in (True, False):
                batch = [
                    (spec, assignment)
                    for spec in specs
                    for assignment in spec.assignments
                    if assignment.is_worker is workers
                ]
                if not batch:
                    continue
                self._advance(
                    operation_id,
                    OperationPhase.STARTING_WORKERS
                    if workers
                    else OperationPhase.STARTING_HEAD,
                    "restoring_workers" if workers else "restoring_serving_roles",
                    "Restoring worker roles."
                    if workers
                    else "Restoring serving head/single roles.",
                )
                for spec, assignment in batch:
                    started.append((spec, assignment))
                    self._require_ok(
                        self.transport.start(assignment.node_id, spec, assignment),
                        "Inspect the runtime recipe and node logs; the node "
                        "update may have changed the runtime.",
                    )
                    self._heartbeat(operation_id)
            self._advance(
                operation_id,
                OperationPhase.LOADING,
                "waiting_for_runtime",
                "Waiting for runtime startup.",
            )
            self._advance(
                operation_id,
                OperationPhase.VERIFYING_READY,
                "verifying_readiness",
                "Verifying identity, health, and expected model alias.",
            )
            evidence: list[dict[str, Any]] = []
            for spec, canary in canaries:
                for assignment in spec.assignments:
                    self._wait_ready(assignment, spec, operation_id)
                if canary is not None:
                    result = self._run_completion_canary(spec, canary, operation_id)
                    if result is not None:
                        evidence.append({"deployment_id": spec.deployment_id, **result})
        except OperationError as error:
            raise self._undo_restore(operation_id, specs, started, error) from None
        for spec in specs:
            self._save_deployment(
                spec, operation_id, desired_state="ready", observed_state="ready"
            )
        return evidence

    def _undo_restore(
        self,
        operation_id: str,
        specs: Sequence[DeploymentSpec],
        started: Sequence[tuple[DeploymentSpec, NodeAssignment]],
        error: OperationError,
    ) -> OperationError:
        """Stop every rank a failed restore started; describe what is left."""
        problems: list[str] = []
        if started:
            self._advance(
                operation_id,
                OperationPhase.ROLLING_BACK,
                "restore_cleanup_started",
                "Stopping the ranks the failed restore started.",
            )
        for spec, assignment in reversed(tuple(started)):
            result = self._stop_escalating(
                assignment.node_id, spec.deployment_id, spec.stop_timeout_seconds
            )
            if not result.ok:
                problems.append(
                    f"could not stop {spec.deployment_id} on {assignment.node_id}: "
                    f"{result.message}"
                )
        for spec, assignment in started:
            try:
                self._wait_stopped(assignment.node_id, spec, operation_id)
            except OperationError as wait_error:
                problems.append(wait_error.message)
        for spec in specs:
            self._save_deployment(
                spec,
                operation_id,
                desired_state="ready",
                observed_state="unknown" if problems else "stopped",
            )
        detail = (
            f" Cleanup could not be proven: {'; '.join(problems)}" if problems else ""
        )
        restored = OperationError(
            error.code,
            f"{error.message}{detail}",
            error.remediation,
        )
        if isinstance(error, CompletionCanaryFailed):
            restored = CompletionCanaryFailed(
                f"{error.message}{detail}", error.evidence
            )
        return restored

    def note(
        self,
        operation_id: str,
        code: str,
        message: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        """Journal an event at the operation's current phase."""
        current = self.journal.get_operation(operation_id)
        if current is None:
            raise KeyError(operation_id)
        self.journal.advance(
            operation_id,
            phase=current.phase,
            code=code,
            message=message,
            details=details,
        )

    def _rollback_load(
        self,
        operation_id: str,
        spec: DeploymentSpec,
        started: Sequence[NodeAssignment],
        prior_records: Sequence[DeploymentRecord],
        original_error: OperationError,
    ) -> OperationRecord:
        """Stop partial new work, then prove best-effort prior-placement restoration."""
        rollback_errors: list[str] = []
        try:
            self._advance(
                operation_id,
                OperationPhase.ROLLING_BACK,
                "rollback_started",
                "Cleaning up partial deployment and restoring prior placement.",
            )
            for assignment in reversed(tuple(started)):
                result = self._stop_escalating(
                    assignment.node_id, spec.deployment_id, spec.stop_timeout_seconds
                )
                if not result.ok:
                    rollback_errors.append(
                        f"could not stop partial deployment on {assignment.node_id}: "
                        f"{result.message}"
                    )
            for assignment in started:
                try:
                    self._wait_stopped(assignment.node_id, spec, operation_id)
                except OperationError as error:
                    rollback_errors.append(error.message)
            for record in prior_records:
                prior_spec = DeploymentSpec.from_dict(record.spec)
                self._restore_record(prior_spec, operation_id)
                self._save_deployment(
                    prior_spec,
                    operation_id,
                    desired_state="ready",
                    observed_state="ready",
                )
        except Exception as error:
            # Rollback must report any uncertainty, never hide it.
            rollback_errors.append(str(error))

        self._save_deployment(
            spec, operation_id, desired_state="stopped", observed_state="failed"
        )
        evidence: dict[str, Any] = {}
        if isinstance(original_error, CompletionCanaryFailed):
            evidence["completion_canary"] = original_error.evidence
        if rollback_errors:
            return self._finish_error(
                operation_id,
                OperationError(
                    "rollback_incomplete",
                    f"{original_error.message} Rollback could not be proven: "
                    f"{'; '.join(rollback_errors)}",
                    "Do not start another deployment. Inspect every affected node and "
                    "run reconciliation.",
                ),
                needs_attention=True,
                result=evidence or None,
            )
        return self._finish_error(
            operation_id,
            original_error,
            result={
                "deployment_id": spec.deployment_id,
                "rollback": "restored",
                **evidence,
            },
        )

    def _restore_record(self, spec: DeploymentSpec, operation_id: str) -> None:
        """Restore a prior deployment in dependency order and prove its readiness."""
        for assignment in sorted(spec.assignments, key=_workers_first):
            self._require_ok(
                self.transport.start(assignment.node_id, spec, assignment),
                "Prior placement restoration failed; inspect the node before another "
                "mutation.",
            )
        for assignment in spec.assignments:
            self._wait_ready(assignment, spec, operation_id)

    def _find_conflicts(
        self, spec: DeploymentSpec, observations: Mapping[str, NodeObservation]
    ) -> list[DeploymentRecord]:
        """Return the managed deployments a load replaces, or refuse the switch.

        Deployments observed on the requested nodes are replaced on every node
        they use. A node outside the placement may therefore hold nothing but a
        replaced deployment; anything else there refuses the switch.
        """
        requested = set(spec.node_ids)
        records: dict[str, DeploymentRecord] = {}
        for node_id in spec.node_ids:
            record = self._managed_owner(node_id, observations[node_id])
            if record is not None:
                records[record.id] = record
        for node_id, observation in observations.items():
            if node_id in requested:
                continue
            record = self._managed_owner(node_id, observation)
            if record is not None and record.id not in records:
                raise OwnershipConflict(
                    "partial_cluster_conflict",
                    f"Node {node_id} is outside the requested placement and is "
                    f"owned by deployment {record.id!r}, not by a deployment this "
                    "switch replaces.",
                    "Reconcile the controller journal, then unload that deployment "
                    "before retrying.",
                )
        return list(records.values())

    def _managed_owner(
        self, node_id: str, observation: NodeObservation
    ) -> DeploymentRecord | None:
        """Return the known managed deployment on a node, refusing anything else."""
        active = observation.deployment_id
        evidence_of_process = (
            observation.is_process_running
            or observation.endpoint_healthy
            or bool(observation.active_alias)
        )
        if not active and not evidence_of_process:
            return None
        if not active or not observation.managed:
            raise OwnershipConflict(
                "unmanaged_conflict",
                f"Node {node_id} has a process or endpoint that is not proven "
                "controller-managed.",
                "Do not stop it automatically. Reconcile or use the documented "
                "migration procedure.",
            )
        record = self.journal.get_deployment(active)
        if record is None:
            raise OwnershipConflict(
                "unknown_managed_conflict",
                f"Node {node_id} reports unknown managed deployment {active!r}.",
                "Reconcile the controller journal before attempting a switch.",
            )
        return record

    def _replaced_nodes_outside(
        self, spec: DeploymentSpec, observations: Mapping[str, NodeObservation]
    ) -> tuple[str, ...]:
        """Nodes outside the placement used by a deployment the load may replace."""
        requested = set(spec.node_ids)
        extra: set[str] = set()
        for observation in observations.values():
            if not (observation.managed and observation.deployment_id):
                continue
            record = self.journal.get_deployment(observation.deployment_id)
            if record is not None:
                extra.update(set(record.node_ids) - requested)
        return tuple(sorted(extra))

    def _observe_nodes_outside(
        self, node_ids: Sequence[str]
    ) -> dict[str, NodeObservation]:
        observations: dict[str, NodeObservation] = {}
        for node_id in node_ids:
            try:
                observations[node_id] = self.transport.observe(node_id)
            except KeyError as error:
                raise OwnershipConflict(
                    "partial_cluster_conflict",
                    f"The replaced deployment also uses node {node_id}, which is "
                    "not configured, so it cannot be stopped on every node.",
                    "Restore the node configuration, or recover the deployment "
                    "deliberately, before switching.",
                ) from error
        return observations

    def preflight(self, spec: DeploymentSpec) -> list[Mapping[str, str]]:
        """Every failed read-only preflight check of a placement."""
        failures: list[Mapping[str, str]] = []
        for assignment in spec.assignments:
            for check in self.transport.preflight(assignment.node_id, spec, assignment):
                if not check.passed:
                    failures.append(
                        {
                            "node_id": assignment.node_id,
                            "name": check.name,
                            "message": check.message,
                            "remediation": check.remediation,
                        }
                    )
        return failures

    def _start_assignments(
        self,
        spec: DeploymentSpec,
        assignments: Sequence[NodeAssignment],
        started: list[NodeAssignment],
        operation_id: str,
    ) -> None:
        for assignment in assignments:
            # Record mutation intent before entering a transport call. A remote
            # start can partially succeed and then lose its SSH response.
            started.append(assignment)
            self._require_ok(
                self.transport.start(assignment.node_id, spec, assignment),
                "Inspect the allowlisted runtime recipe and node logs before retrying.",
            )
            self._heartbeat(operation_id)

    def _wait_ready(
        self, assignment: NodeAssignment, spec: DeploymentSpec, operation_id: str
    ) -> None:
        def ready(observation: NodeObservation) -> bool:
            if (
                observation.reachable
                and observation.unit_state is UnitState.STOPPED
                and observation.facts.get("exited_deployment_id") == spec.deployment_id
            ):
                raise VerificationFailed(
                    "cluster_container_exited",
                    "The reviewed cluster container exited on node "
                    f"{assignment.node_id} after its launcher completed.",
                    "Inspect the bounded rank logs; targeted rollback has been "
                    "attempted.",
                )
            return observation.ready_for_assignment(assignment, spec)

        self._wait_for(
            assignment.node_id,
            ready,
            spec.startup_timeout_seconds,
            "readiness_timeout",
            f"Node {assignment.node_id} did not prove readiness for alias "
            f"{spec.expected_alias!r}.",
            "Check the named unit, endpoint health, and /v1/models alias; rollback has "
            "been attempted.",
            operation_id,
        )

    @staticmethod
    def _canary_request(spec: DeploymentSpec) -> CanaryRequest | None:
        try:
            return canary_request_for(spec)
        except ValueError as error:
            raise PreflightFailed(
                [
                    {
                        "node_id": spec.node_ids[0],
                        "name": "completion_canary",
                        "message": str(error),
                        "remediation": "Correct the profile's completion canary "
                        "settings before loading it.",
                    }
                ]
            ) from error

    def _run_completion_canary(
        self, spec: DeploymentSpec, request: CanaryRequest, operation_id: str
    ) -> dict[str, Any] | None:
        """Send one bounded completion to the serving role and require success.

        Only text-free evidence reaches the journal: pass/fail, HTTP status,
        latency, completion tokens, and finish reason.
        """
        serving = next(
            (item for item in spec.assignments if item.role == "head"),
            next(
                (item for item in spec.assignments if not item.is_worker),
                None,
            ),
        )
        if serving is None:
            return None
        self._advance(
            operation_id,
            OperationPhase.VERIFYING_READY,
            "completion_canary_started",
            f"Sending one bounded completion request on {serving.node_id}.",
        )
        result = self.transport.completion_canary(serving.node_id, spec, request)
        evidence = {"node_id": serving.node_id, **result.evidence()}
        tokens = result.completion_tokens
        latency = result.latency_ms
        summary = (
            f"HTTP {result.http_status or 'none'}, "
            f"{'no' if tokens is None else tokens} completion tokens, "
            f"finish reason {result.finish_reason or 'none'}, "
            f"{'unmeasured' if latency is None else f'{latency} ms'}"
        )
        if not result.passed:
            detail = (result.message or "no detail").strip().rstrip(".")[:300]
            message = (
                f"Completion canary failed on {serving.node_id} ({summary}): {detail}."
            )
            self.journal.advance(
                operation_id,
                phase=OperationPhase.VERIFYING_READY,
                code="completion_canary_failed",
                message=message,
                details=evidence,
            )
            raise CompletionCanaryFailed(message, evidence)
        self.journal.advance(
            operation_id,
            phase=OperationPhase.VERIFYING_READY,
            code="completion_canary_passed",
            message=f"Completion canary passed on {serving.node_id} ({summary}).",
            details=evidence,
        )
        return evidence

    def _wait_released(
        self,
        spec: DeploymentSpec,
        operation_id: str,
        node_ids: Iterable[str] | None = None,
    ) -> None:
        """Prove release of ``spec`` on its nodes, or only on ``node_ids``."""
        for node_id in spec.node_ids if node_ids is None else node_ids:
            self._wait_stopped(node_id, spec, operation_id)

    def _wait_stopped(
        self, node_id: str, spec: DeploymentSpec, operation_id: str
    ) -> None:
        minimum_free_memory_bytes = spec.stopped_free_memory_bytes

        def stopped(observation: NodeObservation) -> bool:
            return observation.stopped_for(
                spec.deployment_id, minimum_free_memory_bytes
            )

        self._wait_for(
            node_id,
            stopped,
            spec.stop_timeout_seconds,
            "memory_release_unverified",
            f"Node {node_id} did not prove process/endpoint/alias removal and memory "
            "recovery.",
            "Do not start another model. Inspect the named unit and memory state, then "
            "reconcile.",
            operation_id,
        )

    def _wait_for(
        self,
        node_id: str,
        predicate: Callable[[NodeObservation], bool],
        timeout_seconds: float,
        code: str,
        message: str,
        remediation: str,
        operation_id: str,
    ) -> NodeObservation:
        deadline = self._monotonic() + timeout_seconds
        while True:
            observation = self.transport.observe(node_id)
            if predicate(observation):
                return observation
            if observation.unit_state is UnitState.FAILED and observation.facts.get(
                "launcher_failed"
            ):
                raise VerificationFailed(
                    "cluster_launcher_failed",
                    f"The reviewed cluster launcher failed on node {node_id}.",
                    "Inspect the bounded rank logs; targeted rollback has been "
                    "attempted.",
                )
            if self._monotonic() >= deadline:
                raise VerificationFailed(code, message, remediation)
            self._heartbeat(operation_id)
            self._sleep(self.poll_interval_seconds)

    def _observe_assignments(
        self, assignments: Sequence[NodeAssignment]
    ) -> dict[str, NodeObservation]:
        return {
            assignment.node_id: self.transport.observe(assignment.node_id)
            for assignment in assignments
        }

    @staticmethod
    def _require_reachable(observations: Mapping[str, NodeObservation]) -> None:
        unreachable = [
            node_id
            for node_id, observation in observations.items()
            if not observation.reachable
        ]
        if unreachable:
            raise OperationError(
                "node_unreachable",
                f"Nodes are unreachable: {', '.join(unreachable)}.",
                "Restore transport connectivity and refresh observations before "
                "retrying.",
            )

    @staticmethod
    def _placement_is_ready(
        spec: DeploymentSpec, observations: Mapping[str, NodeObservation]
    ) -> bool:
        return all(
            observations[assignment.node_id].ready_for_assignment(assignment, spec)
            for assignment in spec.assignments
        )

    @contextmanager
    def _leases(
        self, operation_id: str, spec: DeploymentSpec, message: str
    ) -> Iterator[None]:
        """Hold this operation's leases, renewed in the background, until it ends.

        The body's own failure handling, including a rollback, still runs while
        the leases are held.
        """
        with self._held_leases(
            operation_id, message, lambda: self._acquire(spec, operation_id)
        ):
            yield

    @contextmanager
    def maintenance_leases(
        self, operation_id: str, node_ids: Sequence[str], message: str
    ) -> Iterator[None]:
        """Hold the cluster lease and every given node's lease for maintenance.

        A cluster update holds them from its first check until the deployments
        it stopped are restored, so no load, unload or recovery can interleave.
        """
        scope = _LeaseScope(
            node_ids=tuple(sorted(set(node_ids))), cluster=True, cluster_id="default"
        )

        def acquire() -> None:
            self._renew(operation_id, scope)
            with self._lease_mutex:
                self._lease_scopes[operation_id] = scope

        with self._held_leases(operation_id, message, acquire):
            yield

    @contextmanager
    def _held_leases(
        self, operation_id: str, message: str, acquire: Callable[[], None]
    ) -> Iterator[None]:
        held = False
        keeper: tuple[threading.Event, threading.Thread] | None = None
        try:
            self._advance(
                operation_id, OperationPhase.LOCKING, "acquiring_locks", message
            )
            acquire()
            held = True
            keeper = self._start_lease_keeper(operation_id)
            yield
        finally:
            self._stop_lease_keeper(keeper)
            self._release_leases(operation_id, held=held)

    def _acquire(
        self,
        spec: DeploymentSpec,
        operation_id: str,
        extra_node_ids: Iterable[str] = (),
    ) -> None:
        """Acquire, or extend, this operation's leases all-or-nothing.

        Extending keeps every lease already held. A live lease of another
        operation on any requested resource raises ``LockConflict`` before the
        journal records a new one, so a refused extension mutates nothing.
        """
        with self._lease_mutex:
            held = self._lease_scopes.get(operation_id)
        node_ids = set(spec.node_ids).union(extra_node_ids)
        if held is not None:
            node_ids.update(held.node_ids)
        scope = _LeaseScope(
            node_ids=tuple(sorted(node_ids)),
            cluster=bool(spec.profile.get("cluster_lock", True)),
            cluster_id=str(spec.profile.get("cluster_id", "default")),
        )
        self._renew(operation_id, scope)
        with self._lease_mutex:
            self._lease_scopes[operation_id] = scope

    def _renew(self, operation_id: str, scope: _LeaseScope) -> None:
        self.journal.acquire_locks(
            operation_id,
            scope.node_ids,
            lease_seconds=self.lock_lease_seconds,
            cluster=scope.cluster,
            cluster_id=scope.cluster_id,
        )

    def _heartbeat(self, operation_id: str) -> None:
        with self._lease_mutex:
            scope = self._lease_scopes.get(operation_id)
        if scope is not None:
            self._renew(operation_id, scope)

    def _release_leases(self, operation_id: str, *, held: bool) -> None:
        with self._lease_mutex:
            self._lease_scopes.pop(operation_id, None)
        if held:
            self.journal.release_locks(operation_id)

    def _start_lease_keeper(
        self, operation_id: str
    ) -> tuple[threading.Event, threading.Thread]:
        stop = threading.Event()
        interval = max(0.1, min(30.0, self.lock_lease_seconds / 3.0))

        def keep_alive() -> None:
            while not stop.wait(interval):
                try:
                    self._heartbeat(operation_id)
                except Exception:
                    # The foreground operation remains authoritative and will
                    # surface journal/transport failure at its next checkpoint.
                    return

        thread = threading.Thread(
            target=keep_alive, name=f"lease-{operation_id[:8]}", daemon=True
        )
        thread.start()
        return stop, thread

    @staticmethod
    def _stop_lease_keeper(
        keeper: tuple[threading.Event, threading.Thread] | None,
    ) -> None:
        if keeper is None:
            return
        stop, thread = keeper
        stop.set()
        thread.join(timeout=1.0)

    def _advance(
        self, operation_id: str, phase: OperationPhase, code: str, message: str
    ) -> OperationRecord:
        return self.journal.advance(
            operation_id, phase=phase, code=code, message=message
        )

    def _save_deployment(
        self,
        spec: DeploymentSpec,
        operation_id: str,
        *,
        desired_state: str,
        observed_state: str,
    ) -> DeploymentRecord:
        existing = self.journal.get_deployment(spec.deployment_id)
        now = utc_now()
        return self.journal.save_deployment(
            DeploymentRecord(
                id=spec.deployment_id,
                profile_id=spec.profile_id,
                expected_alias=spec.expected_alias,
                node_ids=spec.node_ids,
                desired_state=desired_state,
                observed_state=observed_state,
                spec=spec.to_dict(),
                endpoint=spec.endpoint,
                owning_operation_id=operation_id,
                created_at=existing.created_at if existing else now,
                updated_at=now,
            )
        )

    def _finish_error(
        self,
        operation_id: str,
        error: OperationError,
        *,
        needs_attention: bool = False,
        result: Mapping[str, Any] | None = None,
    ) -> OperationRecord:
        state = (
            OperationState.NEEDS_ATTENTION if needs_attention else OperationState.FAILED
        )
        current = self.journal.get_operation(operation_id)
        if current is None:
            raise KeyError(operation_id)
        if current.state.terminal:
            return current
        return self.journal.finish(
            operation_id,
            state=state,
            code="operation_needs_attention" if needs_attention else "operation_failed",
            message=error.message,
            result=result,
            error_code=error.code,
            remediation=error.remediation,
        )

    def _mark_uncertain(self, spec: DeploymentSpec, operation_id: str) -> None:
        """Record that a failed stop may have left the deployment partly running."""
        self._save_deployment(
            spec, operation_id, desired_state="stopped", observed_state="unknown"
        )

    def _stop_escalating(
        self, node_id: str, deployment_id: str, timeout_seconds: float
    ) -> CommandResult:
        result = self.transport.stop(
            node_id, deployment_id, timeout_seconds=timeout_seconds, force=False
        )
        if not result.ok:
            # ``stop`` has already observed its bounded graceful timeout;
            # escalation is narrow and still bound to the named deployment.
            result = self.transport.stop(
                node_id, deployment_id, timeout_seconds=timeout_seconds, force=True
            )
        return result

    @staticmethod
    def _require_ok(result: CommandResult, remediation: str) -> None:
        if not result.ok:
            raise OperationError(
                result.code or "transport_failed",
                result.message or "Transport command failed.",
                remediation,
            )


def _workers_first(assignment: NodeAssignment) -> tuple[bool, str]:
    return (not assignment.is_worker, assignment.node_id)


def _serving_first(assignment: NodeAssignment) -> tuple[bool, str]:
    return (assignment.is_worker, assignment.node_id)
