# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Transactional load/unload engine built on the durable operation journal.

The orchestrator has no SSH, shell, or HTTP-server knowledge.  It coordinates a
``NodeTransport`` through an explicit safety sequence: preflight all nodes, stop
and prove release of conflicting deployments, start in role order, then prove
process identity, health, and model alias.  Failures after mutation clean up the
partial new deployment and best-effort restore the recorded prior placement.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
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
from controller.transport.base import NodeObservation, NodeTransport, UnitState


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


@dataclass(frozen=True)
class _Conflict:
    record: DeploymentRecord
    assignments: tuple[NodeAssignment, ...]


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
        except (
            Exception
        ) as error:  # pragma: no cover - defensive last-resort journal preservation
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
        started: list[NodeAssignment] = []
        mutation_started = False
        prior_records: list[DeploymentRecord] = []
        leases_acquired = False
        lease_keeper: tuple[threading.Event, threading.Thread] | None = None
        try:
            self._advance(
                operation.id,
                OperationPhase.LOCKING,
                "acquiring_locks",
                "Acquiring placement leases.",
            )
            self._acquire(spec, operation.id)
            leases_acquired = True
            lease_keeper = self._start_lease_keeper(spec, operation.id)
            self._advance(
                operation.id,
                OperationPhase.REFRESHING,
                "refreshing_nodes",
                "Refreshing selected node facts.",
            )
            observations = self._observe_assignments(spec.assignments)
            self._require_reachable(observations)

            self._advance(
                operation.id,
                OperationPhase.PREFLIGHTING,
                "preflight_started",
                "Running all read-only preflight checks.",
            )
            failures = self._preflight(spec)
            if failures:
                raise PreflightFailed(failures)

            if self._placement_is_ready(spec, observations):
                self._advance(
                    operation.id,
                    OperationPhase.COMMITTING,
                    "placement_already_ready",
                    "Requested deployment is already ready.",
                )
                self._save_deployment(
                    spec, operation.id, desired_state="ready", observed_state="ready"
                )
                return self.journal.finish(
                    operation.id,
                    state=OperationState.SUCCEEDED,
                    code="operation_succeeded",
                    message="Load request is already satisfied by observed state.",
                    result={"deployment_id": spec.deployment_id, "already_ready": True},
                )

            prior_records = self._find_conflicts(spec, observations)
            self._advance(
                operation.id,
                OperationPhase.SNAPSHOTTING,
                "snapshotting_prior_placement",
                "Recording prior placement for rollback.",
            )
            self.journal.record_previous_placement(
                operation.id,
                {
                    "deployments": [
                        self._record_to_dict(record) for record in prior_records
                    ],
                    "observed_node_ids": [
                        assignment.node_id for assignment in spec.assignments
                    ],
                },
            )

            conflicting_assignments = self._conflicting_assignments(
                prior_records, spec.node_ids
            )
            if conflicting_assignments:
                self._advance(
                    operation.id,
                    OperationPhase.DRAINING,
                    "draining_conflicts",
                    "Draining conflicting managed deployments.",
                )
                for record, assignment in conflicting_assignments:
                    result = self.transport.drain(assignment.node_id, record.id)
                    self._require_result(
                        result.ok,
                        result.code,
                        result.message,
                        "Resolve drain failure before stopping the deployment.",
                    )
                self._advance(
                    operation.id,
                    OperationPhase.STOPPING_CONFLICTS,
                    "stopping_conflicts",
                    "Stopping conflicting managed deployments.",
                )
                for record, assignment in self._stop_order(conflicting_assignments):
                    result = self.transport.stop(
                        assignment.node_id,
                        record.id,
                        timeout_seconds=spec.stop_timeout_seconds,
                        force=False,
                    )
                    self._require_result(
                        result.ok,
                        result.code,
                        result.message,
                        "Verify the named unit and stop it safely before starting "
                        "another model.",
                    )
                    mutation_started = True
                    self._heartbeat(spec, operation.id)

            self._advance(
                operation.id,
                OperationPhase.VERIFYING_STOPPED,
                "verifying_memory_release",
                "Verifying processes, endpoints, aliases, and memory are released "
                "before start.",
            )
            post_stop_minimum = max(
                spec.minimum_free_memory_bytes, spec.post_stop_free_memory_bytes
            )
            for assignment in spec.assignments:
                self._wait_stopped(
                    assignment.node_id,
                    post_stop_minimum,
                    spec.stop_timeout_seconds,
                    operation.id,
                    spec,
                )

            self._save_deployment(
                spec, operation.id, desired_state="ready", observed_state="starting"
            )

            workers = [
                assignment
                for assignment in spec.assignments
                if self._is_worker(assignment)
            ]
            others = [
                assignment
                for assignment in spec.assignments
                if not self._is_worker(assignment)
            ]
            mutation_started = bool(workers or others)
            if workers:
                self._advance(
                    operation.id,
                    OperationPhase.STARTING_WORKERS,
                    "starting_workers",
                    "Starting worker roles.",
                )
                self._start_assignments(spec, workers, started, operation.id)
            if others:
                self._advance(
                    operation.id,
                    OperationPhase.STARTING_HEAD,
                    "starting_serving_roles",
                    "Starting serving head/single roles.",
                )
                self._start_assignments(spec, others, started, operation.id)
            self._advance(
                operation.id,
                OperationPhase.LOADING,
                "waiting_for_runtime",
                "Waiting for runtime startup.",
            )
            self._advance(
                operation.id,
                OperationPhase.VERIFYING_READY,
                "verifying_readiness",
                "Verifying identity, health, and expected model alias.",
            )
            for assignment in spec.assignments:
                self._wait_ready(assignment, spec, operation.id)

            self._advance(
                operation.id,
                OperationPhase.COMMITTING,
                "committing_deployment",
                "Persisting ready deployment state.",
            )
            self._save_deployment(
                spec, operation.id, desired_state="ready", observed_state="ready"
            )
            return self.journal.finish(
                operation.id,
                state=OperationState.SUCCEEDED,
                code="operation_succeeded",
                message="Deployment is ready with the expected model alias.",
                result={
                    "deployment_id": spec.deployment_id,
                    "expected_alias": spec.expected_alias,
                },
            )
        except OperationError as error:
            if mutation_started:
                return self._rollback_load(
                    operation.id, spec, started, prior_records, error
                )
            return self._finish_error(operation.id, error)
        finally:
            self._stop_lease_keeper(lease_keeper)
            if leases_acquired:
                self.journal.release_locks(operation.id)

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
        leases_acquired = False
        mutation_started = False
        lease_keeper: tuple[threading.Event, threading.Thread] | None = None
        try:
            self._advance(
                operation.id,
                OperationPhase.LOCKING,
                "acquiring_locks",
                "Acquiring deployment leases.",
            )
            self._acquire(spec, operation.id)
            leases_acquired = True
            lease_keeper = self._start_lease_keeper(spec, operation.id)
            self._advance(
                operation.id,
                OperationPhase.REFRESHING,
                "refreshing_nodes",
                "Refreshing deployment ownership.",
            )
            observations = self._observe_assignments(spec.assignments)
            self._require_reachable(observations)
            for node_id, observation in observations.items():
                if observation.deployment_id not in {None, deployment_id}:
                    raise OwnershipConflict(
                        "ownership_mismatch",
                        f"Node {node_id} is now owned by "
                        f"{observation.deployment_id!r}, not {deployment_id!r}.",
                        "Do not stop it automatically; reconcile the deployment and "
                        "inspect the node.",
                    )
                evidence_of_workload = (
                    observation.deployment_id is not None
                    or observation.is_process_running
                    or observation.endpoint_healthy
                    or observation.active_alias
                    or observation.served_aliases
                )
                if evidence_of_workload and (
                    not observation.managed or not observation.deployment_id
                ):
                    raise OwnershipConflict(
                        "unmanaged_deployment",
                        f"Node {node_id} reports the deployment as unmanaged.",
                        "Use the documented migration/maintenance procedure; generic "
                        "unload will not kill it.",
                    )
            if all(
                observation.deployment_id is None
                for observation in observations.values()
            ):
                # Missing ownership is not stop evidence. Even a genuinely absent
                # runtime must pass the same release checks as a completed stop.
                minimum = max(
                    spec.minimum_free_memory_bytes, spec.post_stop_free_memory_bytes
                )
                for assignment in spec.assignments:
                    self._wait_stopped(
                        assignment.node_id,
                        minimum,
                        spec.stop_timeout_seconds,
                        operation.id,
                        spec,
                    )
                self._save_deployment(
                    spec,
                    operation.id,
                    desired_state="stopped",
                    observed_state="stopped",
                )
                return self.journal.finish(
                    operation.id,
                    state=OperationState.SUCCEEDED,
                    code="already_stopped",
                    message="Deployment was already absent from every selected node.",
                    result={"deployment_id": deployment_id, "already_stopped": True},
                )

            self._advance(
                operation.id,
                OperationPhase.DRAINING,
                "draining_deployment",
                "Draining deployment before unload.",
            )
            for assignment in spec.assignments:
                if observations[assignment.node_id].deployment_id == deployment_id:
                    result = self.transport.drain(assignment.node_id, deployment_id)
                    self._require_result(
                        result.ok,
                        result.code,
                        result.message,
                        "Resolve drain failure before retrying unload.",
                    )
            self._advance(
                operation.id,
                OperationPhase.STOPPING_CONFLICTS,
                "stopping_deployment",
                "Stopping serving roles before worker roles.",
            )
            for assignment in self._unload_order(spec.assignments):
                if observations[assignment.node_id].deployment_id != deployment_id:
                    continue
                # A transport error during stop is ambiguous: a remote unit can
                # have accepted part of the request before its connection failed.
                # Treat it as a potentially partial unload rather than claiming a
                # clean failure that would invite an unsafe immediate retry.
                mutation_started = True
                result = self.transport.stop(
                    assignment.node_id,
                    deployment_id,
                    timeout_seconds=spec.stop_timeout_seconds,
                    force=False,
                )
                if not result.ok:
                    # ``stop`` has already observed its bounded graceful timeout;
                    # escalation is narrow and still bound to the named deployment.
                    result = self.transport.stop(
                        assignment.node_id,
                        deployment_id,
                        timeout_seconds=spec.stop_timeout_seconds,
                        force=True,
                    )
                self._require_result(
                    result.ok,
                    result.code,
                    result.message,
                    "Inspect the named unit; do not use broad process matching.",
                )
                self._heartbeat(spec, operation.id)
            self._advance(
                operation.id,
                OperationPhase.VERIFYING_STOPPED,
                "verifying_unload",
                "Verifying endpoint closure, alias removal, and memory recovery.",
            )
            post_stop_minimum = max(
                spec.minimum_free_memory_bytes, spec.post_stop_free_memory_bytes
            )
            for assignment in spec.assignments:
                self._wait_stopped(
                    assignment.node_id,
                    post_stop_minimum,
                    spec.stop_timeout_seconds,
                    operation.id,
                    spec,
                )
            self._advance(
                operation.id,
                OperationPhase.COMMITTING,
                "committing_unload",
                "Persisting stopped deployment state.",
            )
            self._save_deployment(
                spec, operation.id, desired_state="stopped", observed_state="stopped"
            )
            return self.journal.finish(
                operation.id,
                state=OperationState.SUCCEEDED,
                code="operation_succeeded",
                message="Deployment is stopped and memory release is verified.",
                result={"deployment_id": deployment_id},
            )
        except OperationError as error:
            if mutation_started:
                self._save_deployment(
                    spec,
                    operation.id,
                    desired_state="stopped",
                    observed_state="unknown",
                )
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
            return self._finish_error(operation.id, error)
        finally:
            self._stop_lease_keeper(lease_keeper)
            if leases_acquired:
                self.journal.release_locks(operation.id)

    def _run_recover(
        self, operation: OperationRecord, spec: DeploymentSpec
    ) -> OperationRecord:
        """Reset an exactly recognized reviewed placement to its idle baseline."""
        leases_acquired = False
        mutation_started = False
        lease_keeper: tuple[threading.Event, threading.Thread] | None = None
        try:
            self._advance(
                operation.id,
                OperationPhase.LOCKING,
                "acquiring_locks",
                "Acquiring recovery leases.",
            )
            self._acquire(spec, operation.id)
            leases_acquired = True
            lease_keeper = self._start_lease_keeper(spec, operation.id)
            self._advance(
                operation.id,
                OperationPhase.REFRESHING,
                "refreshing_nodes",
                "Refreshing recovery targets.",
            )
            self._require_reachable(self._observe_assignments(spec.assignments))
            self._advance(
                operation.id,
                OperationPhase.PREFLIGHTING,
                "recovery_identity_check",
                "Proving reviewed runtime identity on every target.",
            )
            self._advance(
                operation.id,
                OperationPhase.SNAPSHOTTING,
                "recovery_scope_recorded",
                "Recording the reviewed recovery scope.",
            )
            self.journal.record_previous_placement(
                operation.id,
                {
                    "recovery_profile_id": spec.profile_id,
                    "observed_node_ids": list(spec.node_ids),
                },
            )
            self._advance(
                operation.id,
                OperationPhase.STOPPING_CONFLICTS,
                "recovering_baseline",
                "Stopping only the reviewed deployment recipe.",
            )
            mutation_started = True
            result = self.transport.recover(spec)
            self._require_result(
                result.ok,
                result.code,
                result.message,
                "Inspect the reviewed recipe identity; recovery will not stop unknown "
                "processes.",
            )
            self._advance(
                operation.id,
                OperationPhase.VERIFYING_STOPPED,
                "verifying_recovery",
                "Verifying process, endpoint, alias, memory, and swap baseline.",
            )
            minimum = max(
                spec.minimum_free_memory_bytes, spec.post_stop_free_memory_bytes
            )
            for assignment in spec.assignments:
                self._wait_stopped(
                    assignment.node_id,
                    minimum,
                    spec.stop_timeout_seconds,
                    operation.id,
                    spec,
                )
            self._save_deployment(
                spec, operation.id, desired_state="stopped", observed_state="stopped"
            )
            return self.journal.finish(
                operation.id,
                state=OperationState.SUCCEEDED,
                code="recovery_succeeded",
                message=(
                    "Reviewed placement is stopped and the node baseline is verified."
                ),
                result={"deployment_id": spec.deployment_id, "recovered": True},
            )
        except OperationError as error:
            if mutation_started:
                self._save_deployment(
                    spec,
                    operation.id,
                    desired_state="stopped",
                    observed_state="unknown",
                )
            return self._finish_error(
                operation.id, error, needs_attention=mutation_started
            )
        finally:
            self._stop_lease_keeper(lease_keeper)
            if leases_acquired:
                self.journal.release_locks(operation.id)

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
                result = self.transport.stop(
                    assignment.node_id,
                    spec.deployment_id,
                    timeout_seconds=spec.stop_timeout_seconds,
                    force=False,
                )
                if not result.ok:
                    result = self.transport.stop(
                        assignment.node_id,
                        spec.deployment_id,
                        timeout_seconds=spec.stop_timeout_seconds,
                        force=True,
                    )
                if not result.ok:
                    rollback_errors.append(
                        f"could not stop partial deployment on {assignment.node_id}: "
                        f"{result.message}"
                    )
            post_stop_minimum = max(
                spec.minimum_free_memory_bytes, spec.post_stop_free_memory_bytes
            )
            for assignment in started:
                try:
                    self._wait_stopped(
                        assignment.node_id,
                        post_stop_minimum,
                        spec.stop_timeout_seconds,
                        operation_id,
                        spec,
                    )
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
        except (
            Exception
        ) as error:  # rollback must report any uncertainty, never hide it
            rollback_errors.append(str(error))

        self._save_deployment(
            spec, operation_id, desired_state="stopped", observed_state="failed"
        )
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
            )
        return self._finish_error(
            operation_id,
            original_error,
            result={"deployment_id": spec.deployment_id, "rollback": "restored"},
        )

    def _restore_record(self, spec: DeploymentSpec, operation_id: str) -> None:
        """Restore a prior deployment in dependency order and prove its readiness."""
        for assignment in self._start_order(spec.assignments):
            result = self.transport.start(assignment.node_id, spec, assignment)
            self._require_result(
                result.ok,
                result.code,
                result.message,
                "Prior placement restoration failed; inspect the node before another "
                "mutation.",
            )
        for assignment in spec.assignments:
            self._wait_ready(assignment, spec, operation_id)

    def _find_conflicts(
        self, spec: DeploymentSpec, observations: Mapping[str, NodeObservation]
    ) -> list[DeploymentRecord]:
        records: dict[str, DeploymentRecord] = {}
        for node_id, observation in observations.items():
            active = observation.deployment_id
            evidence_of_process = (
                observation.is_process_running
                or observation.endpoint_healthy
                or bool(observation.active_alias)
            )
            if not active and not evidence_of_process:
                continue
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
            if not set(record.node_ids).issubset(set(spec.node_ids)):
                raise OwnershipConflict(
                    "partial_cluster_conflict",
                    f"Deployment {active!r} spans nodes outside this requested "
                    "placement.",
                    "Unload or switch the full cluster deployment transactionally.",
                )
            records[record.id] = record
        return list(records.values())

    def _conflicting_assignments(
        self, records: Sequence[DeploymentRecord], node_ids: Iterable[str]
    ) -> list[tuple[DeploymentRecord, NodeAssignment]]:
        target_nodes = set(node_ids)
        result: list[tuple[DeploymentRecord, NodeAssignment]] = []
        for record in records:
            spec = DeploymentSpec.from_dict(record.spec)
            result.extend(
                (record, assignment)
                for assignment in spec.assignments
                if assignment.node_id in target_nodes
            )
        return result

    def _preflight(self, spec: DeploymentSpec) -> list[Mapping[str, str]]:
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
            result = self.transport.start(assignment.node_id, spec, assignment)
            self._require_result(
                result.ok,
                result.code,
                result.message,
                "Inspect the allowlisted runtime recipe and node logs before retrying.",
            )
            self._heartbeat(spec, operation_id)

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
            if self._is_worker(assignment):
                return (
                    observation.reachable
                    and observation.managed
                    and observation.deployment_id == spec.deployment_id
                    and observation.unit_state == UnitState.RUNNING
                )
            return observation.ready_for(spec.deployment_id, spec.expected_alias)

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
            spec,
        )

    def _wait_stopped(
        self,
        node_id: str,
        minimum_free_memory_bytes: int,
        timeout_seconds: float,
        operation_id: str,
        spec: DeploymentSpec,
    ) -> None:
        def stopped(observation: NodeObservation) -> bool:
            return observation.stopped_for(
                spec.deployment_id, minimum_free_memory_bytes
            )

        self._wait_for(
            node_id,
            stopped,
            timeout_seconds,
            "memory_release_unverified",
            f"Node {node_id} did not prove process/endpoint/alias removal and memory "
            "recovery.",
            "Do not start another model. Inspect the named unit and memory state, then "
            "reconcile.",
            operation_id,
            spec,
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
        spec: DeploymentSpec,
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
            self._heartbeat(spec, operation_id)
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
                f"Selected nodes are unreachable: {', '.join(unreachable)}.",
                "Restore transport connectivity and refresh observations before "
                "retrying.",
            )

    @staticmethod
    def _placement_is_ready(
        spec: DeploymentSpec, observations: Mapping[str, NodeObservation]
    ) -> bool:
        for assignment in spec.assignments:
            observation = observations[assignment.node_id]
            if assignment.role in {"worker", "rpc-worker"}:
                if not (
                    observation.reachable
                    and observation.managed
                    and observation.deployment_id == spec.deployment_id
                    and observation.unit_state == UnitState.RUNNING
                ):
                    return False
            elif not observation.ready_for(spec.deployment_id, spec.expected_alias):
                return False
        return True

    def _acquire(self, spec: DeploymentSpec, operation_id: str) -> None:
        cluster_lock = bool(spec.profile.get("cluster_lock", True))
        cluster_id = str(spec.profile.get("cluster_id", "default"))
        self.journal.acquire_locks(
            operation_id,
            spec.node_ids,
            lease_seconds=self.lock_lease_seconds,
            cluster=cluster_lock,
            cluster_id=cluster_id,
        )

    def _heartbeat(self, spec: DeploymentSpec, operation_id: str) -> None:
        self._acquire(spec, operation_id)

    def _start_lease_keeper(
        self, spec: DeploymentSpec, operation_id: str
    ) -> tuple[threading.Event, threading.Thread]:
        stop = threading.Event()
        interval = max(0.1, min(30.0, self.lock_lease_seconds / 3.0))

        def keep_alive() -> None:
            while not stop.wait(interval):
                try:
                    self._heartbeat(spec, operation_id)
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

    @staticmethod
    def _require_result(ok: bool, code: str, message: str, remediation: str) -> None:
        if not ok:
            raise OperationError(
                code or "transport_failed",
                message or "Transport command failed.",
                remediation,
            )

    @staticmethod
    def _is_worker(assignment: NodeAssignment) -> bool:
        return assignment.role in {"worker", "rpc-worker"}

    @classmethod
    def _start_order(
        cls, assignments: Sequence[NodeAssignment]
    ) -> list[NodeAssignment]:
        return sorted(
            assignments,
            key=lambda assignment: (
                0 if cls._is_worker(assignment) else 1,
                assignment.node_id,
            ),
        )

    @classmethod
    def _stop_order(
        cls, pairs: Sequence[tuple[DeploymentRecord, NodeAssignment]]
    ) -> list[tuple[DeploymentRecord, NodeAssignment]]:
        return sorted(
            pairs,
            key=lambda pair: (0 if not cls._is_worker(pair[1]) else 1, pair[1].node_id),
        )

    @classmethod
    def _unload_order(
        cls, assignments: Sequence[NodeAssignment]
    ) -> list[NodeAssignment]:
        return sorted(
            assignments,
            key=lambda assignment: (
                0 if not cls._is_worker(assignment) else 1,
                assignment.node_id,
            ),
        )

    @staticmethod
    def _record_to_dict(record: DeploymentRecord) -> dict[str, Any]:
        return {
            "id": record.id,
            "profile_id": record.profile_id,
            "expected_alias": record.expected_alias,
            "node_ids": list(record.node_ids),
            "desired_state": record.desired_state,
            "observed_state": record.observed_state,
            "spec": dict(record.spec),
            "endpoint": record.endpoint,
            "owning_operation_id": record.owning_operation_id,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }
