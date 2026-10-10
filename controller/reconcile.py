# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Restart reconciliation for durable operations and observed deployments."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, replace

from controller.operations import (
    DeploymentRecord,
    DeploymentSpec,
    OperationAction,
    OperationRecord,
    OperationState,
    utc_now,
)
from controller.persistence import OperationJournal
from controller.transport.base import NodeObservation, NodeTransport, UnitState


@dataclass(frozen=True)
class ReconciliationReport:
    """Result of a read/repair pass; no unmanaged process is ever killed."""

    ready_deployments: tuple[str, ...]
    stopped_deployments: tuple[str, ...]
    uncertain_deployments: tuple[str, ...]
    unmanaged_nodes: tuple[str, ...]
    completed_operations: tuple[str, ...]
    needs_attention_operations: tuple[str, ...]


class Reconciler:
    """Rebuild controller state from transport observations after process restart.

    Only a deployment matching a known persisted specification is adopted.  Other
    observed processes are returned as unmanaged evidence for an operator and are
    intentionally left alone.
    """

    def __init__(
        self,
        journal: OperationJournal,
        transport: NodeTransport,
        *,
        live_operations: Callable[[], Collection[str]] = frozenset,
    ) -> None:
        self.journal = journal
        self.transport = transport
        # Operations still executing in this process (for example a recipe
        # prepare on its own worker) are alive, not interrupted; never resolve them.
        self._live_operations = live_operations

    def reconcile(self, node_ids: Sequence[str] | None = None) -> ReconciliationReport:
        """Refresh known placements.

        Interrupted operations are terminally resolved when provable.
        """
        deployments = self.journal.list_deployments(include_stopped=True)
        live = set(self._live_operations())
        running_operations = [
            operation
            for operation in self.journal.list_operations(
                states=[OperationState.RUNNING]
            )
            if operation.id not in live
        ]
        selected = set(node_ids or ())
        if not selected:
            selected.update(
                node_id for deployment in deployments for node_id in deployment.node_ids
            )
            selected.update(
                node_id
                for operation in running_operations
                for node_id in operation.node_ids
            )
        observations = {node_id: self._safe_observe(node_id) for node_id in selected}

        ready: list[str] = []
        stopped: list[str] = []
        uncertain: list[str] = []
        # Build this before evaluating state so an older stopped entry can
        # recognize a newer known replacement regardless of journal ordering.
        known_by_node: dict[str, set[str]] = {}
        for deployment in deployments:
            for node_id in deployment.node_ids:
                known_by_node.setdefault(node_id, set()).add(deployment.id)
        for operation in running_operations:
            requested = operation.request.get("deployment")
            if not isinstance(requested, Mapping):
                continue
            deployment_id = requested.get("deployment_id")
            if isinstance(deployment_id, str):
                for node_id in operation.node_ids:
                    known_by_node.setdefault(node_id, set()).add(deployment_id)
        for deployment in deployments:
            spec = DeploymentSpec.from_dict(deployment.spec)
            deployment_observations = {
                node_id: observations.get(node_id) for node_id in spec.node_ids
            }
            status = self._status_for(spec, deployment_observations)
            if status == "ready":
                self.journal.save_deployment(
                    replace(
                        deployment,
                        desired_state="ready",
                        observed_state="ready",
                        updated_at=utc_now(),
                    )
                )
                ready.append(deployment.id)
            elif status == "stopped":
                self.journal.save_deployment(
                    replace(deployment, observed_state="stopped", updated_at=utc_now())
                )
                stopped.append(deployment.id)
            elif self._deployment_was_replaced_by_known_managed(
                deployment, deployment_observations, known_by_node
            ):
                # A known replacement proves this historical placement absent,
                # even if its old desired state still says ready after a switch.
                self.journal.save_deployment(
                    replace(deployment, observed_state="stopped", updated_at=utc_now())
                )
                stopped.append(deployment.id)
            else:
                self.journal.save_deployment(
                    replace(deployment, observed_state="unknown", updated_at=utc_now())
                )
                uncertain.append(deployment.id)

        unmanaged = [
            node_id
            for node_id, observation in observations.items()
            if observation is not None
            and (
                observation.is_process_running
                or observation.endpoint_healthy
                or observation.active_alias
            )
            and (
                not observation.managed
                or not observation.deployment_id
                or observation.deployment_id not in known_by_node.get(node_id, set())
            )
        ]
        completed: list[str] = []
        needs_attention: list[str] = []
        for operation in running_operations:
            if self._operation_success_is_proven(operation, observations):
                self.journal.finish(
                    operation.id,
                    state=OperationState.SUCCEEDED,
                    code="interrupted_operation_proven_succeeded",
                    message=(
                        "Restart reconciliation proved the interrupted operation's "
                        "intended state."
                    ),
                    result={"reconciled": True},
                )
                completed.append(operation.id)
            else:
                self.journal.finish(
                    operation.id,
                    state=OperationState.NEEDS_ATTENTION,
                    code="operation_interrupted",
                    message=(
                        "Controller restart interrupted this operation before success "
                        "could be proven."
                    ),
                    error_code="operation_interrupted",
                    remediation=(
                        "Inspect affected nodes and operation events, then run a "
                        "deliberate recovery operation."
                    ),
                )
                needs_attention.append(operation.id)
            self.journal.release_locks(operation.id)

        return ReconciliationReport(
            ready_deployments=tuple(sorted(ready)),
            stopped_deployments=tuple(sorted(stopped)),
            uncertain_deployments=tuple(sorted(uncertain)),
            unmanaged_nodes=tuple(sorted(unmanaged)),
            completed_operations=tuple(sorted(completed)),
            needs_attention_operations=tuple(sorted(needs_attention)),
        )

    def _safe_observe(self, node_id: str) -> NodeObservation | None:
        try:
            return self.transport.observe(node_id)
        except Exception:
            return None

    @staticmethod
    def _status_for(
        spec: DeploymentSpec, observations: Mapping[str, NodeObservation | None]
    ) -> str:
        all_absent = True
        all_ready = True
        for assignment in spec.assignments:
            observation = observations.get(assignment.node_id)
            if observation is None or not observation.reachable:
                return "unknown"
            if assignment.role in {"worker", "rpc-worker"}:
                current_ready = (
                    observation.managed
                    and observation.deployment_id == spec.deployment_id
                    and observation.unit_state == UnitState.RUNNING
                )
            else:
                current_ready = observation.ready_for(
                    spec.deployment_id, spec.expected_alias
                )
            if current_ready:
                all_absent = False
                continue
            all_ready = False
            if (
                observation.deployment_id is not None
                or observation.is_process_running
                or observation.endpoint_healthy
                or observation.active_alias
                or observation.served_aliases
            ):
                return "unknown"
        if all_ready:
            return "ready"
        return "stopped" if all_absent else "unknown"

    @staticmethod
    def _deployment_was_replaced_by_known_managed(
        deployment: DeploymentRecord,
        observations: Mapping[str, NodeObservation | None],
        known_by_node: Mapping[str, set[str]],
    ) -> bool:
        """Mark a historical placement stopped when a known replacement is live.

        Reconciliation intentionally does not infer ownership from a port or a
        process name.  It may make this narrow historical-state exception only
        when every observed active process is explicitly controller-managed and
        identified as another persisted deployment on that node.
        """
        deployment_id = deployment.id
        replacement_seen = False
        for node_id, observation in observations.items():
            if observation is None or not observation.reachable:
                return False
            if not (
                observation.deployment_id
                or observation.is_process_running
                or observation.endpoint_healthy
                or observation.active_alias
                or observation.served_aliases
            ):
                continue
            if (
                observation.managed
                and observation.deployment_id
                and observation.deployment_id != deployment_id
                and observation.deployment_id in known_by_node.get(node_id, set())
            ):
                replacement_seen = True
                continue
            return False
        return replacement_seen

    def _operation_success_is_proven(
        self,
        operation: OperationRecord,
        observations: Mapping[str, NodeObservation | None],
    ) -> bool:
        if operation.action == OperationAction.LOAD:
            try:
                spec = DeploymentSpec.from_dict(operation.request["deployment"])
            except (KeyError, TypeError, ValueError):
                return False
            return (
                self._status_for(
                    spec,
                    {node_id: observations.get(node_id) for node_id in spec.node_ids},
                )
                == "ready"
            )
        if operation.action in {OperationAction.UNLOAD, OperationAction.RECOVER}:
            try:
                if operation.action is OperationAction.RECOVER:
                    spec = DeploymentSpec.from_dict(operation.request["deployment"])
                else:
                    deployment = self.journal.get_deployment(
                        str(operation.request["deployment_id"])
                    )
                    if deployment is None:
                        return False
                    spec = DeploymentSpec.from_dict(deployment.spec)
            except (KeyError, TypeError, ValueError):
                return False
            minimum = max(
                spec.minimum_free_memory_bytes, spec.post_stop_free_memory_bytes
            )
            return all(
                observation is not None
                and observation.stopped_for(spec.deployment_id, minimum)
                for node_id in spec.node_ids
                for observation in (observations.get(node_id),)
            )
        return False
