# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""The ``update`` operation: update every node and restore what was serving.

One operation holds the cluster lease and every node lease from start to end:

1. Refuse while a recipe operation runs, a node is unreachable, a node runs
   a workload the controller does not manage, or a managed deployment is not
   ready. Prove every node's helper and every serving deployment's preflight.
2. With the relay and its tunnels up, check every node. Nothing to install
   and no reboot pending ends the operation without touching the deployments.
3. Download every update on every node while the deployments keep serving.
4. Stop the serving deployments and prove their release, install the updates
   and reboot every node that installed something, then wait until each is
   back with a new boot id, Docker, the GPU and its cluster links.
5. Restore the stopped deployments through the normal start, readiness and
   completion-canary checks.

A node whose install fails or cannot start, or that refuses its reboot, is
not rebooted; the others still are, the deployments are still restored, and
the operation ends ``needs-attention``. Nothing here is retried.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import time
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from controller.operations import (
    DeploymentRecord,
    DeploymentSpec,
    OperationError,
    OperationPhase,
    OperationRecord,
    OperationState,
)
from controller.orchestrator import OperationOrchestrator, PreflightFailed
from controller.persistence import OperationJournal
from controller.resources import safe_text
from controller.transport.base import NodeObservation, NodeTransport
from controller.updates.nodeops import (
    NodeFacts,
    RunStatus,
    UpdateCheck,
    UpdateNodeError,
    UpdateNodeOps,
    relay_hosts,
    relay_tunnels,
)
from controller.updates.relay import RelaySession, UpdateRelay
from controller.updates.settings import UpdateSettings


@contextlib.contextmanager
def _awake() -> Iterator[None]:
    """Keep the controller host from idle-sleeping while an update runs.

    The relay and every tunnel live on this host; macOS idle sleep in the
    middle of an update would strand the nodes with the model stopped.
    """
    caffeinate = shutil.which("caffeinate")
    if caffeinate is None:
        yield
        return
    process = subprocess.Popen(  # noqa: S603 - fixed argv
        [caffeinate, "-i", "-w", str(os.getpid())],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        yield
    finally:
        process.terminate()
        process.wait(timeout=5)


@dataclass
class _Progress:
    stopped: list[DeploymentRecord] = field(default_factory=list)
    mutated: bool = False
    before: dict[str, NodeFacts] = field(default_factory=dict)
    installed: dict[str, RunStatus] = field(default_factory=dict)
    rebooted: set[str] = field(default_factory=set)
    after: dict[str, NodeFacts] = field(default_factory=dict)
    restored: list[str] = field(default_factory=list)
    canary: dict[str, Any] | None = None
    relay: dict[str, object] = field(default_factory=dict)


class ClusterUpdateEngine:
    """Execute queued ``update`` operations on the lifecycle worker."""

    def __init__(
        self,
        journal: OperationJournal,
        orchestrator: OperationOrchestrator,
        transport: NodeTransport,
        node_ops: UpdateNodeOps,
        relay: UpdateRelay,
        settings: Callable[[], UpdateSettings],
        live_recipe_operations: Callable[[], Collection[str]],
        *,
        download_timeout_seconds: float = 4 * 3600,
        install_timeout_seconds: float = 2 * 3600,
        reboot_timeout_seconds: float = 45 * 60,
        poll_seconds: float = 10.0,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.journal = journal
        self.orchestrator = orchestrator
        self.transport = transport
        self.node_ops = node_ops
        self.relay = relay
        self._settings = settings
        self._live_recipe_operations = live_recipe_operations
        self._timeouts = {
            "download": download_timeout_seconds,
            "apply": install_timeout_seconds,
        }
        self._reboot_timeout = reboot_timeout_seconds
        self._poll = poll_seconds
        self._sleep = sleeper
        self._monotonic = monotonic

    def run(self, operation_id: str) -> OperationRecord:
        operation, claimed = self.journal.claim_operation(operation_id)
        if not claimed:
            return operation
        progress = _Progress()
        try:
            with (
                _awake(),
                self.orchestrator.maintenance_leases(
                    operation_id,
                    operation.node_ids,
                    "Acquiring cluster maintenance leases.",
                ),
            ):
                return self._update(operation_id, operation.node_ids, progress)
        except (UpdateNodeError, OSError) as error:
            # Relay and tunnel setup fail outside every node step.
            failure = OperationError(
                getattr(error, "code", "relay_failed"),
                f"The update relay failed: {error}",
                "Check the controller's connection to the nodes and that port "
                "18877 is free on every node, then retry.",
            )
            return self._finish(
                operation_id,
                OperationState.NEEDS_ATTENTION
                if progress.mutated
                else OperationState.FAILED,
                failure,
                progress,
            )
        except OperationError as error:
            return self._finish(
                operation_id,
                OperationState.NEEDS_ATTENTION
                if progress.mutated
                else OperationState.FAILED,
                error,
                progress,
            )
        except Exception as error:  # pragma: no cover - last resort
            # A claimed update must never stay running or hide a stopped model.
            return self._finish(
                operation_id,
                OperationState.NEEDS_ATTENTION
                if progress.mutated
                else OperationState.FAILED,
                OperationError(
                    "internal_error",
                    f"Unexpected update engine error: {error}",
                    "Inspect the operation events and the nodes, then load the "
                    "previous profile if it is not serving.",
                ),
                progress,
            )

    def _update(
        self, operation_id: str, node_ids: Sequence[str], progress: _Progress
    ) -> OperationRecord:
        self._advance(
            operation_id,
            OperationPhase.REFRESHING,
            "refreshing_nodes",
            "Refreshing every node before the update.",
        )
        if self._live_recipe_operations():
            raise OperationError(
                "update_blocked",
                "A recipe prepare or delete is running on the nodes.",
                "Wait for the recipe operation to finish, then update again.",
            )
        serving = self._serving_deployments(node_ids)
        self._advance(
            operation_id,
            OperationPhase.PREFLIGHTING,
            "update_preflight_started",
            "Checking the update helper on every node and the serving "
            "deployments' restore preflight.",
        )
        progress.before = {
            node_id: self.node_ops.facts(node_id) for node_id in node_ids
        }
        not_ready = [
            f"{node_id} ({facts.helper})"
            for node_id, facts in progress.before.items()
            if facts.helper != "ready"
        ]
        if not_ready:
            raise OperationError(
                "update_helper_not_ready",
                f"The update helper is not ready on {', '.join(not_ready)}.",
                "Install the update helper from the Updates page, then retry.",
            )
        failures = [
            failure
            for record in serving
            for failure in self.orchestrator.preflight(
                DeploymentSpec.from_dict(record.spec)
            )
        ]
        if failures:
            raise PreflightFailed(failures)

        with self.relay.serving(
            self._settings().relay_source(), relay_hosts(progress.before.values())
        ) as relay:
            try:
                with relay_tunnels(self.node_ops, node_ids, relay):
                    checks = self._check(operation_id, node_ids)
                    if not any(
                        check.pending or check.reboot_required
                        for check in checks.values()
                    ):
                        return self._up_to_date(operation_id, progress, relay)
                    self._run_all(operation_id, node_ids, "download")
                    # The download can take hours; refuse a workload that
                    # appeared meanwhile before anything stops or reboots.
                    serving = self._serving_deployments(node_ids)
                    self._advance(
                        operation_id,
                        OperationPhase.SNAPSHOTTING,
                        "snapshotting_prior_placement",
                        "Recording the deployments to restore after the update.",
                    )
                    self.journal.record_previous_placement(
                        operation_id,
                        {
                            "deployments": [record.to_dict() for record in serving],
                            "observed_node_ids": list(node_ids),
                        },
                    )
                    progress.mutated = True
                    self.orchestrator.stop_for_maintenance(operation_id, serving)
                    progress.stopped = list(serving)
                    self._advance(
                        operation_id,
                        OperationPhase.UPDATING,
                        "update_install_started",
                        "Installing package and firmware updates on every node.",
                    )
                    progress.installed = self._run_all(operation_id, node_ids, "apply")
            finally:
                progress.relay = relay.stats.snapshot()

        failed = [
            node_id
            for node_id, status in progress.installed.items()
            if status.state != "succeeded"
        ]
        refused = self._reboot(operation_id, node_ids, progress)
        restore_error = self._restore(operation_id, progress)
        restored = restore_error or (
            "The previous deployments were restored."
            if progress.stopped
            else "No deployment was serving."
        )
        if failed:
            raise OperationError(
                "update_install_failed",
                f"Installing updates failed on {', '.join(failed)}. Those nodes "
                f"were not rebooted. {restored}",
                "Read the helper log tail in the operation events and fix the "
                "node's package state before the next update.",
            )
        if refused:
            raise OperationError(
                "node_reboot_refused",
                f"{', '.join(refused)} did not accept the reboot. {restored}",
                "Reboot those nodes by hand in a maintenance window.",
            )
        if restore_error:
            raise OperationError(
                "update_restore_failed",
                restore_error,
                "The nodes are updated. Load the previous profile again, or "
                "inspect the runtime against the updated driver and kernel.",
            )
        self._advance(
            operation_id,
            OperationPhase.COMMITTING,
            "committing_update",
            "Every node is updated and the previous deployments are ready.",
        )
        return self.journal.finish(
            operation_id,
            state=OperationState.SUCCEEDED,
            code="operation_succeeded",
            message="Every node is updated and the previous deployments are ready.",
            result=self._result(progress),
        )

    def _serving_deployments(self, node_ids: Sequence[str]) -> list[DeploymentRecord]:
        """Ready managed deployments on the nodes; refuse anything else running."""
        observations: dict[str, NodeObservation] = {}
        for node_id in node_ids:
            observation = self.transport.observe(node_id)
            if not observation.reachable:
                raise OperationError(
                    "node_unreachable",
                    f"Node {node_id} is unreachable.",
                    "Restore connectivity before updating the cluster.",
                )
            observations[node_id] = observation
        records: dict[str, DeploymentRecord] = {}
        for node_id, observation in observations.items():
            if not observation.shows_workload:
                continue
            record = (
                self.journal.get_deployment(observation.deployment_id)
                if observation.managed and observation.deployment_id
                else None
            )
            if record is None:
                raise OperationError(
                    "unmanaged_conflict",
                    f"Node {node_id} runs a workload the controller does not manage.",
                    "Stop it with the tool that started it. An update reboots the "
                    "node.",
                )
            records[record.id] = record
        for record in records.values():
            spec = DeploymentSpec.from_dict(record.spec)
            if not all(
                assignment.node_id in observations
                and observations[assignment.node_id].ready_for_assignment(
                    assignment, spec
                )
                for assignment in spec.assignments
            ):
                raise OperationError(
                    "deployment_not_ready",
                    f"Deployment {record.id} is not ready on all of its nodes.",
                    "Load or unload it until it is ready or stopped, then update.",
                )
        return list(records.values())

    def _check(
        self, operation_id: str, node_ids: Sequence[str]
    ) -> dict[str, UpdateCheck]:
        checks: dict[str, UpdateCheck] = {}
        for node_id in node_ids:
            try:
                checks[node_id] = self.node_ops.check(node_id)
            except UpdateNodeError as error:
                raise OperationError(
                    error.code,
                    f"Checking for updates failed on {node_id}: {error.message}",
                    "Check the update source on the Updates page.",
                ) from None
        self.orchestrator.note(
            operation_id,
            "updates_found",
            ", ".join(
                f"{node_id} {check.packages} packages and {check.firmware} firmware"
                for node_id, check in checks.items()
            )
            + ".",
        )
        return checks

    def _run_all(
        self, operation_id: str, node_ids: Sequence[str], mode: str
    ) -> dict[str, RunStatus]:
        """Run the helper's ``mode`` on every node at once and wait for all."""
        label = "Downloading" if mode == "download" else "Installing"
        statuses: dict[str, RunStatus] = {}
        for node_id in node_ids:
            try:
                status = self.node_ops.start_run(node_id, mode)
            except UpdateNodeError as error:
                status = self._lost_start(node_id, mode)
                if status.state == "failed":
                    self.orchestrator.note(
                        operation_id,
                        f"update_{mode}_failed",
                        f"{label} could not start on {node_id}: {error.message}",
                    )
            if status.mode != mode:
                # An earlier run still holds the node's helper unit.
                status = RunStatus(state="failed", mode=mode, failed_step="conflict")
            statuses[node_id] = status
        deadline = self._monotonic() + self._timeouts[mode]
        reported: dict[str, tuple[str, str]] = {}
        while True:
            for node_id in node_ids:
                if statuses[node_id].finished:
                    continue
                try:
                    statuses[node_id] = self.node_ops.run_status(node_id)
                except UpdateNodeError:
                    continue
            current = {
                node_id: (status.state, status.step)
                for node_id, status in statuses.items()
            }
            if current != reported:
                reported = current
                self._progress(operation_id, f"{label} updates.", statuses)
            if all(status.finished for status in statuses.values()):
                break
            if self._monotonic() >= deadline:
                raise OperationError(
                    "update_timeout",
                    f"{label} updates did not finish in time.",
                    "The helper keeps running on the nodes. Inspect its log there.",
                )
            self.orchestrator.heartbeat(operation_id)
            self._sleep(self._poll)
        failed = {
            node_id: status
            for node_id, status in statuses.items()
            if status.state != "succeeded"
        }
        for node_id, status in failed.items():
            if status.log_tail:
                # The browser hides any line that reads like a command; the
                # journal keeps the raw tail for local diagnosis.
                shown = [
                    line
                    for line in status.log_tail[-5:]
                    if safe_text(line, fallback="") == line
                ]
                self.orchestrator.note(
                    operation_id,
                    f"update_{mode}_failed",
                    f"{label} failed on {node_id} at "
                    f"{status.failed_step or status.state}. "
                    + (" | ".join(shown) or "See the helper log on the node."),
                    {"log_tail": list(status.log_tail)},
                )
        if mode == "download" and failed:
            raise OperationError(
                "update_download_failed",
                f"Downloading updates failed on {', '.join(failed)}. The "
                "deployments kept serving.",
                "Check the update source. The relay may lack files a node needs.",
            )
        return statuses

    def _lost_start(self, node_id: str, mode: str) -> RunStatus:
        """The run behind a start call whose answer was lost.

        The download that precedes an apply rewrote the helper's state, so an
        apply in that state is this operation's own. A download is never
        adopted: it changes nothing a retry would not redo.
        """
        failed = RunStatus(state="failed", mode=mode, failed_step="starting")
        if mode != "apply":
            return failed
        try:
            status = self.node_ops.run_status(node_id)
        except UpdateNodeError:
            return failed
        return status if status.mode == mode else failed

    def _reboot(
        self, operation_id: str, node_ids: Sequence[str], progress: _Progress
    ) -> list[str]:
        """Reboot nodes that installed something; return those that refused."""
        targets = [
            node_id
            for node_id in node_ids
            if progress.installed[node_id].state == "succeeded"
            and (
                progress.installed[node_id].packages
                or progress.installed[node_id].firmware
                or progress.installed[node_id].reboot_required
            )
        ]
        if not targets:
            return []
        self._advance(
            operation_id,
            OperationPhase.REBOOTING,
            "rebooting_nodes",
            f"Rebooting {', '.join(targets)}.",
        )
        boot_ids: dict[str, str] = {}
        refused: list[str] = []
        for node_id in targets:
            try:
                boot_ids[node_id] = self.node_ops.reboot(node_id)
            except UpdateNodeError as error:
                refused.append(node_id)
                self.orchestrator.note(
                    operation_id,
                    "node_reboot_refused",
                    f"{node_id} did not accept the reboot: {error.message}",
                )
                continue
            progress.rebooted.add(node_id)
        deadline = self._monotonic() + self._reboot_timeout
        pending = set(boot_ids)
        while pending:
            for node_id in sorted(pending):
                facts = self.node_ops.facts(node_id)
                if facts.booted and facts.boot_id not in {"", boot_ids[node_id]}:
                    pending.discard(node_id)
                    progress.after[node_id] = facts
                    self.orchestrator.note(
                        operation_id,
                        "node_back_online",
                        f"{node_id} is back on kernel {facts.kernel or 'unknown'}, "
                        f"driver {facts.driver or 'unknown'}.",
                    )
            if not pending:
                break
            if self._monotonic() >= deadline:
                raise OperationError(
                    "node_reboot_timeout",
                    f"{', '.join(sorted(pending))} did not come back after the "
                    "reboot with Docker, the GPU and its cluster links.",
                    "Inspect the node's console. Firmware updates can take long. "
                    "Load the previous profile once every node is back.",
                )
            self.orchestrator.heartbeat(operation_id)
            self._sleep(self._poll)
        return refused

    def _restore(self, operation_id: str, progress: _Progress) -> str:
        """Restore the stopped deployments; return a failure message, if any."""
        if not progress.stopped:
            return ""
        specs = [DeploymentSpec.from_dict(record.spec) for record in progress.stopped]
        try:
            canaries = self.orchestrator.restore_after_maintenance(operation_id, specs)
        except OperationError as error:
            restored = ", ".join(spec.deployment_id for spec in specs)
            return f"Restoring {restored} failed: {error.message}"
        progress.restored = [spec.deployment_id for spec in specs]
        progress.canary = canaries[0] if canaries else None
        return ""

    def _up_to_date(
        self, operation_id: str, progress: _Progress, relay: RelaySession
    ) -> OperationRecord:
        self._advance(
            operation_id,
            OperationPhase.COMMITTING,
            "cluster_up_to_date",
            "Every node is up to date. The deployments were not touched.",
        )
        progress.relay = relay.stats.snapshot()
        progress.installed = {
            node_id: RunStatus(state="succeeded", packages=0, firmware=0)
            for node_id in progress.before
        }
        return self.journal.finish(
            operation_id,
            state=OperationState.SUCCEEDED,
            code="cluster_up_to_date",
            message="Every node is up to date. The deployments were not touched.",
            result={**self._result(progress), "up_to_date": True},
        )

    def _result(self, progress: _Progress) -> dict[str, Any]:
        result: dict[str, Any] = {
            "up_to_date": False,
            "restored_deployments": progress.restored,
            "nodes": [
                {
                    "node_id": node_id,
                    "packages": status.packages if status else None,
                    "firmware": status.firmware if status else None,
                    "rebooted": node_id in progress.rebooted,
                    "kernel_before": before.kernel,
                    "kernel_after": progress.after.get(node_id, before).kernel,
                    "driver_before": before.driver,
                    "driver_after": progress.after.get(node_id, before).driver,
                }
                for node_id, before in progress.before.items()
                for status in (progress.installed.get(node_id),)
            ],
            "relay": progress.relay,
        }
        if progress.canary is not None:
            result["completion_canary"] = progress.canary
        return result

    def _finish(
        self,
        operation_id: str,
        state: OperationState,
        error: OperationError,
        progress: _Progress,
    ) -> OperationRecord:
        current = self.journal.get_operation(operation_id)
        if current is not None and current.state.terminal:
            return current
        return self.journal.finish(
            operation_id,
            state=state,
            code="operation_needs_attention"
            if state is OperationState.NEEDS_ATTENTION
            else "operation_failed",
            message=error.message,
            result=self._result(progress) if progress.mutated else None,
            error_code=error.code,
            remediation=error.remediation,
        )

    def _progress(
        self, operation_id: str, message: str, statuses: Mapping[str, RunStatus]
    ) -> None:
        self.orchestrator.note(
            operation_id,
            "update_progress",
            message,
            {
                "progress_by_node": {
                    node_id: {
                        "state": status.state
                        if status.state in {"running", "succeeded", "failed"}
                        else "unknown",
                        "step": status.step or status.state,
                    }
                    for node_id, status in statuses.items()
                }
            },
        )

    def _advance(
        self, operation_id: str, phase: OperationPhase, code: str, message: str
    ) -> None:
        self.journal.advance(operation_id, phase=phase, code=code, message=message)
