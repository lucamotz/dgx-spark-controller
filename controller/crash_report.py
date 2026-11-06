# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""The ``crash-report`` and ``quality-report`` operations of the watchdog.

The watchdog (:mod:`controller.watchdog`) submits a crash report when a
deployment the controller last left serving fails its readiness probes, or
when one of its own recovery attempts ends without serving. Under the
deployment's leases it:

1. Observes every rank again. A deployment that is ready again is recorded
   ``ready`` and watched as before; an unreachable node fails the report, so
   the watchdog tries again later.
2. Writes a support bundle with each rank's observed state, container status
   and exit code, last log lines and kernel GPU faults, and the serving
   rank's metrics, before anything is stopped.
3. Records the deployment ``failed``, still desired ``ready``, and decides
   whether the watchdog may start a recovery attempt. That needs positive
   evidence of a crash: at least one rank where nothing runs any more, and on
   every other rank only this deployment's own process. A foreign or
   unrecognized workload, or an endpoint that fails while every rank still
   runs, ends the report ``needs-attention``, as do a profile without
   ``auto_recover``, another revision of the profile in the catalog, a
   placement the profile no longer admits, or a spent attempt budget.
   Otherwise the report succeeds with the attempt number and the time from
   which the attempt may start.

A quality report follows a profile's ``quality_watch`` instead: the serving
rank's draft acceptance stayed below its floor while every rank passed
readiness. It runs the same steps without waiting for a rank to fail, and
schedules a recovery only when the profile enables ``quality_watch.recover``
and no rank runs a workload the controller cannot prove is the deployment's,
``hold_s`` after the report at the earliest.

A report never starts or stops a process.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from controller.domain import ModelProfile, NodeConfig
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
from controller.orchestrator import OperationOrchestrator
from controller.paths import write_private_file
from controller.persistence import OperationJournal
from controller.projection import ProjectionError, validate_placement
from controller.resources import (
    operation_deployment_id,
    readable_lines,
    redact_secrets,
    safe_text,
)
from controller.transport.base import NodeObservation, NodeTransport, UnitState

WATCHDOG = "watchdog"
LOG_LINES = 200
KERNEL_FAULT_LINES = 50
# A deployment whose probes keep failing while every report finds it ready is
# left to the operator after this many such reports within an hour.
FLAPPING_REPORTS = 3
_LOG_LINE_CHARS = 2_000


def watchdog_key(operation_id: str, step: str) -> str:
    """The idempotency key of a watchdog ``step`` that follows ``operation_id``."""
    return f"watchdog:{operation_id}:{step}"


def rank_problem(
    observation: NodeObservation, assignment: NodeAssignment, spec: DeploymentSpec
) -> str:
    """Why one rank fails readiness; empty when it is ready."""
    if observation.ready_for_assignment(assignment, spec):
        return ""
    if not observation.reachable:
        return "node unreachable"
    if not observation.managed or observation.deployment_id != spec.deployment_id:
        status = observation.facts.get("container_status")
        code = observation.facts.get("container_exit_code")
        if status and status != "running":
            return f"container {status}" + (
                "" if code is None else f" with exit code {code}"
            )
        if observation.shows_workload:
            return "runtime identity or ownership does not match"
        return "process or container gone"
    if observation.unit_state is not UnitState.RUNNING:
        return f"unit {observation.unit_state.value}"
    if not observation.endpoint_healthy:
        return "health check did not return 200"
    return "expected model alias missing"


def foreign_ranks(
    deployment_id: str, observations: Mapping[str, NodeObservation]
) -> list[str]:
    """Nodes running a workload the controller cannot prove is this deployment's."""
    return [
        node_id
        for node_id, observation in observations.items()
        if observation.shows_workload
        and not (observation.managed and observation.deployment_id == deployment_id)
    ]


def only_own_workload(
    deployment_id: str, observations: Mapping[str, NodeObservation]
) -> bool:
    """Whether every node answers and runs nothing but this deployment.

    Only then may the watchdog stop a degraded deployment through baseline
    recovery.
    """
    return all(
        observation.reachable for observation in observations.values()
    ) and not foreign_ranks(deployment_id, observations)


def crash_proven(
    deployment_id: str, observations: Mapping[str, NodeObservation]
) -> bool:
    """Whether a rank is gone and the others run only this deployment.

    Only then may the watchdog stop what is left through baseline recovery.
    """
    return only_own_workload(deployment_id, observations) and not all(
        observation.shows_workload for observation in observations.values()
    )


def degradation(request: Mapping[str, Any]) -> str:
    """How the windows that tripped a quality report fell below the floor."""
    windows = request["windows"]
    rates = ", ".join(f"{window['acceptance_percent']:g} %" for window in windows)
    count = "1 window" if len(windows) == 1 else f"{len(windows)} windows in a row"
    return (
        f"had a draft acceptance below {request['min_acceptance_percent']} % "
        f"for {count} ({rates})"
    )


@dataclass(frozen=True)
class _Decision:
    state: OperationState
    code: str
    message: str
    error_code: str | None = None
    remediation: str | None = None
    schedule: dict[str, Any] | None = None


class CrashReporter:
    """Execute queued crash and quality reports on the lifecycle worker."""

    def __init__(
        self,
        journal: OperationJournal,
        orchestrator: OperationOrchestrator,
        transport: NodeTransport,
        nodes: Mapping[str, NodeConfig],
        profiles: Mapping[str, ModelProfile],
        bundle_dir: Path | None,
    ) -> None:
        self.journal = journal
        self.orchestrator = orchestrator
        self.transport = transport
        self._nodes = nodes
        self._profiles = profiles
        self._bundle_dir = bundle_dir
        # Names the catalog this reporter decides with; a schedule from
        # another one is decided again.
        self.catalog = uuid.uuid4().hex

    def run(self, operation_id: str) -> OperationRecord:
        operation, claimed = self.journal.claim_operation(operation_id)
        if not claimed:
            return operation
        try:
            with self.orchestrator.maintenance_leases(
                operation_id, operation.node_ids, "Acquiring the deployment's leases."
            ):
                return self._report(operation)
        except OperationError as error:
            return self.journal.finish(
                operation_id,
                state=OperationState.FAILED,
                code="operation_failed",
                message=error.message,
                error_code=error.code,
                remediation=error.remediation,
            )

    def _report(self, operation: OperationRecord) -> OperationRecord:
        operation_id = operation.id
        # Deployment records are never deleted, and the watchdog reports only
        # known ones.
        record = self.journal.get_deployment(str(operation.request["deployment_id"]))
        assert record is not None
        spec = DeploymentSpec.from_dict(record.spec)
        quality = operation.action is OperationAction.QUALITY_REPORT
        if record.owning_operation_id != operation.request["after_operation_id"]:
            return self.journal.finish(
                operation_id,
                state=OperationState.CANCELLED,
                code="quality_report_superseded"
                if quality
                else "crash_report_superseded",
                message="Another operation changed the deployment before the "
                "report ran. Nothing was changed.",
                result={"deployment_id": spec.deployment_id}
                if quality
                else {"deployment_id": spec.deployment_id, "crashed": False},
            )
        self._advance(
            operation_id,
            OperationPhase.REFRESHING,
            "refreshing_nodes",
            f"Observing every rank of {spec.deployment_id}.",
        )
        observations = {
            assignment.node_id: self.transport.observe(assignment.node_id)
            for assignment in spec.assignments
        }
        unreachable = [
            node_id
            for node_id, observation in observations.items()
            if not observation.reachable
        ]
        if unreachable:
            raise OperationError(
                "node_unreachable",
                f"Nodes are unreachable: {', '.join(unreachable)}.",
                "The watchdog reports again once every node answers.",
            )
        problems = {
            assignment.node_id: problem
            for assignment in spec.assignments
            if (
                problem := rank_problem(
                    observations[assignment.node_id], assignment, spec
                )
            )
        }
        if not problems and not quality:
            self._save(record, "ready", operation_id)
            if self._flapping(record):
                return self.journal.finish(
                    operation_id,
                    state=OperationState.NEEDS_ATTENTION,
                    code="operation_needs_attention",
                    message=f"{record.id} failed its readiness probes "
                    f"{FLAPPING_REPORTS} times within an hour but was ready "
                    "whenever a report checked, so the watchdog stopped watching it.",
                    result={"deployment_id": spec.deployment_id, "crashed": False},
                    error_code="deployment_flapping",
                    remediation="Check the endpoint's health under load, then load "
                    "the profile again to resume watching.",
                )
            return self.journal.finish(
                operation_id,
                state=OperationState.SUCCEEDED,
                code="crash_not_confirmed",
                message=f"Every rank of {spec.deployment_id} is ready again. "
                "Nothing was stopped.",
                result={"deployment_id": spec.deployment_id, "crashed": False},
            )
        self._advance(
            operation_id,
            OperationPhase.RECONCILING,
            "collecting_diagnostics",
            "Collecting each rank's container state, last log lines and kernel "
            "faults, and the serving rank's metrics, before anything is stopped.",
        )
        ranks = [
            self._diagnose(
                spec,
                assignment,
                observations[assignment.node_id],
                problems.get(assignment.node_id, ""),
            )
            for assignment in spec.assignments
        ]
        for rank in ranks:
            self.orchestrator.note(
                operation_id,
                "rank_diagnostics",
                _rank_note(rank),
                {
                    key: rank[key]
                    for key in ("node_id", "container_status", "exit_code")
                },
            )
        serving = next(item for item in spec.assignments if not item.is_worker)
        metrics = self.transport.metrics(serving.node_id, spec)
        bundle = self._write_bundle(operation, record, spec, ranks, metrics)
        decision = self._decide(operation, record, spec, observations)
        self._advance(
            operation_id,
            OperationPhase.COMMITTING,
            "recording_degradation" if quality else "recording_crash",
            f"Recording {spec.deployment_id} failed and still desired ready.",
        )
        self._save(record, "failed", operation_id)
        result: dict[str, Any] = {
            "deployment_id": spec.deployment_id,
            "ranks": [
                {key: rank[key] for key in ("node_id", "role", "problem", "exit_code")}
                for rank in ranks
            ],
            "support_bundle": bundle,
        }
        if not quality:
            result["crashed"] = True
        if decision.schedule is not None:
            result["auto_recovery"] = decision.schedule
        return self.journal.finish(
            operation_id,
            state=decision.state,
            code=decision.code,
            message=decision.message,
            result=result,
            error_code=decision.error_code,
            remediation=decision.remediation,
        )

    def _diagnose(
        self,
        spec: DeploymentSpec,
        assignment: NodeAssignment,
        observation: NodeObservation,
        problem: str,
    ) -> dict[str, Any]:
        facts = observation.facts
        lines = self.transport.logs(
            assignment.node_id, spec.deployment_id, limit=LOG_LINES
        ).lines
        faults = self.transport.kernel_faults(
            assignment.node_id, KERNEL_FAULT_LINES
        ).lines
        return {
            "node_id": assignment.node_id,
            "role": assignment.role,
            "problem": problem,
            "unit_state": observation.unit_state.value,
            "managed": observation.managed,
            "deployment_id": observation.deployment_id,
            "endpoint_healthy": observation.endpoint_healthy,
            "served_aliases": list(observation.served_aliases),
            "free_memory_bytes": observation.free_memory_bytes,
            "swap_used_bytes": facts.get("swap_used_bytes"),
            "container_status": facts.get("container_status"),
            "exit_code": facts.get("container_exit_code"),
            "message": redact_secrets(observation.message),
            "log_tail": [
                redact_secrets(line)[:_LOG_LINE_CHARS] for line in lines[-LOG_LINES:]
            ],
            "kernel_faults": [
                redact_secrets(line)[:_LOG_LINE_CHARS]
                for line in faults[-KERNEL_FAULT_LINES:]
            ],
        }

    def _write_bundle(
        self,
        operation: OperationRecord,
        record: DeploymentRecord,
        spec: DeploymentSpec,
        ranks: list[dict[str, Any]],
        metrics: tuple[str, ...] | None,
    ) -> str | None:
        """Write the support bundle; return its path below the log directory."""
        if self._bundle_dir is None:
            return None
        path = self._bundle_dir / f"{operation.id}.json"
        bundle = {
            "schema_version": 1,
            "operation_id": operation.id,
            "action": operation.action.value,
            "created_at": utc_now(),
            "deployment_id": record.id,
            "profile_id": record.profile_id,
            "profile_revision": spec.profile.get("revision"),
            "trigger": dict(operation.request),
            "ranks": ranks,
            "metrics": None
            if metrics is None
            else [redact_secrets(line)[:_LOG_LINE_CHARS] for line in metrics],
        }
        write_private_file(
            path, json.dumps(bundle, indent=2) + "\n", prefix=".crash-report-"
        )
        return f"{self._bundle_dir.name}/{path.name}"

    def _decide(
        self,
        operation: OperationRecord,
        record: DeploymentRecord,
        spec: DeploymentSpec,
        observations: Mapping[str, NodeObservation],
    ) -> _Decision:
        profile = self._profiles[record.profile_id]
        policy = profile.auto_recover
        quality = operation.action is OperationAction.QUALITY_REPORT
        fault = degradation(operation.request) if quality else "stopped serving"
        foreign = foreign_ranks(record.id, observations)
        if foreign:
            return _needs_attention(
                "deployment_unrecognized",
                f"{record.id} {fault}, but {', '.join(foreign)} run a "
                "workload the controller cannot prove is this deployment's, so "
                "the watchdog stops nothing.",
                "Stop that workload with the tool that started it, then recover "
                "baseline for the profile and load it again.",
            )
        if quality and not profile.quality_watch.recover:
            return _needs_attention(
                "output_degraded",
                f"{record.id} {fault}. Profile {profile.profile_id} does not "
                "enable quality_watch.recover, so the watchdog stops nothing.",
                "Inspect the running server if needed, then recover baseline for "
                "the profile and load it again.",
            )
        if not quality and all(
            observation.shows_workload for observation in observations.values()
        ):
            return _needs_attention(
                "deployment_unhealthy",
                f"{record.id} fails readiness while every rank still runs, so "
                "the watchdog stops nothing.",
                "Check the endpoint. If it hangs, recover baseline and load the "
                "profile. If it serves, load the profile again to resume watching.",
            )
        # An enabled quality_watch.recover implies auto_recover.enabled.
        if not policy.enabled:
            return _needs_attention(
                "deployment_crashed",
                f"{record.id} stopped serving. Profile {profile.profile_id} does "
                "not enable automatic recovery.",
                "Read the crash report, then recover baseline for the profile and "
                "load it again.",
            )
        revision = spec.profile.get("revision")
        if revision != profile.revision:
            return _needs_attention(
                "profile_revision_changed",
                f"{record.id} ran revision {revision} of profile "
                f"{profile.profile_id}, but the catalog holds revision "
                f"{profile.revision}, so the watchdog does not load it.",
                "Load the profile deliberately in a maintenance window.",
            )
        try:
            validate_placement(
                profile, [self._nodes[node_id] for node_id in record.node_ids]
            )
        except ProjectionError as error:
            return _needs_attention(
                "placement_not_allowed",
                f"Profile {profile.profile_id} no longer admits the nodes "
                f"{record.id} ran on, so the watchdog does not load it.",
                error.remediation,
            )
        now = datetime.now(UTC)
        cutoff = (now - timedelta(seconds=policy.window_s)).isoformat(
            timespec="milliseconds"
        )
        attempts = [
            other
            for other in self.journal.list_operations()
            if other.action is OperationAction.RECOVER
            and other.requester == WATCHDOG
            and other.state is not OperationState.CANCELLED
            and other.created_at >= cutoff
            and operation_deployment_id(other) == record.id
        ]
        if len(attempts) >= policy.max_attempts:
            window = (
                f"{policy.window_s // 60} min"
                if policy.window_s < 3600
                else f"{policy.window_s / 3600:g} h"
            )
            return _needs_attention(
                "crash_loop_detected",
                f"{record.id} {fault if quality else 'is not serving'} after "
                f"{len(attempts)} automatic recoveries within {window}, so the "
                "watchdog stopped.",
                f"Read the {'quality' if quality else 'crash'} reports for the "
                "cause, then load the profile deliberately once it is fixed.",
            )
        delay = policy.backoff_s * 2 ** (len(attempts) - 1) if attempts else 0
        if quality:
            delay = max(delay, profile.quality_watch.hold_s)
        attempt = len(attempts) + 1
        return _Decision(
            OperationState.SUCCEEDED,
            "quality_recovery_scheduled" if quality else "crash_recovery_scheduled",
            (f"{record.id} {fault}. " if quality else "")
            + f"Automatic recovery {attempt} of {policy.max_attempts} starts "
            + ("now." if not delay else f"in {delay} s."),
            schedule={
                "catalog": self.catalog,
                "attempt": attempt,
                "max_attempts": policy.max_attempts,
                "not_before": (now + timedelta(seconds=delay)).isoformat(
                    timespec="milliseconds"
                ),
            },
        )

    def _flapping(self, record: DeploymentRecord) -> bool:
        """Whether earlier reports found the deployment ready often enough."""
        cutoff = (datetime.now(UTC) - timedelta(hours=1)).isoformat(
            timespec="milliseconds"
        )
        earlier = [
            other
            for other in self.journal.list_operations()
            if other.action is OperationAction.CRASH_REPORT
            and other.state is OperationState.SUCCEEDED
            and other.created_at >= cutoff
            and other.request.get("deployment_id") == record.id
            and other.result
            and other.result.get("crashed") is False
        ]
        return len(earlier) >= FLAPPING_REPORTS - 1

    def _save(
        self, record: DeploymentRecord, observed_state: str, operation_id: str
    ) -> None:
        self.journal.save_deployment(
            replace(
                record,
                desired_state="ready",
                observed_state=observed_state,
                owning_operation_id=operation_id,
                updated_at=utc_now(),
            )
        )

    def _advance(
        self, operation_id: str, phase: OperationPhase, code: str, message: str
    ) -> None:
        self.journal.advance(operation_id, phase=phase, code=code, message=message)


def _needs_attention(code: str, message: str, remediation: str) -> _Decision:
    return _Decision(
        OperationState.NEEDS_ATTENTION,
        "operation_needs_attention",
        message,
        error_code=code,
        remediation=remediation,
    )


def _rank_note(rank: Mapping[str, Any]) -> str:
    """One rank's state, last log and kernel lines as the browser can show them."""
    summary = f"{rank['node_id']} ({rank['role']}): {rank['problem'] or 'ready'}."
    note = summary
    if lines := readable_lines(rank["log_tail"], limit=3):
        note += f" Last log lines: {' | '.join(lines)}"
    if faults := readable_lines(rank["kernel_faults"], limit=1):
        note += f" Last kernel fault line: {faults[0]}"
    return note if safe_text(note) == note else summary
