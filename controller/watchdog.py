# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Watch serving deployments and drive their automatic crash recovery.

A host-level thread looks at the journal every 30 seconds. It never stops or
starts anything itself; it only queues operations on the lifecycle worker:

- A deployment the controller last left serving (desired ``ready``, and owned
  by a load or update that succeeded, or that a failed one left ``ready``)
  gets the full
  readiness check of every rank. Three failed probes in a row, at least 60
  seconds apart from first to last, with every node reachable, queue a
  ``crash-report`` (:mod:`controller.crash_report`).
- If its profile enables ``quality_watch``, a deployment that passes readiness
  also has its serving rank's speculative decoding counters read once per
  ``window_s``. ``consecutive_windows`` counted windows in a row with a draft
  acceptance below the floor queue a ``quality-report``; a window with too
  few drafted tokens is not counted, and a restarted process starts over.
- A report that scheduled a recovery queues it once its time has come:
  baseline recovery, then a load of the same profile (requester
  ``watchdog``). A schedule made with another catalog (before a restart or
  reload) is decided again by a new report.
- A recovery attempt that ended without serving queues another crash report,
  which retries within the profile's budget or ends ``needs-attention``. An
  attempt that itself ended ``needs-attention`` is left to the operator.

Nothing is queued while the watchdog is paused, while any lifecycle operation
is queued or running, or while any lease is held. The watchdog also leaves a
deployment alone once an operator's operation used its nodes after its crash
report, because the operator has taken over.
"""

from __future__ import annotations

import json
import logging
import math
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from controller.app import ApplicationError, ControllerApplication
from controller.crash_report import (
    WATCHDOG,
    degradation,
    rank_problem,
    watchdog_key,
)
from controller.operations import (
    RECIPE_ACTIONS,
    DeploymentRecord,
    DeploymentSpec,
    OperationAction,
    OperationRecord,
    OperationState,
    utc_now,
)
from controller.paths import write_private_file
from controller.resources import operation_deployment_id, safe_text

logger = logging.getLogger(__name__)

PROBE_SECONDS = 30.0
CONFIRM_PROBES = 3
CONFIRM_SECONDS = 60.0
# A crash report that failed (for example on an unreachable node) is
# repeated at most this often for the same deployment state.
REPORT_RETRY_SECONDS = 300.0
_SERVING_OWNERS = frozenset({OperationAction.LOAD, OperationAction.UPDATE})
_REPORTS = frozenset({OperationAction.CRASH_REPORT, OperationAction.QUALITY_REPORT})
_ACTIVE = frozenset({OperationState.QUEUED, OperationState.RUNNING})
_LEFT_NOTHING = frozenset({OperationState.FAILED, OperationState.CANCELLED})
_ACCEPTED = "vllm:spec_decode_num_accepted_tokens_total"
_DRAFTED = "vllm:spec_decode_num_draft_tokens_total"
_STARTED = "process_start_time_seconds"
# A Prometheus sample: name, optional labels, value and optional timestamp.
_SAMPLE_RE = re.compile(
    r"([A-Za-z_:][A-Za-z0-9_:]*)(?:\{.*\})?[ \t]+(\S+)(?:[ \t]+\S+)?"
)


def desktop_notify(title: str, message: str) -> None:
    """Log the event and show it as a macOS notification where possible."""
    logger.warning("%s: %s", title, message)
    osascript = shutil.which("osascript")
    if osascript is None:
        return
    # Not waited for: a slow notification must never hold up the watchdog.
    subprocess.Popen(  # noqa: S603 - fixed script; title and message are arguments
        [
            osascript,
            "-e",
            "on run argv",
            "-e",
            "display notification (item 2 of argv) with title (item 1 of argv)",
            "-e",
            "end run",
            title,
            message,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@dataclass
class _Suspect:
    """Failed probes of one deployment since its owner last changed."""

    owner_id: str
    since: float
    first_failed_at: str
    failures: int = 0


@dataclass(frozen=True)
class _Counters:
    """Speculative decoding totals of one serving process."""

    started: float | None
    accepted: float
    drafted: float

    def restarted_since(self, earlier: _Counters) -> bool:
        return (
            self.started != earlier.started
            or self.accepted < earlier.accepted
            or self.drafted < earlier.drafted
        )


@dataclass
class _Quality:
    """Draft acceptance windows of one deployment since its owner last changed."""

    owner_id: str
    since: float
    counters: _Counters
    low: list[dict[str, Any]] = field(default_factory=list)
    acceptance_percent: float | None = None


@dataclass(frozen=True)
class _Status:
    state: str
    owner: OperationRecord
    not_before: datetime | None = None


class DeploymentWatchdog:
    """Detect crashed or degraded deployments; queue reports and recoveries."""

    def __init__(
        self,
        *,
        app: Callable[[], ControllerApplication | None],
        state_path: Path,
        monotonic: Callable[[], float] = time.monotonic,
        notify: Callable[[str, str], None] = desktop_notify,
    ) -> None:
        self._app = app
        self._state_path = state_path
        self._monotonic = monotonic
        self._notify = notify
        self._mutex = threading.Lock()
        self._paused = _read_paused(state_path)
        self._suspects: dict[str, _Suspect] = {}
        self._quality: dict[str, _Quality] = {}
        self._announced: set[str] = set()
        # Outcomes that finished before this process started were announced
        # by the process that saw them.
        self._started_at = utc_now()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="dgx-watchdog", daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def paused(self) -> bool:
        with self._mutex:
            return self._paused

    def save(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Pause or resume the watchdog; the switch survives a restart."""
        if set(body) != {"paused"} or not isinstance(body["paused"], bool):
            raise ApplicationError(
                "invalid_request",
                "Send only a boolean paused field.",
                'Send {"paused": true} or {"paused": false}.',
            )
        with self._mutex:
            write_private_file(
                self._state_path,
                json.dumps({"schema_version": 1, "paused": body["paused"]}) + "\n",
                prefix=".watchdog-",
            )
            self._paused = body["paused"]
            self._suspects.clear()
            self._quality.clear()
        return self.overview()

    def overview(self) -> dict[str, Any]:
        """The pause switch and every watched or recovering deployment."""
        app = self._app()
        items = []
        if app is not None:
            busy = {
                operation_deployment_id(operation)
                for operation in app.journal.list_operations(states=list(_ACTIVE))
            }
            for record in app.journal.list_deployments():
                status = self._status(app, record)
                if record.profile_id not in app.profiles or (
                    status is None and record.id not in busy
                ):
                    continue
                with self._mutex:
                    suspect = self._suspects.get(record.id)
                    quality = self._quality.get(record.id)
                profile = app.profiles[record.profile_id]
                watch = profile.quality_watch
                schedule = (
                    (status.owner.result or {}).get("auto_recovery") or {}
                    if status
                    else {}
                )
                state = (
                    "busy"
                    if record.id in busy or status is None
                    else "failing"
                    if status.state == "watching" and suspect
                    else status.state
                )
                items.append(
                    {
                        "deployment_id": record.id,
                        "profile_id": record.profile_id,
                        "auto_recover": profile.auto_recover.enabled,
                        "quality_watch": {
                            "min_acceptance_percent": watch.min_acceptance_percent,
                            "consecutive_windows": watch.consecutive_windows,
                            "recover": watch.recover,
                            "acceptance_percent": quality.acceptance_percent
                            if quality
                            else None,
                            "low_windows": len(quality.low) if quality else 0,
                        }
                        if watch.enabled
                        else None,
                        "degraded": status is not None
                        and status.owner.action is OperationAction.QUALITY_REPORT,
                        "state": state,
                        "failed_probes": suspect.failures if suspect else 0,
                        "operation_id": record.owning_operation_id,
                        "attempt": schedule.get("attempt"),
                        "max_attempts": schedule.get("max_attempts"),
                        "next_attempt_at": schedule.get("not_before")
                        if state == "recovery-scheduled"
                        else None,
                        "message": safe_text(status.owner.error_message)
                        if status and state == "needs-attention"
                        else None,
                    }
                )
        return {"paused": self.paused(), "deployments": items}

    def tick(self) -> None:
        """Probe watched deployments and queue at most one operation."""
        app = self._app()
        if app is None or self.paused() or not app.lifecycle_idle():
            with self._mutex:
                self._suspects.clear()
                self._quality.clear()
            return
        for record in app.journal.list_deployments():
            status = self._status(app, record)
            if status is None or status.state != "watching":
                with self._mutex:
                    self._suspects.pop(record.id, None)
                    self._quality.pop(record.id, None)
            if status is None:
                continue
            self._announce(app, record, status.owner)
            if status.state == "watching" and self._probe(app, record, status.owner):
                return
            if status.state == "report-due" and self._may_report(app, status.owner):
                spec = DeploymentSpec.from_dict(record.spec)
                if all(
                    observation.reachable
                    for observation in app.refresh_observations(spec.node_ids).values()
                ):
                    # A degradation is decided again on the same windows under
                    # the current floor; any other follow-up is a recovery
                    # attempt that did not serve.
                    owner = status.owner
                    if owner.action is OperationAction.QUALITY_REPORT:
                        watch = app.profiles[record.profile_id].quality_watch
                        app.submit_report(
                            owner.action,
                            record.id,
                            owner.id,
                            {
                                "min_acceptance_percent": watch.min_acceptance_percent,
                                "windows": owner.request["windows"],
                            },
                        )
                    else:
                        app.submit_report(
                            OperationAction.CRASH_REPORT, record.id, owner.id, {}
                        )
                    return
            if (
                status.state == "recovery-scheduled"
                and status.not_before
                and (status.not_before <= datetime.now(UTC))
            ):
                app.submit_recovery(status.owner.id, lambda: not self.paused())
                return

    def _status(
        self, app: ControllerApplication, record: DeploymentRecord
    ) -> _Status | None:
        """Where the watchdog stands with one deployment, or ``None``."""
        owner = (
            app.journal.get_operation(record.owning_operation_id)
            if record.owning_operation_id
            else None
        )
        if (
            owner is None
            or record.profile_id not in app.profiles
            or not set(record.node_ids).issubset(app.nodes)
            or owner.state in _ACTIVE
        ):
            return None
        result = owner.result or {}
        left_serving = owner.state is OperationState.SUCCEEDED and (
            owner.action in _SERVING_OWNERS
            or (owner.action is OperationAction.CRASH_REPORT and not result["crashed"])
            or (
                owner.action is OperationAction.QUALITY_REPORT
                and "auto_recovery" not in result
            )
        )
        # A rolled-back switch, or an update that failed after its restore,
        # leaves a deployment it proved ready owned by the failed operation.
        if record.desired_state == "ready" and (
            left_serving
            or (
                record.observed_state == "ready"
                and owner.action in _SERVING_OWNERS
                and owner.requester != WATCHDOG
            )
        ):
            return _Status("watching", owner)
        if owner.requester != WATCHDOG:
            return None
        if owner.state is OperationState.NEEDS_ATTENTION:
            return _Status("needs-attention", owner)
        if _foreign_since(app, record, owner):
            return _Status("superseded", owner)
        if owner.action in _REPORTS:
            schedule = result.get("auto_recovery")
            if owner.state is not OperationState.SUCCEEDED or not schedule:
                return None
            attempt = watchdog_key(owner.id, "recover")
            # The catalog may have changed since the report decided, and a
            # recovery that did not start or failed before changing anything
            # leaves the deployment to its report.
            if schedule["catalog"] != app.crash_reporter.catalog or any(
                operation.idempotency_key == attempt
                for operation in app.journal.list_operations()
            ):
                return _Status("report-due", owner)
            return _Status(
                "recovery-scheduled",
                owner,
                datetime.fromisoformat(str(schedule["not_before"])),
            )
        # A recovery attempt that ended without a serving load.
        return _Status("report-due", owner)

    def _probe(
        self,
        app: ControllerApplication,
        record: DeploymentRecord,
        owner: OperationRecord,
    ) -> bool:
        """Probe one watched deployment; return whether a report was queued."""
        spec = DeploymentSpec.from_dict(record.spec)
        observations = app.refresh_observations(spec.node_ids)
        problems = {
            assignment.node_id: problem
            for assignment in spec.assignments
            if (
                problem := rank_problem(
                    observations[assignment.node_id], assignment, spec
                )
            )
        }
        if not problems:
            with self._mutex:
                self._suspects.pop(record.id, None)
            return self._watch_quality(app, record, owner, spec)
        with self._mutex:
            # A network loss is no evidence of a crash, and recovery needs
            # every node anyway.
            if not all(observation.reachable for observation in observations.values()):
                self._suspects.pop(record.id, None)
                return False
            now = self._monotonic()
            suspect = self._suspects.get(record.id)
            if suspect is None or suspect.owner_id != owner.id:
                suspect = _Suspect(owner.id, now, utc_now())
                self._suspects[record.id] = suspect
            suspect.failures += 1
            if suspect.failures < CONFIRM_PROBES or now - suspect.since < (
                CONFIRM_SECONDS
            ):
                return False
            del self._suspects[record.id]
        if not self._may_report(app, owner):
            return False
        app.submit_report(
            OperationAction.CRASH_REPORT,
            record.id,
            owner.id,
            {
                "failed_probes": suspect.failures,
                "first_failed_at": suspect.first_failed_at,
                "problems": problems,
            },
        )
        return True

    def _watch_quality(
        self,
        app: ControllerApplication,
        record: DeploymentRecord,
        owner: OperationRecord,
        spec: DeploymentSpec,
    ) -> bool:
        """Close a draft acceptance window when it is due.

        Returns whether a quality report was queued.
        """
        watch = app.profiles[record.profile_id].quality_watch
        if not watch.enabled:
            return False
        now = self._monotonic()
        with self._mutex:
            quality = self._quality.get(record.id)
        if (
            quality is not None
            and quality.owner_id == owner.id
            and now - quality.since < watch.window_s
        ):
            return False
        serving = next(item for item in spec.assignments if not item.is_worker)
        lines = app.transport.metrics(serving.node_id, spec)
        counters = None if lines is None else _counters(lines)
        if counters is None:
            # The window stays open until the counters can be read again.
            return False
        with self._mutex:
            # A pause may have cleared the window while the counters were read.
            quality = self._quality.get(record.id)
            if (
                quality is None
                or quality.owner_id != owner.id
                or counters.restarted_since(quality.counters)
            ):
                self._quality[record.id] = _Quality(owner.id, now, counters)
                return False
            accepted = counters.accepted - quality.counters.accepted
            drafted = counters.drafted - quality.counters.drafted
            window = {
                "ended_at": utc_now(),
                "seconds": round(now - quality.since),
                "accepted_tokens": int(accepted),
                "drafted_tokens": int(drafted),
            }
            quality.since, quality.counters = now, counters
            if drafted < watch.min_draft_tokens:
                return False
            # Rounded down, so a window shown at the floor is never below it.
            quality.acceptance_percent = math.floor(1000 * accepted / drafted) / 10
            if 100 * accepted >= watch.min_acceptance_percent * drafted:
                quality.low.clear()
                return False
            quality.low.append(
                {**window, "acceptance_percent": quality.acceptance_percent}
            )
            if len(quality.low) < watch.consecutive_windows:
                return False
            windows = quality.low
            del self._quality[record.id]
        if not self._may_report(app, owner):
            return False
        app.submit_report(
            OperationAction.QUALITY_REPORT,
            record.id,
            owner.id,
            {
                "min_acceptance_percent": watch.min_acceptance_percent,
                "windows": windows,
            },
        )
        return True

    @staticmethod
    def _may_report(app: ControllerApplication, owner: OperationRecord) -> bool:
        """Whether no report on the same deployment state failed recently."""
        for operation in app.journal.list_operations():
            if (
                operation.action in _REPORTS
                and operation.request.get("after_operation_id") == owner.id
            ):
                age = datetime.now(UTC) - datetime.fromisoformat(operation.updated_at)
                return (
                    operation.state is not OperationState.FAILED
                    or age.total_seconds() >= REPORT_RETRY_SECONDS
                )
        return True

    def _announce(
        self,
        app: ControllerApplication,
        record: DeploymentRecord,
        owner: OperationRecord,
    ) -> None:
        """Notify once about a crash, a recovery, or an end needing attention."""
        if (
            owner.requester != WATCHDOG
            or owner.id in self._announced
            or owner.updated_at < self._started_at
        ):
            return
        name = app.profiles[record.profile_id].display_name
        result = owner.result or {}
        if owner.state is OperationState.NEEDS_ATTENTION:
            title, message = f"{name} needs attention", owner.error_message
        elif owner.action is OperationAction.LOAD and owner.state is (
            OperationState.SUCCEEDED
        ):
            title, message = f"{name} is serving again", "Automatic recovery finished."
        elif owner.action is OperationAction.CRASH_REPORT and result.get("crashed"):
            title = f"{name} stopped serving"
            message = (
                ", ".join(
                    f"{rank['node_id']}: {rank['problem']}"
                    for rank in result["ranks"]
                    if rank["problem"]
                )
                + f". Automatic recovery {result['auto_recovery']['attempt']} of "
                f"{result['auto_recovery']['max_attempts']} is scheduled."
            )
        elif owner.action is OperationAction.QUALITY_REPORT and result.get(
            "auto_recovery"
        ):
            title = f"{name} output is degraded"
            message = (
                f"It {degradation(owner.request)}. Automatic recovery "
                f"{result['auto_recovery']['attempt']} of "
                f"{result['auto_recovery']['max_attempts']} is scheduled."
            )
        else:
            return
        self._announced.add(owner.id)
        self._notify(title, safe_text(message))

    def _loop(self) -> None:
        while not self._stop.wait(PROBE_SECONDS):
            try:
                self.tick()
            except Exception:
                # The watchdog must outlive one bad tick (e.g. a reload).
                logger.exception("watchdog tick failed")


def _foreign_since(
    app: ControllerApplication, record: DeploymentRecord, since: OperationRecord
) -> bool:
    """Whether an operator's operation used the nodes after ``since``.

    Reconciliation only reads, and an operation that failed or was cancelled
    left the nodes as it found them.
    """
    nodes = set(record.node_ids)
    for operation in app.journal.list_operations():
        if operation.id == since.id:
            return False
        if (
            operation.action in RECIPE_ACTIONS
            or operation.action is OperationAction.RECONCILE
            or operation.state in _LEFT_NOTHING
            or (
                operation.requester == WATCHDOG
                and operation_deployment_id(operation) == record.id
            )
        ):
            continue
        if nodes.intersection(operation.node_ids):
            return True
    return False


def _read_paused(path: Path) -> bool:
    """The saved pause switch; an unreadable file keeps the watchdog paused."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return False
    except (OSError, ValueError):
        logger.warning("watchdog state %s is unreadable, so it stays paused", path)
        return True
    return not isinstance(value, Mapping) or value.get("paused") is not False


def _counters(lines: Sequence[str]) -> _Counters | None:
    """The acceptance counters summed over their label sets, if both exist."""
    totals: dict[str, float] = {}
    for line in lines:
        match = _SAMPLE_RE.fullmatch(line.strip())
        if match is None or match[1] not in (_ACCEPTED, _DRAFTED, _STARTED):
            continue
        try:
            value = float(match[2])
        except ValueError:
            continue
        if math.isfinite(value):
            totals[match[1]] = totals.get(match[1], 0.0) + value
    if _ACCEPTED not in totals or _DRAFTED not in totals:
        return None
    return _Counters(totals.get(_STARTED), totals[_ACCEPTED], totals[_DRAFTED])
