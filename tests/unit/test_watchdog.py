# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Crash and degradation detection, reports and automatic recovery."""

from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from harness import wait_until

from controller.app import ControllerApplication
from controller.config import load_nodes, load_profiles
from controller.crash_report import rank_problem
from controller.domain import (
    AutoRecovery,
    Backend,
    DeploymentMode,
    ModelProfile,
    NodeSelector,
    QualityWatch,
)
from controller.operations import (
    DeploymentSpec,
    NodeAssignment,
    OperationAction,
    OperationRecord,
    OperationState,
)
from controller.persistence import OperationJournal
from controller.transport.base import CommandResult, NodeObservation, UnitState
from controller.transport.fake import FakeNode, FakeNodeTransport
from controller.watchdog import CONFIRM_PROBES, PROBE_SECONDS, DeploymentWatchdog

ROOT = Path(__file__).resolve().parents[2]
NODES = load_nodes(ROOT / "config" / "examples" / "nodes.cluster.example.json")
MIMO = next(iter(load_profiles(ROOT / "config" / "examples" / "profiles")))
HEAD, WORKER = "spark-a", "spark-b"
DEPLOYMENT = "dep-" + MIMO.profile_id
OTHER = replace(
    MIMO,
    profile_id="other-model",
    api_alias="other-alias",
    readiness=replace(MIMO.readiness, expected_alias="other-alias"),
)
ACCEPTED = 'vllm:spec_decode_num_accepted_tokens_total{engine="0",model_name="m"}'
DRAFTED = 'vllm:spec_decode_num_draft_tokens_total{engine="0",model_name="m"}'
STARTED = "process_start_time_seconds"
# A window lasts two probes and counts from 1,000 drafted tokens.
WATCH = QualityWatch(
    enabled=True,
    min_acceptance_percent=12,
    window_s=2 * int(PROBE_SECONDS),
    consecutive_windows=2,
    min_draft_tokens=1_000,
)
RECOVERING = replace(WATCH, recover=True, hold_s=0)
XID = "kernel: NVRM: Xid (PCI:0000:01:00): 31, pid=7, name=VLLM::Worker"


def exit_container(transport: FakeNodeTransport, node_id: str) -> None:
    """Make one rank's container exit while the other ranks keep running."""
    node = transport.node(node_id)
    node.active_deployment_id = None
    node.active_alias = None
    node.active_memory_bytes = 0
    node.managed = False
    node.unit_state = UnitState.STOPPED
    node.endpoint_healthy = False


class WatchdogTests(unittest.TestCase):
    def build(
        self,
        policy: AutoRecovery | None = None,
        *extra: ModelProfile,
        quality: QualityWatch | None = None,
    ) -> None:
        self.profile = replace(
            MIMO,
            auto_recover=policy or AutoRecovery(),
            quality_watch=quality or QualityWatch(),
        )
        self.journal = OperationJournal()
        self.transport = FakeNodeTransport(
            {
                node.node_id: FakeNode(
                    node.node_id, baseline_free_memory_bytes=128 * 1024**3
                )
                for node in NODES
            }
        )
        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        self.home = Path(home.name)
        self.app = self.application(self.profile, *extra)
        self.clock = 0.0
        self.notes: list[str] = []
        self.watchdog = self.make_watchdog()
        self.load(self.profile.profile_id, "load-1")

    def application(self, *profiles: ModelProfile) -> ControllerApplication:
        app = ControllerApplication(
            nodes=NODES,
            profiles=profiles,
            journal=self.journal,
            transport=self.transport,
            start_reconciliation=False,
            crash_reports_dir=self.home / "logs" / "crash-reports",
        )
        self.addCleanup(app.close)
        return app

    def make_watchdog(self) -> DeploymentWatchdog:
        return DeploymentWatchdog(
            app=lambda: self.app,
            state_path=self.home / "watchdog.json",
            monotonic=lambda: self.clock,
            notify=lambda title, _message: self.notes.append(title),
        )

    def load(self, profile_id: str, key: str) -> None:
        self.app.submit_load(
            {"profile_id": profile_id, "node_ids": [HEAD, WORKER]},
            idempotency_key=key,
        )
        self.settle()

    def settle(self) -> None:
        wait_until(self, self.app.lifecycle_idle)

    def tick(self, times: int = 1) -> None:
        for _ in range(times):
            self.watchdog.tick()
            self.settle()
            self.clock += PROBE_SECONDS

    def crash(self) -> OperationRecord:
        """Exit the head container and let the watchdog confirm the crash."""
        exit_container(self.transport, HEAD)
        return self.confirm()

    def confirm(self) -> OperationRecord:
        self.tick(CONFIRM_PROBES)
        return self.operations(OperationAction.CRASH_REPORT)[0]

    def operations(self, action: OperationAction) -> list[OperationRecord]:
        return [
            operation
            for operation in self.journal.list_operations()
            if operation.action is action
        ]

    def watch(
        self, quality: QualityWatch = WATCH, policy: AutoRecovery | None = None
    ) -> None:
        """Serve with the quality watch on and take its first counter reading."""
        self.build(policy, quality=quality)
        self.transport.node(HEAD).metrics.update(
            {STARTED: 1.0, ACCEPTED: 0.0, DRAFTED: 0.0}
        )
        self.tick()

    def window(self, accepted: int, drafted: int) -> None:
        """Generate one window's draft tokens on the head and close the window."""
        metrics = self.transport.node(HEAD).metrics
        metrics[ACCEPTED] += accepted
        metrics[DRAFTED] += drafted
        self.tick(2)

    def degrade(self) -> None:
        """Close two windows below the floor, which trips the watch."""
        self.window(50, 1_000)
        self.window(20, 1_000)

    def quality(self) -> dict:
        return self.watchdog.overview()["deployments"][0]["quality_watch"]

    def test_failed_probes_that_do_not_last_are_not_a_crash(self) -> None:
        self.build(AutoRecovery(enabled=True))
        exit_container(self.transport, HEAD)
        self.tick(CONFIRM_PROBES - 1)
        self.transport.seed_deployment(
            HEAD, deployment_id=DEPLOYMENT, alias=MIMO.api_alias
        )
        self.tick()
        exit_container(self.transport, HEAD)
        self.tick(CONFIRM_PROBES - 1)

        self.assertEqual([], self.operations(OperationAction.CRASH_REPORT))
        self.assertEqual(
            "ready", self.journal.get_deployment(DEPLOYMENT).observed_state
        )

    def test_failed_probes_must_span_a_minute(self) -> None:
        self.build(AutoRecovery(enabled=True))
        exit_container(self.transport, HEAD)
        for _probe in range(4):
            self.watchdog.tick()
            self.clock += 15
        self.assertEqual([], self.operations(OperationAction.CRASH_REPORT))
        self.watchdog.tick()
        self.settle()

        self.assertEqual(1, len(self.operations(OperationAction.CRASH_REPORT)))

    def test_an_unreachable_node_is_not_counted_as_a_crash(self) -> None:
        self.build(AutoRecovery(enabled=True))
        self.transport.node(HEAD).reachable = False
        self.tick(CONFIRM_PROBES + 2)

        self.assertEqual([], self.operations(OperationAction.CRASH_REPORT))

    def test_crash_without_auto_recover_is_reported_and_left_alone(self) -> None:
        self.build()
        report = self.crash()
        self.tick(2)

        self.assertIs(OperationState.NEEDS_ATTENTION, report.state)
        self.assertEqual("deployment_crashed", report.error_code)
        self.assertEqual([], self.operations(OperationAction.RECOVER))
        self.assertEqual(1, len(self.operations(OperationAction.LOAD)))
        deployment = self.journal.get_deployment(DEPLOYMENT)
        self.assertEqual(
            ("ready", "failed", report.id),
            (
                deployment.desired_state,
                deployment.observed_state,
                deployment.owning_operation_id,
            ),
        )
        self.assertEqual([f"{MIMO.display_name} needs attention"], self.notes)

    def test_crash_report_keeps_every_rank_with_redacted_log_lines(self) -> None:
        self.build()
        self.transport.node(WORKER)._logs.append(
            "HF_TOKEN=hf_secret Authorization: Bearer abc123"
        )
        self.transport.node(HEAD).kernel_log.append(XID)
        resource = self.app.get_operation(self.crash().id)
        self.assertEqual("watchdog", resource["requester"])
        self.assertEqual(
            [
                {
                    "node_id": HEAD,
                    "role": "head",
                    "problem": "process or container gone",
                    "exit_code": None,
                },
                {"node_id": WORKER, "role": "worker", "problem": "", "exit_code": None},
            ],
            resource["result"]["ranks"],
        )
        text = (self.home / "logs" / resource["result"]["support_bundle"]).read_text()
        bundle = json.loads(text)
        self.assertEqual(CONFIRM_PROBES, bundle["trigger"]["failed_probes"])
        self.assertIn(
            f"started worker for {DEPLOYMENT}", bundle["ranks"][1]["log_tail"]
        )
        self.assertEqual([XID], bundle["ranks"][0]["kernel_faults"])
        self.assertIsNone(bundle["metrics"])
        self.assertIn(
            f"Last kernel fault line: {XID}",
            next(
                event["message"]
                for event in resource["events"]
                if event["code"] == "rank_diagnostics"
            ),
        )
        self.assertNotIn("hf_secret", text)
        self.assertNotIn("abc123", text)

    def test_a_foreign_workload_is_reported_and_nothing_is_stopped(self) -> None:
        self.build(AutoRecovery(enabled=True))
        exit_container(self.transport, HEAD)
        self.transport.seed_deployment(
            HEAD, deployment_id="tts", alias="tts", managed=False
        )
        report = self.confirm()
        self.tick(2)

        self.assertEqual("deployment_unrecognized", report.error_code)
        self.assertEqual([], self.operations(OperationAction.RECOVER))
        self.assertEqual("tts", self.transport.node(HEAD).active_deployment_id)

    def test_a_running_but_unhealthy_deployment_is_not_stopped(self) -> None:
        self.build(AutoRecovery(enabled=True))
        self.transport.node(HEAD).endpoint_healthy = False
        report = self.confirm()
        self.tick(2)

        self.assertEqual("deployment_unhealthy", report.error_code)
        self.assertEqual([], self.operations(OperationAction.RECOVER))

    def test_crash_is_recovered_with_recover_and_load_of_the_profile(self) -> None:
        self.build(AutoRecovery(enabled=True))
        report = self.crash()
        self.assertEqual(
            "crash_recovery_scheduled", self.journal.events(report.id)[-1].code
        )
        self.tick(2)

        recover = self.operations(OperationAction.RECOVER)[0]
        load = self.operations(OperationAction.LOAD)[0]
        self.assertEqual(
            [
                ("watchdog", f"watchdog:{report.id}:recover", OperationState.SUCCEEDED),
                ("watchdog", f"watchdog:{report.id}:load", OperationState.SUCCEEDED),
            ],
            [
                (operation.requester, operation.idempotency_key, operation.state)
                for operation in (recover, load)
            ],
        )
        self.assertTrue(load.result["completion_canary"]["passed"])
        deployment = self.journal.get_deployment(DEPLOYMENT)
        self.assertEqual(
            ("ready", load.id),
            (deployment.observed_state, deployment.owning_operation_id),
        )
        self.assertEqual(
            [
                f"{MIMO.display_name} stopped serving",
                f"{MIMO.display_name} is serving again",
            ],
            self.notes,
        )

    def test_a_deployment_restored_by_a_rollback_is_watched(self) -> None:
        self.build(AutoRecovery(enabled=True), OTHER)
        start = self.transport.start
        self.transport.start = lambda node_id, spec, assignment: (
            CommandResult(False, "simulated start failure", "start_failed")
            if spec.profile_id == OTHER.profile_id
            else start(node_id, spec, assignment)
        )
        self.load(OTHER.profile_id, "failed-switch")
        switch = self.operations(OperationAction.LOAD)[0]
        self.assertEqual(
            ("ready", switch.id),
            (
                self.journal.get_deployment(DEPLOYMENT).observed_state,
                self.journal.get_deployment(DEPLOYMENT).owning_operation_id,
            ),
        )
        report = self.crash()

        self.assertEqual(switch.id, report.request["after_operation_id"])
        self.assertIs(OperationState.SUCCEEDED, report.state)

    def test_a_model_reconciled_ready_after_an_unload_is_not_watched(self) -> None:
        self.build(AutoRecovery(enabled=True))
        self.app.submit_unload(DEPLOYMENT, idempotency_key="operator-unload")
        self.settle()
        for node_id in (HEAD, WORKER):
            self.transport.seed_deployment(
                node_id, deployment_id=DEPLOYMENT, alias=MIMO.api_alias
            )
        self.app.submit_reconcile(idempotency_key="operator-reconcile")
        self.settle()
        self.assertEqual("ready", self.journal.get_deployment(DEPLOYMENT).desired_state)
        exit_container(self.transport, HEAD)
        self.tick(CONFIRM_PROBES + 1)

        self.assertEqual([], self.operations(OperationAction.CRASH_REPORT))

    def test_pausing_during_a_recovery_skips_its_load(self) -> None:
        self.build(AutoRecovery(enabled=True))
        recover = self.transport.recover

        def pause_then_recover(spec: DeploymentSpec) -> CommandResult:
            self.watchdog.save({"paused": True})
            return recover(spec)

        self.transport.recover = pause_then_recover
        self.crash()
        self.tick(2)

        self.assertIs(
            OperationState.SUCCEEDED, self.operations(OperationAction.RECOVER)[0].state
        )
        self.assertEqual(1, len(self.operations(OperationAction.LOAD)))

    def test_recovery_does_not_start_once_an_operator_unloaded(self) -> None:
        self.build(AutoRecovery(enabled=True))
        report = self.crash()
        self.app.submit_unload(DEPLOYMENT, idempotency_key="operator-unload")
        self.settle()
        self.app.submit_recovery(report.id, lambda: True)
        self.settle()

        self.assertIs(
            OperationState.CANCELLED, self.operations(OperationAction.RECOVER)[0].state
        )
        self.assertEqual(
            "stopped", self.journal.get_deployment(DEPLOYMENT).desired_state
        )

    def test_a_deployment_ready_again_is_not_recovered(self) -> None:
        self.build(AutoRecovery(enabled=True))
        self.crash()
        self.transport.seed_deployment(
            HEAD, deployment_id=DEPLOYMENT, alias=MIMO.api_alias
        )
        self.tick(2)

        self.assertIs(
            OperationState.CANCELLED, self.operations(OperationAction.RECOVER)[0].state
        )
        self.assertEqual(1, len(self.operations(OperationAction.LOAD)))
        self.assertEqual(
            "watching", self.watchdog.overview()["deployments"][0]["state"]
        )

    def test_a_model_started_by_hand_during_the_backoff_is_not_stopped(self) -> None:
        self.build(AutoRecovery(enabled=True))
        self.crash()
        for node_id in (HEAD, WORKER):
            self.transport.seed_deployment(
                node_id, deployment_id=DEPLOYMENT, alias=MIMO.api_alias, managed=False
            )
        self.tick(2)

        self.assertIs(
            OperationState.CANCELLED, self.operations(OperationAction.RECOVER)[0].state
        )
        self.assertEqual(
            "deployment_unrecognized",
            self.operations(OperationAction.CRASH_REPORT)[0].error_code,
        )
        self.assertEqual(DEPLOYMENT, self.transport.node(HEAD).active_deployment_id)

    def test_recovery_does_not_start_while_paused(self) -> None:
        self.build(AutoRecovery(enabled=True))
        report = self.crash()
        self.app.submit_recovery(report.id, lambda: False)
        self.settle()

        self.assertIs(
            OperationState.CANCELLED, self.operations(OperationAction.RECOVER)[0].state
        )

    def test_probes_that_fail_while_reports_find_it_ready_end_the_watch(self) -> None:
        self.build(AutoRecovery(enabled=True))
        for _report in range(3):
            owner = self.journal.get_deployment(DEPLOYMENT).owning_operation_id
            self.app.submit_report(OperationAction.CRASH_REPORT, DEPLOYMENT, owner, {})
            self.settle()

        self.assertEqual(
            [None, None, "deployment_flapping"],
            [
                report.error_code
                for report in reversed(self.operations(OperationAction.CRASH_REPORT))
            ],
        )
        self.assertEqual(
            "needs-attention", self.watchdog.overview()["deployments"][0]["state"]
        )

    def test_a_failed_report_is_not_repeated_at_once(self) -> None:
        self.build(AutoRecovery(enabled=True))
        owner = self.journal.get_deployment(DEPLOYMENT).owning_operation_id
        self.transport.node(HEAD).reachable = False
        self.app.submit_report(OperationAction.CRASH_REPORT, DEPLOYMENT, owner, {})
        self.settle()
        self.transport.node(HEAD).reachable = True
        self.crash()

        self.assertEqual(
            [OperationState.FAILED],
            [report.state for report in self.operations(OperationAction.CRASH_REPORT)],
        )

    def test_an_unreachable_node_holds_back_the_next_report(self) -> None:
        self.build(AutoRecovery(enabled=True))
        self.crash()
        self.transport.node(HEAD).fail_start = True
        self.tick()
        self.transport.node(WORKER).reachable = False
        self.tick(5)

        self.assertIs(
            OperationState.FAILED, self.operations(OperationAction.LOAD)[0].state
        )
        self.assertEqual(1, len(self.operations(OperationAction.CRASH_REPORT)))

    def test_failed_load_is_retried_within_the_budget(self) -> None:
        self.build(AutoRecovery(enabled=True, backoff_s=0))
        self.crash()
        self.transport.node(HEAD).fail_start = True
        self.tick()
        self.transport.node(HEAD).fail_start = False
        self.tick(3)

        self.assertEqual(
            [OperationState.SUCCEEDED, OperationState.FAILED, OperationState.SUCCEEDED],
            [load.state for load in self.operations(OperationAction.LOAD)][:3],
        )
        self.assertEqual(2, len(self.operations(OperationAction.CRASH_REPORT)))
        self.assertEqual(
            "ready", self.journal.get_deployment(DEPLOYMENT).observed_state
        )

    def test_repeated_crashes_end_in_needs_attention(self) -> None:
        self.build(AutoRecovery(enabled=True, max_attempts=2, backoff_s=0))
        for _attempt in range(2):
            self.crash()
            self.tick()
        report = self.crash()
        self.tick(2)

        self.assertIs(OperationState.NEEDS_ATTENTION, report.state)
        self.assertEqual("crash_loop_detected", report.error_code)
        self.assertEqual(2, len(self.operations(OperationAction.RECOVER)))
        self.assertEqual(
            "failed", self.journal.get_deployment(DEPLOYMENT).observed_state
        )

    def test_second_attempt_waits_for_the_backoff(self) -> None:
        self.build(AutoRecovery(enabled=True, backoff_s=3600))
        self.crash()
        self.tick()
        report = self.crash()
        self.tick(2)

        not_before = datetime.fromisoformat(
            report.result["auto_recovery"]["not_before"]
        )
        self.assertGreater((not_before - datetime.now(UTC)).total_seconds(), 3500)
        self.assertEqual(1, len(self.operations(OperationAction.RECOVER)))
        self.assertEqual(
            "recovery-scheduled", self.watchdog.overview()["deployments"][0]["state"]
        )

    def test_a_schedule_is_decided_again_after_a_catalog_change(self) -> None:
        self.build(AutoRecovery(enabled=True))
        self.crash()
        self.app = self.application(replace(self.profile, revision=MIMO.revision + 1))
        self.tick()

        self.assertEqual(
            "profile_revision_changed",
            self.operations(OperationAction.CRASH_REPORT)[0].error_code,
        )
        self.assertEqual([], self.operations(OperationAction.RECOVER))

    def test_operator_work_on_the_nodes_after_a_report_stops_the_watchdog(self) -> None:
        self.build(AutoRecovery(enabled=True), OTHER)
        exit_container(self.transport, WORKER)
        self.crash()
        self.load(OTHER.profile_id, "operator-load")
        self.tick(2)

        self.assertEqual([], self.operations(OperationAction.RECOVER))
        self.assertEqual(
            "superseded",
            next(
                item["state"]
                for item in self.watchdog.overview()["deployments"]
                if item["deployment_id"] == DEPLOYMENT
            ),
        )

    def test_nothing_is_queued_while_paused_and_the_pause_persists(self) -> None:
        self.build(AutoRecovery(enabled=True))
        self.watchdog.save({"paused": True})
        exit_container(self.transport, HEAD)
        self.tick(CONFIRM_PROBES + 1)

        self.assertEqual([], self.operations(OperationAction.CRASH_REPORT))
        self.assertTrue(self.make_watchdog().paused())

    def test_nothing_is_queued_while_another_operation_holds_a_lease(self) -> None:
        self.build(AutoRecovery(enabled=True))
        other, _ = self.journal.create_or_get_operation(
            action=OperationAction.LOAD,
            idempotency_key="other-tool",
            request={},
            node_ids=(HEAD,),
        )
        self.journal.finish(
            other.id,
            state=OperationState.CANCELLED,
            code="operation_cancelled",
            message="Held by another tool.",
        )
        self.journal.acquire_locks(other.id, [HEAD], lease_seconds=600)
        exit_container(self.transport, HEAD)
        for _probe in range(CONFIRM_PROBES + 1):
            self.watchdog.tick()
            self.clock += PROBE_SECONDS

        self.assertEqual([], self.operations(OperationAction.CRASH_REPORT))

    def test_acceptance_above_the_floor_is_not_reported(self) -> None:
        self.watch()
        for _window in range(3):
            self.window(300, 1_000)

        self.assertEqual([], self.operations(OperationAction.QUALITY_REPORT))
        self.assertEqual(
            {
                "min_acceptance_percent": 12,
                "consecutive_windows": 2,
                "recover": False,
                "acceptance_percent": 30.0,
                "low_windows": 0,
            },
            self.quality(),
        )

    def test_low_acceptance_is_reported_and_left_alone(self) -> None:
        self.watch()
        self.window(50, 1_000)
        self.assertEqual(1, self.quality()["low_windows"])
        self.window(20, 1_000)
        self.tick()

        report = self.operations(OperationAction.QUALITY_REPORT)[0]
        self.assertEqual(
            ("watchdog", OperationState.NEEDS_ATTENTION, "output_degraded"),
            (report.requester, report.state, report.error_code),
        )
        self.assertIn(
            "below 12 % for 2 windows in a row (5 %, 2 %)", report.error_message
        )
        self.assertEqual([], self.operations(OperationAction.RECOVER))
        deployment = self.journal.get_deployment(DEPLOYMENT)
        self.assertEqual(
            ("ready", "failed", report.id),
            (
                deployment.desired_state,
                deployment.observed_state,
                deployment.owning_operation_id,
            ),
        )
        item = self.watchdog.overview()["deployments"][0]
        self.assertEqual(("needs-attention", True), (item["state"], item["degraded"]))
        self.assertEqual([f"{MIMO.display_name} needs attention"], self.notes)

    def test_a_quality_report_keeps_the_windows_metrics_and_kernel_faults(
        self,
    ) -> None:
        self.watch()
        self.transport.node(HEAD).kernel_log.append(XID)
        self.degrade()

        report = self.operations(OperationAction.QUALITY_REPORT)[0]
        bundle = json.loads(
            (self.home / "logs" / report.result["support_bundle"]).read_text()
        )
        self.assertEqual(
            [(50, 1_000, 5.0), (20, 1_000, 2.0)],
            [
                (
                    window["accepted_tokens"],
                    window["drafted_tokens"],
                    window["acceptance_percent"],
                )
                for window in bundle["trigger"]["windows"]
            ],
        )
        self.assertIn(f"{DRAFTED} 2000.0", bundle["metrics"])
        self.assertEqual([XID], bundle["ranks"][0]["kernel_faults"])

    def test_counters_that_cannot_be_read_keep_the_window_open(self) -> None:
        self.watch()
        self.window(50, 1_000)
        metrics = self.transport.node(HEAD).metrics
        counters = dict(metrics)
        metrics.clear()
        self.tick(2)
        metrics.update(counters)
        self.window(20, 1_000)

        self.assertEqual(1, len(self.operations(OperationAction.QUALITY_REPORT)))

    def test_a_load_starts_the_windows_over(self) -> None:
        self.watch()
        self.window(50, 1_000)
        self.load(self.profile.profile_id, "operator-load")
        self.window(20, 1_000)
        self.window(20, 1_000)

        self.assertEqual([], self.operations(OperationAction.QUALITY_REPORT))
        self.assertEqual(1, self.quality()["low_windows"])

    def test_a_window_above_the_floor_clears_the_low_count(self) -> None:
        self.watch()
        self.window(50, 1_000)
        self.window(300, 1_000)
        self.window(50, 1_000)

        self.assertEqual([], self.operations(OperationAction.QUALITY_REPORT))
        self.assertEqual(1, self.quality()["low_windows"])

    def test_windows_with_few_drafted_tokens_are_not_counted(self) -> None:
        self.watch()
        self.window(50, 1_000)
        self.window(5, 999)
        self.window(900, 999)
        self.assertEqual([], self.operations(OperationAction.QUALITY_REPORT))
        self.window(20, 1_000)

        self.assertEqual(1, len(self.operations(OperationAction.QUALITY_REPORT)))

    def test_a_restarted_server_starts_the_windows_over(self) -> None:
        for name, restart in (
            ("counters reset", {ACCEPTED: 0.0, DRAFTED: 0.0}),
            ("new process", {STARTED: 2.0, ACCEPTED: 5_000.0, DRAFTED: 50_000.0}),
        ):
            with self.subTest(name):
                self.watch()
                self.window(50, 1_000)
                self.transport.node(HEAD).metrics.update(restart)
                self.tick(2)
                self.window(20, 1_000)

                self.assertEqual([], self.operations(OperationAction.QUALITY_REPORT))
                self.assertEqual(1, self.quality()["low_windows"])

    def test_a_server_without_draft_counters_is_never_reported(self) -> None:
        self.build(quality=WATCH)
        self.tick(6)

        self.assertEqual([], self.operations(OperationAction.QUALITY_REPORT))
        self.assertIsNone(self.quality()["acceptance_percent"])

    def test_nothing_is_read_while_paused_and_resuming_starts_over(self) -> None:
        self.watch()
        self.window(50, 1_000)
        reads = self.transport.calls.count(("metrics", HEAD, DEPLOYMENT))
        self.watchdog.save({"paused": True})
        self.window(20, 1_000)
        self.assertEqual(
            reads, self.transport.calls.count(("metrics", HEAD, DEPLOYMENT))
        )
        self.watchdog.save({"paused": False})
        self.tick()
        self.window(20, 1_000)

        self.assertEqual([], self.operations(OperationAction.QUALITY_REPORT))
        self.assertEqual(1, self.quality()["low_windows"])

    def test_degraded_output_is_recovered_with_recover_and_load(self) -> None:
        self.watch(RECOVERING, AutoRecovery(enabled=True))
        self.degrade()
        report = self.operations(OperationAction.QUALITY_REPORT)[0]
        self.assertEqual(
            "quality_recovery_scheduled", self.journal.events(report.id)[-1].code
        )
        self.tick(2)

        recover = self.operations(OperationAction.RECOVER)[0]
        load = self.operations(OperationAction.LOAD)[0]
        self.assertEqual(
            [
                ("watchdog", f"watchdog:{report.id}:recover", OperationState.SUCCEEDED),
                ("watchdog", f"watchdog:{report.id}:load", OperationState.SUCCEEDED),
            ],
            [
                (operation.requester, operation.idempotency_key, operation.state)
                for operation in (recover, load)
            ],
        )
        deployment = self.journal.get_deployment(DEPLOYMENT)
        self.assertEqual(
            ("ready", load.id),
            (deployment.observed_state, deployment.owning_operation_id),
        )
        self.assertEqual(
            [
                f"{MIMO.display_name} output is degraded",
                f"{MIMO.display_name} is serving again",
            ],
            self.notes,
        )

    def test_the_hold_keeps_the_degraded_server_running(self) -> None:
        self.watch(replace(RECOVERING, hold_s=3_600), AutoRecovery(enabled=True))
        self.degrade()
        self.tick(2)

        report = self.operations(OperationAction.QUALITY_REPORT)[0]
        not_before = datetime.fromisoformat(
            report.result["auto_recovery"]["not_before"]
        )
        self.assertGreater((not_before - datetime.now(UTC)).total_seconds(), 3_500)
        self.assertEqual([], self.operations(OperationAction.RECOVER))
        self.assertEqual(DEPLOYMENT, self.transport.node(HEAD).active_deployment_id)
        item = self.watchdog.overview()["deployments"][0]
        self.assertEqual(
            ("recovery-scheduled", True), (item["state"], item["degraded"])
        )

    def test_a_quality_recovery_cancelled_by_a_lost_node_is_decided_again(
        self,
    ) -> None:
        self.watch(RECOVERING, AutoRecovery(enabled=True))
        self.degrade()
        self.transport.node(WORKER).reachable = False
        self.tick()
        cancelled = self.operations(OperationAction.RECOVER)[0]
        self.assertEqual(
            (OperationState.CANCELLED, "recovery_not_started"),
            (cancelled.state, self.journal.events(cancelled.id)[-1].code),
        )
        self.assertEqual(DEPLOYMENT, self.transport.node(HEAD).active_deployment_id)
        self.transport.node(WORKER).reachable = True
        self.tick(2)

        self.assertEqual([], self.operations(OperationAction.CRASH_REPORT))
        first, second = reversed(self.operations(OperationAction.QUALITY_REPORT))
        self.assertEqual(first.request["windows"], second.request["windows"])
        self.assertIs(
            OperationState.SUCCEEDED, self.operations(OperationAction.RECOVER)[0].state
        )
        self.assertEqual(
            "ready", self.journal.get_deployment(DEPLOYMENT).observed_state
        )

    def test_a_quality_schedule_is_decided_again_after_a_catalog_change(
        self,
    ) -> None:
        self.watch(replace(RECOVERING, hold_s=3_600), AutoRecovery(enabled=True))
        self.degrade()
        self.app = self.application(self.profile)
        self.tick()

        self.assertEqual([], self.operations(OperationAction.CRASH_REPORT))
        self.assertEqual(
            [OperationState.SUCCEEDED, OperationState.SUCCEEDED],
            [
                report.state
                for report in self.operations(OperationAction.QUALITY_REPORT)
            ],
        )
        self.assertEqual(
            "recovery-scheduled", self.watchdog.overview()["deployments"][0]["state"]
        )
        self.assertEqual([], self.operations(OperationAction.RECOVER))

    def test_degradation_after_a_spent_budget_needs_attention(self) -> None:
        self.watch(RECOVERING, AutoRecovery(enabled=True, max_attempts=1))
        self.degrade()
        self.tick(2)
        self.degrade()

        report = self.operations(OperationAction.QUALITY_REPORT)[0]
        self.assertEqual("crash_loop_detected", report.error_code)
        self.assertEqual(1, len(self.operations(OperationAction.RECOVER)))

    def test_an_exited_container_is_reported_with_its_exit_code(self) -> None:
        spec = DeploymentSpec(
            DEPLOYMENT, MIMO.profile_id, MIMO.api_alias, (NodeAssignment(HEAD, "head"),)
        )
        observation = NodeObservation(
            HEAD,
            reachable=True,
            free_memory_bytes=0,
            unit_state=UnitState.STOPPED,
            facts={"container_status": "exited", "container_exit_code": 137},
        )

        self.assertEqual(
            "container exited with exit code 137",
            rank_problem(observation, spec.assignments[0], spec),
        )

    def test_recovery_and_quality_watch_are_refused_for_llama_cpp(self) -> None:
        for name, policy, message in (
            ("auto_recover", {"auto_recover": AutoRecovery(enabled=True)}, "shared"),
            ("quality_watch", {"quality_watch": WATCH}, "vLLM's speculative"),
        ):
            with self.subTest(name), self.assertRaisesRegex(ValueError, message):
                replace(
                    MIMO,
                    profile_id="single",
                    backend=Backend.LLAMA_CPP,
                    deployment_mode=DeploymentMode.INDEPENDENT,
                    node_count=1,
                    node_selector=NodeSelector(allowed_node_ids=frozenset({HEAD})),
                    **policy,
                )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
