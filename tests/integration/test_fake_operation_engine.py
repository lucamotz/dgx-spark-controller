# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Fake-node integration scenarios mirroring the load/unload safety runbook."""

from __future__ import annotations

import unittest
from dataclasses import replace
from unittest.mock import patch

from controller.operations import DeploymentSpec, NodeAssignment, OperationState
from controller.orchestrator import OperationOrchestrator
from controller.persistence import OperationJournal
from controller.reconcile import Reconciler
from controller.transport.base import NodeObservation, UnitState
from controller.transport.fake import FakeNode, FakeNodeTransport


def spec(
    deployment_id: str,
    alias: str,
    *assignments: NodeAssignment,
    required_memory: int = 8,
    minimum_free: int = 0,
    post_stop_free: int = 0,
    startup_timeout: float = 0,
) -> DeploymentSpec:
    return DeploymentSpec(
        deployment_id=deployment_id,
        profile_id=f"profile-{deployment_id}",
        expected_alias=alias,
        assignments=tuple(assignments),
        profile={"required_memory_bytes": required_memory},
        minimum_free_memory_bytes=minimum_free,
        post_stop_free_memory_bytes=post_stop_free,
        startup_timeout_seconds=startup_timeout,
        stop_timeout_seconds=0,
    )


class FakeOperationEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.journal = OperationJournal()
        self.transport = FakeNodeTransport(
            {"node-a": FakeNode("node-a", baseline_free_memory_bytes=32)}
        )
        self.engine = OperationOrchestrator(
            self.journal, self.transport, poll_interval_seconds=0
        )

    def tearDown(self) -> None:
        self.journal.close()

    def test_idle_load_then_unload_proves_alias_and_memory_release(self) -> None:
        deployment = spec(
            "dep-a",
            "expected-model",
            NodeAssignment("node-a"),
            required_memory=12,
            post_stop_free=32,
        )

        loaded = self.engine.execute_load(deployment, idempotency_key="load-idle")
        self.assertEqual(OperationState.SUCCEEDED, loaded.state)
        observation = self.transport.observe("node-a")
        self.assertEqual(("expected-model",), observation.served_aliases)
        self.assertEqual(20, observation.free_memory_bytes)

        unloaded = self.engine.execute_unload("dep-a", idempotency_key="unload-idle")
        self.assertEqual(OperationState.SUCCEEDED, unloaded.state)
        observation = self.transport.observe("node-a")
        self.assertIsNone(observation.deployment_id)
        self.assertEqual(32, observation.free_memory_bytes)

    def test_exited_container_after_launcher_success_fails_readiness_and_rolls_back_promptly(  # noqa: E501
        self,
    ) -> None:
        class ExitedContainerTransport(FakeNodeTransport):
            def observe(self, node_id):
                observed = super().observe(node_id)
                if observed.deployment_id == "exited":
                    return replace(
                        observed,
                        unit_state=UnitState.STOPPED,
                        managed=False,
                        deployment_id=None,
                        endpoint_healthy=False,
                        active_alias=None,
                        served_aliases=(),
                        facts={
                            "launcher_failed": False,
                            "exited_deployment_id": "exited",
                            "container_status": "exited",
                            "container_exit_code": 137,
                        },
                    )
                return observed

        transport = ExitedContainerTransport(
            {"node-a": FakeNode("node-a", baseline_free_memory_bytes=32)}
        )
        clock = [0.0]
        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds

        engine = OperationOrchestrator(
            self.journal,
            transport,
            poll_interval_seconds=100,
            sleeper=sleep,
            monotonic=lambda: clock[0],
        )
        deployment = spec(
            "exited",
            "expected-model",
            NodeAssignment("node-a", "head"),
            post_stop_free=32,
            startup_timeout=600,
        )
        operation = engine.execute_load(
            deployment, idempotency_key="exited-after-launcher-success"
        )

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("cluster_container_exited", operation.error_code)
        self.assertEqual([], sleeps)
        self.assertIn(("stop", "node-a", "exited"), transport.calls)
        self.assertEqual(32, transport.observe("node-a").free_memory_bytes)

    def test_stopped_verification_ignores_stale_exit_evidence(self) -> None:
        class StaleExitTransport(FakeNodeTransport):
            def observe(self, node_id):
                return replace(
                    super().observe(node_id), facts={"exited_deployment_id": "fresh"}
                )

        transport = StaleExitTransport(
            {"node-a": FakeNode("node-a", baseline_free_memory_bytes=32)}
        )
        engine = OperationOrchestrator(self.journal, transport, poll_interval_seconds=0)
        deployment = spec(
            "fresh", "expected-model", NodeAssignment("node-a"), post_stop_free=32
        )

        loaded = engine.execute_load(
            deployment, idempotency_key="load-after-stale-exit"
        )
        self.assertEqual(OperationState.SUCCEEDED, loaded.state)
        unloaded = engine.execute_unload(
            "fresh", idempotency_key="unload-after-stale-exit"
        )
        self.assertEqual(OperationState.SUCCEEDED, unloaded.state)

    def test_exact_already_ready_placement_commits_without_remote_mutation(
        self,
    ) -> None:
        deployment = spec(
            "already-ready",
            "expected-model",
            NodeAssignment("node-a"),
            required_memory=12,
        )
        self.transport.seed_deployment(
            "node-a",
            deployment_id=deployment.deployment_id,
            alias=deployment.expected_alias,
            managed=True,
            memory_bytes=12,
        )
        calls_before_load = len(self.transport.calls)

        operation = self.engine.execute_load(
            deployment, idempotency_key="adopt-already-ready"
        )

        self.assertEqual(OperationState.SUCCEEDED, operation.state)
        self.assertTrue(operation.result["already_ready"])
        mutation_calls = [
            call
            for call in self.transport.calls[calls_before_load:]
            if call[0] in {"start", "stop", "drain"}
        ]
        self.assertEqual([], mutation_calls)
        self.assertEqual(
            "ready",
            self.journal.get_deployment(deployment.deployment_id).observed_state,
        )

    def test_switch_defers_static_memory_gate_until_managed_model_is_stopped(
        self,
    ) -> None:
        old = spec("old", "old-alias", NodeAssignment("node-a"), required_memory=19)
        self.assertEqual(
            OperationState.SUCCEEDED,
            self.engine.execute_load(old, idempotency_key="load-old").state,
        )
        self.assertEqual(13, self.transport.observe("node-a").free_memory_bytes)
        before_switch = len(self.transport.calls)

        # This would fail if preflight required 16 bytes while the old model still
        # occupies memory.  The post-stop guard still requires those 16 bytes.
        replacement = spec(
            "new",
            "new-alias",
            NodeAssignment("node-a"),
            required_memory=16,
            minimum_free=16,
            post_stop_free=16,
        )
        switched = self.engine.execute_load(replacement, idempotency_key="switch")

        self.assertEqual(OperationState.SUCCEEDED, switched.state)
        switch_calls = self.transport.calls[before_switch:]
        stop_index = switch_calls.index(("stop", "node-a", "old"))
        start_index = switch_calls.index(("start", "node-a", "new"))
        self.assertLess(stop_index, start_index)
        self.assertEqual("new", self.transport.observe("node-a").deployment_id)
        replaced = self.journal.get_deployment("old")
        self.assertEqual(
            ("stopped", "stopped"), (replaced.desired_state, replaced.observed_state)
        )

    def test_wrong_alias_despite_healthy_endpoint_fails_and_cleans_partial_start(
        self,
    ) -> None:
        self.transport.node("node-a").alias_override = "wrong-model"
        deployment = spec("wrong", "expected-model", NodeAssignment("node-a"))

        operation = self.engine.execute_load(deployment, idempotency_key="wrong-alias")

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("readiness_timeout", operation.error_code)
        self.assertIsNone(self.transport.observe("node-a").deployment_id)
        self.assertEqual("failed", self.journal.get_deployment("wrong").observed_state)

    def test_unmanaged_process_is_never_stopped_by_switch(self) -> None:
        self.transport.seed_deployment(
            "node-a",
            deployment_id="foreign",
            alias="foreign",
            managed=False,
            memory_bytes=8,
        )
        operation = self.engine.execute_load(
            spec("wanted", "wanted", NodeAssignment("node-a")), idempotency_key="unsafe"
        )

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("unmanaged_conflict", operation.error_code)
        self.assertFalse(
            any(call[0].startswith("stop") for call in self.transport.calls)
        )
        self.assertEqual("foreign", self.transport.observe("node-a").deployment_id)

    def test_partial_unload_ends_needs_attention(self) -> None:
        deployment = spec("dep-a", "alias", NodeAssignment("node-a"))
        self.engine.execute_load(deployment, idempotency_key="load-for-failed-unload")
        self.transport.node("node-a").fail_stop = True

        operation = self.engine.execute_unload("dep-a", idempotency_key="unload-fails")

        self.assertEqual(OperationState.NEEDS_ATTENTION, operation.state)
        self.assertEqual("unknown", self.journal.get_deployment("dep-a").observed_state)

    def test_unload_never_confuses_missing_ownership_with_stopped(self) -> None:
        deployment = spec("dep-a", "alias", NodeAssignment("node-a"), post_stop_free=32)
        self.engine.execute_load(deployment, idempotency_key="load-before-owner-loss")
        baseline = NodeObservation("node-a", True, 32, unit_state=UnitState.STOPPED)
        cases = (
            replace(baseline, unit_state=UnitState.RUNNING),
            replace(baseline, endpoint_healthy=True),
            replace(baseline, active_alias="alias"),
            replace(baseline, served_aliases=("alias",)),
        )
        for index, observed in enumerate(cases):
            with (
                self.subTest(observed=observed),
                patch.object(self.transport, "observe", return_value=observed),
            ):
                before = len(self.transport.calls)
                operation = self.engine.execute_unload(
                    "dep-a", idempotency_key=f"lost-owner-{index}"
                )
                self.assertEqual(OperationState.FAILED, operation.state)
                self.assertEqual("unmanaged_deployment", operation.error_code)
                self.assertEqual(
                    "ready", self.journal.get_deployment("dep-a").desired_state
                )
                self.assertFalse(
                    any(
                        call[0] in {"stop", "drain"}
                        for call in self.transport.calls[before:]
                    )
                )

    def test_already_absent_unload_still_requires_memory_recovery(self) -> None:
        deployment = spec("dep-a", "alias", NodeAssignment("node-a"), post_stop_free=32)
        self.engine.execute_load(
            deployment, idempotency_key="load-before-external-stop"
        )
        self.transport.stop("node-a", "dep-a", timeout_seconds=0)
        self.transport.node("node-a").active_memory_bytes = 31

        failed = self.engine.execute_unload(
            "dep-a", idempotency_key="absent-low-memory"
        )
        self.assertEqual(OperationState.FAILED, failed.state)
        self.assertEqual("memory_release_unverified", failed.error_code)
        self.transport.node("node-a").active_memory_bytes = 0
        succeeded = self.engine.execute_unload(
            "dep-a", idempotency_key="absent-recovered"
        )
        self.assertEqual(OperationState.SUCCEEDED, succeeded.state)
        self.assertTrue(succeeded.result["already_stopped"])

    def test_restart_does_not_complete_unload_with_alias_or_low_memory(self) -> None:
        deployment = spec("dep-a", "alias", NodeAssignment("node-a"), post_stop_free=32)
        self.engine.execute_load(deployment, idempotency_key="load-before-interruption")
        baseline = NodeObservation("node-a", True, 32, unit_state=UnitState.STOPPED)
        for index, observed in enumerate(
            (
                replace(baseline, served_aliases=("alias",)),
                replace(baseline, free_memory_bytes=1),
            )
        ):
            with (
                self.subTest(observed=observed),
                patch.object(self.transport, "observe", return_value=observed),
            ):
                operation, _ = self.engine.submit_unload(
                    "dep-a", idempotency_key=f"interrupted-unload-{index}"
                )
                self.journal.claim_operation(operation.id)
                report = Reconciler(self.journal, self.transport).reconcile()
                self.assertIn(operation.id, report.needs_attention_operations)

    def test_cluster_starts_worker_before_head(self) -> None:
        self.journal.close()
        self.journal = OperationJournal()
        self.transport = FakeNodeTransport(["worker", "head"])
        self.engine = OperationOrchestrator(
            self.journal, self.transport, poll_interval_seconds=0
        )
        deployment = spec(
            "cluster",
            "cluster-alias",
            NodeAssignment("head", "head"),
            NodeAssignment("worker", "worker"),
        )

        operation = self.engine.execute_load(deployment, idempotency_key="cluster-load")

        self.assertEqual(OperationState.SUCCEEDED, operation.state)
        starts = [call for call in self.transport.calls if call[0] == "start"]
        self.assertEqual(["worker", "head"], [call[1] for call in starts])

    def test_cluster_head_start_failure_rolls_back_the_started_worker(self) -> None:
        self.journal.close()
        self.journal = OperationJournal()
        self.transport = FakeNodeTransport(["worker", "head"])
        self.transport.node("head").fail_start = True
        self.engine = OperationOrchestrator(
            self.journal, self.transport, poll_interval_seconds=0
        )
        deployment = spec(
            "cluster-fail",
            "cluster-alias",
            NodeAssignment("head", "head"),
            NodeAssignment("worker", "worker"),
        )

        operation = self.engine.execute_load(
            deployment, idempotency_key="cluster-head-fails"
        )

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertIsNone(self.transport.observe("worker").deployment_id)
        self.assertIn(("stop", "worker", "cluster-fail"), self.transport.calls)

    def test_recovery_resets_an_exact_orphaned_placement(self) -> None:
        self.journal.close()
        self.journal = OperationJournal()
        self.transport = FakeNodeTransport(["worker", "head"])
        self.engine = OperationOrchestrator(
            self.journal, self.transport, poll_interval_seconds=0
        )
        deployment = spec(
            "cluster-orphan",
            "cluster-alias",
            NodeAssignment("head", "head"),
            NodeAssignment("worker", "worker"),
            post_stop_free=32,
        )
        for node_id in ("head", "worker"):
            self.transport.seed_deployment(
                node_id,
                deployment_id=deployment.deployment_id,
                alias=deployment.expected_alias,
                memory_bytes=8,
                managed=False,
                endpoint_healthy=False,
            )
        queued, _ = self.engine.submit_recover(
            deployment, idempotency_key="recover-orphan"
        )

        operation = self.engine.run(queued.id)

        self.assertEqual(OperationState.SUCCEEDED, operation.state)
        self.assertTrue(operation.result["recovered"])
        self.assertTrue(
            all(
                self.transport.observe(node_id).unit_state.value == "stopped"
                for node_id in ("head", "worker")
            )
        )

    def test_reconciliation_completes_interrupted_load_only_when_observation_proves_it(
        self,
    ) -> None:
        deployment = spec("recover", "recovered", NodeAssignment("node-a"))
        operation, _ = self.engine.submit_load(
            deployment, idempotency_key="interrupted"
        )
        self.journal.claim_operation(operation.id)
        self.transport.seed_deployment(
            "node-a", deployment_id="recover", alias="recovered", managed=True
        )

        report = Reconciler(self.journal, self.transport).reconcile()

        self.assertIn(operation.id, report.completed_operations)
        self.assertEqual(
            OperationState.SUCCEEDED, self.journal.get_operation(operation.id).state
        )

    def test_reconciliation_keeps_stopped_history_when_known_replacement_is_active(
        self,
    ) -> None:
        old = spec("old", "old-alias", NodeAssignment("node-a"))
        new = spec("new", "new-alias", NodeAssignment("node-a"))
        self.assertEqual(
            OperationState.SUCCEEDED,
            self.engine.execute_load(old, idempotency_key="load-old").state,
        )
        self.assertEqual(
            OperationState.SUCCEEDED,
            self.engine.execute_unload("old", idempotency_key="unload-old").state,
        )
        self.assertEqual(
            OperationState.SUCCEEDED,
            self.engine.execute_load(new, idempotency_key="load-new").state,
        )

        report = Reconciler(self.journal, self.transport).reconcile()

        self.assertEqual("stopped", self.journal.get_deployment("old").desired_state)
        self.assertEqual("stopped", self.journal.get_deployment("old").observed_state)
        self.assertEqual("ready", self.journal.get_deployment("new").observed_state)
        self.assertIn("old", report.stopped_deployments)
        self.assertNotIn("node-a", report.unmanaged_nodes)

    def test_reconciliation_keeps_stopped_history_unknown_for_unmanaged_replacement(
        self,
    ) -> None:
        old = spec("old", "old-alias", NodeAssignment("node-a"))
        self.assertEqual(
            OperationState.SUCCEEDED,
            self.engine.execute_load(old, idempotency_key="load-old").state,
        )
        self.assertEqual(
            OperationState.SUCCEEDED,
            self.engine.execute_unload("old", idempotency_key="unload-old").state,
        )
        self.transport.seed_deployment(
            "node-a", deployment_id="foreign", alias="foreign", managed=False
        )

        report = Reconciler(self.journal, self.transport).reconcile()

        self.assertEqual("unknown", self.journal.get_deployment("old").observed_state)
        self.assertIn("old", report.uncertain_deployments)
        self.assertIn("node-a", report.unmanaged_nodes)

    def test_reconciliation_recognizes_replaced_history_despite_stale_desired_ready(
        self,
    ) -> None:
        old = spec("old", "old-alias", NodeAssignment("node-a"))
        new = spec("new", "new-alias", NodeAssignment("node-a"))
        self.engine.execute_load(old, idempotency_key="load-old")
        self.engine.execute_load(new, idempotency_key="switch-new")
        # Journals written before switches recorded replaced deployments as
        # stopped still hold this stale history.
        self.journal.save_deployment(
            replace(
                self.journal.get_deployment("old"),
                desired_state="ready",
                observed_state="ready",
            )
        )
        report = Reconciler(self.journal, self.transport).reconcile()
        self.assertIn("old", report.stopped_deployments)
        self.assertEqual(("new",), report.ready_deployments)
        self.assertEqual((), report.uncertain_deployments)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
