# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Fake-node integration scenarios mirroring the load/unload safety runbook."""

from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from controller.operations import (
    DeploymentRecord,
    DeploymentSpec,
    NodeAssignment,
    OperationAction,
    OperationState,
)
from controller.orchestrator import OperationOrchestrator
from controller.persistence import OperationJournal
from controller.reconcile import Reconciler
from controller.resources import safe_text
from controller.transport.base import CommandResult, NodeObservation, UnitState
from controller.transport.fake import FakeNode, FakeNodeTransport
from controller.updates.engine import ClusterUpdateEngine
from controller.updates.nodeops import FakeUpdateNodeOps, UpdateNodeError
from controller.updates.relay import UpdateRelay
from controller.updates.settings import UpdateSettings

CANARY = {
    "enabled": True,
    "timeout_s": 120,
    "max_tokens": 16,
    "prompt": "Reply with OK.",
    "completion_path": "/v1/chat/completions",
}


def spec(
    deployment_id: str,
    alias: str,
    *assignments: NodeAssignment,
    required_memory: int = 8,
    minimum_free: int = 0,
    post_stop_free: int = 0,
    startup_timeout: float = 0,
    canary: dict | None = None,
) -> DeploymentSpec:
    profile: dict = {"required_memory_bytes": required_memory}
    if canary is not None:
        profile["completion_canary"] = canary
    return DeploymentSpec(
        deployment_id=deployment_id,
        profile_id=f"profile-{deployment_id}",
        expected_alias=alias,
        assignments=tuple(assignments),
        profile=profile,
        minimum_free_memory_bytes=minimum_free,
        post_stop_free_memory_bytes=post_stop_free,
        startup_timeout_seconds=startup_timeout,
        stop_timeout_seconds=0,
    )


class EngineTestCase(unittest.TestCase):
    """A fresh journal and an engine over one 32-byte fake node per test."""

    def setUp(self) -> None:
        self.journal = OperationJournal()
        self.addCleanup(self.journal.close)
        self.use_nodes({"node-a": FakeNode("node-a", baseline_free_memory_bytes=32)})

    def use_nodes(self, nodes: dict[str, FakeNode] | list[str]) -> None:
        """Replace the fake nodes; the engine polls them without sleeping."""
        self.transport = FakeNodeTransport(nodes)
        self.engine = OperationOrchestrator(
            self.journal, self.transport, poll_interval_seconds=0
        )


class FakeOperationEngineTests(EngineTestCase):
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
        self.use_nodes(["worker", "head"])
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
        self.use_nodes(["worker", "head"])
        self.transport.node("head").fail_start = True
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
        self.use_nodes(["worker", "head"])
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
        # The replacement also settles the stale intent.
        self.assertEqual("stopped", self.journal.get_deployment("old").desired_state)

    def test_reconciliation_keeps_intent_of_a_deployment_that_just_stopped(
        self,
    ) -> None:
        loaded = spec("loaded", "loaded-alias", NodeAssignment("node-a"))
        self.engine.execute_load(loaded, idempotency_key="load-loaded")
        # The model exits on its own; nothing else starts.
        crashed = self.transport.node("node-a")
        crashed.active_deployment_id = None
        crashed.active_alias = None
        crashed.active_memory_bytes = 0
        crashed.managed = False
        crashed.unit_state = UnitState.STOPPED
        crashed.endpoint_healthy = False
        report = Reconciler(self.journal, self.transport).reconcile()
        self.assertIn("loaded", report.stopped_deployments)
        # Nothing replaced it, so the operator's intent stays visible as drift.
        self.assertEqual("ready", self.journal.get_deployment("loaded").desired_state)


class CrossPlacementSwitchTests(EngineTestCase):
    """A switch replaces a deployment on every node it uses, never on part."""

    def setUp(self) -> None:
        super().setUp()
        self.use_nodes(
            {
                node_id: FakeNode(node_id, baseline_free_memory_bytes=32)
                for node_id in ("node-a", "node-b")
            }
        )
        self.cluster = spec(
            "tp",
            "tp-alias",
            NodeAssignment("node-a", "head"),
            NodeAssignment("node-b", "worker"),
            post_stop_free=32,
        )
        loaded = self.engine.execute_load(self.cluster, idempotency_key="load-tp")
        self.assertEqual(OperationState.SUCCEEDED, loaded.state)
        self.single = spec(
            "single", "single-alias", NodeAssignment("node-a"), post_stop_free=32
        )

    def _calls_since(self, index: int, *kinds: str) -> list[tuple[str, str, str]]:
        return [call for call in self.transport.calls[index:] if call[0] in kinds]

    def test_two_node_to_one_node_switch_stops_both_ranks_then_starts_one(
        self,
    ) -> None:
        before = len(self.transport.calls)

        switched = self.engine.execute_load(self.single, idempotency_key="switch")

        self.assertEqual(OperationState.SUCCEEDED, switched.state, switched.error_code)
        self.assertEqual(
            [
                ("drain", "node-a", "tp"),
                ("drain", "node-b", "tp"),
                ("stop", "node-a", "tp"),
                ("stop", "node-b", "tp"),
                ("start", "node-a", "single"),
            ],
            self._calls_since(before, "drain", "stop", "start"),
        )
        # Release is proven on the extra node before anything starts.
        calls = self.transport.calls[before:]
        verify_b = max(
            index
            for index, call in enumerate(calls)
            if call[:2] == ("observe", "node-b")
        )
        self.assertLess(verify_b, calls.index(("start", "node-a", "single")))
        self.assertNotIn("canary", [call[0] for call in calls])
        self.assertEqual("single", self.transport.observe("node-a").deployment_id)
        node_b = self.transport.observe("node-b")
        self.assertIsNone(node_b.deployment_id)
        self.assertEqual(32, node_b.free_memory_bytes)
        replaced = self.journal.get_deployment("tp")
        self.assertEqual(
            ("stopped", "stopped"), (replaced.desired_state, replaced.observed_state)
        )
        self.assertEqual("ready", self.journal.get_deployment("single").observed_state)
        self.assertEqual(["node-b"], switched.previous_placement["extra_node_ids"])
        self.assertEqual(
            ["node-a", "node-b"], switched.previous_placement["observed_node_ids"]
        )
        self.assertIn(
            "extending_switch_scope",
            [event.code for event in self.journal.events(switched.id)],
        )
        self.assertEqual({}, self.journal.active_locks())

    def test_failed_start_restores_every_rank_of_the_replaced_deployment(
        self,
    ) -> None:
        class FailingNewStart(FakeNodeTransport):
            def start(self, node_id, spec, assignment):
                if spec.deployment_id == "single":
                    self.calls.append(("start", node_id, spec.deployment_id))
                    return CommandResult(False, "simulated start failure", "boom")
                return super().start(node_id, spec, assignment)

        transport = FailingNewStart(self.transport.nodes)
        engine = OperationOrchestrator(self.journal, transport, poll_interval_seconds=0)

        operation = engine.execute_load(self.single, idempotency_key="switch-fails")

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("boom", operation.error_code)
        self.assertEqual("restored", operation.result["rollback"])
        restarts = [call for call in transport.calls if call[0] == "start"]
        self.assertEqual(
            [("start", "node-a", "single"), ("start", "node-b", "tp")],
            restarts[:2],
        )
        self.assertEqual(("start", "node-a", "tp"), restarts[2])
        for node_id in ("node-a", "node-b"):
            self.assertEqual("tp", transport.observe(node_id).deployment_id)
        self.assertEqual("ready", self.journal.get_deployment("tp").observed_state)
        self.assertEqual("failed", self.journal.get_deployment("single").observed_state)

    def test_lease_held_on_the_extra_node_refuses_before_any_mutation(self) -> None:
        other, _ = self.journal.create_or_get_operation(
            action=OperationAction.RECONCILE,
            idempotency_key="holds-node-b",
            request={"kind": "reconcile"},
            node_ids=["node-b"],
        )
        self.journal.acquire_locks(other.id, ["node-b"], cluster=False)
        before = len(self.transport.calls)

        operation = self.engine.execute_load(self.single, idempotency_key="blocked")

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("operation_locked", operation.error_code)
        self.assertEqual([], self._calls_since(before, "drain", "stop", "start"))
        self.assertIsNone(operation.previous_placement)
        for node_id in ("node-a", "node-b"):
            self.assertEqual("tp", self.transport.observe(node_id).deployment_id)
        self.assertEqual("ready", self.journal.get_deployment("tp").observed_state)
        self.assertEqual({"node:node-b": other.id}, self.journal.active_locks())

    def test_unmanaged_process_on_the_extra_node_is_refused_and_never_stopped(
        self,
    ) -> None:
        self.transport.seed_deployment(
            "node-b", deployment_id="foreign", alias="foreign", managed=False
        )
        before = len(self.transport.calls)

        operation = self.engine.execute_load(self.single, idempotency_key="unsafe")

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("unmanaged_conflict", operation.error_code)
        self.assertIn("node-b", operation.error_message)
        self.assertEqual([], self._calls_since(before, "drain", "stop", "start"))
        self.assertEqual("foreign", self.transport.observe("node-b").deployment_id)
        self.assertEqual("tp", self.transport.observe("node-a").deployment_id)

    def test_switch_refuses_when_replaced_deployment_cannot_stop_everywhere(
        self,
    ) -> None:
        other = spec("other", "other-alias", NodeAssignment("node-b"))
        self.transport.seed_deployment(
            "node-b", deployment_id="other", alias="other-alias", managed=True
        )
        record = self.journal.get_deployment("tp")
        self.journal.save_deployment(
            replace(
                record,
                id="other",
                profile_id=other.profile_id,
                expected_alias=other.expected_alias,
                node_ids=other.node_ids,
                spec=other.to_dict(),
            )
        )
        stray = replace(
            self.cluster,
            deployment_id="stray",
            assignments=(
                NodeAssignment("node-a", "head"),
                NodeAssignment("node-z", "worker"),
            ),
        )
        cases = {
            "owned by another deployment": None,
            "not configured": stray,
        }
        for name, replaced in cases.items():
            with self.subTest(name):
                if replaced is not None:
                    self.transport.seed_deployment(
                        "node-a", deployment_id="stray", alias="tp-alias"
                    )
                    self.journal.save_deployment(
                        DeploymentRecord(
                            id="stray",
                            profile_id=replaced.profile_id,
                            expected_alias=replaced.expected_alias,
                            node_ids=replaced.node_ids,
                            desired_state="ready",
                            observed_state="ready",
                            spec=replaced.to_dict(),
                            endpoint=None,
                            owning_operation_id=None,
                            created_at=record.created_at,
                            updated_at=record.updated_at,
                        )
                    )
                before = len(self.transport.calls)

                operation = self.engine.execute_load(
                    self.single, idempotency_key=f"partial-{name}"
                )

                self.assertEqual(OperationState.FAILED, operation.state)
                self.assertEqual("partial_cluster_conflict", operation.error_code)
                self.assertEqual(
                    [], self._calls_since(before, "drain", "stop", "start")
                )
                self.assertEqual({}, self.journal.active_locks())


class CompletionCanaryTests(EngineTestCase):
    def test_passing_canary_runs_after_readiness_and_records_text_free_evidence(
        self,
    ) -> None:
        self.transport.node("node-a").canary_finish_reason = "length"
        self.transport.node("node-a").canary_completion_tokens = 16
        deployment = spec(
            "dep-a", "alias", NodeAssignment("node-a"), canary=dict(CANARY)
        )

        operation = self.engine.execute_load(deployment, idempotency_key="canary-ok")

        self.assertEqual(OperationState.SUCCEEDED, operation.state)
        self.assertEqual(
            {
                "node_id": "node-a",
                "passed": True,
                "http_status": 200,
                "latency_ms": 1,
                "completion_tokens": 16,
                "finish_reason": "length",
            },
            operation.result["completion_canary"],
        )
        calls = self.transport.calls
        self.assertLess(
            calls.index(("start", "node-a", "dep-a")),
            calls.index(("canary", "node-a", "dep-a")),
        )
        events = self.journal.events(operation.id)
        codes = [event.code for event in events]
        self.assertLess(
            codes.index("verifying_readiness"), codes.index("completion_canary_passed")
        )
        self.assertLess(
            codes.index("completion_canary_passed"),
            codes.index("committing_deployment"),
        )
        recorded = repr([(event.message, event.details) for event in events])
        self.assertNotIn(CANARY["prompt"], recorded)
        self.assertNotIn(CANARY["prompt"], repr(operation.result))

    def test_failing_canary_fails_readiness_and_rolls_back_the_switch(self) -> None:
        old = spec("old", "old-alias", NodeAssignment("node-a"), post_stop_free=32)
        self.assertEqual(
            OperationState.SUCCEEDED,
            self.engine.execute_load(old, idempotency_key="load-old").state,
        )
        self.transport.node("node-a").canary_completion_tokens = 0
        new = spec(
            "new",
            "new-alias",
            NodeAssignment("node-a"),
            post_stop_free=32,
            canary=dict(CANARY),
        )

        operation = self.engine.execute_load(new, idempotency_key="canary-fails")

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("completion_canary_failed", operation.error_code)
        self.assertEqual("restored", operation.result["rollback"])
        self.assertFalse(operation.result["completion_canary"]["passed"])
        self.assertEqual(0, operation.result["completion_canary"]["completion_tokens"])
        self.assertIn(("stop", "node-a", "new"), self.transport.calls)
        self.assertEqual("old", self.transport.observe("node-a").deployment_id)
        self.assertEqual("ready", self.journal.get_deployment("old").observed_state)
        self.assertEqual("failed", self.journal.get_deployment("new").observed_state)
        self.assertIn(
            "completion_canary_failed",
            [event.code for event in self.journal.events(operation.id)],
        )

    def test_malformed_canary_settings_fail_preflight_without_mutation(self) -> None:
        deployment = spec(
            "dep-a",
            "alias",
            NodeAssignment("node-a"),
            canary={**CANARY, "max_tokens": "16"},
        )

        operation = self.engine.execute_load(deployment, idempotency_key="bad-canary")

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("preflight_failed", operation.error_code)
        self.assertNotIn("start", [call[0] for call in self.transport.calls])


class ClusterUpdateTests(EngineTestCase):
    """A cluster update downloads while serving, then installs and restores."""

    def setUp(self) -> None:
        super().setUp()
        self.use_nodes(
            {
                node_id: FakeNode(node_id, baseline_free_memory_bytes=32)
                for node_id in ("node-a", "node-b")
            }
        )
        self.cluster = spec(
            "tp",
            "tp-alias",
            NodeAssignment("node-a", "head"),
            NodeAssignment("node-b", "worker"),
            post_stop_free=32,
            canary=CANARY,
        )
        loaded = self.engine.execute_load(self.cluster, idempotency_key="load-tp")
        self.assertEqual(OperationState.SUCCEEDED, loaded.state)
        self.updates = FakeUpdateNodeOps(("node-a", "node-b"), helper="ready")
        cache = tempfile.TemporaryDirectory()
        self.addCleanup(cache.cleanup)
        self.relay = UpdateRelay(Path(cache.name))
        self.updater = self.make_updater()

    def make_updater(self, **timeouts: float) -> ClusterUpdateEngine:
        return ClusterUpdateEngine(
            self.journal,
            self.engine,
            self.transport,
            self.updates,
            self.relay,
            UpdateSettings,
            frozenset,
            poll_seconds=0,
            sleeper=lambda _seconds: None,
            **timeouts,
        )

    def update(self, key: str = "update"):
        operation, _ = self.journal.create_or_get_operation(
            action=OperationAction.UPDATE,
            idempotency_key=key,
            request={"node_ids": ["node-a", "node-b"]},
            node_ids=("node-a", "node-b"),
        )
        return self.updater.run(operation.id)

    def assertReadable(self, operation) -> None:  # noqa: N802 - unittest style
        """The browser shows every event, message and remediation unredacted."""
        texts = [event.message for event in self.journal.events(operation.id)]
        texts += [operation.error_message or "", operation.remediation or ""]
        for text in filter(None, texts):
            self.assertEqual(text, safe_text(text))

    def serving(self) -> bool:
        return self.transport.observe("node-a").ready_for(
            self.cluster.deployment_id, "tp-alias"
        )

    def test_downloads_while_serving_then_installs_reboots_and_restores(self) -> None:
        serving_at: dict[str, bool] = {}
        start_run = self.updates.start_run

        def record(node_id, mode):
            serving_at.setdefault(mode, self.serving())
            return start_run(node_id, mode)

        self.updates.start_run = record
        boot_ids = {
            node_id: node.boot_id for node_id, node in self.updates.nodes.items()
        }

        operation = self.update()

        self.assertEqual(
            OperationState.SUCCEEDED, operation.state, operation.error_message
        )
        self.assertEqual({"download": True, "apply": False}, serving_at)
        self.assertTrue(self.serving())
        for node_id, node in self.updates.nodes.items():
            self.assertNotEqual(boot_ids[node_id], node.boot_id)
        self.assertEqual(["tp"], operation.result["restored_deployments"])
        self.assertTrue(all(node["rebooted"] for node in operation.result["nodes"]))
        self.assertTrue(operation.result["completion_canary"]["passed"])
        starts = [call[1] for call in self.transport.calls if call[0] == "start"]
        self.assertEqual(["node-b", "node-a"], starts[-2:])
        self.assertReadable(operation)

    def test_up_to_date_cluster_keeps_serving_untouched(self) -> None:
        for node in self.updates.nodes.values():
            node.packages = node.firmware = node.reboot_packages = 0
        calls = len(self.transport.calls)

        operation = self.update()

        self.assertEqual(OperationState.SUCCEEDED, operation.state)
        self.assertTrue(operation.result["up_to_date"])
        self.assertFalse(
            any(call[0].startswith("stop") for call in self.transport.calls[calls:])
        )
        self.assertNotIn(("node-a", "download"), self.updates.calls)

    def test_failed_download_fails_before_the_deployment_stops(self) -> None:
        self.updates.nodes["node-b"].fail_step["download"] = "apt-download"

        operation = self.update()

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("update_download_failed", operation.error_code)
        self.assertTrue(self.serving())
        self.assertReadable(operation)

    def test_failed_install_reboots_only_the_nodes_that_installed(self) -> None:
        self.updates.nodes["node-b"].fail_step["apply"] = "apt-upgrade"

        operation = self.update()

        self.assertEqual(OperationState.NEEDS_ATTENTION, operation.state)
        self.assertEqual("update_install_failed", operation.error_code)
        self.assertIn(("node-a", "reboot"), self.updates.calls)
        self.assertNotIn(("node-b", "reboot"), self.updates.calls)
        self.assertEqual(["tp"], operation.result["restored_deployments"])
        self.assertTrue(self.serving())
        self.assertReadable(operation)

    def test_install_that_cannot_start_still_restores(self) -> None:
        start_run = self.updates.start_run

        def refuse_apply_on_b(node_id, mode):
            if (node_id, mode) == ("node-b", "apply"):
                raise UpdateNodeError("node_unreachable", "connection reset")
            return start_run(node_id, mode)

        self.updates.start_run = refuse_apply_on_b

        operation = self.update()

        self.assertEqual("update_install_failed", operation.error_code)
        self.assertNotIn(("node-b", "reboot"), self.updates.calls)
        self.assertTrue(self.serving())

    def test_refused_reboot_still_restores(self) -> None:
        reboot = self.updates.reboot

        def refuse_on_b(node_id):
            if node_id == "node-b":
                raise UpdateNodeError("reboot_failed", "busy")
            return reboot(node_id)

        self.updates.reboot = refuse_on_b

        operation = self.update()

        self.assertEqual(OperationState.NEEDS_ATTENTION, operation.state)
        self.assertEqual("node_reboot_refused", operation.error_code)
        self.assertEqual(["tp"], operation.result["restored_deployments"])
        self.assertTrue(self.serving())
        self.assertReadable(operation)

    def test_apply_whose_start_answer_was_lost_is_followed(self) -> None:
        start_run = self.updates.start_run

        def lose_answer_on_b(node_id, mode):
            status = start_run(node_id, mode)
            if (node_id, mode) == ("node-b", "apply"):
                raise UpdateNodeError("node_unreachable", "connection reset")
            return status

        self.updates.start_run = lose_answer_on_b

        operation = self.update()

        self.assertEqual(OperationState.SUCCEEDED, operation.state)
        self.assertIn(("node-b", "reboot"), self.updates.calls)

    def test_node_that_never_returns_needs_attention_and_restores_nothing(
        self,
    ) -> None:
        self.updates.reboot_polls = 3
        self.updater = self.make_updater(reboot_timeout_seconds=0)

        operation = self.update()

        self.assertEqual(OperationState.NEEDS_ATTENTION, operation.state)
        self.assertEqual("node_reboot_timeout", operation.error_code)
        self.assertEqual([], operation.result["restored_deployments"])
        record = self.journal.get_deployment("tp")
        self.assertEqual(
            ("ready", "stopped"), (record.desired_state, record.observed_state)
        )

    def test_relay_tunnel_failure_fails_before_the_deployment_stops(self) -> None:
        def refuse(node_ids, relay):
            raise UpdateNodeError("relay_tunnel_failed", "port 18877 in use")

        self.updates.open_relay = refuse

        operation = self.update()

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("relay_tunnel_failed", operation.error_code)
        self.assertTrue(self.serving())

    def test_pending_reboot_alone_reboots_and_restores(self) -> None:
        for node in self.updates.nodes.values():
            node.packages = node.firmware = 0
        self.updates.nodes["node-b"].reboot_required = True

        operation = self.update()

        self.assertEqual(OperationState.SUCCEEDED, operation.state)
        self.assertFalse(operation.result["up_to_date"])
        self.assertEqual(
            [("node-b", "reboot")],
            [call for call in self.updates.calls if call[1] == "reboot"],
        )
        self.assertTrue(self.serving())

    def test_failed_restore_stops_its_ranks_and_needs_attention(self) -> None:
        self.transport.node("node-a").fail_start = True

        operation = self.update()

        self.assertEqual(OperationState.NEEDS_ATTENTION, operation.state)
        self.assertEqual("update_restore_failed", operation.error_code)
        self.assertIsNone(self.transport.observe("node-b").deployment_id)
        record = self.journal.get_deployment("tp")
        self.assertEqual(
            ("ready", "stopped"), (record.desired_state, record.observed_state)
        )
        self.assertReadable(operation)

    def test_unmanaged_workload_refuses_before_any_change(self) -> None:
        self.engine.execute_unload("tp", idempotency_key="unload-tp")
        self.transport.seed_deployment(
            "node-b", deployment_id="foreign", alias="foreign", managed=False
        )

        operation = self.update()

        self.assertEqual(OperationState.FAILED, operation.state)
        self.assertEqual("unmanaged_conflict", operation.error_code)
        self.assertEqual([], self.updates.calls)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
