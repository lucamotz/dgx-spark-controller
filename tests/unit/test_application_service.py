# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Unit coverage for the identifier-only local application service."""

from __future__ import annotations

import itertools
import tempfile
import threading
import unittest
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from harness import wait_until

from controller.app import ApplicationError, ControllerApplication, safe_text
from controller.config import load_nodes, load_profiles
from controller.domain import Backend, DeploymentMode, NodeSelector
from controller.host import ServiceHost
from controller.main import start_parent_watchdog
from controller.operations import OperationAction, OperationPhase
from controller.paths import AppPaths
from controller.persistence import OperationJournal
from controller.projection import ProjectionError, project_deployment
from controller.resources import safe_result
from controller.transport.base import NodeObservation, UnitState
from controller.transport.fake import FakeNode, FakeNodeTransport
from controller.transport.managed_ssh import ManagedSSHTransport

ROOT = Path(__file__).resolve().parents[2]
NODES = load_nodes(ROOT / "config" / "examples" / "nodes.cluster.example.json")
PROFILES = load_profiles(ROOT / "config" / "examples" / "profiles")
MIMO_PROFILE = next(
    profile
    for profile in PROFILES
    if profile.profile_id == "mimo-v26-flash-rl-bf16-884736-c2-shared-vllm"
)
MIMO_LOAD = {
    "profile_id": MIMO_PROFILE.profile_id,
    "node_ids": [node.node_id for node in NODES],
}
SINGLE_PROFILE = replace(
    MIMO_PROFILE,
    profile_id="single-node",
    api_alias="single-alias",
    backend=Backend.LLAMA_CPP,
    deployment_mode=DeploymentMode.INDEPENDENT,
    node_count=1,
    node_selector=NodeSelector(allowed_node_ids=frozenset({NODES[0].node_id})),
    artifact=replace(MIMO_PROFILE.artifact, size_bytes=8 * 1024**3),
    readiness=replace(MIMO_PROFILE.readiness, expected_alias="single-alias"),
)
TERMINAL_STATES = {"succeeded", "failed", "cancelled", "needs-attention"}


class ControllerApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.journal = OperationJournal()
        self.transport = FakeNodeTransport(
            {
                node.node_id: FakeNode(
                    node.node_id, baseline_free_memory_bytes=128 * 1024 * 1024 * 1024
                )
                for node in NODES
            }
        )
        self.app = ControllerApplication(
            nodes=NODES,
            profiles=PROFILES,
            journal=self.journal,
            transport=self.transport,
            start_reconciliation=False,
        )

    def tearDown(self) -> None:
        self.app.close()

    def test_unobserved_node_is_unknown_and_failure_is_offline(self) -> None:
        node_id = NODES[0].node_id
        self.assertEqual("unknown", self.app.get_node(node_id)["state"])
        self.assertIsNone(self.app.get_node(node_id)["last_refresh_at"])
        self.transport.node(node_id).reachable = False
        self.app.refresh_observations((node_id,))
        self.assertEqual("offline", self.app.get_node(node_id)["state"])

    def test_desktop_parent_watchdog_stops_only_the_local_server(self) -> None:
        class Server:
            def __init__(self) -> None:
                self.called = threading.Event()

            def shutdown(self) -> None:
                self.called.set()

        server = Server()
        shutdown_started = threading.Event()
        parent = [4321]
        thread = start_parent_watchdog(
            server,
            shutdown_started,
            enabled=True,
            getppid=lambda: parent[0],
            interval_seconds=0.001,
        )
        self.assertIsNotNone(thread)
        parent[0] = 1
        self.assertTrue(server.called.wait(1))
        self.assertTrue(shutdown_started.is_set())
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

        self.assertIsNone(
            start_parent_watchdog(server, threading.Event(), enabled=False)
        )

    def test_projection_only_uses_reviewed_configuration_and_preserves_alias(
        self,
    ) -> None:
        profile = MIMO_PROFILE
        spec = project_deployment(profile, NODES, deployment_id="dep-reviewed")

        self.assertEqual(profile.api_alias, spec.expected_alias)
        self.assertEqual(tuple(node.node_id for node in NODES), spec.node_ids)
        self.assertEqual(profile.runner.script_id, spec.profile["runner"]["script_id"])
        self.assertEqual(
            profile.runner.expected_sha256, spec.profile["runner"]["expected_sha256"]
        )
        self.assertEqual(
            profile.safety.minimum_free_memory_mib_before_start * 1024 * 1024,
            spec.minimum_free_memory_bytes,
        )
        self.assertNotIn("command", spec.profile)
        self.assertEqual(profile.readiness.poll_interval_s, spec.readiness_poll_seconds)
        canary = profile.readiness.completion_canary
        self.assertEqual(
            {
                "enabled": canary.enabled,
                "timeout_s": canary.timeout_s,
                "max_tokens": canary.max_tokens,
                "prompt": canary.prompt,
                "completion_path": profile.endpoint.completion_path,
            },
            spec.profile["completion_canary"],
        )

    def test_switch_refreshes_the_node_it_stopped_outside_the_new_placement(
        self,
    ) -> None:
        self.app.close()
        self.app = ControllerApplication(
            nodes=NODES,
            profiles=(MIMO_PROFILE, SINGLE_PROFILE),
            journal=OperationJournal(),
            transport=self.transport,
            start_reconciliation=False,
        )
        first, second = (node.node_id for node in NODES)
        pair = self.app.submit_load(MIMO_LOAD, idempotency_key="load-pair")
        self.assertEqual("succeeded", self._wait_terminal(pair["id"])["state"])

        switch = self.app.submit_load(
            {"profile_id": SINGLE_PROFILE.profile_id, "node_ids": [first]},
            idempotency_key="switch-to-one",
        )

        self.assertEqual("succeeded", self._wait_terminal(switch["id"])["state"])
        self._wait_until(lambda: self.app.get_node(second)["state"] == "idle")

    def test_each_model_record_names_the_profile_it_describes(self) -> None:
        # Profiles that share weights share a model id; the UI tells them apart
        # by profile id.
        self.assertEqual(
            sorted(profile.profile_id for profile in PROFILES),
            sorted(model["profile_id"] for model in self.app.list_models()),
        )

    def test_reviewed_profile_reports_weights_but_no_recipe_reservation(self) -> None:
        profile = next(
            item
            for item in self.app.list_profiles()
            if item["id"] == MIMO_PROFILE.profile_id
        )

        self.assertEqual(
            MIMO_PROFILE.artifact.size_bytes
            or MIMO_PROFILE.artifact.primary_size_bytes,
            profile["weights_bytes"],
        )
        self.assertIsNone(profile["gpu_memory_utilization"])
        self.assertIsNone(profile["reserved_memory_bytes"])

    def test_operation_result_exposes_only_text_free_canary_evidence(self) -> None:
        result = safe_result(
            {
                "deployment_id": "dep-a",
                "completion_canary": {
                    "node_id": "spark-a",
                    "passed": True,
                    "http_status": 200,
                    "latency_ms": 840,
                    "completion_tokens": 3,
                    "finish_reason": "stop",
                    "prompt": "Reply with OK.",
                    "content": "OK",
                },
            }
        )

        self.assertEqual(
            {
                "node_id": "spark-a",
                "passed": True,
                "http_status": 200,
                "latency_ms": 840,
                "completion_tokens": 3,
                "finish_reason": "stop",
            },
            result["completion_canary"],
        )

    def test_invalid_duplicate_placement_is_rejected_before_journaling(self) -> None:
        with self.assertRaises(ProjectionError) as raised:
            project_deployment(
                MIMO_PROFILE, (NODES[0], NODES[0]), deployment_id="dep-duplicate"
            )

        self.assertEqual("invalid_placement", raised.exception.code)
        self.assertEqual([], self.journal.list_operations())

    def test_mimo_projection_assigns_worker_then_control_head(self) -> None:
        spec = project_deployment(MIMO_PROFILE, NODES, deployment_id="dep-mimo")

        self.assertEqual(
            ("head", "worker"),
            tuple(assignment.role for assignment in spec.assignments),
        )
        self.assertEqual("http://192.0.2.10:8888", spec.endpoint)
        self.assertEqual(88883822114, spec.profile["required_memory_bytes"])

    def test_load_is_idempotent_durable_and_never_accepts_script_input(self) -> None:
        with self.assertRaises(ApplicationError) as raised:
            self.app.submit_load(
                {**MIMO_LOAD, "script_path": "/unreviewed.sh"},
                idempotency_key="load-with-path",
            )
        self.assertEqual("invalid_request", raised.exception.code)
        self.assertEqual([], self.journal.list_operations())

        first = self.app.submit_load(MIMO_LOAD, idempotency_key="load-reviewed")
        replay = self.app.submit_load(MIMO_LOAD, idempotency_key="load-reviewed")

        self.assertEqual(first["id"], replay["id"])
        operation = self._wait_terminal(first["id"])
        self.assertEqual("succeeded", operation["state"])
        self.assertEqual(
            MIMO_PROFILE.api_alias,
            self.transport.observe(NODES[0].node_id).served_aliases[0],
        )
        deployment = self.app.list_deployments()[0]
        self.assertEqual("ready", deployment["state"])
        self.assertEqual(MIMO_PROFILE.api_alias, deployment["alias"])
        self.assertEqual(
            ["vLLM", "vLLM"],
            [node["runtime_version"] for node in self.app.list_nodes()],
        )

    def test_unload_reuses_existing_operation_for_idempotency_key(self) -> None:
        load = self.app.submit_load(MIMO_LOAD, idempotency_key="load-before-unload")
        self._wait_terminal(load["id"])
        deployment_id = self.app.list_deployments()[0]["id"]

        first = self.app.submit_unload(deployment_id, idempotency_key="unload-reviewed")
        replay = self.app.submit_unload(
            deployment_id, idempotency_key="unload-reviewed"
        )

        self.assertEqual(first["id"], replay["id"])
        self.assertEqual("succeeded", self._wait_terminal(first["id"])["state"])
        self.assertEqual("stopped", self.app.list_deployments()[0]["state"])
        node = self.app.get_node(NODES[0].node_id)
        self.assertEqual("idle", node["state"])
        self.assertIsNone(node["active_model_alias"])
        self.assertIsNone(node["deployment"])

    def test_reconcile_is_a_durable_read_only_operation(self) -> None:
        submitted = self.app.submit_reconcile(idempotency_key="reconcile-all")

        operation = self._wait_terminal(submitted["id"])

        self.assertEqual("reconcile", operation["action"])
        self.assertEqual("succeeded", operation["state"])
        self.assertIn("ready_deployments", operation["result"])

    def test_finished_reconcile_leaves_a_fresh_node_view(self) -> None:
        node_id = NODES[0].node_id
        self.app.refresh_observations()
        self.transport.seed_deployment(
            node_id, deployment_id="foreign", alias="foreign", managed=False
        )

        submitted = self.app.submit_reconcile(idempotency_key="reconcile-fresh")
        self._wait_terminal(submitted["id"])

        self.assertEqual("unmanaged", self.app.get_node(node_id)["state"])

    def test_unrecognized_runtime_is_not_assigned_historical_deployment(self) -> None:
        spec = project_deployment(MIMO_PROFILE, NODES, deployment_id="dep-history")
        queued, _ = self.app.orchestrator.submit_load(
            spec, idempotency_key="load-history"
        )
        loaded = self.app.orchestrator.run(queued.id)
        self.assertEqual("succeeded", loaded.state.value)
        record = self.journal.get_deployment(spec.deployment_id)
        self.journal.save_deployment(replace(record, observed_state="unknown"))
        observed = NodeObservation(
            NODES[0].node_id,
            True,
            1,
            unit_state=UnitState.RUNNING,
            active_alias=MIMO_PROFILE.api_alias,
            served_aliases=(MIMO_PROFILE.api_alias,),
            endpoint_healthy=True,
        )
        with patch.object(self.transport, "observe", return_value=observed):
            self.app.refresh_observations((NODES[0].node_id,))
        node = self.app.get_node(NODES[0].node_id)
        self.assertIsNone(node["deployment"])
        self.assertEqual("unmanaged", node["state"])
        self.assertEqual(MIMO_PROFILE.api_alias, node["active_model_alias"])

    def test_observed_deployment_wins_over_newer_historical_record(self) -> None:
        spec = project_deployment(MIMO_PROFILE, NODES, deployment_id="dep-current")
        queued, _ = self.app.orchestrator.submit_load(
            spec, idempotency_key="load-current"
        )
        self.app.orchestrator.run(queued.id)
        record = self.journal.get_deployment(spec.deployment_id)
        self.journal.save_deployment(
            replace(record, id="dep-history", observed_state="unknown")
        )
        self.app.refresh_observations()
        self.assertEqual(
            spec.deployment_id, self.app.get_node(NODES[0].node_id)["deployment"]["id"]
        )

    def test_recover_resets_only_the_reviewed_cluster_placement(self) -> None:
        deployment_id = "dep-" + MIMO_PROFILE.profile_id
        for node in NODES:
            self.transport.seed_deployment(
                node.node_id,
                deployment_id=deployment_id,
                alias=MIMO_PROFILE.api_alias,
                memory_bytes=32 * 1024**3,
                managed=False,
                endpoint_healthy=False,
            )
        submitted = self.app.submit_recover(
            {"profile_id": MIMO_PROFILE.profile_id, "confirmation": True},
            idempotency_key="recover-reviewed-cluster",
        )
        operation = self._wait_terminal(submitted["id"])
        self.assertEqual("succeeded", operation["state"])
        self.assertTrue(operation["result"]["recovered"])
        self.assertTrue(
            all(
                self.transport.observe(node.node_id).unit_state.value == "stopped"
                for node in NODES
            )
        )

    def test_operation_resource_uses_immutable_claim_event_for_started_at(self) -> None:
        ticks = itertools.count()
        clock = patch(
            "controller.persistence.utc_now",
            lambda: f"2026-01-01T00:00:{next(ticks):02d}.000+00:00",
        )
        with clock:
            queued, _created = self.journal.create_or_get_operation(
                action=OperationAction.RECONCILE,
                idempotency_key="started-at-regression",
                request={},
                node_ids=(NODES[0].node_id,),
            )
            self.assertIsNone(self.app.get_operation(queued.id)["started_at"])

            claimed, claimed_now = self.journal.claim_operation(queued.id)
            self.assertTrue(claimed_now)
            started_event = next(
                event
                for event in self.journal.events(claimed.id)
                if event.code == "operation_started"
            )
            self.journal.advance(
                claimed.id,
                phase=OperationPhase.LOCKING,
                code="later_checkpoint",
                message="A later checkpoint must not replace the start time.",
            )

        resource = self.app.get_operation(claimed.id)
        self.assertEqual(started_event.created_at, resource["started_at"])
        self.assertNotEqual(resource["started_at"], resource["updated_at"])

    def test_log_and_error_text_are_redacted_before_browser_serialization(self) -> None:
        self.assertEqual(
            "Remote command detail redacted.", safe_text("ssh host bash -s")
        )
        self.assertNotIn("/private/key", safe_text("failed at /private/key"))
        self.assertIn("[redacted]", safe_text("token=top-secret"))

    def test_live_factory_wires_only_configured_ssh_and_systemd_boundaries(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            host = ServiceHost(
                paths=AppPaths(home / "config", home / "data", home / "logs").ensure(),
                mode="live",
                nodes_path=ROOT / "config" / "examples" / "nodes.cluster.example.json",
                profile_dirs=(ROOT / "config" / "examples" / "profiles",),
                start_reconciliation=False,
            )
            try:
                app = host.app
                self.assertIsNotNone(app)
                self.assertIsInstance(app.transport, ManagedSSHTransport)
                self.assertEqual(NODES[0].node_id, app.list_nodes()[0]["id"])
            finally:
                host.close()

    def _wait_terminal(self, operation_id: str) -> dict:
        self._wait_until(
            lambda: self.app.get_operation(operation_id)["state"] in TERMINAL_STATES
        )
        return self.app.get_operation(operation_id)

    def _wait_until(self, condition: Callable[[], bool]) -> None:
        wait_until(self, condition, timeout=5)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
