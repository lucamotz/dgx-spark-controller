# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Unit coverage for the identifier-only local application service."""

from __future__ import annotations

import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from controller.app import ApplicationError, ControllerApplication, safe_text
from controller.config import load_nodes, load_profiles
from controller.host import ServiceHost
from controller.main import parse_args, start_parent_watchdog
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
TEST_PROFILES = PROFILES
CLUSTER_NODES = NODES


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
            profiles=TEST_PROFILES,
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

    def test_default_loopback_port_does_not_collide_with_peer_apps(self) -> None:
        self.assertEqual(18876, parse_args([]).port)

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

    def test_switch_refreshes_nodes_stopped_outside_the_requested_placement(
        self,
    ) -> None:
        first, second = (node.node_id for node in NODES[:2])
        operation, _ = self.journal.create_or_get_operation(
            action=OperationAction.LOAD,
            idempotency_key="switch-across",
            request={"deployment": {"deployment_id": "dep-one"}},
            node_ids=[first],
        )
        self.journal.record_previous_placement(
            operation.id,
            {
                "deployments": [],
                "observed_node_ids": [first, second, "not-configured"],
                "extra_node_ids": [second, "not-configured"],
            },
        )
        record = self.journal.get_operation(operation.id)

        self.assertEqual((first, second), self.app._affected_node_ids(record))

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
        spec = project_deployment(MIMO_PROFILE, CLUSTER_NODES, deployment_id="dep-mimo")

        self.assertEqual(
            ("head", "worker"),
            tuple(assignment.role for assignment in spec.assignments),
        )
        self.assertEqual("http://192.0.2.10:8888", spec.endpoint)
        self.assertEqual(88883822114, spec.profile["required_memory_bytes"])

    def test_load_is_idempotent_durable_and_never_accepts_script_input(self) -> None:
        with self.assertRaises(ApplicationError) as raised:
            self.app.submit_load(
                {
                    "profile_id": MIMO_PROFILE.profile_id,
                    "node_ids": [node.node_id for node in NODES],
                    "script_path": "/unreviewed.sh",
                },
                idempotency_key="load-with-path",
            )
        self.assertEqual("invalid_request", raised.exception.code)
        self.assertEqual([], self.journal.list_operations())

        first = self.app.submit_load(
            {
                "profile_id": MIMO_PROFILE.profile_id,
                "node_ids": [node.node_id for node in NODES],
            },
            idempotency_key="load-reviewed",
        )
        replay = self.app.submit_load(
            {
                "profile_id": MIMO_PROFILE.profile_id,
                "node_ids": [node.node_id for node in NODES],
            },
            idempotency_key="load-reviewed",
        )

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
        load = self.app.submit_load(
            {
                "profile_id": MIMO_PROFILE.profile_id,
                "node_ids": [node.node_id for node in NODES],
            },
            idempotency_key="load-before-unload",
        )
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

    def test_unrecognized_runtime_is_not_assigned_historical_deployment(self) -> None:
        spec = project_deployment(MIMO_PROFILE, NODES, deployment_id="dep-history")
        loaded = self.app.orchestrator.execute_load(
            spec, idempotency_key="load-history"
        )
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
        self.app.orchestrator.execute_load(spec, idempotency_key="load-current")
        record = self.journal.get_deployment(spec.deployment_id)
        self.journal.save_deployment(
            replace(record, id="dep-history", observed_state="unknown")
        )
        self.app.refresh_observations()
        self.assertEqual(
            spec.deployment_id, self.app.get_node(NODES[0].node_id)["deployment"]["id"]
        )

    def test_recover_resets_only_the_reviewed_cluster_placement(self) -> None:
        journal = OperationJournal()
        transport = FakeNodeTransport(
            {
                node.node_id: FakeNode(
                    node.node_id, baseline_free_memory_bytes=128 * 1024**3
                )
                for node in CLUSTER_NODES
            }
        )
        app = ControllerApplication(
            nodes=CLUSTER_NODES,
            profiles=TEST_PROFILES,
            journal=journal,
            transport=transport,
            start_reconciliation=False,
        )
        deployment_id = "dep-" + MIMO_PROFILE.profile_id
        try:
            for node in CLUSTER_NODES:
                transport.seed_deployment(
                    node.node_id,
                    deployment_id=deployment_id,
                    alias=MIMO_PROFILE.api_alias,
                    memory_bytes=32 * 1024**3,
                    managed=False,
                    endpoint_healthy=False,
                )
            submitted = app.submit_recover(
                {"profile_id": MIMO_PROFILE.profile_id, "confirmation": True},
                idempotency_key="recover-reviewed-cluster",
            )
            deadline = time.monotonic() + 5
            operation = app.get_operation(submitted["id"])
            while (
                operation["state"] not in {"succeeded", "failed", "needs-attention"}
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
                operation = app.get_operation(submitted["id"])
            self.assertEqual("succeeded", operation["state"])
            self.assertTrue(operation["result"]["recovered"])
            self.assertTrue(
                all(
                    transport.observe(node.node_id).unit_state.value == "stopped"
                    for node in CLUSTER_NODES
                )
            )
        finally:
            app.close()

    def test_operation_resource_uses_immutable_claim_event_for_started_at(self) -> None:
        queued, _created = self.journal.create_or_get_operation(
            action=OperationAction.RECONCILE,
            idempotency_key="started-at-regression",
            request={"kind": "reconcile"},
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
        time.sleep(0.01)
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
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            operation = self.app.get_operation(operation_id)
            if operation["state"] in {
                "succeeded",
                "failed",
                "cancelled",
                "needs-attention",
            }:
                return operation
            time.sleep(0.01)
        self.fail(f"operation {operation_id} did not finish")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
