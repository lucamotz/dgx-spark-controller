# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""End-to-end HTTP contract tests against the deterministic fake controller."""

from __future__ import annotations

import http.client
import json
import tempfile
import threading
import time
import unittest
from datetime import datetime
from pathlib import Path

from controller.app import ControllerApplication
from controller.config import load_nodes, load_profiles
from controller.host import ServiceHost
from controller.operations import OperationAction
from controller.paths import AppPaths
from controller.persistence import OperationJournal
from controller.server import create_server
from controller.transport.fake import FakeNode, FakeNodeTransport
from controller.updates.service import UpdateService
from controller.updates.settings import (
    SettingsStore,
    UpdateSettings,
    parse_request,
    slot_key,
)

ROOT = Path(__file__).resolve().parents[2]
NODES = load_nodes(ROOT / "config" / "examples" / "nodes.cluster.example.json")
PROFILES = load_profiles(ROOT / "config" / "examples" / "profiles")
MIMO_PROFILE = next(
    profile
    for profile in PROFILES
    if profile.profile_id == "mimo-v26-flash-rl-bf16-884736-c2-shared-vllm"
)


class HTTPAPITests(unittest.TestCase):
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
        self._home = tempfile.TemporaryDirectory()
        home = Path(self._home.name)
        self.paths = AppPaths(home / "config", home / "data", home / "logs").ensure()
        self.host = ServiceHost.wrap(self.app, paths=self.paths)
        self.server = create_server(self.host, host="127.0.0.1", port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.app.close()
        self.host.recipes.close()
        self._home.cleanup()

    def test_health_bootstrap_static_assets_and_loopback_only_binding(self) -> None:
        status, headers, health = self._request("GET", "/api/health")
        self.assertEqual(200, status)
        self.assertEqual("no-store", headers["cache-control"])
        self.assertEqual("ok", health["status"])

        status, _headers, bootstrap = self._request("GET", "/api/v1/bootstrap")
        self.assertEqual(200, status)
        self.assertEqual(
            [node.node_id for node in NODES],
            [node["id"] for node in bootstrap["nodes"]],
        )
        profiles = {profile["id"]: profile for profile in bootstrap["profiles"]}
        self.assertIn(MIMO_PROFILE.profile_id, profiles)
        mimo = profiles[MIMO_PROFILE.profile_id]
        self.assertEqual(
            MIMO_PROFILE.artifact.size_bytes
            or MIMO_PROFILE.artifact.primary_size_bytes,
            mimo["weights_bytes"],
        )
        # Reviewed profiles have no recipe reservation to report.
        self.assertIsNone(mimo["gpu_memory_utilization"])
        self.assertIsNone(mimo["reserved_memory_bytes"])

        status, headers, body = self._raw_request("GET", "/")
        self.assertEqual(200, status)
        self.assertIn("content-security-policy", headers)
        self.assertIn(b"DGX Spark", body)

        with self.assertRaises(ValueError):
            create_server(self.host, host="0.0.0.0", port=8765)

    def test_telemetry_is_cached_read_only_and_marks_simulated_values(self) -> None:
        self.transport.seed_deployment(
            NODES[0].node_id,
            deployment_id="dep-test",
            alias="test",
            memory_bytes=32 * 1024**3,
        )
        status, _headers, body = self._request("GET", "/api/v1/telemetry")
        self.assertEqual(200, status)
        self.assertEqual(10, body["interval_seconds"])
        sample = body["items"][0]
        self.assertTrue(sample["simulated"])
        self.assertEqual(25, sample["memory"]["used_percent"])
        self.assertEqual("ok", sample["status"])
        self.assertEqual(body, self._request("GET", "/api/v1/telemetry")[2])
        self.assertEqual(
            [("telemetry", node.node_id, "") for node in NODES], self.transport.calls
        )
        self.assertEqual([], self.journal.list_operations())
        self.assertEqual([], self.journal.list_deployments())

    def test_mutation_rejects_missing_idempotency_and_raw_runtime_fields(self) -> None:
        payload = {
            "profile_id": MIMO_PROFILE.profile_id,
            "node_ids": [node.node_id for node in NODES],
        }
        status, _headers, body = self._request("POST", "/api/v1/deployments", payload)
        self.assertEqual(400, status)
        self.assertEqual("invalid_idempotency_key", body["error"]["code"])

        payload["script_path"] = "/unsafe.sh"
        status, _headers, body = self._request(
            "POST",
            "/api/v1/deployments",
            payload,
            headers={"Idempotency-Key": "path-injection"},
        )
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", body["error"]["code"])
        self.assertEqual([], self.journal.list_operations())

    def test_async_load_replay_logs_refresh_and_unload(self) -> None:
        request = {
            "profile_id": MIMO_PROFILE.profile_id,
            "node_ids": [node.node_id for node in NODES],
        }
        headers = {"Idempotency-Key": "http-load"}
        status, _headers, body = self._request(
            "POST", "/api/v1/deployments", request, headers=headers
        )
        self.assertEqual(202, status)
        operation_id = body["operation"]["id"]

        status, _headers, replay = self._request(
            "POST", "/api/v1/deployments", request, headers=headers
        )
        self.assertEqual(202, status)
        self.assertEqual(operation_id, replay["operation"]["id"])
        operation = self._wait_terminal(operation_id)
        self.assertEqual("succeeded", operation["state"])
        self.assertEqual(MIMO_PROFILE.api_alias, operation["result"]["expected_alias"])
        # The reviewed example enables the canary; only text-free evidence leaves.
        canary = operation["result"]["completion_canary"]
        self.assertTrue(canary["passed"])
        self.assertEqual(NODES[0].node_id, canary["node_id"])
        self.assertNotIn(
            MIMO_PROFILE.readiness.completion_canary.prompt, json.dumps(operation)
        )
        passed = next(
            event
            for event in operation["events"]
            if event["code"] == "completion_canary_passed"
        )
        self.assertTrue(passed["message"].startswith("Completion canary passed on"))

        status, _headers, deployments = self._request("GET", "/api/v1/deployments")
        self.assertEqual("ready", deployments["items"][0]["state"])
        deployment_id = deployments["items"][0]["id"]

        status, _headers, logs = self._request(
            "GET", f"/api/v1/operations/{operation_id}/logs?node={NODES[0].node_id}"
        )
        self.assertEqual(200, status)
        self.assertTrue(logs["logs"])
        self.assertNotIn("/home/", " ".join(item["message"] for item in logs["logs"]))

        status, _headers, node = self._request(
            "POST", f"/api/v1/nodes/{NODES[0].node_id}/refresh", {}
        )
        self.assertEqual(200, status)
        self.assertEqual("ready", node["state"])

        status, _headers, unload = self._request(
            "DELETE",
            f"/api/v1/deployments/{deployment_id}",
            headers={"Idempotency-Key": "http-unload"},
        )
        self.assertEqual(202, status)
        self.assertEqual(
            "succeeded", self._wait_terminal(unload["operation"]["id"])["state"]
        )

    def test_reconcile_and_safe_not_found_envelope(self) -> None:
        status, _headers, body = self._request(
            "POST",
            "/api/v1/reconcile",
            {},
            headers={"Idempotency-Key": "http-reconcile"},
        )
        self.assertEqual(202, status)
        self.assertEqual(
            "succeeded", self._wait_terminal(body["operation"]["id"])["state"]
        )

        status, _headers, missing = self._request(
            "GET", "/api/v1/operations/not-a-real-operation"
        )
        self.assertEqual(404, status)
        self.assertEqual("operation_not_found", missing["error"]["code"])
        self.assertNotIn("traceback", json.dumps(missing).lower())

        status, _headers, unsupported = self._request(
            "POST",
            "/api/v1/recover",
            {"profile_id": "missing-profile"},
            headers={"Idempotency-Key": "http-recover-unsupported"},
        )
        self.assertEqual(404, status)
        self.assertEqual("profile_not_found", unsupported["error"]["code"])

    def _wait_terminal(self, operation_id: str) -> dict:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status, _headers, operation = self._request(
                "GET", f"/api/v1/operations/{operation_id}"
            )
            self.assertEqual(200, status)
            if operation["state"] in {
                "succeeded",
                "failed",
                "cancelled",
                "needs-attention",
            }:
                return operation
            time.sleep(0.01)
        self.fail(f"operation {operation_id} did not reach a terminal state")

    def _request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        headers: dict | None = None,
    ):
        response_status, response_headers, raw = self._raw_request(
            method, path, body, headers
        )
        return response_status, response_headers, json.loads(raw.decode("utf-8"))

    def _raw_request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        headers: dict | None = None,
    ):
        request_headers = dict(headers or {})
        encoded = None
        if body is not None:
            encoded = json.dumps(body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request(method, path, body=encoded, headers=request_headers)
        response = connection.getresponse()
        raw = response.read()
        headers_lower = {name.lower(): value for name, value in response.getheaders()}
        connection.close()
        return response.status, headers_lower, raw


class UpdateRoutesTests(unittest.TestCase):
    """The Updates page routes on a fake-mode host built from configuration."""

    def setUp(self) -> None:
        self._home = tempfile.TemporaryDirectory()
        self.addCleanup(self._home.cleanup)
        home = Path(self._home.name)
        self.paths = AppPaths(home / "config", home / "data", home / "logs").ensure()
        self.host = ServiceHost(
            paths=self.paths,
            mode="fake",
            nodes_path=ROOT / "config" / "examples" / "nodes.cluster.example.json",
            profile_dirs=(ROOT / "config" / "examples" / "profiles",),
            start_reconciliation=False,
        )
        self.addCleanup(self.host.close)
        self.server = create_server(self.host, host="127.0.0.1", port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.thread.join, 2)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.port = self.server.server_address[1]

    _request = HTTPAPITests._request
    _raw_request = HTTPAPITests._raw_request

    def test_settings_are_validated_and_saved_privately(self) -> None:
        status, _headers, error = self._request(
            "POST", "/api/v1/updates/settings", {"time": "25:00"}
        )
        self.assertEqual(400, status)
        self.assertEqual("invalid_schedule", error["error"]["code"])

        status, _headers, updates = self._request(
            "POST",
            "/api/v1/updates/settings",
            {"enabled": True, "weekdays": ["sun", "wed"], "time": "03:30"},
        )
        self.assertEqual(200, status)
        self.assertEqual(["wed", "sun"], updates["settings"]["weekdays"])
        self.assertIsNotNone(updates["next_run_at"])
        settings = self.paths.data_dir / "update-settings.json"
        self.assertEqual(0o600, settings.stat().st_mode & 0o777)

    def test_helper_install_then_update_now_runs_to_success(self) -> None:
        status, _headers, installed = self._request(
            "POST", "/api/v1/updates/helper", {"password": "correct-horse-7"}
        )
        self.assertEqual(200, status)
        self.assertEqual(
            {"installed"}, {result["state"] for result in installed["results"]}
        )
        self.assertEqual(
            {"ready"}, {node["helper"] for node in installed["updates"]["nodes"]}
        )
        self.assertNotIn("correct-horse-7", json.dumps(installed))

        status, _headers, error = self._request("POST", "/api/v1/updates/run", {})
        self.assertEqual(400, status)
        self.assertEqual("invalid_idempotency_key", error["error"]["code"])
        status, _headers, accepted = self._request(
            "POST",
            "/api/v1/updates/run",
            {"confirmation": True},
            headers={"Idempotency-Key": "update-now-1"},
        )
        self.assertEqual(202, status)
        operation = accepted["operation"]
        self.assertEqual("update", operation["action"])

        deadline = time.monotonic() + 10
        while operation["state"] in {"queued", "running"}:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.05)
            _status, _headers, operation = self._request(
                "GET", f"/api/v1/operations/{operation['id']}"
            )
        self.assertEqual("succeeded", operation["state"], operation.get("error"))
        self.assertTrue(all(node["rebooted"] for node in operation["result"]["nodes"]))
        _status, _headers, updates = self._request("GET", "/api/v1/updates")
        self.assertEqual(operation["id"], updates["last_operation"]["id"])

    def test_api_refuses_a_rebound_host_name(self) -> None:
        status, _headers, error = self._request(
            "GET", "/api/v1/updates", headers={"Host": f"evil.example:{self.port}"}
        )
        self.assertEqual(403, status)
        self.assertEqual("invalid_host_header", error["error"]["code"])
        # An SSH tunnel to another local port keeps a loopback name.
        status, _headers, _body = self._request(
            "GET", "/api/v1/updates", headers={"Host": "localhost:9000"}
        )
        self.assertEqual(200, status)

    def test_second_update_is_refused_while_one_is_queued(self) -> None:
        queued, _created = self.host.journal.create_or_get_operation(
            action=OperationAction.UPDATE,
            idempotency_key="scheduled-elsewhere",
            request={"node_ids": ["spark-a", "spark-b"]},
            node_ids=("spark-a", "spark-b"),
        )
        status, _headers, error = self._request(
            "POST",
            "/api/v1/updates/run",
            {},
            headers={"Idempotency-Key": "update-now-2"},
        )
        self.assertEqual(409, status)
        self.assertEqual("update_in_progress", error["error"]["code"])
        self.assertEqual(queued.id, error["error"]["operation_id"])

    def test_due_slot_starts_one_update_per_slot(self) -> None:
        now = datetime(2026, 11, 1, 3, 10).astimezone()
        store = SettingsStore(self.paths.data_dir / "schedule-test.json")
        store.save(
            parse_request(
                {"enabled": True, "weekdays": ["sun"], "time": "03:00"},
                UpdateSettings(),
            )
        )
        service = UpdateService(
            store=store,
            relay=self.host.update_relay,
            app=lambda: self.host.app,
            node_ops=self.host.current_update_ops,
            now=lambda: now,
        )

        service.tick()
        service.tick()

        key = slot_key(datetime(2026, 11, 1, 3, 0).astimezone())
        updates = [
            record
            for record in self.host.journal.list_operations()
            if record.action is OperationAction.UPDATE
        ]
        self.assertEqual([key], [record.idempotency_key for record in updates])
        self.assertIsNone(service.overview()["schedule_error"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
