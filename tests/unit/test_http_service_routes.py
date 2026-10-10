# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""HTTP contract for setup mode, onboarding, catalog, and recipe routes."""

from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from catalog_fakes import QWEN_LINK, fixture_catalog
from test_setup_and_host import FAKE_HOSTS, fake_keys

from controller.host import ServiceHost
from controller.paths import AppPaths
from controller.server import create_server


@patch("controller.discovery.sshkeys.find_key_candidates", fake_keys)
class ServiceRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        home = Path(self.directory.name)
        paths = AppPaths(home / "config", home / "data", home / "logs").ensure()
        self.host = ServiceHost(
            paths=paths,
            mode="fake",
            catalog=fixture_catalog(),
            recipe_poll_interval_seconds=0,
        )
        self.server = create_server(self.host, host="127.0.0.1", port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.host.close()
        self.directory.cleanup()

    def request(
        self, method: str, path: str, body: dict | None = None
    ) -> tuple[int, dict]:
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_address[1], timeout=5
        )
        headers = {"Idempotency-Key": f"test-{method}-{path}"}
        payload = None
        if body is not None:
            payload = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        data = json.loads(response.read() or b"{}")
        connection.close()
        return response.status, data

    def test_unconfigured_controller_reports_setup_required(self) -> None:
        status, health = self.request("GET", "/api/health")
        self.assertEqual((200, True), (status, health["setup_required"]))
        status, bootstrap = self.request("GET", "/api/v1/bootstrap")
        self.assertEqual(200, status)
        self.assertTrue(bootstrap["setup"]["setup_required"])
        status, error = self.request("GET", "/api/v1/nodes")
        self.assertEqual((409, "setup_required"), (status, error["error"]["code"]))

    def test_onboarding_over_http_configures_the_controller(self) -> None:
        for host in FAKE_HOSTS:
            status, keys = self.request(
                "POST", "/api/v1/setup/host-keys", {"host": host, "port": 22}
            )
            self.assertEqual(200, status)
            fingerprints = [key["fingerprint_sha256"] for key in keys["keys"]]
            status, _ = self.request(
                "POST",
                "/api/v1/setup/host-keys/trust",
                {"host": host, "port": 22, "fingerprints": fingerprints},
            )
            self.assertEqual(200, status)
            status, identity = self.request(
                "POST",
                "/api/v1/setup/identify",
                {"host": host, "port": 22, "user": "spark", "key_id": "0123456789ab"},
            )
            self.assertTrue(identity["identity"]["is_dgx_spark"])
        status, saved = self.request(
            "POST",
            "/api/v1/setup/nodes",
            {
                "nodes": [
                    {"host": host, "key_id": "0123456789ab"} for host in FAKE_HOSTS
                ]
            },
        )
        self.assertEqual(200, status, saved)
        status, nodes = self.request("GET", "/api/v1/nodes")
        self.assertEqual((200, 2), (status, len(nodes["items"])))

    def test_validation_errors_use_the_stable_envelope(self) -> None:
        status, error = self.request(
            "POST", "/api/v1/setup/scans", {"targets": ["8.8.8.0/24"]}
        )
        self.assertEqual(
            (400, "scan_target_not_private"), (status, error["error"]["code"])
        )
        status, error = self.request(
            "POST",
            "/api/v1/setup/identify",
            {"host": "-oProxyCommand=x", "user": "a", "key_id": "x"},
        )
        self.assertEqual(400, status)
        status, error = self.request("GET", "/api/v1/recipes/r-000000000000")
        self.assertEqual((404, "recipe_not_found"), (status, error["error"]["code"]))
        status, error = self.request(
            "POST",
            "/api/v1/catalog/models/inspect",
            {"link": "https://example.com/x/y"},
        )
        self.assertEqual((400, "invalid_model_link"), (status, error["error"]["code"]))

    def test_catalog_routes_return_pinned_metadata(self) -> None:
        status, inspected = self.request(
            "POST", "/api/v1/catalog/models/inspect", {"link": QWEN_LINK}
        )
        self.assertEqual(200, status)
        self.assertRegex(inspected["model"]["revision"], r"^[0-9a-f]{40}$")
        status, releases = self.request("GET", "/api/v1/catalog/vllm-releases")
        self.assertEqual(200, status)
        self.assertIn("v0.31.0", [release["tag"] for release in releases["items"]])


if __name__ == "__main__":
    unittest.main()
