# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Onboarding wizard rules and the setup-mode -> configured -> recipe host flow."""

from __future__ import annotations

import json
import math
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from catalog_fakes import QWEN_LINK, fixture_catalog

from controller.config import load_nodes
from controller.discovery.hostkeys import KnownHostEntry
from controller.discovery.network import DiscoveryError
from controller.discovery.setup import (
    FakeDiscoveryBackend,
    RealDiscoveryBackend,
    SetupService,
)
from controller.discovery.sshkeys import KeyCandidate
from controller.host import ServiceHost
from controller.operations import OperationAction, OperationPhase
from controller.paths import AppPaths

FAKE_HOSTS = list(FakeDiscoveryBackend.NODES)
KEY_ID = "0123456789ab"
SAVE_FAKE_HOSTS = {"nodes": [{"host": host, "key_id": KEY_ID} for host in FAKE_HOSTS]}
QWEN_DRAFT = {
    "model_link": QWEN_LINK,
    "release_tag": "v0.31.0",
    "node_ids": ["spark-1a2b", "spark-3c4d"],
}


def temporary_paths(directory: str) -> AppPaths:
    home = Path(directory)
    return AppPaths(home / "config", home / "data", home / "logs").ensure()


def fake_keys(*_args, **_kwargs) -> list[KeyCandidate]:
    return [
        KeyCandidate(
            id=KEY_ID,
            path="/home/operator/.ssh/id_ed25519",
            display_path="~/.ssh/id_ed25519",
            source="ssh-dir",
            has_public_key=True,
            key_type="ssh-ed25519",
        )
    ]


class SetupFlowMixin:
    paths: AppPaths

    def connect(self, setup: SetupService, host: str) -> dict:
        keys = setup.host_keys({"host": host, "port": 22})["keys"]
        setup.trust_host_key(
            {
                "host": host,
                "port": 22,
                "fingerprints": [key["fingerprint_sha256"] for key in keys],
            }
        )
        return setup.identify(
            {"host": host, "port": 22, "user": "spark", "key_id": KEY_ID}
        )

    def onboard(self, setup: SetupService) -> list[dict]:
        """Identify both fake Sparks, then save them as the configured nodes."""
        identities = [self.connect(setup, host) for host in FAKE_HOSTS]
        setup.save_nodes(SAVE_FAKE_HOSTS)
        return identities


@patch("controller.discovery.sshkeys.find_key_candidates", fake_keys)
class SetupServiceTests(SetupFlowMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.paths = temporary_paths(self.directory.name)
        self.saved: list[bool] = []
        self.setup = SetupService(
            paths=self.paths,
            backend=FakeDiscoveryBackend(),
            on_nodes_saved=lambda: self.saved.append(True),
        )

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_scan_rejects_public_and_oversized_targets(self) -> None:
        with self.assertRaises(DiscoveryError):
            self.setup.start_scan({"targets": ["8.8.8.0/24"]})
        with self.assertRaises(DiscoveryError):
            self.setup.start_scan({"targets": ["10.0.0.0/8"]})

    def test_scan_finds_the_fake_sparks(self) -> None:
        scan = self.setup.start_scan({})
        deadline = time.monotonic() + 2
        while scan["state"] == "running" and time.monotonic() < deadline:
            time.sleep(0.01)
            scan = self.setup.get_scan(scan["id"])
        self.assertEqual(
            FAKE_HOSTS, [candidate["address"] for candidate in scan["candidates"]]
        )
        self.assertTrue(
            all(candidate["likely_spark"] for candidate in scan["candidates"])
        )

    def test_overview_defaults_the_ssh_user_to_the_local_login(self) -> None:
        for login, expected in (("operator", "operator"), ("Bad User", "")):
            with (
                self.subTest(login=login),
                patch("controller.discovery.setup.getpass.getuser", return_value=login),
            ):
                self.assertEqual(expected, self.setup.overview()["default_ssh_user"])
        with patch(
            "controller.discovery.setup.getpass.getuser", side_effect=OSError("none")
        ):
            self.assertEqual("", self.setup.overview()["default_ssh_user"])

    def test_host_keys_are_compared_with_the_operators_known_hosts(self) -> None:
        presented = self.setup.host_keys({"host": FAKE_HOSTS[0], "port": 22})
        fingerprint = presented["keys"][0]["fingerprint_sha256"]
        self.assertEqual("absent", presented["keys"][0]["user_known_hosts"])
        setup = SetupService(
            paths=self.paths,
            backend=FakeDiscoveryBackend(
                user_known_hosts={
                    FAKE_HOSTS[0]: [KnownHostEntry("ssh-ed25519", fingerprint)],
                    FAKE_HOSTS[1]: [KnownHostEntry("ssh-ed25519", "SHA256:other")],
                    "192.168.50.12": None,
                }
            ),
        )
        statuses = {
            host: setup.host_keys({"host": host, "port": 22})["keys"][0][
                "user_known_hosts"
            ]
            for host in (*FAKE_HOSTS, "192.168.50.12")
        }
        self.assertEqual(
            {
                FAKE_HOSTS[0]: "match",
                FAKE_HOSTS[1]: "mismatch",
                "192.168.50.12": "unknown",
            },
            statuses,
        )

    def test_real_backend_reads_only_the_given_known_hosts_file(self) -> None:
        missing = Path(self.directory.name) / "absent" / "known_hosts"
        backend = RealDiscoveryBackend(user_known_hosts_path=missing)
        self.assertEqual([], backend.user_known_hosts(FAKE_HOSTS[0], 22))
        relative = RealDiscoveryBackend(user_known_hosts_path=Path("known_hosts"))
        self.assertIsNone(relative.user_known_hosts(FAKE_HOSTS[0], 22))

    def test_trust_requires_the_currently_presented_fingerprint(self) -> None:
        with self.assertRaises(DiscoveryError) as raised:
            self.setup.trust_host_key(
                {"host": FAKE_HOSTS[0], "port": 22, "fingerprints": ["SHA256:forged"]}
            )
        self.assertEqual("host_key_mismatch", raised.exception.code)

    def test_only_identified_nodes_can_be_saved(self) -> None:
        with self.assertRaises(DiscoveryError) as raised:
            self.setup.save_nodes(
                {"nodes": [{"host": FAKE_HOSTS[0], "key_id": KEY_ID}]}
            )
        self.assertEqual("node_not_identified", raised.exception.code)
        self.assertFalse(self.paths.nodes_file.exists())

    def test_saved_nodes_come_from_the_identity_probe(self) -> None:
        for identity in self.onboard(self.setup):
            self.assertTrue(identity["ok"], identity)
        nodes = load_nodes(self.paths.nodes_file)
        self.assertEqual(["spark-1a2b", "spark-3c4d"], [node.node_id for node in nodes])
        self.assertEqual(("10.10.10.1",), nodes[0].cluster_addresses)
        self.assertEqual("~/.ssh/id_ed25519", nodes[0].ssh.key_path)
        self.assertEqual([True], self.saved)

    def test_cluster_addresses_must_be_reported_by_the_node(self) -> None:
        self.connect(self.setup, FAKE_HOSTS[0])
        with self.assertRaises(DiscoveryError) as raised:
            self.setup.save_nodes(
                {
                    "nodes": [
                        {
                            "host": FAKE_HOSTS[0],
                            "key_id": KEY_ID,
                            "cluster_addresses": ["203.0.113.9"],
                        }
                    ]
                }
            )
        self.assertEqual("invalid_cluster_address", raised.exception.code)


@patch("controller.discovery.sshkeys.find_key_candidates", fake_keys)
class HostFlowTests(SetupFlowMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.paths = temporary_paths(self.directory.name)
        self.host = ServiceHost(
            paths=self.paths,
            mode="fake",
            catalog=fixture_catalog(),
            recipe_poll_interval_seconds=0,
        )

    def tearDown(self) -> None:
        self.host.close()
        self.directory.cleanup()

    def _wait(self, predicate, timeout: float = 5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.02)
        self.fail("condition not reached")

    def test_setup_mode_to_loaded_generated_recipe(self) -> None:
        self.assertIsNone(self.host.app)
        self.assertTrue(self.host.status()["setup_required"])
        self.onboard(self.host.setup)
        app = self.host.app
        self.assertIsNotNone(app)
        self.assertEqual([], app.list_profiles())

        draft = self.host.recipes.create_draft(QWEN_DRAFT)
        self.host.recipes.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-flow"
        )
        profile_id = self._wait(
            lambda: self.host.recipes.get_draft(draft["draft_id"]).get("profile_id")
        )
        app = self._wait(lambda: (a := self.host.app) and a.profiles and a)
        self.assertEqual(
            [profile_id], [profile["id"] for profile in app.list_profiles()]
        )
        self.assertEqual("generated", app.list_profiles()[0]["origin"])
        # Recipes sharing weights differ in the memory they reserve.
        profile = app.list_profiles()[0]
        draft_record = self.host.recipes.get_draft(draft["draft_id"])
        self.assertEqual(
            draft_record["model"]["weight_bytes"], profile["weights_bytes"]
        )
        self.assertEqual(0.85, profile["gpu_memory_utilization"])
        self.assertEqual(
            math.floor(0.85 * 128 * 1024) * 1024**2, profile["reserved_memory_bytes"]
        )
        self.assertEqual("Qwen/Qwen3-8B", profile["provenance"]["hf_repo"])
        # Recipes sharing weights differ in context, so it is reported per profile.
        draft_settings = self.host.recipes.get_draft(draft["draft_id"])["settings"]
        self.assertEqual(
            draft_settings["max_model_len"], app.list_profiles()[0]["context_length"]
        )

        operation = app.submit_load(
            {"profile_id": profile_id, "node_ids": ["spark-1a2b", "spark-3c4d"]},
            idempotency_key="load-flow",
        )
        finished = self._wait(
            lambda: (record := app.get_operation(operation["id"]))["state"] != "queued"
            and record["state"] != "running"
            and record
        )
        self.assertEqual("succeeded", finished["state"], finished)
        deployment = app.list_deployments()[0]
        self.assertEqual("ready", deployment["state"])
        self.assertEqual("qwen3-8b", deployment["alias"])

    def test_invalid_nodes_file_is_reported_not_fatal(self) -> None:
        self.paths.nodes_file.write_text(
            json.dumps({"schema_version": 1, "nodes": [], "extra": 1})
        )
        self.host.request_reload()
        self.assertIsNone(self.host.app)
        self.assertIsNotNone(self.host.status()["config_error"])

    def test_restart_hands_a_running_prepare_to_the_recipe_service(self) -> None:
        self.onboard(self.host.setup)
        draft = self.host.recipes.create_draft(QWEN_DRAFT)
        # A prepare that was downloading when the previous controller exited.
        journal = self.host.journal
        operation, _ = journal.create_or_get_operation(
            action=OperationAction.PREPARE,
            idempotency_key="prepare-before-restart",
            request={"kind": "prepare", "draft_id": draft["draft_id"]},
            node_ids=("spark-1a2b", "spark-3c4d"),
        )
        journal.claim_operation(operation.id)
        journal.advance(
            operation.id,
            phase=OperationPhase.PREPARING,
            code="prepare_started",
            message="Preparing.",
            details={"profile_id": "g-qwen3-8b-00aa11", "prepare_id": "00aa11bb22"},
        )
        store = self.host.recipes.store
        store.save(
            store.get(draft["draft_id"]).with_status(
                "preparing", last_operation_id=operation.id
            )
        )
        self.host.close()

        self.host = ServiceHost(
            paths=self.paths,
            mode="fake",
            catalog=fixture_catalog(),
            recipe_poll_interval_seconds=0,
        )
        record = self._wait(
            lambda: (item := self.host.journal.get_operation(operation.id))
            and item.state.terminal
            and item
        )
        # Startup reconciliation left it alone; the recipe service probed the
        # (fresh, empty) fake nodes, found no unit, and failed it cleanly.
        codes = [event.code for event in self.host.journal.events(operation.id)]
        self.assertNotIn("operation_interrupted", codes)
        self.assertIn("prepare_reattached", codes)
        self.assertEqual("prepare_interrupted", record.error_code)
        self.assertEqual(
            "failed", self.host.recipes.get_draft(draft["draft_id"])["status"]
        )


if __name__ == "__main__":
    unittest.main()
