# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import copy
import json
import tempfile
import unittest
from pathlib import Path

from controller.config import (
    ConfigError,
    load_nodes,
    load_profile,
    load_profiles,
    parse_nodes_document,
    parse_profile_document,
)
from controller.domain import ErrorCode

ROOT = Path(__file__).parents[2]
NODES_PATH = ROOT / "config/examples/nodes.example.json"
PROFILE_PATH = (
    ROOT / "config/examples/profiles/mimo-v26-flash-rl-bf16-884736-c2-shared-vllm.json"
)


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


class ConfigContractTests(unittest.TestCase):
    def assert_code(self, context, code):
        self.assertIs(context.exception.code, code)

    def test_every_committed_example_loads(self):
        examples = ROOT / "config/examples"
        for path in sorted(examples.glob("nodes*.json")):
            with self.subTest(path.name):
                load_nodes(path)
        for path in sorted((examples / "profiles").glob("*.json")):
            with self.subTest(path.name):
                load_profile(path)

    def test_unknown_keys_are_rejected_at_root_and_nested_levels(self):
        node_data = _json(NODES_PATH)
        node_data["unexpected"] = True
        with self.assertRaises(ConfigError) as context:
            parse_nodes_document(node_data)
        self.assert_code(context, ErrorCode.CONFIG_UNKNOWN_KEY)

        profile_data = _json(PROFILE_PATH)
        profile_data["runner"]["arbitrary_command"] = "rm -rf /"
        with self.assertRaises(ConfigError) as context:
            parse_profile_document(profile_data)
        self.assert_code(context, ErrorCode.CONFIG_UNKNOWN_KEY)

    def test_missing_and_wrong_type_are_rejected(self):
        node_data = _json(NODES_PATH)
        del node_data["nodes"][0]["ssh"]
        with self.assertRaises(ConfigError) as context:
            parse_nodes_document(node_data)
        self.assert_code(context, ErrorCode.CONFIG_MISSING_KEY)

        profile_data = _json(PROFILE_PATH)
        profile_data["node_count"] = True
        with self.assertRaises(ConfigError) as context:
            parse_profile_document(profile_data)
        self.assert_code(context, ErrorCode.CONFIG_INVALID_VALUE)

    def test_schema_version_is_explicit_and_fail_closed(self):
        data = _json(PROFILE_PATH)
        data["schema_version"] = 2
        with self.assertRaises(ConfigError) as context:
            parse_profile_document(data)
        self.assert_code(context, ErrorCode.CONFIG_SCHEMA_UNSUPPORTED)

        data = _json(NODES_PATH)
        del data["schema_version"]
        with self.assertRaises(ConfigError) as context:
            parse_nodes_document(data)
        self.assert_code(context, ErrorCode.CONFIG_MISSING_KEY)

    def test_duplicate_node_ids_are_rejected(self):
        data = _json(NODES_PATH)
        data["nodes"].append(copy.deepcopy(data["nodes"][0]))
        with self.assertRaises(ConfigError) as context:
            parse_nodes_document(data)
        self.assert_code(context, ErrorCode.DUPLICATE_ID)

    def test_profile_directory_rejects_duplicate_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = _json(PROFILE_PATH)
            (root / "a.json").write_text(json.dumps(payload), encoding="utf-8")
            (root / "b.json").write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaises(ConfigError) as context:
                load_profiles(root)
            self.assert_code(context, ErrorCode.DUPLICATE_ID)

    def test_optional_defaults_are_applied_in_one_parser_boundary(self):
        data = _json(PROFILE_PATH)
        del data["revision"]
        del data["readiness"]["completion_canary"]
        del data["runner"]["interpreter"]
        profile = parse_profile_document(data)

        self.assertEqual(profile.revision, 1)
        self.assertFalse(profile.readiness.completion_canary.enabled)
        self.assertEqual(profile.runner.interpreter, "bash")

    def test_always_enforced_checks_cannot_be_disabled(self):
        for section, key in (
            ("safety", "require_allowlisted_script"),
            ("safety", "require_script_readable"),
            ("safety", "stop_conflicts_before_start"),
            ("safety", "verify_process_exit_after_stop"),
            ("safety", "verify_endpoint_closed_after_stop"),
            ("safety", "verify_alias_absent_after_stop"),
            ("safety", "verify_memory_recovery_after_stop"),
            ("readiness", "exact_alias_set"),
        ):
            with self.subTest(key):
                data = _json(PROFILE_PATH)
                data[section][key] = False
                with self.assertRaises(ConfigError) as context:
                    parse_profile_document(data)
                self.assert_code(context, ErrorCode.CONFIG_INVALID_VALUE)

    def test_safety_hash_requirement_cannot_be_disabled_in_runner(self):
        data = _json(PROFILE_PATH)
        data["runner"]["require_sha256"] = False
        with self.assertRaises(ConfigError) as context:
            parse_profile_document(data)
        self.assert_code(context, ErrorCode.CONFIG_INVALID_VALUE)


if __name__ == "__main__":
    unittest.main()
