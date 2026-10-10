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

    def test_committed_examples_are_versioned_and_load(self):
        nodes = load_nodes(NODES_PATH)
        profile = load_profile(PROFILE_PATH)

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].node_id, "spark-a")
        self.assertEqual(nodes[0].management_address, "192.0.2.10")
        self.assertEqual(
            profile.profile_id, "mimo-v26-flash-rl-bf16-884736-c2-shared-vllm"
        )
        self.assertEqual(
            profile.runner.script_path,
            "/home/spark/mimo-v26-experiment/start-spark-tp2.sh",
        )

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

    def test_mimo_profile_pins_reviewed_recipe_and_bf16_runtime(self):
        profile = load_profile(
            ROOT
            / "config/examples/profiles"
            / "mimo-v26-flash-rl-bf16-884736-c2-shared-vllm.json"
        )
        self.assertEqual(profile.api_alias, "mimo-v2.6-flash-rl")
        self.assertEqual(
            profile.runner.expected_recipe_commit,
            "9d52a35aa78ac9ce1b23330da13c2b88d045daa9",
        )
        self.assertEqual(profile.revision, 3)
        self.assertIn(
            "fixed-kv-cache-bytes-18253611008", profile.safety.required_invariant_ids
        )
        env = profile.runner.required_environment
        self.assertEqual(env["MAX_MODEL_LEN"], "884736")
        self.assertEqual(env["MAX_NUM_SEQS"], "2")
        self.assertEqual(env["MAX_NUM_BATCHED_TOKENS"], "2048")
        self.assertEqual(env["LONG_PREFILL_TOKEN_THRESHOLD"], "1024")
        self.assertEqual(env["KV_CACHE_MEMORY_BYTES"], "18253611008")
        self.assertEqual(env["ENFORCE_EAGER"], "0")
        self.assertIn("bf16-kv", profile.runtime.required_features)
        fragments = profile.readiness.process.required_command_fragments
        draft = json.loads(fragments[fragments.index("--speculative-config") + 1])
        self.assertEqual(draft["method"], "dflash")
        self.assertEqual(draft["num_speculative_tokens"], 3)
        self.assertTrue(draft["use_local_argmax_reduction"])
        graphs = json.loads(fragments[fragments.index("--compilation-config") + 1])
        self.assertEqual(graphs["cudagraph_capture_sizes"], [1, 2, 4, 8])
        self.assertEqual(
            profile.artifact.manifest_sha256,
            "09d9b96a77ed9765fa4e02a6a45f92797702eef432da426efc6b45c1131b1812",
        )
        self.assertTrue(
            profile.artifact.required_files[-1].endswith(
                "/model.safetensors.index.json"
            )
        )
        self.assertNotIn("/dflash/", profile.artifact.required_files[-1])
        self.assertIn(
            "live-qualified-context-131072-c2", profile.safety.required_invariant_ids
        )

    def test_mimo_profile_pins_recipe_model_and_runtime(self):
        profile = load_profile(PROFILE_PATH)

        self.assertEqual(profile.revision, 3)
        self.assertEqual(
            profile.runner.expected_sha256,
            "5be46af0bae6ac9cbafb0ad51cbfa6d326cbfbc089f9040f1938502ad659c8a8",
        )
        self.assertEqual(
            profile.runner.expected_stop_sha256,
            "3f5b0bc152e80e1f4bb229319ae3d96c320dd39f07708435cff66fcc4a61af69",
        )
        self.assertTrue(profile.runner.require_sha256)
        self.assertEqual(profile.artifact.size_bytes, 177767644228)
        self.assertEqual(
            profile.artifact.manifest_sha256,
            "09d9b96a77ed9765fa4e02a6a45f92797702eef432da426efc6b45c1131b1812",
        )
        self.assertEqual(
            profile.runtime.build_commit, "70fc359d257a984903b7ab9e123fbc45085c5254"
        )
        self.assertEqual(profile.runtime.architecture, "linux/arm64")
        self.assertEqual(
            profile.runner.required_environment["VLLM_HEAD_IMAGE_ID"],
            "sha256:e69e4924c7982283a3f8b06ad07131a32f9c794b58bb47ad079407b581cf853c",
        )
        self.assertEqual(
            profile.runner.required_environment["WORKER_VLLM_IMAGE_ID"],
            "sha256:a2e75908fc3c8e231db8c96f27d592a3d553ffefdfcd36fce5697f389915ebca",
        )
        self.assertEqual(
            profile.runner.required_environment["GPU_MEMORY_UTILIZATION"], "0.90"
        )
        self.assertEqual(
            profile.runner.required_environment["KV_CACHE_MEMORY_BYTES"], "18253611008"
        )
        self.assertEqual(
            profile.runner.required_environment["VLLM_NATIVE_SOURCE_COMMIT"],
            "70fc359d257a984903b7ab9e123fbc45085c5254",
        )
        self.assertEqual(
            profile.runner.expected_recipe_commit,
            "9d52a35aa78ac9ce1b23330da13c2b88d045daa9",
        )

    def test_mimo_profile_requires_qualified_serving_shape(self):
        profile = load_profile(PROFILE_PATH)
        fragments = profile.readiness.process.required_command_fragments

        self.assertEqual(profile.readiness.expected_health_status, 200)
        self.assertTrue(profile.readiness.exact_alias_set)
        self.assertEqual(profile.readiness.expected_alias, "mimo-v2.6-flash-rl")
        self.assertIn("--tensor-parallel-size", fragments)
        self.assertIn("--max-model-len", fragments)
        self.assertIn("--max-num-seqs", fragments)
        self.assertIn("--mm-processor-cache-gb", fragments)
        self.assertEqual(fragments[fragments.index("--kv-cache-dtype") + 1], "auto")
        self.assertIn("marlin", fragments)
        self.assertIn("--no-enable-flashinfer-autotune", fragments)
        self.assertEqual(profile.runner.required_environment["MAX_MODEL_LEN"], "884736")
        self.assertEqual(
            profile.runner.required_environment["MAX_NUM_BATCHED_TOKENS"], "2048"
        )
        self.assertEqual(
            profile.runner.required_environment["KV_CACHE_MEMORY_BYTES"], "18253611008"
        )
        self.assertEqual(profile.runner.required_environment["DFLASH_TOKENS"], "3")
        self.assertEqual(profile.runner.required_environment["ENFORCE_EAGER"], "0")
        self.assertEqual(
            profile.runner.required_environment["CUDAGRAPH_MODE"], "FULL_DECODE_ONLY"
        )

    def test_mimo_profile_safety_guards(self):
        safety = load_profile(PROFILE_PATH).safety

        self.assertTrue(safety.require_allowlisted_script)
        self.assertTrue(safety.require_script_readable)
        self.assertTrue(safety.require_script_syntax_check)
        self.assertTrue(safety.require_script_hash)
        self.assertTrue(safety.stop_conflicts_before_start)
        self.assertTrue(safety.verify_process_exit_after_stop)
        self.assertTrue(safety.verify_endpoint_closed_after_stop)
        self.assertTrue(safety.verify_alias_absent_after_stop)
        self.assertTrue(safety.verify_memory_recovery_after_stop)
        self.assertEqual(safety.minimum_free_memory_mib_before_start, 112640)
        self.assertEqual(safety.minimum_free_memory_mib_after_stop, 112640)
        self.assertEqual(safety.maximum_swap_used_mib, 12288)

    def test_safety_hash_requirement_cannot_be_disabled_in_runner(self):
        data = _json(PROFILE_PATH)
        data["runner"]["require_sha256"] = False
        with self.assertRaises(ConfigError) as context:
            parse_profile_document(data)
        self.assert_code(context, ErrorCode.CONFIG_INVALID_VALUE)


if __name__ == "__main__":
    unittest.main()
