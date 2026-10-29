# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Offline tests for Hugging Face link parsing, pinned metadata and fit estimates."""

import copy
import dataclasses
import json
import re
import unittest

from catalog_fakes import JUNK, RouteGetJson, fixture

from controller.catalog.http import CatalogError
from controller.catalog.huggingface import (
    DGX_SPARK_MEMORY_BYTES,
    GIB,
    HFModelInfo,
    HFReference,
    default_served_alias,
    estimate_fit,
    fetch_model_info,
    parse_hf_link,
)

SECRET = "hf_TOPSECRETtokenValue123"
QWEN_SHA = "b968826d9c46dd6066d109eabc6255188de91218"
QWEN_API = "https://huggingface.co/api/models/Qwen/Qwen3-8B?blobs=true"
QWEN_CONFIG = f"https://huggingface.co/Qwen/Qwen3-8B/resolve/{QWEN_SHA}/config.json"
QWEN_REVISION_API = (
    "https://huggingface.co/api/models/Qwen/Qwen3-8B/revision/{}?blobs=true"
)


def _model_routes(model_fixture, config_fixture=None):
    model = fixture(model_fixture)
    repo, sha = model["id"], model["sha"]
    routes = {f"https://huggingface.co/api/models/{repo}?blobs=true": model}
    if config_fixture is not None:
        routes[f"https://huggingface.co/{repo}/resolve/{sha}/config.json"] = fixture(
            config_fixture
        )
    return routes


def _qwen_hub(**overrides):
    routes = _model_routes("hf_model_qwen3_8b.json", "hf_config_qwen3_8b.json")
    routes.update(overrides)
    return RouteGetJson(routes)


def _info(**changes):
    base = HFModelInfo(
        repo_id="Org/Model",
        revision="0123456789abcdef0123456789abcdef01234567",
        requested_revision="main",
        gated=False,
        private=False,
        license=None,
        pipeline_tag=None,
        library_name=None,
        architectures=(),
        model_type=None,
        max_position_embeddings=None,
        torch_dtype=None,
        quantization=None,
        parameter_count=None,
        weight_format="safetensors",
        weight_files=(),
        weight_bytes=10 * GIB,
        total_bytes=10 * GIB,
        file_count=1,
        warnings=(),
    )
    return dataclasses.replace(base, **changes)


class ParseLinkTests(unittest.TestCase):
    def test_accepted_links(self):
        cases = [
            ("https://huggingface.co/Qwen/Qwen3-8B", "Qwen/Qwen3-8B", None),
            ("http://huggingface.co/Qwen/Qwen3-8B", "Qwen/Qwen3-8B", None),
            ("https://huggingface.co/Qwen/Qwen3-8B/", "Qwen/Qwen3-8B", None),
            (
                "https://huggingface.co/Qwen/Qwen3-8B?library=transformers",
                "Qwen/Qwen3-8B",
                None,
            ),
            ("https://huggingface.co/Qwen/Qwen3-8B#model-card", "Qwen/Qwen3-8B", None),
            ("  https://huggingface.co/Qwen/Qwen3-8B \n", "Qwen/Qwen3-8B", None),
            ("HTTPS://HuggingFace.co/Qwen/Qwen3-8B", "Qwen/Qwen3-8B", None),
            ("https://www.huggingface.co/Qwen/Qwen3-8B", "Qwen/Qwen3-8B", None),
            ("https://hf.co/Qwen/Qwen3-8B", "Qwen/Qwen3-8B", None),
            ("huggingface.co/Qwen/Qwen3-8B", "Qwen/Qwen3-8B", None),
            ("hf.co/openai/gpt-oss-120b", "openai/gpt-oss-120b", None),
            ("https://huggingface.co/Qwen/Qwen3-8B/tree/main", "Qwen/Qwen3-8B", "main"),
            (
                "https://huggingface.co/Qwen/Qwen3-8B/tree/v1.0/subdir/deeper",
                "Qwen/Qwen3-8B",
                "v1.0",
            ),
            (
                "https://huggingface.co/Qwen/Qwen3-8B/tree/refs%2Fpr%2F1",
                "Qwen/Qwen3-8B",
                "refs/pr/1",
            ),
            (
                "https://huggingface.co/Qwen/Qwen3-8B/blob/main/config.json",
                "Qwen/Qwen3-8B",
                "main",
            ),
            (
                "https://hf.co/Qwen/Qwen3-8B/resolve/dev/model.safetensors?download=1",
                "Qwen/Qwen3-8B",
                "dev",
            ),
            (
                f"https://huggingface.co/Qwen/Qwen3-8B/commit/{QWEN_SHA}",
                "Qwen/Qwen3-8B",
                QWEN_SHA,
            ),
            (
                "https://huggingface.co/Qwen/Qwen3-8B/commit/B968826",
                "Qwen/Qwen3-8B",
                "b968826",
            ),
            ("https://huggingface.co/Qwen%2DLab/Qwen3-8B", "Qwen-Lab/Qwen3-8B", None),
            ("Qwen/Qwen3-8B", "Qwen/Qwen3-8B", None),
            ("Qwen/Qwen3-8B@main", "Qwen/Qwen3-8B", "main"),
            ("Qwen/Qwen3-8B@refs/pr/1", "Qwen/Qwen3-8B", "refs/pr/1"),
            (
                f"meta-llama/Llama-3.1-8B-Instruct@{QWEN_SHA}",
                "meta-llama/Llama-3.1-8B-Instruct",
                QWEN_SHA,
            ),
            ("a/b", "a/b", None),
            ("org_1/name.v2_final", "org_1/name.v2_final", None),
            ("o/" + "n" * 96, "o/" + "n" * 96, None),
        ]
        for text, repo_id, revision in cases:
            with self.subTest(text=text):
                self.assertEqual(HFReference(repo_id, revision), parse_hf_link(text))

    def test_rejected_links(self):
        rejected = [
            None,
            42,
            b"Qwen/Qwen3-8B",
            "",
            "   ",
            "x" * 3000,
            "Qwen3-8B",
            "gpt2",
            "https://huggingface.co/gpt2",
            "https://huggingface.co/",
            "https://huggingface.co",
            "https://huggingface.co/datasets/org/name",
            "https://huggingface.co/spaces/org/name",
            "https://huggingface.co/collections/org/name-123",
            "datasets/org",
            "spaces/x",
            "https://huggingface.co/models",
            "https://huggingface.co/models/Qwen/Qwen3-8B",
            "https://huggingface.co/api/models/Qwen/Qwen3-8B",
            "https://huggingface.co/docs/hub",
            "https://huggingface.co/settings/tokens",
            "https://example.com/Qwen/Qwen3-8B",
            "https://huggingface.co.evil.example/Qwen/Qwen3-8B",
            "https://evil.example/huggingface.co/Qwen/Qwen3-8B",
            "ftp://huggingface.co/Qwen/Qwen3-8B",
            "javascript:alert(1)",
            "file:///etc/passwd",
            "ssh://git@hf.co/Qwen/Qwen3-8B",
            "https://user:pw@huggingface.co/Qwen/Qwen3-8B",
            "https://user@huggingface.co/Qwen/Qwen3-8B",
            "https://huggingface.co:443/Qwen/Qwen3-8B",
            "https://huggingface.co:8080/Qwen/Qwen3-8B",
            "https://huggingface.co/Qwen/Qwen3-8B/discussions/3",
            "https://huggingface.co/Qwen/Qwen3-8B/settings",
            "https://huggingface.co/Qwen/Qwen3-8B/tree",
            "https://huggingface.co/Qwen/Qwen3-8B/tree/",
            "https://huggingface.co/Qwen/Qwen3-8B/commit/nothex",
            "https://huggingface.co/Qwen/Qwen3-8B/commit/abc",
            f"https://huggingface.co/Qwen/Qwen3-8B/commit/{QWEN_SHA}/extra",
            "https://huggingface.co/Qwen//Qwen3-8B",
            "https://huggingface.co/Qwen/Qwen3-8B//",
            "https://huggingface.co/../Qwen/Qwen3-8B",
            "https://huggingface.co/Qwen/../Qwen3-8B",
            "https://huggingface.co/Qwen/Qwen3-8B/tree/main/../../x",
            "https://huggingface.co/Qwen/%2e%2e",
            "https://huggingface.co/Qwen/Qwen3-8B/tree/..%2F..",
            "https://huggingface.co/Qwen/Qwen3-8B/tree/a..b",
            "https://huggingface.co/Qwen/Qwen3-8B/tree/%FF",
            "https://huggingface.co/Qwen%2FQwen3-8B/x",
            "https://huggingface.co/Qwen/Qwen3-8B/tree/refs%252Fpr%252F1",
            "https://huggingface.co/Qwen/Qwen3-8B.git",
            "Qwen/Qwen3-8B.git",
            "Qwen/Qwen3-8B.GIT",
            "Qwen/Qwen3 8B",
            "Qwen/Qwen3-8B\n@main",
            "Qwen/Qwen3-8B\x00",
            "Qwen\\Qwen3-8B",
            "Qwen/Qwen3-8B/extra",
            "/Qwen/Qwen3-8B",
            "Qwen/",
            "/Qwen3-8B",
            "Qwen/Qwen3-8B@",
            "Qwen/Qwen3-8B@@main",
            "Qwen/Qwen3-8B@..",
            "Qwen/Qwen3-8B@a/../b",
            "Qwen/Qwen3-8B@-main",
            "Qwen/Qwen3-8B@main/",
            "Qwen/Qwen3-8B@a//b",
            "Qwen/Qwen3-8B@" + "r" * 129,
            "Qwen/Qwen3-8B@main;rm -rf /",
            "Qwen/Qwen3-8B@$(id)",
            "Qwen/Qwen3-8B@`id`",
            "-Qwen/Qwen3-8B",
            ".Qwen/Qwen3-8B",
            "Qwen/.hidden",
            "Qwen/name.",
            "Qwen/name-",
            "Qwen/a..b",
            "Qwen/a--b",
            "o/" + "n" * 97,
            "Qwen/Qwén3",
            "Qwen/Qwen3-8B​",
            "Qwen/$(id)",
            "Qwen/name;ls",
        ]
        for text in rejected:
            with self.subTest(text=text), self.assertRaises(CatalogError) as context:
                parse_hf_link(text)
            self.assertEqual("invalid_model_link", context.exception.code)
            self.assertTrue(context.exception.remediation)

    def test_dataset_and_space_errors_are_specific(self):
        with self.assertRaises(CatalogError) as context:
            parse_hf_link("https://huggingface.co/datasets/org/name")
        self.assertIn("Dataset", context.exception.message)
        with self.assertRaises(CatalogError) as context:
            parse_hf_link("https://hf.co/spaces/org/name")
        self.assertIn("Space", context.exception.message)


class FetchModelInfoTests(unittest.TestCase):
    def test_qwen_metadata_is_pinned_and_complete(self):
        hub = _qwen_hub()
        info = fetch_model_info(
            parse_hf_link("https://huggingface.co/Qwen/Qwen3-8B"), get_json=hub
        )
        self.assertEqual([QWEN_API, QWEN_CONFIG], hub.urls)
        self.assertTrue(all(headers == {} for _, headers, _ in hub.calls))
        self.assertEqual(
            ("Qwen/Qwen3-8B", QWEN_SHA, "main"),
            (info.repo_id, info.revision, info.requested_revision),
        )
        self.assertFalse(info.gated or info.private or info.requires_remote_code)
        self.assertEqual(
            ("apache-2.0", "text-generation", "transformers"),
            (info.license, info.pipeline_tag, info.library_name),
        )
        self.assertEqual(("Qwen3ForCausalLM",), info.architectures)
        self.assertEqual(
            ("qwen3", 40960, "bfloat16", None),
            (
                info.model_type,
                info.max_position_embeddings,
                info.torch_dtype,
                info.quantization,
            ),
        )
        self.assertEqual(8190735360, info.parameter_count)
        self.assertEqual("safetensors", info.weight_format)
        self.assertEqual(
            [f"model-0000{index}-of-00005.safetensors" for index in range(1, 6)],
            [item.name for item in info.weight_files],
        )
        self.assertEqual(16381516776, info.weight_bytes)
        self.assertEqual((16397461266, 15), (info.total_bytes, info.file_count))
        self.assertEqual((), info.warnings)
        data = info.to_dict()
        self.assertEqual(json.loads(json.dumps(data)), data)
        self.assertEqual(
            {"name": "model-00001-of-00005.safetensors", "size_bytes": 3996250744},
            data["weight_files"][0],
        )
        self.assertIs(False, data["requires_remote_code"])
        self.assertEqual(
            set(field.name for field in dataclasses.fields(HFModelInfo)), set(data)
        )

    def test_token_is_sent_only_as_authorization_header(self):
        hub = _qwen_hub()
        info = fetch_model_info(
            HFReference("Qwen/Qwen3-8B", None), token=SECRET, get_json=hub
        )
        self.assertEqual(
            [{"Authorization": f"Bearer {SECRET}"}] * 2,
            [headers for _, headers, _ in hub.calls],
        )
        self.assertNotIn(SECRET, json.dumps(info.to_dict()) + repr(info))

    def test_revision_uses_encoded_revision_endpoint_and_records_request(self):
        model = fixture("hf_model_qwen3_8b.json")
        hub = RouteGetJson(
            {
                QWEN_REVISION_API.format("refs%2Fpr%2F1"): model,
                QWEN_CONFIG: fixture("hf_config_qwen3_8b.json"),
            }
        )
        info = fetch_model_info(
            parse_hf_link("https://huggingface.co/Qwen/Qwen3-8B/tree/refs%2Fpr%2F1"),
            get_json=hub,
        )
        self.assertEqual(
            ("refs/pr/1", QWEN_SHA), (info.requested_revision, info.revision)
        )

    def test_full_commit_must_match_resolved_sha(self):
        model = fixture("hf_model_qwen3_8b.json")
        other = "f" * 40
        hub = RouteGetJson({QWEN_REVISION_API.format(other): model})
        with self.assertRaises(CatalogError) as context:
            fetch_model_info(HFReference("Qwen/Qwen3-8B", other), get_json=hub)
        self.assertEqual("upstream_unavailable", context.exception.code)

    def test_case_redirect_uses_canonical_id_and_rename_warns(self):
        model = fixture("hf_model_qwen3_8b.json")
        hub = RouteGetJson(
            {
                "https://huggingface.co/api/models/qwen/qwen3-8b?blobs=true": model,
                QWEN_CONFIG: fixture("hf_config_qwen3_8b.json"),
            }
        )
        info = fetch_model_info(parse_hf_link("qwen/qwen3-8b"), get_json=hub)
        self.assertEqual("Qwen/Qwen3-8B", info.repo_id)
        self.assertEqual((), info.warnings)
        hub = RouteGetJson(
            {
                "https://huggingface.co/api/models/Old/Name?blobs=true": model,
                QWEN_CONFIG: fixture("hf_config_qwen3_8b.json"),
            }
        )
        info = fetch_model_info(parse_hf_link("Old/Name"), get_json=hub)
        self.assertEqual("Qwen/Qwen3-8B", info.repo_id)
        self.assertTrue(
            any(
                "redirected Old/Name to Qwen/Qwen3-8B" in warning
                for warning in info.warnings
            )
        )

    def test_gated_model_without_config_access_still_returns_info(self):
        routes = _model_routes("hf_model_llama31_8b_instruct.json")
        sha = "0e9e39f249a16976918f6564b8830bc894c89659"
        repo_url = "https://huggingface.co/meta-llama/Llama-3.1-8B-Instruct"
        routes[f"{repo_url}/resolve/{sha}/config.json"] = CatalogError(
            "upstream_unauthorized", "denied", status=401, upstream_code="GatedRepo"
        )
        info = fetch_model_info(
            parse_hf_link("meta-llama/Llama-3.1-8B-Instruct"),
            get_json=RouteGetJson(routes),
        )
        self.assertTrue(info.gated)
        self.assertIsNone(info.max_position_embeddings)
        self.assertEqual(("LlamaForCausalLM",), info.architectures)
        self.assertEqual("llama", info.model_type)
        self.assertEqual("llama3.1", info.license)
        self.assertEqual(4, len(info.weight_files))
        self.assertEqual(
            4976698672 + 4999802720 + 4915916176 + 1168138808, info.weight_bytes
        )
        joined = " ".join(info.warnings)
        self.assertIn("behind the model gate", joined)
        self.assertIn("hf auth login", joined)
        self.assertIn("original/", joined)

    def test_non_gated_config_refusal_is_model_gated(self):
        hub = _qwen_hub(
            **{QWEN_CONFIG: CatalogError("upstream_unauthorized", "denied", status=403)}
        )
        with self.assertRaises(CatalogError) as context:
            fetch_model_info(HFReference("Qwen/Qwen3-8B", None), get_json=hub)
        self.assertEqual("model_gated", context.exception.code)
        self.assertIn("hf auth login", context.exception.remediation)

    def test_config_failures(self):
        cases = [
            (CatalogError("not_found", "x", status=404), "upstream_unavailable"),
            (
                CatalogError("upstream_rate_limited", "x", status=429),
                "upstream_rate_limited",
            ),
            (CatalogError("upstream_unavailable", "x"), "upstream_unavailable"),
            (CatalogError("response_too_large", "x"), "response_too_large"),
            ([], "upstream_unavailable"),
            ("text", "upstream_unavailable"),
        ]
        for value, code in cases:
            with (
                self.subTest(code=code, value=type(value).__name__),
                self.assertRaises(CatalogError) as context,
            ):
                fetch_model_info(
                    HFReference("Qwen/Qwen3-8B", None),
                    get_json=_qwen_hub(**{QWEN_CONFIG: value}),
                )
            self.assertEqual(code, context.exception.code)
        info = fetch_model_info(
            HFReference("Qwen/Qwen3-8B", None),
            get_json=_qwen_hub(
                **{QWEN_CONFIG: CatalogError("host_not_allowed", "cdn")}
            ),
        )
        self.assertIsNone(info.max_position_embeddings)
        self.assertTrue(any("CDN" in warning for warning in info.warnings))

    def test_gpt_oss_quantization_and_nested_duplicates(self):
        hub = RouteGetJson(
            _model_routes("hf_model_gpt_oss_120b.json", "hf_config_gpt_oss_120b.json")
        )
        info = fetch_model_info(
            parse_hf_link("https://huggingface.co/openai/gpt-oss-120b"), get_json=hub
        )
        self.assertEqual("mxfp4", info.quantization)
        self.assertEqual(131072, info.max_position_embeddings)
        self.assertEqual(15, len(info.weight_files))
        self.assertTrue(all("/" not in item.name for item in info.weight_files))
        self.assertEqual(116829156672, info.parameter_count)
        joined = " ".join(info.warnings)
        self.assertIn(
            "Ignored 8 weight-like file(s) in subdirectories (metal/, original/)",
            joined,
        )
        self.assertIn("Large download", joined)
        self.assertFalse(info.requires_remote_code)

    def test_gguf_only_repository(self):
        hub = RouteGetJson(_model_routes("hf_model_qwen3_8b_gguf.json"))
        info = fetch_model_info(parse_hf_link("Qwen/Qwen3-8B-GGUF"), get_json=hub)
        self.assertEqual(
            1, len(hub.calls)
        )  # No config.json is listed, so none is fetched.
        self.assertEqual("gguf", info.weight_format)
        self.assertEqual(5, len(info.weight_files))
        self.assertEqual(40960, info.max_position_embeddings)
        self.assertEqual(8190735360, info.parameter_count)
        self.assertEqual((), info.architectures)
        self.assertTrue(any("GGUF-only" in warning for warning in info.warnings))
        self.assertTrue(
            any(
                "several alternative quantizations" in warning
                for warning in info.warnings
            )
        )

    def test_mistral_consolidated_duplicate_is_excluded(self):
        hub = RouteGetJson(
            _model_routes(
                "hf_model_mistral_7b_instruct_v03.json",
                "hf_config_mistral_7b_instruct_v03.json",
            )
        )
        info = fetch_model_info(
            parse_hf_link("mistralai/Mistral-7B-Instruct-v0.3"), get_json=hub
        )
        self.assertEqual(
            [f"model-0000{index}-of-00003.safetensors" for index in range(1, 4)],
            [item.name for item in info.weight_files],
        )
        self.assertEqual(4949453792 + 4999819336 + 4546807800, info.weight_bytes)
        self.assertEqual(32768, info.max_position_embeddings)
        self.assertIsNone(info.pipeline_tag)
        self.assertEqual("vllm", info.library_name)
        self.assertTrue(any("consolidated" in warning for warning in info.warnings))

    def test_kimi_remote_code_fp8_and_custom_license(self):
        hub = RouteGetJson(
            _model_routes(
                "hf_model_kimi_k2_instruct.json", "hf_config_kimi_k2_instruct.json"
            )
        )
        info = fetch_model_info(
            parse_hf_link("https://huggingface.co/moonshotai/Kimi-K2-Instruct"),
            get_json=hub,
        )
        self.assertTrue(info.requires_remote_code)
        self.assertIs(True, info.to_dict()["requires_remote_code"])
        self.assertEqual(
            ("fp8", "kimi_k2", "modified-mit"),
            (info.quantization, info.model_type, info.license),
        )
        self.assertEqual(61, len(info.weight_files))
        self.assertEqual(131072, info.max_position_embeddings)
        joined = " ".join(info.warnings)
        self.assertIn("--trust-remote-code", joined)
        self.assertIn("Large download", joined)
        fit = estimate_fit(info, 2)
        self.assertFalse(fit.fits)
        self.assertIn("exceeds", fit.message)

    def test_requires_remote_code_from_either_config_source(self):
        model = fixture("hf_model_qwen3_8b.json")
        model["config"]["auto_map"] = {"AutoConfig": "configuration_x.XConfig"}
        info = fetch_model_info(
            HFReference("Qwen/Qwen3-8B", None),
            get_json=RouteGetJson(
                {QWEN_API: model, QWEN_CONFIG: fixture("hf_config_qwen3_8b.json")}
            ),
        )
        self.assertTrue(info.requires_remote_code)
        config = fixture("hf_config_qwen3_8b.json")
        config["auto_map"] = {}
        info = fetch_model_info(
            HFReference("Qwen/Qwen3-8B", None),
            get_json=_qwen_hub(**{QWEN_CONFIG: config}),
        )
        self.assertTrue(info.requires_remote_code)
        self.assertTrue(any("auto_map" in warning for warning in info.warnings))

    def test_config_fallbacks_for_context_dtype_and_quantization(self):
        cases = [
            (
                {
                    "text_config": {
                        "max_position_embeddings": 131072,
                        "torch_dtype": "bfloat16",
                    }
                },
                131072,
                "bfloat16",
            ),
            ({"max_seq_len": 8192, "dtype": "float16"}, 8192, "float16"),
            ({"seq_length": 4096}, 4096, None),
            ({"n_positions": 1024}, 1024, None),
            ({"max_position_embeddings": 10**30, "seq_length": 2048}, 2048, None),
            ({"max_position_embeddings": "huge"}, None, None),
            ({"max_position_embeddings": True}, None, None),
            ({"max_position_embeddings": -5}, None, None),
            ({"max_position_embeddings": 1.5e6}, None, None),
        ]
        for config, context_length, dtype in cases:
            with self.subTest(config=config):
                info = fetch_model_info(
                    HFReference("Qwen/Qwen3-8B", None),
                    get_json=_qwen_hub(**{QWEN_CONFIG: config}),
                )
                self.assertEqual(context_length, info.max_position_embeddings)
                self.assertEqual(dtype, info.torch_dtype)
        info = fetch_model_info(
            HFReference("Qwen/Qwen3-8B", None),
            get_json=_qwen_hub(
                **{
                    QWEN_CONFIG: {
                        "compression_config": {"quant_method": "Compressed-Tensors"}
                    }
                }
            ),
        )
        self.assertEqual("compressed-tensors", info.quantization)

    def test_model_api_error_mapping(self):
        cases = [
            (CatalogError("not_found", "x", status=404), None, None, "model_not_found"),
            (
                CatalogError(
                    "not_found", "x", status=404, upstream_code="RevisionNotFound"
                ),
                "nope",
                None,
                "model_not_found",
            ),
            (
                CatalogError("upstream_unauthorized", "x", status=401),
                None,
                None,
                "model_not_found",
            ),
            (
                CatalogError("upstream_unauthorized", "x", status=401),
                None,
                SECRET,
                "model_gated",
            ),
            (
                CatalogError(
                    "upstream_unauthorized", "x", status=401, upstream_code="GatedRepo"
                ),
                None,
                None,
                "model_gated",
            ),
            (
                CatalogError("upstream_unauthorized", "x", status=403),
                None,
                None,
                "model_gated",
            ),
            (
                CatalogError("upstream_rate_limited", "x", status=429),
                None,
                None,
                "upstream_rate_limited",
            ),
            (
                CatalogError("upstream_unavailable", "x"),
                None,
                None,
                "upstream_unavailable",
            ),
            (CatalogError("response_too_large", "x"), None, None, "response_too_large"),
        ]
        for error, revision, token, code in cases:
            ref = HFReference("Qwen/Qwen3-8B", revision)
            url = QWEN_API if revision is None else QWEN_REVISION_API.format(revision)
            with (
                self.subTest(code=error.code, status=error.status, token=bool(token)),
                self.assertRaises(CatalogError) as context,
            ):
                fetch_model_info(ref, token=token, get_json=RouteGetJson({url: error}))
            self.assertEqual(code, context.exception.code)
            rendered = str(context.exception) + context.exception.remediation
            self.assertNotIn(SECRET, rendered)
            if revision:
                self.assertIn("nope", context.exception.message)
            if code == "model_gated":
                self.assertIn("hf auth login", context.exception.remediation)
                self.assertIn("HF_TOKEN", context.exception.remediation)

    def test_malformed_token_is_rejected_without_echo(self):
        with self.assertRaises(CatalogError) as context:
            fetch_model_info(
                HFReference("Qwen/Qwen3-8B", None),
                token=f"{SECRET}\nX: y",
                get_json=_qwen_hub(),
            )
        self.assertEqual("invalid_token", context.exception.code)
        self.assertNotIn(SECRET, str(context.exception))

    def test_unusable_required_shapes_are_upstream_unavailable(self):
        def mutate(change):
            model = fixture("hf_model_qwen3_8b.json")
            change(model)
            return model

        mutations = {
            "list": lambda m: None,
            "missing sha": lambda m: m.pop("sha"),
            "short sha": lambda m: m.update(sha="b968826"),
            "upper sha": lambda m: m.update(sha=QWEN_SHA.upper()),
            "int sha": lambda m: m.update(sha=10**40),
            "missing id": lambda m: (m.pop("id"), m.pop("modelId")),
            "bad id": lambda m: m.update(id="../x"),
            "id with three parts": lambda m: m.update(id="a/b/c"),
            "gated int": lambda m: m.update(gated=1),
            "gated list": lambda m: m.update(gated=["manual"]),
            "private string": lambda m: m.update(private="yes"),
            "siblings missing": lambda m: m.pop("siblings"),
            "siblings dict": lambda m: m.update(siblings={"a": 1}),
            "sibling string": lambda m: m["siblings"].append("model.safetensors"),
            "sibling no name": lambda m: m["siblings"].append({"size": 1}),
            "sibling traversal": lambda m: m["siblings"].append(
                {"rfilename": "../../x.safetensors", "size": 1}
            ),
            "sibling absolute": lambda m: m["siblings"].append(
                {"rfilename": "/etc/x.safetensors", "size": 1}
            ),
            "sibling control": lambda m: m["siblings"].append(
                {"rfilename": "a\x00.safetensors", "size": 1}
            ),
            "sibling backslash": lambda m: m["siblings"].append(
                {"rfilename": "a\\b.safetensors", "size": 1}
            ),
            "size missing": lambda m: m["siblings"][6].pop("size"),
            "size bool": lambda m: m["siblings"][6].update(size=True),
            "size negative": lambda m: m["siblings"][6].update(size=-1),
            "size huge": lambda m: m["siblings"][6].update(size=10**30),
            "size string": lambda m: m["siblings"][6].update(size="3996250744"),
            "size float": lambda m: m["siblings"][6].update(size=1.5),
            "config list": lambda m: m.update(config=["x"]),
        }
        for label, change in mutations.items():
            body = [] if label == "list" else mutate(change)
            with self.subTest(label=label), self.assertRaises(CatalogError) as context:
                fetch_model_info(
                    HFReference("Qwen/Qwen3-8B", None),
                    get_json=_qwen_hub(**{QWEN_API: body}),
                )
            self.assertEqual("upstream_unavailable", context.exception.code)

    def test_disabled_repository_is_not_found(self):
        model = fixture("hf_model_qwen3_8b.json")
        model["disabled"] = True
        with self.assertRaises(CatalogError) as context:
            fetch_model_info(
                HFReference("Qwen/Qwen3-8B", None),
                get_json=_qwen_hub(**{QWEN_API: model}),
            )
        self.assertEqual("model_not_found", context.exception.code)

    def test_malformed_optional_fields_are_ignored_with_warning(self):
        model = fixture("hf_model_qwen3_8b.json")
        model.update(
            pipeline_tag="<img src=x onerror=alert(1)>",
            library_name=5,
            cardData={"license": {"x": 1}},
        )
        model["safetensors"] = {"total": 10**30}
        config = fixture("hf_config_qwen3_8b.json")
        config.update(
            architectures=["Qwen3ForCausalLM", "<b>", 7],
            model_type=["x"],
            torch_dtype="bfloat16; rm",
            quantization_config={"quant_method": "../../x"},
        )
        info = fetch_model_info(
            HFReference("Qwen/Qwen3-8B", None),
            get_json=_qwen_hub(**{QWEN_API: model, QWEN_CONFIG: config}),
        )
        self.assertEqual(
            (None, None, None, None),
            (info.pipeline_tag, info.library_name, info.license, info.parameter_count),
        )
        self.assertEqual(("Qwen3ForCausalLM",), info.architectures)
        self.assertEqual(
            "qwen3", info.model_type
        )  # Falls back to the Hub's embedded config.
        self.assertIsNone(info.torch_dtype)
        self.assertIsNone(info.quantization)
        warning = [
            item for item in info.warnings if item.startswith("Ignored malformed")
        ]
        self.assertEqual(1, len(warning))
        self.assertNotIn("<", warning[0])

    def test_junk_values_never_escape_as_python_errors(self):
        top_keys = [
            "id",
            "sha",
            "gated",
            "private",
            "disabled",
            "pipeline_tag",
            "library_name",
            "config",
            "cardData",
            "siblings",
            "safetensors",
            "gguf",
        ]
        config_keys = [
            "architectures",
            "model_type",
            "max_position_embeddings",
            "torch_dtype",
            "dtype",
            "text_config",
            "quantization_config",
            "compression_config",
            "auto_map",
            "max_seq_len",
        ]
        attempts = 0
        for key in top_keys:
            for junk in JUNK:
                model = fixture("hf_model_qwen3_8b.json")
                model[key] = copy.deepcopy(junk)
                attempts += self._attempt({QWEN_API: model})
        for key in config_keys:
            for junk in JUNK:
                config = fixture("hf_config_qwen3_8b.json")
                config[key] = copy.deepcopy(junk)
                attempts += self._attempt({QWEN_CONFIG: config})
        for junk in JUNK:
            attempts += self._attempt({QWEN_API: copy.deepcopy(junk)})
            attempts += self._attempt({QWEN_CONFIG: copy.deepcopy(junk)})
            model = fixture("hf_model_qwen3_8b.json")
            model["siblings"][0] = copy.deepcopy(junk)
            attempts += self._attempt({QWEN_API: model})
        self.assertGreater(attempts, 300)

    def _attempt(self, overrides):
        try:
            info = fetch_model_info(
                HFReference("Qwen/Qwen3-8B", None), get_json=_qwen_hub(**overrides)
            )
        except CatalogError:
            return 1
        json.dumps(info.to_dict())
        estimate_fit(info, 1)
        self.assertRegex(default_served_alias(info), r"^[a-z0-9][a-z0-9._-]{0,63}$")
        return 1


class EstimateFitTests(unittest.TestCase):
    def setUp(self):
        self.usable = int(DGX_SPARK_MEMORY_BYTES * 0.85)
        self.required = max(4 * GIB, -(-self.usable * 5 // 100))

    def test_single_node_boundary(self):
        exact = estimate_fit(_info(weight_bytes=self.usable - self.required), 1)
        self.assertTrue(exact.fits)
        self.assertEqual(self.required, exact.headroom_per_node_bytes)
        self.assertEqual(self.required, exact.required_headroom_bytes)
        self.assertEqual(self.usable, exact.usable_per_node_bytes)
        over = estimate_fit(_info(weight_bytes=self.usable - self.required + 1), 1)
        self.assertFalse(over.fits)
        self.assertIn("below", over.message)
        self.assertIn("Fits on 1 node", exact.message)

    def test_tensor_parallel_split_uses_ceiling(self):
        fit = estimate_fit(_info(weight_bytes=2 * (self.usable - self.required) + 1), 2)
        self.assertEqual(self.usable - self.required + 1, fit.weights_per_node_bytes)
        self.assertFalse(fit.fits)
        fit = estimate_fit(_info(weight_bytes=2 * (self.usable - self.required)), 2)
        self.assertTrue(fit.fits)
        self.assertIn("2 nodes", fit.message)

    def test_negative_headroom_and_unknown_weights(self):
        fit = estimate_fit(_info(weight_bytes=500 * GIB), 1)
        self.assertLess(fit.headroom_per_node_bytes, 0)
        self.assertFalse(fit.fits)
        self.assertIn("exceeds", fit.message)
        for weight_bytes in (0, -1, None, True):
            with self.subTest(weight_bytes=weight_bytes):
                fit = estimate_fit(_info(weight_bytes=weight_bytes), 1)
                self.assertFalse(fit.fits)
                self.assertIn("unknown", fit.message)
        data = estimate_fit(_info(), 1).to_dict()
        self.assertEqual(json.loads(json.dumps(data)), data)
        self.assertEqual(
            {
                "node_count",
                "weights_per_node_bytes",
                "usable_per_node_bytes",
                "headroom_per_node_bytes",
                "required_headroom_bytes",
                "fits",
                "message",
            },
            set(data),
        )

    def test_invalid_node_count(self):
        for node_count in (0, -1, True, 65, "2", 1.0):
            with (
                self.subTest(node_count=node_count),
                self.assertRaises(CatalogError) as context,
            ):
                estimate_fit(_info(), node_count)  # type: ignore[arg-type]
            self.assertEqual("invalid_fit_parameters", context.exception.code)


class NamingTests(unittest.TestCase):
    def test_default_served_alias(self):
        cases = [
            ("Qwen/Qwen3-8B", "qwen3-8b"),
            ("meta-llama/Llama-3.1-8B-Instruct", "llama-3.1-8b-instruct"),
            ("openai/gpt-oss-120b", "gpt-oss-120b"),
            ("Org/Name_With.Dots", "name_with.dots"),
            ("Org/_Leading", "leading"),
            ("Org/a---b", "a-b"),
            ("Org/" + "x" * 96, "x" * 64),
            ("Org/" + "a" * 63 + "-b", "a" * 63),
            ("Org/Qwén 3!!8B", "qw-n-3-8b"),
            ("Org/___", "model"),
        ]
        for repo_id, alias in cases:
            with self.subTest(repo_id=repo_id):
                result = default_served_alias(_info(repo_id=repo_id))
                self.assertEqual(alias, result)
                self.assertTrue(re.fullmatch(r"^[a-z0-9][a-z0-9._-]{0,63}$", result))


if __name__ == "__main__":
    unittest.main()
