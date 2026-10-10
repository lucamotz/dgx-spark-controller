# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Offline catalog responses shared by recipe, setup, and host tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from controller.catalog.http import CatalogError
from controller.recipes.catalog_client import CatalogClient

FIXTURES = Path(__file__).parents[1] / "fixtures" / "catalog"
QWEN_LINK = "https://huggingface.co/Qwen/Qwen3-8B"


def fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class FixtureGetJson:
    """Route catalog URLs to recorded responses; anything else is a 404."""

    def __init__(self) -> None:
        self.urls: list[str] = []

    def __call__(self, url: str, **_kwargs: Any) -> Any:
        self.urls.append(url)
        if "/api/models/Qwen/Qwen3-8B" in url:
            return fixture("hf_model_qwen3_8b.json")
        if "/Qwen/Qwen3-8B/resolve/" in url and url.endswith("/config.json"):
            return fixture("hf_config_qwen3_8b.json")
        if "api.github.com/repos/vllm-project/vllm/releases" in url:
            return fixture("gh_vllm_releases.json")
        if url.endswith("/tags/v0.31.0"):
            return fixture("dockerhub_tag_v0.31.0.json")
        raise CatalogError("not_found", f"no fixture for {url}", status=404)


def fixture_catalog() -> CatalogClient:
    return CatalogClient(env={}, get_json=FixtureGetJson())
