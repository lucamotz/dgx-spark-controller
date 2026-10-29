# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Offline catalog responses shared by catalog, recipe, setup, and host tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from controller.catalog.http import CatalogError
from controller.recipes.catalog_client import CatalogClient

FIXTURES = Path(__file__).parents[1] / "fixtures" / "catalog"
QWEN_LINK = "https://huggingface.co/Qwen/Qwen3-8B"
# Malformed upstream JSON values that every parser must reject as CatalogError.
JUNK = [
    None,
    0,
    -1,
    True,
    False,
    1.5,
    "",
    "x",
    "<script>",
    [],
    [None],
    [{}],
    {},
    {"a": 1},
    10**30,
    -(10**30),
]


def fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class RouteGetJson:
    """A ``get_json`` that answers exact URLs from ``routes`` and records calls.

    A ``CatalogError`` route is raised; a URL without a route fails the test.
    """

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, dict[str, str], int | None]] = []

    def __call__(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        max_bytes: int | None = None,
        **_kwargs: Any,
    ) -> Any:
        self.calls.append((url, dict(headers or {}), max_bytes))
        if url not in self.routes:
            raise AssertionError(f"unexpected request {url}")
        value = self.routes[url]
        if isinstance(value, CatalogError):
            raise value
        return copy.deepcopy(value)

    @property
    def urls(self) -> list[str]:
        return [url for url, _, _ in self.calls]


def fixture_catalog() -> CatalogClient:
    """A catalog that knows Qwen/Qwen3-8B and vLLM v0.31.0 from recordings."""
    model = fixture("hf_model_qwen3_8b.json")
    hub = "https://huggingface.co"
    routes = {
        f"{hub}/api/models/Qwen/Qwen3-8B?blobs=true": model,
        f"{hub}/Qwen/Qwen3-8B/resolve/{model['sha']}/config.json": fixture(
            "hf_config_qwen3_8b.json"
        ),
        "https://api.github.com/repos/vllm-project/vllm/releases?per_page=30": (
            fixture("gh_vllm_releases.json")
        ),
        "https://hub.docker.com/v2/repositories/vllm/vllm-openai/tags/v0.31.0": (
            fixture("dockerhub_tag_v0.31.0.json")
        ),
    }
    return CatalogClient(env={}, get_json=RouteGetJson(routes))
