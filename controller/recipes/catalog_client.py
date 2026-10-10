# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Cached access to Hugging Face and vLLM release metadata for the recipe builder.

The controller host may hold optional ``HF_TOKEN`` / ``GITHUB_TOKEN`` environment
variables for gated metadata or higher API limits. They are read here, passed
only as request headers by :mod:`controller.catalog.http`, and never stored,
logged, or returned to the browser. Weight downloads on the Sparks use the
token stored on each node, never this one.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any

from controller.catalog import huggingface, vllm_releases
from controller.catalog.http import CatalogError, TTLCache


class CatalogClient:
    """Thin caching facade over the catalog modules (injectable for tests)."""

    def __init__(
        self,
        *,
        env: Mapping[str, str] | None = None,
        get_json: Callable[..., Any] | None = None,
        ttl_seconds: float = 600.0,
    ) -> None:
        self._env = os.environ if env is None else env
        self._get_json = get_json
        self._cache = TTLCache(ttl_seconds=ttl_seconds, max_entries=256)

    def _kwargs(self) -> dict[str, Any]:
        return {"get_json": self._get_json} if self._get_json is not None else {}

    def model_info(self, link: str) -> huggingface.HFModelInfo:
        reference = huggingface.parse_hf_link(link)
        token = self._env.get("HF_TOKEN") or None
        key = ("model", reference.repo_id, reference.revision or "main", bool(token))
        return self._cache.get_or_compute(
            key,
            lambda: huggingface.fetch_model_info(
                reference, token=token, **self._kwargs()
            ),
        )

    def releases(
        self, *, include_prereleases: bool = False
    ) -> list[vllm_releases.VllmRelease]:
        token = self._env.get("GITHUB_TOKEN") or None
        key = ("releases", include_prereleases)
        return self._cache.get_or_compute(
            key,
            lambda: vllm_releases.list_releases(
                include_prereleases=include_prereleases, token=token, **self._kwargs()
            ),
        )

    def runtime_image(
        self, release_tag: str, cuda_variant: str = "default"
    ) -> vllm_releases.RuntimeImage:
        key = ("image", release_tag, cuda_variant)
        return self._cache.get_or_compute(
            key,
            lambda: vllm_releases.resolve_release_image(
                release_tag, cuda_variant=cuda_variant, **self._kwargs()
            ),
        )


__all__ = ["CatalogClient", "CatalogError"]
