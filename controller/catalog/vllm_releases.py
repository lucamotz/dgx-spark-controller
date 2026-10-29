# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""vLLM release discovery and digest-pinned ``linux/arm64`` runtime images.

Safety boundary: release tags come from GitHub and image digests from Docker
Hub; both are untrusted. Only tags matching ``RELEASE_TAG_RE`` are surfaced,
only manylinux ``aarch64`` CUDA wheels are listed, and an image is returned
only as an immutable ``vllm/vllm-openai@sha256:<arm64 manifest>`` reference.
Pinning the per-platform arm64 manifest (not the multi-arch index or a mutable
tag) means every Spark node pulls byte-identical image content.

GB10 compatibility: DGX Spark is a GB10 superchip (aarch64 Grace CPU, Blackwell
GPU with compute capability sm_121, 128 GB unified memory). The official vLLM
wheels and images are built and tested upstream for other targets, and this
project has not qualified any of them on GB10. A resolved image is therefore
only a pinned candidate: recipes generated from it must stay marked
unqualified until a live readiness check (process identity, ``/health`` and
the exact ``/v1/models`` alias) and a completion canary pass on the actual
Spark nodes.
"""

from __future__ import annotations

import re
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from . import http
from .http import CatalogError

RELEASE_TAG_RE = re.compile(r"^v\d+\.\d+\.\d+(?:\.post\d+)?(?:rc\d+)?$")
GITHUB_RELEASES_URL = "https://api.github.com/repos/vllm-project/vllm/releases"
RELEASE_HTML_PREFIX = "https://github.com/vllm-project/vllm/releases/tag/"
RELEASE_DOWNLOAD_PREFIX = "https://github.com/vllm-project/vllm/releases/download/"
IMAGE_REPOSITORY = "vllm/vllm-openai"
DOCKER_HUB_TAG_URL = "https://hub.docker.com/v2/repositories/vllm/vllm-openai/tags/"
CUDA_VARIANTS = ("default", "cu129", "cu130")
RELEASE_LIMIT = 15
MAX_WHEEL_BYTES = 2**40
RELEASES_MAX_BYTES = 8 * 1024 * 1024
TAG_MAX_BYTES = 1024 * 1024

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$")
_WHEEL_RE = re.compile(
    r"^vllm-(?P<version>\d+\.\d+\.\d+(?:\.post\d+)?(?:rc\d+)?)"
    r"(?:\+(?P<local>[A-Za-z0-9.]+))?"
    r"-(?P<python>cp\d+)-(?P<abi>abi3|cp\d+|none)"
    r"-(?P<platform>[A-Za-z0-9_.]+)\.whl$"
)
_AARCH64_PLATFORM_RE = re.compile(r"^manylinux(?:_\d+_\d+|2014)_aarch64$")
_CUDA_LOCAL_RE = re.compile(r"^cu\d{3}$")
_ARM64_VARIANTS = (None, "", "v8")
_IMAGE_REMEDIATION = (
    "Choose another vLLM release or CUDA variant. Official linux/arm64 images "
    "exist only for recent releases."
)


@dataclass(frozen=True)
class WheelAsset:
    """One manylinux aarch64 CUDA wheel attached to a GitHub release.

    ``sha256`` is the bare lowercase hex digest GitHub reports, or ``None``
    for older releases without asset digests. ``cuda_variant`` is
    ``default`` for the plain wheel and e.g. ``cu129`` for ``+cu129`` builds.
    """

    name: str
    size_bytes: int
    sha256: str | None
    cuda_variant: str
    download_url: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "name": self.name,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "cuda_variant": self.cuda_variant,
            "download_url": self.download_url,
        }


@dataclass(frozen=True)
class VllmRelease:
    """A published vLLM release and its aarch64 CUDA wheels."""

    tag: str
    version: str
    name: str
    published_at: str
    prerelease: bool
    html_url: str
    aarch64_wheels: tuple[WheelAsset, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "tag": self.tag,
            "version": self.version,
            "name": self.name,
            "published_at": self.published_at,
            "prerelease": self.prerelease,
            "html_url": self.html_url,
            "aarch64_wheels": [wheel.to_dict() for wheel in self.aarch64_wheels],
        }


@dataclass(frozen=True)
class RuntimeImage:
    """A ``vllm/vllm-openai`` tag resolved to its immutable arm64 manifest.

    ``reference`` pins the linux/arm64 manifest digest, not the multi-arch
    ``index_digest``, so every node pulls identical content.
    """

    repository: str
    tag: str
    release_tag: str
    cuda_variant: str
    index_digest: str
    arm64_digest: str
    reference: str
    last_updated: str | None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "repository": self.repository,
            "tag": self.tag,
            "release_tag": self.release_tag,
            "cuda_variant": self.cuda_variant,
            "index_digest": self.index_digest,
            "arm64_digest": self.arm64_digest,
            "reference": self.reference,
            "last_updated": self.last_updated,
        }


def _github_shape_error(detail: str) -> CatalogError:
    return CatalogError(
        "upstream_unavailable",
        f"GitHub returned unusable vLLM release data: {detail}.",
        "Retry later. If this persists, inspect the vLLM releases on github.com.",
    )


def _docker_shape_error(detail: str) -> CatalogError:
    return CatalogError(
        "upstream_unavailable",
        f"Docker Hub returned unusable image data: {detail}.",
        "Retry later. If this persists, inspect the tag on hub.docker.com.",
    )


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
        return None
    try:
        return datetime.fromisoformat(value[:19] + "+00:00")
    except ValueError:
        return None


def _display_name(value: Any, fallback: str) -> str:
    if not isinstance(value, str):
        return fallback
    cleaned = "".join(char for char in value if char.isprintable()).strip()
    return cleaned[:200] or fallback


def _wheel_variant(name: str, version: str) -> str | None:
    match = _WHEEL_RE.fullmatch(name)
    if match is None or match["version"] != version:
        return None
    platforms = match["platform"].split(".")
    if not all(_AARCH64_PLATFORM_RE.fullmatch(item) for item in platforms):
        return None
    local = match["local"]
    if local is None:
        return "default"
    # Skip +cpu, +xpu, +rocm and other non-CUDA builds.
    return local if _CUDA_LOCAL_RE.fullmatch(local) else None


def _wheel(asset: dict[str, Any], tag: str, version: str) -> WheelAsset | None:
    name = asset.get("name")
    if not isinstance(name, str):
        raise _github_shape_error("a release asset has no name")
    cuda_variant = _wheel_variant(name, version)
    if cuda_variant is None:
        return None
    state = asset.get("state")
    if state is not None and state != "uploaded":
        return None
    size = asset.get("size")
    if not http.is_int(size) or not 0 < size <= MAX_WHEEL_BYTES:
        raise _github_shape_error("a wheel asset has an implausible size")
    digest = asset.get("digest")
    sha256: str | None = None
    if digest is not None:
        if not isinstance(digest, str) or not _DIGEST_RE.fullmatch(digest):
            raise _github_shape_error("a wheel asset has a malformed digest")
        sha256 = digest.split(":", 1)[1]
    url = asset.get("browser_download_url")
    prefix = f"{RELEASE_DOWNLOAD_PREFIX}{tag}/"
    if (
        not isinstance(url, str)
        or not url.startswith(prefix)
        or urllib.parse.unquote(url[len(prefix) :]) != name
    ):
        raise _github_shape_error("a wheel asset has an unexpected download URL")
    return WheelAsset(
        name=name,
        size_bytes=size,
        sha256=sha256,
        cuda_variant=cuda_variant,
        download_url=url,
    )


def _wheels(assets: Any, tag: str, version: str) -> tuple[WheelAsset, ...]:
    if assets is None:
        return ()
    if not isinstance(assets, list):
        raise _github_shape_error("release assets are not a list")
    wheels: list[WheelAsset] = []
    for asset in assets:
        if not isinstance(asset, dict):
            raise _github_shape_error("a release asset is not an object")
        wheel = _wheel(asset, tag, version)
        if wheel is not None:
            wheels.append(wheel)
    wheels.sort(key=lambda item: (item.cuda_variant != "default", item.cuda_variant))
    return tuple(wheels)


def _release(
    entry: Any, include_prereleases: bool
) -> tuple[datetime, VllmRelease] | None:
    if not isinstance(entry, dict):
        raise _github_shape_error("a release is not an object")
    tag = entry.get("tag_name")
    if not isinstance(tag, str):
        raise _github_shape_error("a release has no tag")
    draft = entry.get("draft")
    if draft is not None and not isinstance(draft, bool):
        raise _github_shape_error("a release has a malformed draft flag")
    if draft or not RELEASE_TAG_RE.fullmatch(tag):
        return None
    flagged = entry.get("prerelease")
    if flagged is not None and not isinstance(flagged, bool):
        raise _github_shape_error("a release has a malformed prerelease flag")
    prerelease = bool(flagged) or "rc" in tag
    if prerelease and not include_prereleases:
        return None
    published_at = entry.get("published_at")
    published = _timestamp(published_at)
    if published is None or not isinstance(published_at, str):
        raise _github_shape_error(f"release {tag} has a malformed publication time")
    html_url = entry.get("html_url")
    if html_url != f"{RELEASE_HTML_PREFIX}{tag}":
        raise _github_shape_error(f"release {tag} has an unexpected page URL")
    version = tag[1:]
    release = VllmRelease(
        tag=tag,
        version=version,
        name=_display_name(entry.get("name"), tag),
        published_at=published_at,
        prerelease=prerelease,
        html_url=html_url,
        aarch64_wheels=_wheels(entry.get("assets"), tag, version),
    )
    return published, release


def list_releases(
    *,
    include_prereleases: bool = False,
    token: str | None = None,
    get_json: Callable[..., Any] = http.get_json,
) -> list[VllmRelease]:
    """List published vLLM releases, newest first, with aarch64 CUDA wheels.

    Reads one page of ``repos/vllm-project/vllm/releases``. Drafts and tags
    that do not match ``RELEASE_TAG_RE`` (nightlies, branches) are skipped,
    as are prereleases unless requested.

    Args:
        include_prereleases: Also return ``rc`` and GitHub-flagged
            prereleases.
        token: Optional GitHub token for higher rate limits.
        get_json: JSON fetcher with the ``http.get_json`` signature.

    Returns:
        Up to ``RELEASE_LIMIT`` releases sorted by ``published_at`` descending.

    Raises:
        CatalogError: ``invalid_token`` for a malformed token,
            ``upstream_unavailable`` for unusable response shapes, or any
            transport error from ``get_json``.

    """
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token is not None:
        headers.update(
            http.bearer_header(token, service="GitHub", env_name="GITHUB_TOKEN")
        )
    url = f"{GITHUB_RELEASES_URL}?per_page=30"
    try:
        data = get_json(url, headers=headers, max_bytes=RELEASES_MAX_BYTES)
    except CatalogError as exc:
        if exc.code == "upstream_unauthorized" and token is not None:
            raise CatalogError(
                "upstream_unauthorized",
                "GitHub rejected the configured token.",
                "Replace GITHUB_TOKEN or unset it to use anonymous access.",
                status=exc.status,
            ) from None
        raise
    if not isinstance(data, list):
        raise _github_shape_error("the release list is not an array")
    found: dict[str, tuple[datetime, VllmRelease]] = {}
    for entry in data:
        parsed = _release(entry, include_prereleases)
        if parsed is not None and parsed[1].tag not in found:
            found[parsed[1].tag] = parsed
    ordered = sorted(
        found.values(), key=lambda item: (item[0], item[1].tag), reverse=True
    )
    return [release for _, release in ordered[:RELEASE_LIMIT]]


def _arm64_digest(images: Any, docker_tag: str) -> str:
    if not isinstance(images, list):
        raise _docker_shape_error("the platform image list is missing")
    candidates: set[str] = set()
    for image in images:
        if not isinstance(image, dict):
            raise _docker_shape_error("a platform image entry is not an object")
        if image.get("architecture") != "arm64" or image.get("os") != "linux":
            continue
        if image.get("variant") not in _ARM64_VARIANTS:
            continue
        digest = image.get("digest")
        if not isinstance(digest, str) or not _DIGEST_RE.fullmatch(digest):
            raise _docker_shape_error("the linux/arm64 digest is missing or malformed")
        candidates.add(digest)
    if not candidates:
        raise CatalogError(
            "runtime_image_unavailable",
            f"{IMAGE_REPOSITORY}:{docker_tag} has no linux/arm64 image.",
            _IMAGE_REMEDIATION,
        )
    if len(candidates) > 1:
        raise CatalogError(
            "runtime_image_unavailable",
            f"{IMAGE_REPOSITORY}:{docker_tag} lists several linux/arm64 images, "
            "so no unique pin exists.",
            _IMAGE_REMEDIATION,
        )
    return candidates.pop()


def resolve_release_image(
    release_tag: str,
    *,
    cuda_variant: str = "default",
    get_json: Callable[..., Any] = http.get_json,
) -> RuntimeImage:
    """Resolve an official vLLM image tag to its pinned arm64 manifest.

    ``default`` maps to the plain release tag (``v0.31.0``); ``cu129`` and
    ``cu130`` map to ``v0.31.0-cu129`` and ``v0.31.0-cu130``.

    Args:
        release_tag: A tag matching ``RELEASE_TAG_RE``.
        cuda_variant: One of ``CUDA_VARIANTS``.
        get_json: JSON fetcher with the ``http.get_json`` signature.

    Returns:
        The image with ``reference`` set to
        ``vllm/vllm-openai@<arm64 manifest digest>``.

    Raises:
        CatalogError: ``invalid_release_tag`` or ``invalid_cuda_variant`` for
            bad input; ``runtime_image_unavailable`` when the tag does not
            exist or has no unique linux/arm64 image; ``upstream_unavailable``
            for malformed digests or shapes; or any transport error.

    """
    if not isinstance(release_tag, str) or not RELEASE_TAG_RE.fullmatch(release_tag):
        raise CatalogError(
            "invalid_release_tag",
            "The vLLM release tag must look like v0.31.0, v0.11.0.post1 or v0.12.0rc1.",
            "Choose a release from the vLLM release list.",
        )
    if cuda_variant not in CUDA_VARIANTS:
        raise CatalogError(
            "invalid_cuda_variant",
            f"The CUDA variant must be one of {', '.join(CUDA_VARIANTS)}.",
            "Use 'default' unless a specific CUDA build is required.",
        )
    if cuda_variant == "default":
        docker_tag = release_tag
    else:
        docker_tag = f"{release_tag}-{cuda_variant}"
    try:
        data = get_json(f"{DOCKER_HUB_TAG_URL}{docker_tag}", max_bytes=TAG_MAX_BYTES)
    except CatalogError as exc:
        if exc.code == "not_found":
            raise CatalogError(
                "runtime_image_unavailable",
                f"Docker Hub has no {IMAGE_REPOSITORY}:{docker_tag} image.",
                _IMAGE_REMEDIATION,
                status=exc.status,
            ) from None
        raise
    if not isinstance(data, dict):
        raise _docker_shape_error("the tag document is not an object")
    name = data.get("name")
    if name is not None and name != docker_tag:
        raise _docker_shape_error("the tag document names a different tag")
    index_digest = data.get("digest")
    if not isinstance(index_digest, str) or not _DIGEST_RE.fullmatch(index_digest):
        raise _docker_shape_error("the tag digest is missing or malformed")
    arm64_digest = _arm64_digest(data.get("images"), docker_tag)
    last_updated = data.get("last_updated")
    return RuntimeImage(
        repository=IMAGE_REPOSITORY,
        tag=docker_tag,
        release_tag=release_tag,
        cuda_variant=cuda_variant,
        index_digest=index_digest,
        arm64_digest=arm64_digest,
        reference=f"{IMAGE_REPOSITORY}@{arm64_digest}",
        last_updated=last_updated if _timestamp(last_updated) is not None else None,
    )
