# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Hugging Face model links, pinned model metadata, and memory-fit estimates.

Safety boundary: an operator pastes free text. ``parse_hf_link`` reduces it to
a validated ``org/name`` repository id and an optional revision, or rejects it;
nothing from the link is ever used as a URL host, path traversal, shell word or
flag. ``fetch_model_info`` then reads only two public documents from
``huggingface.co`` (the model API and ``config.json`` at the resolved commit),
treats both as untrusted, and returns metadata pinned to the exact 40-hex
commit the Hub resolved. A token, when supplied, is sent only as an
``Authorization`` header to ``huggingface.co`` and never appears in a result,
warning or error.

The fit estimate is advisory arithmetic over weight bytes, not a
qualification: KV-cache layout, activation peaks, CUDA graphs and the GB10
runtime are proven only by a live readiness check and canary.
"""

from __future__ import annotations

import math
import re
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from . import http
from .http import CatalogError

HF_API_ROOT = "https://huggingface.co"
LINK_HOSTS = frozenset({"huggingface.co", "www.huggingface.co", "hf.co"})
DGX_SPARK_MEMORY_BYTES = 128 * 1024**3
GPU_MEMORY_UTILIZATION = 0.85
GIB = 1024**3
MIN_KV_HEADROOM_BYTES = 4 * GIB
MIN_KV_HEADROOM_FRACTION = 0.05
LARGE_DOWNLOAD_BYTES = 100 * GIB
MODEL_API_MAX_BYTES = 8 * 1024 * 1024
CONFIG_MAX_BYTES = 1024 * 1024
MAX_LINK_LENGTH = 2048
MAX_FILE_BYTES = 2**50
MAX_PARAMETER_COUNT = 10**15
MAX_CONTEXT_LENGTH = 2**30
MAX_NODE_COUNT = 64

_REPO_PART_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")
_REVISION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_COMMIT_LINK_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_ARCHITECTURE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_DTYPE_RE = re.compile(r"^[A-Za-z0-9_]{1,32}$")
_LICENSE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")
_PYTORCH_BIN_RE = re.compile(r"^pytorch_model(?:-\d{1,5}-of-\d{1,5})?\.bin$")
_WEIGHT_SUFFIXES = (".safetensors", ".gguf", ".bin", ".pth", ".pt")
_NON_MODEL_SECTIONS = {
    "datasets": "Dataset links are not models.",
    "spaces": "Space links are not models.",
    "collections": "Collection links are not models; paste one model's link.",
}
_RESERVED_FIRST_SEGMENTS = frozenset(
    {
        "api",
        "blog",
        "chat",
        "docs",
        "enterprise",
        "join",
        "learn",
        "login",
        "models",
        "new",
        "notifications",
        "oauth",
        "organizations",
        "papers",
        "posts",
        "pricing",
        "search",
        "settings",
        "tasks",
    }
)
_LINK_REMEDIATION = (
    "Paste a model page link such as huggingface.co/Qwen/Qwen3-8B, "
    "an 'org/name' id, or 'org/name@revision'."
)
_ACCESS_REMEDIATION = (
    "Accept the model's terms on huggingface.co with the account that owns "
    "the token, set HF_TOKEN for the controller so it can read config.json, "
    "and run `hf auth login` on every Spark node so the weights can be "
    "downloaded there."
)


@dataclass(frozen=True)
class HFReference:
    """A validated model repository id and optional requested revision."""

    repo_id: str
    revision: str | None


@dataclass(frozen=True)
class WeightFile:
    """One weight file chosen for download and size estimates."""

    name: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {"name": self.name, "size_bytes": self.size_bytes}


@dataclass(frozen=True)
class HFModelInfo:
    """Hub metadata pinned to ``revision``, the commit the Hub resolved.

    ``requires_remote_code`` is true when ``config.json`` or the Hub's
    embedded config declares ``auto_map``, i.e. the repository ships custom
    modelling code that vLLM would need ``--trust-remote-code`` for.
    """

    repo_id: str
    revision: str
    requested_revision: str
    gated: bool
    private: bool
    license: str | None
    pipeline_tag: str | None
    library_name: str | None
    architectures: tuple[str, ...]
    model_type: str | None
    max_position_embeddings: int | None
    torch_dtype: str | None
    quantization: str | None
    parameter_count: int | None
    weight_format: str
    weight_files: tuple[WeightFile, ...]
    weight_bytes: int
    total_bytes: int
    file_count: int
    warnings: tuple[str, ...]
    requires_remote_code: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "repo_id": self.repo_id,
            "revision": self.revision,
            "requested_revision": self.requested_revision,
            "gated": self.gated,
            "private": self.private,
            "license": self.license,
            "pipeline_tag": self.pipeline_tag,
            "library_name": self.library_name,
            "architectures": list(self.architectures),
            "model_type": self.model_type,
            "max_position_embeddings": self.max_position_embeddings,
            "torch_dtype": self.torch_dtype,
            "quantization": self.quantization,
            "parameter_count": self.parameter_count,
            "weight_format": self.weight_format,
            "weight_files": [item.to_dict() for item in self.weight_files],
            "weight_bytes": self.weight_bytes,
            "total_bytes": self.total_bytes,
            "file_count": self.file_count,
            "warnings": list(self.warnings),
            "requires_remote_code": self.requires_remote_code,
        }


@dataclass(frozen=True)
class FitEstimate:
    """Advisory per-node memory fit for an even tensor-parallel split."""

    node_count: int
    weights_per_node_bytes: int
    usable_per_node_bytes: int
    headroom_per_node_bytes: int
    fits: bool
    message: str
    required_headroom_bytes: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "node_count": self.node_count,
            "weights_per_node_bytes": self.weights_per_node_bytes,
            "usable_per_node_bytes": self.usable_per_node_bytes,
            "headroom_per_node_bytes": self.headroom_per_node_bytes,
            "required_headroom_bytes": self.required_headroom_bytes,
            "fits": self.fits,
            "message": self.message,
        }


# Links ---------------------------------------------------------------------


def _invalid_link(message: str) -> CatalogError:
    return CatalogError("invalid_model_link", message, _LINK_REMEDIATION)


def _validate_repo_part(part: str, label: str) -> str:
    if (
        not _REPO_PART_RE.fullmatch(part)
        or ".." in part
        or "--" in part
        or part.endswith((".", "-"))
        or part.lower().endswith(".git")
    ):
        raise _invalid_link(
            f"The {label} must be 1-96 letters, digits, '.', '_' or '-', start "
            "with a letter or digit, contain no '..' or '--', and not end with "
            "'.', '-' or '.git'."
        )
    return part


def _validate_revision(revision: str) -> str:
    if (
        not _REVISION_RE.fullmatch(revision)
        or ".." in revision
        or "//" in revision
        or revision.endswith("/")
    ):
        raise _invalid_link(
            "The revision must be a branch, tag or commit of 1-128 letters, "
            "digits, '.', '_', '-' or '/' and must not contain '..'."
        )
    return revision


def _check_first_segment(segment: str) -> None:
    lowered = segment.lower()
    if lowered in _NON_MODEL_SECTIONS:
        raise _invalid_link(_NON_MODEL_SECTIONS[lowered])
    if lowered in _RESERVED_FIRST_SEGMENTS:
        raise _invalid_link("The link is not a model page.")


def _reference(org: str, name: str, revision: str | None) -> HFReference:
    _check_first_segment(org)
    _validate_repo_part(org, "organization")
    _validate_repo_part(name, "model name")
    if revision is not None:
        _validate_revision(revision)
    return HFReference(repo_id=f"{org}/{name}", revision=revision)


def _url_segments(value: str) -> list[str]:
    parts = urllib.parse.urlsplit(value)
    if parts.scheme.lower() not in ("http", "https"):
        raise _invalid_link("Only https links to huggingface.co models are accepted.")
    if "@" in parts.netloc:
        raise _invalid_link("Model links must not contain credentials.")
    if ":" in parts.netloc:
        raise _invalid_link("Model links must not contain a port.")
    if parts.netloc.lower() not in LINK_HOSTS:
        raise _invalid_link("Only huggingface.co and hf.co model links are accepted.")
    path = parts.path
    if not path.startswith("/") or path == "/":
        raise _invalid_link("The link does not name a model.")
    segments = path[1:].split("/")
    if segments[-1] == "":
        segments.pop()
    if any(segment == "" for segment in segments):
        raise _invalid_link("The link contains an empty path segment.")
    try:
        decoded = [
            urllib.parse.unquote(segment, encoding="utf-8", errors="strict")
            for segment in segments
        ]
    except UnicodeDecodeError:
        raise _invalid_link("The link has an invalid percent-encoding.") from None
    if any(segment in ("", ".", "..") for segment in decoded):
        raise _invalid_link("The link must not contain '.' or '..' segments.")
    return decoded


def _parse_url(value: str) -> HFReference:
    decoded = _url_segments(value)
    _check_first_segment(decoded[0])
    if len(decoded) < 2:
        raise _invalid_link(
            "The link must include the organization, e.g. huggingface.co/org/name."
        )
    org, name, rest = decoded[0], decoded[1], decoded[2:]
    revision: str | None = None
    if rest:
        section = rest[0]
        if section in ("tree", "blob", "resolve"):
            if len(rest) < 2:
                raise _invalid_link(f"The {section} link does not name a revision.")
            revision = rest[1]
        elif section == "commit":
            if len(rest) != 2 or not _COMMIT_LINK_RE.fullmatch(rest[1]):
                raise _invalid_link(
                    "A commit link must end with a 7-40 character hex commit id."
                )
            revision = rest[1].lower()
        else:
            raise _invalid_link(
                "Only model page, tree, blob, resolve and commit links are supported."
            )
    return _reference(org, name, revision)


def _parse_plain(value: str) -> HFReference:
    repo, separator, revision = value.partition("@")
    if separator and not revision:
        raise _invalid_link("The revision after '@' is empty.")
    segments = repo.split("/")
    _check_first_segment(segments[0])
    if len(segments) == 1:
        raise _invalid_link("The model id must include the organization: org/name.")
    if len(segments) != 2:
        raise _invalid_link("A model id has exactly one '/', for example org/name.")
    return _reference(segments[0], segments[1], revision if separator else None)


def parse_hf_link(text: str) -> HFReference:
    """Parse a pasted Hugging Face model link or ``org/name[@revision]`` id.

    Accepted forms: ``https://huggingface.co/org/name`` and its ``http``,
    ``www.``, ``hf.co`` and scheme-less equivalents (a trailing slash, query
    and fragment are ignored), optionally followed by ``/tree/<rev>[/...]``,
    ``/blob/<rev>/<file>``, ``/resolve/<rev>/<file>`` or ``/commit/<sha>``;
    plain ``org/name``; and ``org/name@rev``. Path segments are
    percent-decoded exactly once, so ``refs%2Fpr%2F1`` becomes ``refs/pr/1``.

    Args:
        text: Untrusted operator input.

    Returns:
        The validated reference; ``revision`` is ``None`` when not given.

    Raises:
        CatalogError: ``invalid_model_link`` for datasets, spaces,
            collections, other hosts, credentials, ports, ``..``, ``--``, a
            ``.git`` suffix, ids without an organization, or characters
            outside the Hub's repository and revision syntax.

    """
    if not isinstance(text, str):
        raise _invalid_link("The model link must be text.")
    value = text.strip()
    if not value:
        raise _invalid_link("The model link is empty.")
    if len(value) > MAX_LINK_LENGTH:
        raise _invalid_link("The model link is too long.")
    if (
        any(ord(char) <= 0x20 or 0x7F <= ord(char) <= 0x9F for char in value)
        or "\\" in value
    ):
        raise _invalid_link(
            "The model link must not contain spaces, control characters or backslashes."
        )
    schemeless_prefixes = tuple(f"{host}/" for host in LINK_HOSTS)
    if "://" not in value and value.lower().startswith(schemeless_prefixes):
        value = "https://" + value
    if "://" in value:
        return _parse_url(value)
    return _parse_plain(value)


# Hub metadata --------------------------------------------------------------


def _shape_error(detail: str) -> CatalogError:
    return CatalogError(
        "upstream_unavailable",
        f"Hugging Face returned unusable model metadata: {detail}.",
        "Retry later. If this persists, inspect the repository on huggingface.co.",
    )


def _model_api_error(
    exc: CatalogError, ref: HFReference, has_token: bool
) -> CatalogError:
    repo = ref.repo_id
    if exc.code == "not_found":
        if exc.upstream_code == "RevisionNotFound" and ref.revision is not None:
            message = f"Revision {ref.revision!r} does not exist in {repo}."
            remediation = "Check the branch, tag or commit in the link."
        else:
            message = f"Hugging Face has no model named {repo}."
            remediation = "Check the organization and model name in the link."
        code = "model_not_found"
    elif exc.code == "upstream_unauthorized":
        if exc.upstream_code == "GatedRepo" or exc.status == 403 or has_token:
            code = "model_gated"
            message = f"Hugging Face denied access to {repo}."
            remediation = _ACCESS_REMEDIATION
        else:
            code = "model_not_found"
            message = f"{repo} does not exist or is private."
            remediation = (
                "Check the link. For a private repository, set HF_TOKEN for the "
                "controller and run `hf auth login` on every Spark node."
            )
    else:
        return exc
    return CatalogError(
        code, message, remediation, status=exc.status, upstream_code=exc.upstream_code
    )


def _bool_field(data: Mapping[str, Any], key: str) -> bool:
    value = data.get(key)
    if value is None or value is False:
        return False
    if value is True:
        return True
    if key == "gated" and isinstance(value, str) and value.strip():
        return True  # The Hub reports gated repositories as "auto" or "manual".
    raise _shape_error(f"{key!r} has an unexpected type")


def _safe_file_name(name: Any) -> bool:
    if not isinstance(name, str) or not 1 <= len(name) <= 1024:
        return False
    if name.startswith("/") or "\\" in name:
        return False
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in name):
        return False
    return all(segment not in ("", ".", "..") for segment in name.split("/"))


def _siblings(data: Mapping[str, Any]) -> list[WeightFile]:
    raw = data.get("siblings")
    if not isinstance(raw, list):
        raise _shape_error("the file listing is missing")
    files: list[WeightFile] = []
    for item in raw:
        if not isinstance(item, dict):
            raise _shape_error("a file entry is not an object")
        name = item.get("rfilename")
        size = item.get("size")
        if not isinstance(name, str) or not _safe_file_name(name):
            raise _shape_error("a file entry has an unsafe or missing name")
        if not http.is_int(size) or not 0 <= size <= MAX_FILE_BYTES:
            raise _shape_error("a file entry has a missing or implausible size")
        files.append(WeightFile(name=name, size_bytes=size))
    return files


def _gib(value: int) -> str:
    return f"{value / GIB:.1f} GiB"


def _display_folder(name: str) -> str:
    folder = re.sub(r"[^A-Za-z0-9._-]", "_", name.split("/", 1)[0])[:32]
    return f"{folder}/"


def _select_safetensors(
    files: list[WeightFile], safetensors: list[WeightFile], warnings: list[str]
) -> list[WeightFile]:
    sharded = [item for item in safetensors if not item.name.startswith("consolidated")]
    chosen = safetensors
    if sharded and len(sharded) < len(safetensors):
        chosen = sharded
        warnings.append(
            "Ignored duplicate Mistral-format consolidated*.safetensors; sizes "
            "count only the Hugging Face-format shards that vLLM loads."
        )
    elif not sharded:
        warnings.append(
            "Only Mistral-native consolidated*.safetensors weights are present; "
            "the recipe must use vLLM's Mistral config and load formats."
        )
    nested = [
        item
        for item in files
        if "/" in item.name and item.name.endswith(_WEIGHT_SUFFIXES)
    ]
    if nested:
        folders = ", ".join(sorted({_display_folder(item.name) for item in nested})[:4])
        warnings.append(
            f"Ignored {len(nested)} weight-like file(s) in subdirectories "
            f"({folders}); vLLM loads only the top-level safetensors."
        )
    has_index = any(
        "/" not in item.name and item.name.endswith(".safetensors.index.json")
        for item in files
    )
    if len(chosen) > 1 and not has_index:
        warnings.append(
            "Sharded safetensors weights have no *.safetensors.index.json; "
            "vLLM may not load them."
        )
    return chosen


def _select_weights(
    files: list[WeightFile], warnings: list[str]
) -> tuple[str, list[WeightFile]]:
    top_level = [item for item in files if "/" not in item.name]
    safetensors = [item for item in top_level if item.name.endswith(".safetensors")]
    if safetensors:
        return "safetensors", _select_safetensors(files, safetensors, warnings)
    gguf = [item for item in files if item.name.endswith(".gguf")]
    if gguf:
        warnings.append(
            "GGUF-only repository: the vLLM recipe does not support GGUF; "
            "choose a safetensors repository."
        )
        if len(gguf) > 1:
            warnings.append(
                f"Weight size sums all {len(gguf)} GGUF files, which usually "
                "hold several alternative quantizations."
            )
        return "gguf", gguf
    pytorch_bin = [item for item in top_level if _PYTORCH_BIN_RE.fullmatch(item.name)]
    if pytorch_bin:
        warnings.append(
            "Only legacy pytorch_model*.bin (pickle) weights are present; "
            "prefer a safetensors repository."
        )
        return "pytorch-bin", pytorch_bin
    warnings.append(
        "No loadable top-level weights (safetensors, GGUF or "
        "pytorch_model*.bin) were found."
    )
    return "unknown", []


def _token_field(
    value: Any, pattern: re.Pattern[str], malformed: list[str], label: str
) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and pattern.fullmatch(value.strip()):
        return value.strip()
    malformed.append(label)
    return None


def _license(card: Any, malformed: list[str]) -> str | None:
    if card is None:
        return None
    if not isinstance(card, dict):
        malformed.append("cardData")
        return None
    value = card.get("license")
    if isinstance(value, list):
        names = [
            item.strip()
            for item in value
            if isinstance(item, str) and _LICENSE_RE.fullmatch(item.strip())
        ]
        if len(names) != len(value):
            malformed.append("license")
        return ", ".join(names[:4]) or None
    result = _token_field(value, _LICENSE_RE, malformed, "license")
    if result == "other":
        name = card.get("license_name")
        if isinstance(name, str) and _LICENSE_RE.fullmatch(name.strip()):
            return name.strip()
    return result


def _bounded_int(value: Any, maximum: int) -> int | None:
    return value if http.is_int(value) and 0 < value <= maximum else None


def _parameter_count(data: Mapping[str, Any], malformed: list[str]) -> int | None:
    for key in ("safetensors", "gguf"):
        section = data.get(key)
        if section is None:
            continue
        total = section.get("total") if isinstance(section, dict) else None
        count = _bounded_int(total, MAX_PARAMETER_COUNT)
        if count is not None:
            return count
        malformed.append(f"{key}.total")
    return None


def _context_length(
    config: Mapping[str, Any], gguf: Any, malformed: list[str]
) -> int | None:
    candidates: list[tuple[str, Any]] = [
        ("max_position_embeddings", config.get("max_position_embeddings"))
    ]
    text_config = config.get("text_config")
    if isinstance(text_config, dict):
        candidates.append(
            (
                "text_config.max_position_embeddings",
                text_config.get("max_position_embeddings"),
            )
        )
    for key in ("max_seq_len", "seq_length", "n_positions"):
        candidates.append((key, config.get(key)))
    if isinstance(gguf, dict):
        candidates.append(("gguf.context_length", gguf.get("context_length")))
    for label, value in candidates:
        if value is None:
            continue
        length = _bounded_int(value, MAX_CONTEXT_LENGTH)
        if length is not None:
            return length
        malformed.append(label)
    return None


def _dtype(config: Mapping[str, Any], malformed: list[str]) -> str | None:
    sources: list[Mapping[str, Any]] = [config]
    text_config = config.get("text_config")
    if isinstance(text_config, dict):
        sources.append(text_config)
    for source in sources:
        for key in ("torch_dtype", "dtype"):
            value = _token_field(source.get(key), _DTYPE_RE, malformed, key)
            if value is not None:
                return value
    return None


def _quantization(
    configs: tuple[Mapping[str, Any], ...], malformed: list[str]
) -> str | None:
    for config in configs:
        for key in ("quantization_config", "compression_config"):
            section = config.get(key)
            if section is None:
                continue
            if not isinstance(section, dict):
                malformed.append(key)
                continue
            method = _token_field(
                section.get("quant_method"), _TOKEN_RE, malformed, f"{key}.quant_method"
            )
            if method is not None:
                return method.lower()
    return None


def _architectures(
    configs: tuple[Mapping[str, Any], ...], malformed: list[str]
) -> tuple[str, ...]:
    for config in configs:
        value = config.get("architectures")
        if value is None:
            continue
        if not isinstance(value, list):
            malformed.append("architectures")
            continue
        names = tuple(
            item
            for item in value[:16]
            if isinstance(item, str) and _ARCHITECTURE_RE.fullmatch(item)
        )
        if len(names) != len(value):
            malformed.append("architectures")
        if names:
            return names
    return ()


def _first_token(
    configs: tuple[Mapping[str, Any], ...], key: str, malformed: list[str]
) -> str | None:
    for config in configs:
        value = _token_field(config.get(key), _TOKEN_RE, malformed, key)
        if value is not None:
            return value
    return None


def _model_api_url(ref: HFReference) -> str:
    repo = "/".join(
        urllib.parse.quote(part, safe="") for part in ref.repo_id.split("/")
    )
    if ref.revision is None:
        return f"{HF_API_ROOT}/api/models/{repo}?blobs=true"
    revision = urllib.parse.quote(ref.revision, safe="")
    return f"{HF_API_ROOT}/api/models/{repo}/revision/{revision}?blobs=true"


def _canonical_repo_id(
    data: Mapping[str, Any], ref: HFReference, warnings: list[str]
) -> str:
    raw = data.get("id", data.get("modelId"))
    if not isinstance(raw, str) or raw.count("/") != 1:
        raise _shape_error("the repository id is missing")
    org, name = raw.split("/")
    try:
        _validate_repo_part(org, "organization")
        _validate_repo_part(name, "model name")
    except CatalogError:
        raise _shape_error("the repository id is not a valid model id") from None
    if raw.lower() != ref.repo_id.lower():
        warnings.append(
            f"Hugging Face redirected {ref.repo_id} to {raw}; the canonical id is used."
        )
    return raw


def _fetch_config(
    get_json: Callable[..., Any],
    url: str,
    headers: dict[str, str] | None,
    gated: bool,
    warnings: list[str],
) -> dict[str, Any]:
    try:
        raw_config = get_json(url, headers=headers, max_bytes=CONFIG_MAX_BYTES)
    except CatalogError as exc:
        if exc.code == "upstream_unauthorized" and gated:
            warnings.append(
                "config.json is behind the model gate; context length is "
                "unknown until HF_TOKEN for an account with accepted access is "
                "configured."
            )
            return {}
        if exc.code == "upstream_unauthorized":
            raise CatalogError(
                "model_gated",
                "Hugging Face denied access to the model's config.json.",
                _ACCESS_REMEDIATION,
                status=exc.status,
                upstream_code=exc.upstream_code,
            ) from None
        if exc.code == "host_not_allowed":
            warnings.append(
                "config.json is served from a CDN host outside the catalog "
                "allowlist; context length is unknown."
            )
            return {}
        if exc.code == "not_found":
            raise _shape_error(
                "config.json is listed but missing at the resolved commit"
            ) from None
        raise
    if not isinstance(raw_config, dict):
        raise _shape_error("config.json is not an object")
    return raw_config


def fetch_model_info(
    ref: HFReference,
    *,
    token: str | None = None,
    get_json: Callable[..., Any] = http.get_json,
) -> HFModelInfo:
    """Fetch Hub metadata for ``ref`` pinned to the commit the Hub resolved.

    Reads ``/api/models/{repo}[/revision/{rev}]?blobs=true`` and, when the
    repository lists one, ``/{repo}/resolve/{sha}/config.json``. A gated
    model whose ``config.json`` is refused is still returned, with a warning
    and an unknown context length.

    Args:
        ref: Reference from ``parse_hf_link``.
        token: Optional Hugging Face token, sent only as ``Authorization``.
        get_json: JSON fetcher with the ``http.get_json`` signature.

    Returns:
        Validated metadata; malformed optional fields become ``None`` and
        are named in a warning.

    Raises:
        CatalogError: ``invalid_model_link`` or ``invalid_token`` for bad
            input; ``model_not_found`` for a 404, a disabled repository, or
            a 401 without a token (the Hub hides private repositories);
            ``model_gated`` for a 403, ``GatedRepo`` or a 401 with a token;
            ``upstream_unavailable`` for unusable required fields; or any
            transport error from ``get_json``.

    """
    headers = (
        None
        if token is None
        else http.bearer_header(token, service="Hugging Face", env_name="HF_TOKEN")
    )
    try:
        data = get_json(
            _model_api_url(ref), headers=headers, max_bytes=MODEL_API_MAX_BYTES
        )
    except CatalogError as exc:
        raise _model_api_error(exc, ref, token is not None) from None
    if not isinstance(data, dict):
        raise _shape_error("the response is not an object")

    warnings: list[str] = []
    malformed: list[str] = []
    repo_id = _canonical_repo_id(data, ref, warnings)
    sha = data.get("sha")
    if not isinstance(sha, str) or not _COMMIT_RE.fullmatch(sha):
        raise _shape_error("the resolved commit is not a 40-hex lowercase sha")
    requested = ref.revision if ref.revision is not None else "main"
    pinned_request = ref.revision is not None and _COMMIT_RE.fullmatch(
        ref.revision.lower()
    )
    if pinned_request and sha != requested.lower():
        raise _shape_error("the resolved commit does not match the requested one")
    gated = _bool_field(data, "gated")
    private = _bool_field(data, "private")
    if data.get("disabled") is True:
        raise CatalogError(
            "model_not_found",
            f"{repo_id} has been disabled on Hugging Face.",
            "Choose another repository.",
        )
    files = _siblings(data)
    weight_format, weights = _select_weights(files, warnings)

    api_config = data.get("config")
    if api_config is None:
        api_config = {}
    elif not isinstance(api_config, dict):
        raise _shape_error("the embedded config is not an object")

    config: dict[str, Any] = {}
    if any(item.name == "config.json" for item in files):
        quoted = urllib.parse.quote(repo_id, safe="/")
        config_url = f"{HF_API_ROOT}/{quoted}/resolve/{sha}/config.json"
        config = _fetch_config(get_json, config_url, headers, gated, warnings)
    elif weight_format != "gguf":
        warnings.append(
            "No config.json at the repository root; architecture details come "
            "from Hub metadata only."
        )

    configs = (config, api_config)
    requires_remote_code = any("auto_map" in item for item in configs)
    if requires_remote_code:
        warnings.append(
            "config.json declares auto_map custom code; vLLM may need "
            "--trust-remote-code. Review the repository's Python files before "
            "enabling it."
        )
    total_bytes = sum(item.size_bytes for item in files)
    if gated:
        warnings.append(
            "Gated model: accept its terms on huggingface.co and run "
            "`hf auth login` on every Spark node before downloading."
        )
    if private:
        warnings.append(
            "Private repository: every Spark node needs a Hugging Face token "
            "with read access."
        )
    if total_bytes >= LARGE_DOWNLOAD_BYTES:
        warnings.append(
            f"Large download: {_gib(total_bytes)} in total; every node that "
            "serves the model needs a local copy."
        )
    license_name = _license(data.get("cardData"), malformed)
    pipeline_tag = _token_field(
        data.get("pipeline_tag"), _TOKEN_RE, malformed, "pipeline_tag"
    )
    library_name = _token_field(
        data.get("library_name"), _TOKEN_RE, malformed, "library_name"
    )
    architectures = _architectures(configs, malformed)
    model_type = _first_token(configs, "model_type", malformed)
    context_length = _context_length(config, data.get("gguf"), malformed)
    torch_dtype = _dtype(config, malformed)
    quantization = _quantization(configs, malformed)
    parameter_count = _parameter_count(data, malformed)
    if malformed:
        fields = ", ".join(sorted(set(malformed))[:8])
        warnings.append(f"Ignored malformed metadata field(s): {fields}.")
    return HFModelInfo(
        repo_id=repo_id,
        revision=sha,
        requested_revision=requested,
        gated=gated,
        private=private,
        license=license_name,
        pipeline_tag=pipeline_tag,
        library_name=library_name,
        architectures=architectures,
        model_type=model_type,
        max_position_embeddings=context_length,
        torch_dtype=torch_dtype,
        quantization=quantization,
        parameter_count=parameter_count,
        weight_format=weight_format,
        weight_files=tuple(weights),
        weight_bytes=sum(item.size_bytes for item in weights),
        total_bytes=total_bytes,
        file_count=len(files),
        warnings=tuple(warnings),
        requires_remote_code=requires_remote_code,
    )


# Fit estimate --------------------------------------------------------------


def estimate_fit(info: HFModelInfo, node_count: int) -> FitEstimate:
    """Estimate whether the weights fit when split evenly across DGX Sparks.

    ``usable = DGX_SPARK_MEMORY_BYTES * GPU_MEMORY_UTILIZATION`` (the vLLM
    ``--gpu-memory-utilization`` the estimate assumes) and weights per node are
    ``ceil(weight_bytes / node_count)`` (even tensor-parallel split). The
    model fits when the remaining headroom is at least
    ``max(4 GiB, 5% of usable)`` for KV cache and activations. Unknown
    weight sizes never fit.

    Args:
        info: Metadata from ``fetch_model_info``.
        node_count: Tensor-parallel node count, 1 to ``MAX_NODE_COUNT``.

    Returns:
        The estimate with a one-sentence explanation.

    Raises:
        CatalogError: ``invalid_fit_parameters`` for an out-of-range node count.

    """
    if not http.is_int(node_count) or not 1 <= node_count <= MAX_NODE_COUNT:
        raise CatalogError(
            "invalid_fit_parameters",
            f"Node count must be an integer from 1 to {MAX_NODE_COUNT}.",
        )
    weight_bytes = info.weight_bytes
    usable = int(DGX_SPARK_MEMORY_BYTES * GPU_MEMORY_UTILIZATION)
    required = max(MIN_KV_HEADROOM_BYTES, math.ceil(usable * MIN_KV_HEADROOM_FRACTION))
    if not http.is_int(weight_bytes) or weight_bytes <= 0:
        return FitEstimate(
            node_count=node_count,
            weights_per_node_bytes=0,
            usable_per_node_bytes=usable,
            headroom_per_node_bytes=usable,
            fits=False,
            message="Weight size is unknown, so memory fit cannot be estimated.",
            required_headroom_bytes=required,
        )
    per_node = -(-weight_bytes // node_count)
    headroom = usable - per_node
    fits = headroom >= required
    nodes = "1 node" if node_count == 1 else f"{node_count} nodes"
    budget = f"{_gib(usable)} usable at {GPU_MEMORY_UTILIZATION:.0%} utilization"
    if fits:
        message = (
            f"Fits on {nodes}: {_gib(per_node)} of weights per node leaves "
            f"{_gib(headroom)} of the {budget} for KV cache and activations."
        )
    elif headroom < 0:
        message = (
            f"Does not fit on {nodes}: {_gib(per_node)} of weights per node "
            f"exceeds the {budget}."
        )
    else:
        message = (
            f"Does not fit on {nodes}: {_gib(per_node)} of weights per node "
            f"leaves only {_gib(headroom)} of the {budget}, below the "
            f"{_gib(required)} needed for KV cache and activations."
        )
    return FitEstimate(
        node_count=node_count,
        weights_per_node_bytes=per_node,
        usable_per_node_bytes=usable,
        headroom_per_node_bytes=headroom,
        fits=fits,
        message=message,
        required_headroom_bytes=required,
    )


# Naming --------------------------------------------------------------------


def default_served_alias(info: HFModelInfo) -> str:
    """Return a default ``--served-model-name`` alias for the model.

    Args:
        info: Metadata from ``fetch_model_info``.

    Returns:
        The lowercase model name with invalid characters replaced by ``-``,
        dashes collapsed, at most 64 characters, matching
        ``^[a-z0-9][a-z0-9._-]{0,63}$``; ``model`` if nothing remains.

    """
    name = info.repo_id.rsplit("/", 1)[-1].lower()
    alias = re.sub(r"[^a-z0-9._-]+", "-", name)
    alias = re.sub(r"-{2,}", "-", alias)
    alias = re.sub(r"^[^a-z0-9]+", "", alias)[:64].rstrip("-._")
    return alias or "model"
