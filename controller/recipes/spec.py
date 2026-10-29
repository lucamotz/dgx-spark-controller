# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Validated recipe drafts for the generic vLLM release recipe.

A recipe draft is the only way the browser influences what runs on a Spark, so
it is deliberately narrow: a pinned Hugging Face revision, a digest-pinned vLLM
release image, a placement chosen from configured nodes, and a short list of
bounded tunables. There is no free-form flag, path, environment variable, or
command anywhere in it. Every string that later reaches a remote environment
file is restricted to ``[A-Za-z0-9._:/@+-]`` so the pinned scripts can parse it
as data.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from typing import Any

from controller.domain import ID_RE as NODE_ID_RE

RECIPE_SCHEMA_VERSION = 1
DRAFT_ID_RE = re.compile(r"r-[0-9a-f]{12}\Z")
ALIAS_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")
REPO_ID_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}/[A-Za-z0-9][A-Za-z0-9._-]{0,95}\Z"
)
COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")
RELEASE_TAG_RE = re.compile(r"v\d+\.\d+\.\d+(?:\.post\d+)?(?:rc\d+)?\Z")
IMAGE_REFERENCE_RE = re.compile(r"vllm/vllm-openai@sha256:[0-9a-f]{64}\Z")
DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
PARSER_RE = re.compile(r"[a-z0-9_]{1,40}\Z")
KV_CACHE_DTYPES = ("auto", "fp8", "fp8_e4m3", "fp8_e5m2")
DRAFT_STATES = ("draft", "preparing", "prepared", "failed", "deleting")
# A draft in one of these states is owned by a running recipe operation.
BUSY_STATES = frozenset({"preparing", "deleting"})
DISPLAY_NAME_RE = re.compile(r"[^\x00-\x1f\x7f]{1,120}\Z")


class RecipeError(ValueError):
    """A safe, user-facing recipe validation error."""

    def __init__(self, code: str, message: str, remediation: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = (
            remediation or "Correct the highlighted recipe field and save again."
        )


@dataclass(frozen=True)
class RecipeModel:
    """Pinned Hugging Face facts captured when the link was inspected."""

    repo_id: str
    revision: str
    weight_bytes: int
    weight_files: tuple[tuple[str, int], ...]
    max_position_embeddings: int | None = None
    architectures: tuple[str, ...] = ()
    quantization: str | None = None
    gated: bool = False
    needs_remote_code: bool = False

    def __post_init__(self) -> None:
        if not REPO_ID_RE.fullmatch(self.repo_id) or ".." in self.repo_id:
            raise RecipeError(
                "invalid_model", "The Hugging Face repository id is invalid."
            )
        if not COMMIT_RE.fullmatch(self.revision):
            raise RecipeError(
                "model_revision_unpinned",
                "The model must be pinned to a 40-character commit revision.",
                "Inspect the model link again so the controller can resolve its "
                "commit.",
            )
        if self.weight_bytes <= 0 or not self.weight_files:
            raise RecipeError(
                "model_has_no_weights",
                "The model revision has no safetensors weights the recipe can serve.",
                "Choose a repository that publishes safetensors weights.",
            )
        for name, size in self.weight_files:
            if (
                not name
                or name.startswith("/")
                or ".." in name.split("/")
                or not re.fullmatch(r"[A-Za-z0-9._/+-]{1,255}", name)
                or size < 0
            ):
                raise RecipeError(
                    "invalid_model",
                    "The model revision lists an unsafe weight file name.",
                )


@dataclass(frozen=True)
class RecipeRuntime:
    """A released vLLM image pinned to its linux/arm64 manifest digest."""

    release_tag: str
    reference: str
    arm64_digest: str
    cuda_variant: str = "default"

    def __post_init__(self) -> None:
        if not RELEASE_TAG_RE.fullmatch(self.release_tag):
            raise RecipeError(
                "invalid_release", "Choose a released vLLM version such as v0.31.0."
            )
        if not IMAGE_REFERENCE_RE.fullmatch(self.reference) or not DIGEST_RE.fullmatch(
            self.arm64_digest
        ):
            raise RecipeError(
                "runtime_unpinned", "The vLLM runtime image must be pinned by digest."
            )
        if not self.reference.endswith(self.arm64_digest):
            raise RecipeError(
                "runtime_unpinned",
                "The pinned image reference must name its arm64 digest.",
            )
        if self.cuda_variant not in {"default", "cu129", "cu130"}:
            raise RecipeError("invalid_release", "Unsupported vLLM CUDA image variant.")


@dataclass(frozen=True)
class RecipeSettings:
    """Bounded serving tunables. Defaults suit one or two 128 GB DGX Sparks."""

    max_model_len: int = 32_768
    max_num_seqs: int = 8
    max_num_batched_tokens: int = 8_192
    gpu_memory_utilization: float = 0.85
    kv_cache_dtype: str = "auto"
    enable_prefix_caching: bool = True
    trust_remote_code: bool = False
    enforce_eager: bool = False
    tool_call_parser: str | None = None
    reasoning_parser: str | None = None
    port: int = 8000
    startup_timeout_s: int = 2_400

    def __post_init__(self) -> None:
        _bounded_int(self.max_model_len, 512, 4_194_304, "max_model_len")
        _bounded_int(self.max_num_seqs, 1, 1_024, "max_num_seqs")
        _bounded_int(
            self.max_num_batched_tokens, 256, 1_048_576, "max_num_batched_tokens"
        )
        _bounded_int(self.port, 1_024, 65_535, "port")
        _bounded_int(self.startup_timeout_s, 120, 14_400, "startup_timeout_s")
        if (
            isinstance(self.gpu_memory_utilization, bool)
            or not isinstance(self.gpu_memory_utilization, (int, float))
            or not 0.30 <= float(self.gpu_memory_utilization) <= 0.95
        ):
            raise RecipeError(
                "invalid_setting",
                "gpu_memory_utilization must be between 0.30 and 0.95.",
            )
        if round(float(self.gpu_memory_utilization), 2) != float(
            self.gpu_memory_utilization
        ):
            raise RecipeError(
                "invalid_setting", "gpu_memory_utilization supports two decimal places."
            )
        if self.kv_cache_dtype not in KV_CACHE_DTYPES:
            raise RecipeError(
                "invalid_setting",
                f"kv_cache_dtype must be one of {', '.join(KV_CACHE_DTYPES)}.",
            )
        for name in ("enable_prefix_caching", "trust_remote_code", "enforce_eager"):
            if not isinstance(getattr(self, name), bool):
                raise RecipeError("invalid_setting", f"{name} must be true or false.")
        for name in ("tool_call_parser", "reasoning_parser"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, str) or not PARSER_RE.fullmatch(value)
            ):
                raise RecipeError(
                    "invalid_setting",
                    f"{name} must be a vLLM parser name such as 'hermes'.",
                )


@dataclass(frozen=True)
class RecipePlacement:
    """Configured nodes for tensor parallelism; the head serves the endpoint."""

    node_ids: tuple[str, ...]
    head_node_id: str

    def __post_init__(self) -> None:
        if not self.node_ids or len(self.node_ids) > 8:
            raise RecipeError(
                "invalid_placement", "Choose between one and eight configured nodes."
            )
        if len(set(self.node_ids)) != len(self.node_ids):
            raise RecipeError("invalid_placement", "A node can be selected only once.")
        if any(not NODE_ID_RE.fullmatch(node_id) for node_id in self.node_ids):
            raise RecipeError("invalid_placement", "A selected node id is invalid.")
        if self.head_node_id not in self.node_ids:
            raise RecipeError(
                "invalid_placement", "The head node must be one of the selected nodes."
            )

    @property
    def ordered_node_ids(self) -> tuple[str, ...]:
        """Head first (rank 0), then workers in a stable order."""
        return (
            self.head_node_id,
            *sorted(node for node in self.node_ids if node != self.head_node_id),
        )


@dataclass(frozen=True)
class RecipeDraft:
    """A complete, validated recipe draft as persisted by the recipe store."""

    draft_id: str
    display_name: str
    served_alias: str
    model: RecipeModel
    runtime: RecipeRuntime
    placement: RecipePlacement
    settings: RecipeSettings = field(default_factory=RecipeSettings)
    status: str = "draft"
    profile_id: str | None = None
    last_operation_id: str | None = None
    message: str = ""
    created_at: str = ""
    updated_at: str = ""
    schema_version: int = RECIPE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECIPE_SCHEMA_VERSION:
            raise RecipeError(
                "unsupported_recipe",
                "This recipe was saved by an incompatible controller version.",
            )
        if not DRAFT_ID_RE.fullmatch(self.draft_id):
            raise RecipeError("invalid_recipe", "The recipe id is invalid.")
        if (
            not DISPLAY_NAME_RE.fullmatch(self.display_name.strip() or "x")
            or not self.display_name.strip()
        ):
            raise RecipeError(
                "invalid_recipe", "The recipe name must be 1-120 printable characters."
            )
        if not ALIAS_RE.fullmatch(self.served_alias):
            raise RecipeError(
                "invalid_alias",
                "The served model name must be 1-64 lowercase letters, digits, '.', "
                "'_' or '-'.",
            )
        if self.status not in DRAFT_STATES:
            raise RecipeError("invalid_recipe", "The recipe status is invalid.")
        model_max = self.model.max_position_embeddings
        if model_max and self.settings.max_model_len > model_max:
            raise RecipeError(
                "context_exceeds_model",
                f"max_model_len {self.settings.max_model_len} exceeds the model's "
                f"{model_max} positions.",
                "Lower the context length to the model's maximum or less.",
            )
        if self.settings.max_num_batched_tokens < min(
            self.settings.max_model_len, 2_048
        ):
            raise RecipeError(
                "invalid_setting",
                "max_num_batched_tokens must be at least 2048 or the context length, "
                "whichever is smaller.",
            )
        if self.model.needs_remote_code and not self.settings.trust_remote_code:
            raise RecipeError(
                "remote_code_required",
                "This model ships custom modelling code and needs trust_remote_code.",
                "Review the repository's code, then enable trust_remote_code "
                "explicitly.",
            )

    @property
    def node_count(self) -> int:
        return len(self.placement.node_ids)

    def with_status(
        self, status: str, *, message: str = "", **changes: Any
    ) -> RecipeDraft:
        return replace(self, status=status, message=message, **changes)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["model"]["weight_files"] = [
            [name, size] for name, size in self.model.weight_files
        ]
        value["model"]["architectures"] = list(self.model.architectures)
        value["placement"]["node_ids"] = list(self.placement.node_ids)
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RecipeDraft:
        try:
            model = dict(value["model"])
            model["weight_files"] = tuple(
                (str(name), int(size)) for name, size in model.get("weight_files", ())
            )
            model["architectures"] = tuple(
                str(item) for item in model.get("architectures", ())
            )
            placement = dict(value["placement"])
            placement["node_ids"] = tuple(str(item) for item in placement["node_ids"])
            return cls(
                schema_version=int(value.get("schema_version", 0)),
                draft_id=str(value["draft_id"]),
                display_name=str(value["display_name"]),
                served_alias=str(value["served_alias"]),
                model=RecipeModel(**model),
                runtime=RecipeRuntime(**dict(value["runtime"])),
                placement=RecipePlacement(**placement),
                settings=settings_from_mapping(value.get("settings") or {}),
                status=str(value.get("status", "draft")),
                profile_id=value.get("profile_id"),
                last_operation_id=value.get("last_operation_id"),
                message=str(value.get("message", "")),
                created_at=str(value.get("created_at", "")),
                updated_at=str(value.get("updated_at", "")),
            )
        except RecipeError:
            raise
        except (KeyError, TypeError, ValueError) as error:
            raise RecipeError(
                "invalid_recipe",
                f"The stored recipe is malformed: {type(error).__name__}.",
            ) from error


SETTING_KEYS = tuple(RecipeSettings.__dataclass_fields__)


def settings_from_mapping(
    value: Mapping[str, Any], *, base: RecipeSettings | None = None
) -> RecipeSettings:
    """Build settings from a request body, rejecting unknown keys (fail closed)."""
    if not isinstance(value, Mapping):
        raise RecipeError("invalid_setting", "settings must be a JSON object.")
    unknown = sorted(set(value) - set(SETTING_KEYS))
    if unknown:
        raise RecipeError("invalid_setting", f"Unknown recipe setting {unknown[0]!r}.")
    current = asdict(base or RecipeSettings())
    for key, item in value.items():
        if key in {"tool_call_parser", "reasoning_parser"} and item in {"", None}:
            item = None
        if (
            key == "gpu_memory_utilization"
            and isinstance(item, int)
            and not isinstance(item, bool)
        ):
            item = float(item)
        current[key] = item
    try:
        return RecipeSettings(**current)
    except TypeError as error:
        raise RecipeError(
            "invalid_setting", "A recipe setting has the wrong type."
        ) from error


def _bounded_int(value: Any, minimum: int, maximum: int, name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise RecipeError(
            "invalid_setting",
            f"{name} must be an integer between {minimum} and {maximum}.",
        )


def validate_node_selection(node_ids: Sequence[str], configured: Sequence[str]) -> None:
    missing = [node_id for node_id in node_ids if node_id not in configured]
    if missing:
        raise RecipeError(
            "node_not_configured",
            f"Node {missing[0]!r} is not configured.",
            "Choose nodes from the configured cluster or finish setup first.",
        )
