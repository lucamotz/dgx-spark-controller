# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Render a validated recipe draft into remote files and a generated profile.

Everything produced here is derived from a :class:`RecipeDraft` and reviewed
node configuration. Remote files are the three pinned template scripts plus
``KEY=value`` environment files whose values already passed the draft's
character allowlist; the generated profile reuses the existing shared-vLLM
lifecycle contract (pinned script hashes, digest-pinned image, per-rank image
IDs, exact served alias, process identity, and memory-recovery gates).
"""

from __future__ import annotations

import hashlib
import ipaddress
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from importlib import resources
from typing import Any

from controller.config import parse_profile_document
from controller.domain import ModelProfile, NodeConfig
from controller.recipes.spec import RecipeDraft, RecipeError
from controller.transport.vllm_identity import VLLM_EXECUTABLE

TEMPLATE_NAME = "vllm-release"
TEMPLATE_FILES = ("start.sh", "stop.sh", "prepare.sh")
CACHE_VOLUME = "dgx-spark-controller-cache"
MASTER_PORT = "29501"
REMOTE_RECIPES_SUBDIR = ".local/share/dgx-spark-controller/recipes"
SAFE_VALUE_RE = re.compile(r"[A-Za-z0-9._:/@+-]+\Z")
# Shapes produced by profile_id_for and model_dir_for. Remote deletion accepts
# only names of exactly these shapes, never a path a profile or request names.
GENERATED_PROFILE_ID_RE = re.compile(r"g-[a-z0-9]+(?:-[a-z0-9]+)*-[0-9a-f]{6}\Z")
MODEL_DIR_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}--[A-Za-z0-9][A-Za-z0-9._-]{0,95}-[0-9a-f]{8}\Z"
)
MIB = 1024 * 1024
GIB = 1024 * MIB
CANARY_PROMPT = "Reply with OK."
CANARY_MAX_TOKENS = 16
CANARY_TIMEOUT_S = 120


def template_bytes(name: str) -> bytes:
    """Return one packaged template script exactly as it will be installed."""
    if name not in TEMPLATE_FILES:
        raise KeyError(name)
    return (
        resources.files("controller.recipes")
        .joinpath("templates", TEMPLATE_NAME, name)
        .read_bytes()
    )


def template_sha256(name: str) -> str:
    return hashlib.sha256(template_bytes(name)).hexdigest()


def profile_id_for(draft: RecipeDraft, prepare_id: str) -> str:
    """Profile id unique per prepare run that fits the 53-character deployment id.

    Every successful prepare registers a *new* profile. A deployment that is
    still running keeps matching its own profile's container identity, and a
    later load of the new profile replaces it through the normal stop-verify-
    start path instead of mutating a live deployment's contract.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", draft.served_alias).strip("-") or "model"
    return f"g-{slug[:40].rstrip('-')}-{prepare_id[:6]}"


def is_generated_profile_id(value: object) -> bool:
    """Whether ``value`` has the exact shape :func:`profile_id_for` produces."""
    return (
        isinstance(value, str)
        and GENERATED_PROFILE_ID_RE.fullmatch(value) is not None
        and len(value) <= 2 + 40 + 7
    )


def container_name_for(profile_id: str) -> str:
    return f"dgxc-{profile_id}"[:63].rstrip("-_")


def model_dir_name(repo_id: str, revision: str) -> str:
    """Directory name for a pinned revision inside a node's model root."""
    org, name = repo_id.split("/", 1)
    raw = f"{org}--{name}-{revision[:8]}"
    return re.sub(r"[^A-Za-z0-9._-]", "-", raw)


def model_dir_for(draft: RecipeDraft) -> str:
    return model_dir_name(draft.model.repo_id, draft.model.revision)


def model_path_for(model_root: str, draft: RecipeDraft) -> str:
    """Where a node keeps the draft's pinned weights below ``model_root``."""
    return f"{model_root.rstrip('/')}/{model_dir_for(draft)}"


def working_directory_for(home: str, profile_id: str) -> str:
    return f"{home.rstrip('/')}/{REMOTE_RECIPES_SUBDIR}/{profile_id}"


@dataclass(frozen=True)
class NodeFacts:
    """Facts reported by a successful prepare run on one node."""

    node_id: str
    home: str
    image_id: str
    vllm_version: str
    manifest_sha256: str
    manifest_path: str
    memory_total_bytes: int


@dataclass(frozen=True)
class RenderedRecipe:
    """All remote files and the generated profile for one prepared draft."""

    profile_id: str
    working_directory: str
    base_environment: Mapping[str, str]
    node_environment: Mapping[str, Mapping[str, str]]
    recipe_revision: str
    profile_document: Mapping[str, Any]

    def files(self) -> dict[str, tuple[bytes, int]]:
        """Remote files (name -> content, mode) installed in the working directory."""
        return {
            "start.sh": (template_bytes("start.sh"), 0o755),
            "stop.sh": (template_bytes("stop.sh"), 0o755),
            "recipe.env": (
                environment_text(self.base_environment).encode("utf-8"),
                0o600,
            ),
            ".dgx-controller-recipe-commit": (
                f"{self.recipe_revision}\n".encode(),
                0o644,
            ),
        }

    def profile(self) -> ModelProfile:
        return parse_profile_document(dict(self.profile_document))


def environment_text(values: Mapping[str, str]) -> str:
    """Serialise a recipe environment, refusing any value outside the allowlist."""
    lines = []
    for key, value in values.items():
        if not re.fullmatch(
            r"[A-Z][A-Z0-9_]{0,63}", key
        ) or not SAFE_VALUE_RE.fullmatch(value):
            raise RecipeError(
                "unsafe_recipe_value",
                f"Recipe value for {key} contains unsupported characters.",
            )
        lines.append(f"{key}={value}")
    return "\n".join(lines) + "\n"


def placement_nodes(
    draft: RecipeDraft, nodes: Mapping[str, NodeConfig]
) -> list[NodeConfig]:
    try:
        return [nodes[node_id] for node_id in draft.placement.ordered_node_ids]
    except KeyError as error:
        raise RecipeError(
            "node_not_configured",
            f"Node {error.args[0]!r} is no longer configured.",
            "Edit the recipe placement to use configured nodes.",
        ) from None


def common_model_root(selected: Sequence[NodeConfig]) -> str:
    roots = {node.model_roots[0] for node in selected}
    if len(roots) != 1:
        raise RecipeError(
            "model_root_mismatch",
            "Selected nodes use different model roots.",
            "Give every node in this placement the same first model root in the node "
            "configuration.",
        )
    return roots.pop()


def single_home(homes: Iterable[str]) -> str:
    """Return the one home directory every selected node reports."""
    distinct = set(homes)
    if len(distinct) != 1:
        raise RecipeError(
            "home_directory_mismatch",
            "Selected nodes report different home directories.",
            "Use the same Linux user name on every node in a multi-node placement.",
        )
    return distinct.pop()


def check_port(draft: RecipeDraft, selected: Sequence[NodeConfig]) -> None:
    for node in selected:
        if (
            not node.endpoint_ports.start
            <= draft.settings.port
            <= node.endpoint_ports.end
        ):
            raise RecipeError(
                "port_not_allowed",
                f"Port {draft.settings.port} is outside node {node.node_id!r}'s "
                "endpoint range "
                f"{node.endpoint_ports.start}-{node.endpoint_ports.end}.",
                "Pick a port inside every selected node's configured endpoint range.",
            )


def resolve_placement(
    draft: RecipeDraft, nodes: Mapping[str, NodeConfig]
) -> tuple[list[NodeConfig], str]:
    """Return the placement's nodes (head first) and their shared model root.

    Raises:
        RecipeError: A node is no longer configured, the port is outside a
            node's endpoint range, or the nodes use different model roots.

    """
    selected = placement_nodes(draft, nodes)
    check_port(draft, selected)
    return selected, common_model_root(selected)


def rank_addresses(selected: Sequence[NodeConfig]) -> dict[str, str]:
    """Choose one address per rank, preferring a shared direct cluster subnet.

    For a single node no collective traffic exists and the value is ``auto``.
    For several nodes, the first head cluster address whose /24 also contains a
    cluster address on every worker wins (the ConnectX-7 direct link); without
    such a subnet the management addresses are used.
    """
    if len(selected) == 1:
        return {selected[0].node_id: "auto"}
    head, workers = selected[0], selected[1:]
    for candidate in head.cluster_addresses:
        try:
            subnet = ipaddress.ip_network(f"{candidate}/24", strict=False)
        except ValueError:
            continue
        chosen = {head.node_id: candidate}
        for worker in workers:
            match = next(
                (
                    address
                    for address in worker.cluster_addresses
                    if _in_subnet(address, subnet)
                ),
                None,
            )
            if match is None:
                break
            chosen[worker.node_id] = match
        else:
            return chosen
    return {node.node_id: node.management_address for node in selected}


def _in_subnet(
    address: str, subnet: ipaddress.IPv4Network | ipaddress.IPv6Network
) -> bool:
    try:
        return ipaddress.ip_address(address) in subnet
    except ValueError:
        return False


def prepare_environment(
    draft: RecipeDraft,
    *,
    prepare_id: str,
    model_root: str,
    status_file: str,
    expected_files: str,
) -> dict[str, str]:
    required_free = int(draft.model.weight_bytes * 1.05) + 2 * GIB
    return {
        "PREPARE_ID": prepare_id,
        "VLLM_IMAGE": draft.runtime.reference,
        "HF_REPO": draft.model.repo_id,
        "HF_REVISION": draft.model.revision,
        "MODEL_ROOT": model_root,
        "MODEL_DIR": model_dir_for(draft),
        "STATUS_FILE": status_file,
        "EXPECTED_FILES": expected_files,
        "REQUIRED_FREE_BYTES": str(required_free),
        "CHECK_MULTINODE": _flag(draft.node_count > 1),
    }


def expected_files_text(draft: RecipeDraft) -> str:
    return "".join(f"{size} {name}\n" for name, size in draft.model.weight_files)


def _flag(value: bool) -> str:
    return "1" if value else "0"


def _agreed_facts(
    selected: Sequence[NodeConfig], facts: Mapping[str, NodeFacts]
) -> tuple[NodeFacts, str]:
    """Check that every rank prepared identical content.

    Returns:
        The head's facts and the image ID every worker reports (the head's
        when there are no workers).

    """
    missing = [node.node_id for node in selected if node.node_id not in facts]
    if missing:
        raise RecipeError(
            "prepare_incomplete",
            f"Prepare results are missing for node {missing[0]!r}.",
        )
    single_home(facts[node.node_id].home for node in selected)
    for field_name in ("vllm_version", "manifest_sha256", "manifest_path"):
        values = {getattr(facts[node.node_id], field_name) for node in selected}
        if len(values) != 1:
            raise RecipeError(
                "prepare_mismatch",
                f"Nodes disagree about {field_name.replace('_', ' ')} after prepare.",
                "Re-run prepare. Every rank must hold byte-identical runtime images "
                "and weights.",
            )
    head_facts = facts[selected[0].node_id]
    # Every rank pulled the same manifest digest, but the local image ID depends
    # on the Docker image store: the containerd store reports the manifest
    # digest, the classic overlay2 store the config digest. Pin the head and the
    # workers separately, as reviewed cluster profiles do.
    worker_image_ids = {facts[node.node_id].image_id for node in selected[1:]}
    if len(worker_image_ids) > 1:
        raise RecipeError(
            "prepare_mismatch",
            "Worker nodes report different local image IDs for the pinned runtime.",
            "Use the same Docker image store on every worker node.",
        )
    return head_facts, next(iter(worker_image_ids), head_facts.image_id)


def _recipe_revision(
    start_hash: str,
    stop_hash: str,
    base_environment: Mapping[str, str],
    node_environment: Mapping[str, Mapping[str, str]],
) -> str:
    """Hash everything installed on the nodes into the recipe commit marker."""
    revision_source = "\n".join(
        [start_hash, stop_hash, environment_text(base_environment)]
        + [
            f"{node_id}:{environment_text(overlay)}"
            for node_id, overlay in sorted(node_environment.items())
        ]
    )
    return hashlib.sha256(revision_source.encode("utf-8")).hexdigest()[:40]


def _command_fragments(
    draft: RecipeDraft, model_dir: str, node_count: int
) -> list[str]:
    """The ``vllm serve`` argv that readiness requires in the running process."""
    settings = draft.settings
    return [
        VLLM_EXECUTABLE,
        "serve",
        f"/models/{model_dir}",
        "--served-model-name",
        draft.served_alias,
        "--host",
        "0.0.0.0",
        "--port",
        str(settings.port),
        "--tensor-parallel-size",
        str(node_count),
        "--max-model-len",
        str(settings.max_model_len),
        "--max-num-seqs",
        str(settings.max_num_seqs),
        "--max-num-batched-tokens",
        str(settings.max_num_batched_tokens),
        "--gpu-memory-utilization",
        f"{settings.gpu_memory_utilization:.2f}",
        "--kv-cache-dtype",
        settings.kv_cache_dtype,
    ]


def _artifact_id(draft: RecipeDraft) -> str:
    org_name = re.sub(
        r"[^a-z0-9._-]+", "-", draft.model.repo_id.lower().replace("/", "-")
    ).strip("-.")
    return f"hf-{org_name[:100]}-{draft.model.revision[:8]}"


def render_recipe(
    draft: RecipeDraft,
    nodes: Mapping[str, NodeConfig],
    facts: Mapping[str, NodeFacts],
    *,
    prepare_id: str,
) -> RenderedRecipe:
    """Build the installed environment and the generated profile after prepare."""
    selected, model_root = resolve_placement(draft, nodes)
    head_facts, worker_image_id = _agreed_facts(selected, facts)
    profile_id = profile_id_for(draft, prepare_id)
    deployment_id = f"dep-{profile_id}"
    container_name = container_name_for(profile_id)
    working_directory = working_directory_for(head_facts.home, profile_id)
    model_dir = model_dir_for(draft)
    settings = draft.settings
    addresses = rank_addresses(selected)
    node_count = len(selected)
    master_address = addresses[selected[0].node_id] if node_count > 1 else "127.0.0.1"

    base_environment = {
        "RECIPE_FORMAT": "1",
        "CONTAINER_NAME": container_name,
        "DEPLOYMENT_LABEL": deployment_id,
        "VLLM_IMAGE": draft.runtime.reference,
        "VLLM_HEAD_IMAGE_ID": head_facts.image_id,
        "WORKER_VLLM_IMAGE_ID": worker_image_id,
        "MODEL_DIR": model_dir,
        "SERVED_MODEL_NAME": draft.served_alias,
        "PORT": str(settings.port),
        "TENSOR_PARALLEL_SIZE": str(node_count),
        "NNODES": str(node_count),
        "MASTER_ADDR": master_address,
        "MASTER_PORT": MASTER_PORT,
        "MAX_MODEL_LEN": str(settings.max_model_len),
        "MAX_NUM_SEQS": str(settings.max_num_seqs),
        "MAX_NUM_BATCHED_TOKENS": str(settings.max_num_batched_tokens),
        "GPU_MEMORY_UTILIZATION": f"{settings.gpu_memory_utilization:.2f}",
        "KV_CACHE_DTYPE": settings.kv_cache_dtype,
        "ENABLE_PREFIX_CACHING": _flag(settings.enable_prefix_caching),
        "TRUST_REMOTE_CODE": _flag(settings.trust_remote_code),
        "ENFORCE_EAGER": _flag(settings.enforce_eager),
        "TOOL_CALL_PARSER": settings.tool_call_parser or "none",
        "REASONING_PARSER": settings.reasoning_parser or "none",
        "CACHE_VOLUME": CACHE_VOLUME,
    }
    node_environment = {
        node.node_id: {
            "NODE_RANK": str(rank),
            "VLLM_HOST_IP": addresses[node.node_id],
            "MODEL_ROOT": model_root,
        }
        for rank, node in enumerate(selected)
    }
    start_hash = template_sha256("start.sh")
    stop_hash = template_sha256("stop.sh")
    recipe_revision = _recipe_revision(
        start_hash, stop_hash, base_environment, node_environment
    )

    model_path = model_path_for(model_root, draft)
    config_path = f"{model_path}/config.json"
    manifest_path = f"{model_path}/{head_facts.manifest_path}"
    required_files = (
        [config_path] if manifest_path == config_path else [config_path, manifest_path]
    )
    minimum_free_mib = math.floor(
        settings.gpu_memory_utilization
        * min(facts[node.node_id].memory_total_bytes for node in selected)
        / MIB
    )
    document: dict[str, Any] = {
        "schema_version": 1,
        "profile_id": profile_id,
        "revision": 1,
        "display_name": f"{draft.display_name} (vLLM {draft.runtime.release_tag})",
        "origin": "generated",
        "provenance": {
            "draft_id": draft.draft_id,
            "prepare_id": prepare_id,
            "hf_repo": draft.model.repo_id,
            "hf_revision": draft.model.revision,
            "vllm_release": draft.runtime.release_tag,
            "vllm_image": draft.runtime.reference,
            "vllm_version": head_facts.vllm_version,
            "template": f"{TEMPLATE_NAME}/1",
        },
        "artifact": {
            "artifact_id": _artifact_id(draft),
            "display_name": draft.model.repo_id,
            "format": "safetensors",
            "quantization": draft.model.quantization,
            "architecture": draft.model.architectures[0]
            if draft.model.architectures
            else None,
            "primary_path": config_path,
            "required_files": required_files,
            "size_bytes": draft.model.weight_bytes,
            "manifest_sha256": head_facts.manifest_sha256,
        },
        "api_alias": draft.served_alias,
        "backend": "vllm",
        "deployment_mode": "shared-vllm",
        "node_count": node_count,
        "node_selector": {"allowed_node_ids": [node.node_id for node in selected]},
        "runner": {
            "kind": "allowlisted-script",
            "script_id": f"{profile_id}-start",
            "script_path": f"{working_directory}/start.sh",
            "interpreter": "bash",
            "syntax_check": True,
            "expected_sha256": start_hash,
            "require_sha256": True,
            "stop_script_path": f"{working_directory}/stop.sh",
            "expected_stop_sha256": stop_hash,
            "working_directory": working_directory,
            "environment_path": f"{working_directory}/recipe.env",
            "required_environment": base_environment,
            "control_node_id": selected[0].node_id,
            "project_name": container_name,
            "container_name": container_name,
            "expected_recipe_commit": recipe_revision,
            "image_digest": draft.runtime.reference,
            "launch_mode": "per-node",
            "node_environment": node_environment,
        },
        "endpoint": {"port": settings.port},
        "readiness": {
            "process_identity": {
                "executable_path": VLLM_EXECUTABLE,
                "required_command_fragments": _command_fragments(
                    draft, model_dir, node_count
                ),
            },
            "expected_alias": draft.served_alias,
            "startup_timeout_s": settings.startup_timeout_s,
            "poll_interval_s": 10,
            # One bounded request after readiness proves the endpoint generates;
            # the timeout tolerates first-request warmup (graph capture, JIT).
            "completion_canary": {
                "enabled": True,
                "timeout_s": CANARY_TIMEOUT_S,
                "max_tokens": CANARY_MAX_TOKENS,
                "prompt": CANARY_PROMPT,
            },
        },
        "runtime": {
            "name": "vLLM",
            "executable_path": VLLM_EXECUTABLE,
            "build_revision": head_facts.vllm_version,
            "architecture": "linux/arm64",
            "required_features": ["generated-recipe", f"tensor-parallel-{node_count}"],
        },
        "safety": {
            "require_script_hash": True,
            "minimum_free_memory_mib_before_start": minimum_free_mib,
            "minimum_free_memory_mib_after_stop": minimum_free_mib,
            "maximum_swap_used_mib": 8_192,
            "recovery_timeout_s": 180,
            "required_invariant_ids": [
                "generated-recipe-unqualified",
                "image-digest-pinned",
                "model-revision-pinned",
                "start-and-stop-script-sha256-pinned",
                "per-rank-image-id-pinned",
                "served-alias-exact-match",
                "health-http-200-required",
                "memory-recovery-required",
            ],
        },
    }
    rendered = RenderedRecipe(
        profile_id=profile_id,
        working_directory=working_directory,
        base_environment=base_environment,
        node_environment=node_environment,
        recipe_revision=recipe_revision,
        profile_document=document,
    )
    # Fail before anything is installed if the generated profile would be rejected.
    try:
        rendered.profile()
    except ValueError as error:
        raise RecipeError(
            "generated_profile_invalid", f"Generated profile failed validation: {error}"
        ) from error
    return rendered
