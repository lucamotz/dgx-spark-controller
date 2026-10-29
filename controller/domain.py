# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Stable, side-effect-free domain contracts for the controller.

The controller deliberately models desired configuration separately from
observed state. These frozen dataclasses describe reviewed inputs (nodes and
model profiles); durable operation and deployment records and their states
live in :mod:`controller.operations`. No class in this module opens a socket,
executes a command, or expands a shell string.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum

# Node, profile and artifact identifiers in configuration and API payloads.
ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")


class _StrEnum(str, Enum):
    """Enum whose JSON representation is its stable wire value."""

    def __str__(self) -> str:
        return self.value


class Backend(_StrEnum):
    LLAMA_CPP = "llama.cpp"
    VLLM = "vllm"


class DeploymentMode(_StrEnum):
    INDEPENDENT = "independent"
    SHARED_VLLM = "shared-vllm"
    SHARED_LLAMA_RPC = "shared-llama-rpc"


class ProfileOrigin(_StrEnum):
    """Who authored a profile: an operator review or the recipe builder."""

    REVIEWED = "reviewed"
    GENERATED = "generated"


class LaunchMode(_StrEnum):
    """How a shared vLLM recipe starts its ranks.

    ``head-script`` runs one reviewed launcher on the control node that owns every
    rank. ``per-node`` (used by generated recipes) has the controller start the
    same pinned script on each node over its own SSH session, workers first, so
    nodes never need SSH access to one another.
    """

    HEAD_SCRIPT = "head-script"
    PER_NODE = "per-node"


class ArtifactFormat(_StrEnum):
    GGUF = "gguf"
    SAFETENSORS = "safetensors"
    PACKAGED = "runtime-packaged"


class RunnerKind(_StrEnum):
    ALLOWLISTED_SCRIPT = "allowlisted-script"


class ErrorCode(_StrEnum):
    CONFIG_FILE_NOT_FOUND = "config-file-not-found"
    CONFIG_INVALID_JSON = "config-invalid-json"
    CONFIG_SCHEMA_UNSUPPORTED = "config-schema-unsupported"
    CONFIG_UNKNOWN_KEY = "config-unknown-key"
    CONFIG_MISSING_KEY = "config-missing-key"
    CONFIG_INVALID_VALUE = "config-invalid-value"
    DUPLICATE_ID = "duplicate-id"


@dataclass(frozen=True)
class PortRange:
    """Inclusive endpoint port range allowed for a node."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if not 1 <= self.start <= 65535 or not 1 <= self.end <= 65535:
            raise ValueError("port range values must be between 1 and 65535")
        if self.start > self.end:
            raise ValueError("port range start must not exceed end")


@dataclass(frozen=True)
class SSHConfig:
    """Local SSH connection reference; the key contents never enter config."""

    address: str
    user: str
    key_path: str
    connect_timeout_s: int = 10

    def __post_init__(self) -> None:
        if (
            not self.address.strip()
            or not self.user.strip()
            or not self.key_path.strip()
        ):
            raise ValueError("SSH address, user, and key_path are required")
        if self.connect_timeout_s <= 0:
            raise ValueError("SSH connect timeout must be positive")


@dataclass(frozen=True)
class NodeConfig:
    """Reviewed, desired facts for one reachable DGX Spark node."""

    schema_version: int
    node_id: str
    display_name: str
    ssh: SSHConfig
    management_address: str
    cluster_addresses: tuple[str, ...] = ()
    labels: frozenset[str] = frozenset()
    capabilities: frozenset[str] = frozenset()
    model_roots: tuple[str, ...] = ()
    endpoint_ports: PortRange = field(default_factory=lambda: PortRange(11434, 11434))

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported node schema version")
        if not self.node_id.strip() or not self.display_name.strip():
            raise ValueError("node_id and display_name are required")
        if not self.management_address.strip():
            raise ValueError("management_address is required")
        if not self.model_roots:
            raise ValueError("at least one model root is required")


@dataclass(frozen=True)
class ModelArtifact:
    """Inventory identity; paths are not launch flags and never become commands."""

    artifact_id: str
    display_name: str
    format: ArtifactFormat
    primary_path: str
    required_files: tuple[str, ...]
    quantization: str | None = None
    architecture: str | None = None
    size_bytes: int | None = None
    primary_size_bytes: int | None = None
    manifest_sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.artifact_id.strip() or not self.display_name.strip():
            raise ValueError("artifact_id and display_name are required")
        if not self.primary_path.startswith("/"):
            raise ValueError("primary_path must be an absolute remote path")
        if not self.required_files or self.primary_path not in self.required_files:
            raise ValueError("required_files must include primary_path")
        if self.size_bytes is not None and self.size_bytes <= 0:
            raise ValueError("size_bytes must be positive when supplied")
        if self.primary_size_bytes is not None and self.primary_size_bytes <= 0:
            raise ValueError("primary_size_bytes must be positive when supplied")


@dataclass(frozen=True)
class NodeSelector:
    """Allowlist constraints used before a profile can target a node."""

    required_labels: frozenset[str] = frozenset()
    required_capabilities: frozenset[str] = frozenset()
    allowed_node_ids: frozenset[str] = frozenset()


@dataclass(frozen=True)
class ScriptRunner:
    """Reference to an allowlisted remote script, never an arbitrary command."""

    kind: RunnerKind
    script_id: str
    script_path: str
    interpreter: str = "bash"
    syntax_check: bool = True
    expected_sha256: str | None = None
    require_sha256: bool = False
    expected_mode: str | None = None
    expected_size_bytes: int | None = None
    log_path: str | None = None
    stop_script_path: str | None = None
    expected_stop_sha256: str | None = None
    working_directory: str | None = None
    environment_path: str | None = None
    required_environment: Mapping[str, str] = field(default_factory=dict)
    control_node_id: str | None = None
    project_name: str | None = None
    container_name: str | None = None
    expected_recipe_commit: str | None = None
    image_digest: str | None = None
    launch_mode: LaunchMode = LaunchMode.HEAD_SCRIPT
    node_environment: Mapping[str, Mapping[str, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.script_id.strip() or not self.script_path.startswith("/"):
            raise ValueError("script_id and an absolute script_path are required")
        if self.interpreter != "bash":
            raise ValueError("only the reviewed bash interpreter is supported")
        if not self.syntax_check:
            raise ValueError("the script syntax check cannot be disabled")
        if self.require_sha256 and not self.expected_sha256:
            raise ValueError("require_sha256 needs expected_sha256")
        if self.expected_mode is not None and (
            len(self.expected_mode) != 4
            or any(character not in "01234567" for character in self.expected_mode)
        ):
            raise ValueError("expected script mode must be four octal digits")
        if self.expected_size_bytes is not None and self.expected_size_bytes <= 0:
            raise ValueError("expected_size_bytes must be positive when supplied")
        for path in (
            self.stop_script_path,
            self.working_directory,
            self.environment_path,
        ):
            if path is not None and not path.startswith("/"):
                raise ValueError("cluster recipe paths must be absolute")
        if (
            self.expected_stop_sha256 is not None
            and len(self.expected_stop_sha256) != 64
        ):
            raise ValueError("expected_stop_sha256 must be a SHA-256 digest")
        if any(
            not key or not value for key, value in self.required_environment.items()
        ):
            raise ValueError("required environment keys and values must be non-empty")
        for node_id, overlay in self.node_environment.items():
            if not node_id or any(
                not key or not value for key, value in overlay.items()
            ):
                raise ValueError(
                    "node environment overlays need non-empty node ids, keys, "
                    "and values"
                )
            if set(overlay).intersection(self.required_environment):
                raise ValueError(
                    "node environment overlays must not redefine profile-wide "
                    "environment keys"
                )
        if self.node_environment and self.launch_mode is not LaunchMode.PER_NODE:
            raise ValueError(
                "node environment overlays are only valid for per-node launch"
            )


@dataclass(frozen=True)
class EndpointContract:
    """The fixed HTTP surface used for readiness and client discovery."""

    port: int
    health_path: str = "/health"
    models_path: str = "/v1/models"
    completion_path: str = "/v1/chat/completions"
    host_source: str = "management-address"
    scheme: str = "http"

    def __post_init__(self) -> None:
        if not 1 <= self.port <= 65535:
            raise ValueError("endpoint port must be between 1 and 65535")
        if self.scheme != "http" or self.host_source != "management-address":
            raise ValueError("only a management-address HTTP endpoint is supported")
        for path in (self.health_path, self.models_path, self.completion_path):
            if not path.startswith("/"):
                raise ValueError("endpoint paths must start with '/'")


@dataclass(frozen=True)
class ProcessIdentity:
    """Identity checks over executable path and individual ``/proc`` argv values.

    ``required_command_fragments`` are not shell-quoted command text. A transport
    compares each element with the target process's argv/cmdline representation
    as one complete token; flags and values are therefore separate entries,
    including the reasoning-budget message as one literal argv value.
    """

    executable_path: str
    required_command_fragments: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.executable_path.startswith("/"):
            raise ValueError("process executable_path must be absolute")
        if not self.required_command_fragments:
            raise ValueError("process identity needs command fragments")


@dataclass(frozen=True)
class CompletionCanary:
    """Optional bounded request used after readiness probes pass."""

    enabled: bool = False
    timeout_s: int = 30
    max_tokens: int = 32
    prompt: str = "Say OK."

    def __post_init__(self) -> None:
        if self.timeout_s <= 0 or self.max_tokens <= 0 or not self.prompt.strip():
            raise ValueError("completion canary values must be positive and non-empty")


@dataclass(frozen=True)
class ReadinessContract:
    """Readiness requires process identity, health, and the expected model alias."""

    process: ProcessIdentity
    endpoint: EndpointContract
    expected_alias: str
    expected_health_status: int = 200
    exact_alias_set: bool = True
    startup_timeout_s: int = 420
    poll_interval_s: int = 2
    completion_canary: CompletionCanary = field(default_factory=CompletionCanary)

    def __post_init__(self) -> None:
        if not self.expected_alias.strip():
            raise ValueError("expected model alias is required")
        if self.expected_health_status != 200:
            raise ValueError("readiness requires HTTP 200")
        if not self.exact_alias_set:
            raise ValueError("readiness always requires the exact model alias set")
        if self.startup_timeout_s <= 0 or self.poll_interval_s <= 0:
            raise ValueError("readiness timeouts must be positive")


@dataclass(frozen=True)
class RuntimeRequirements:
    """Version/build facts that a node preflight must verify."""

    name: str
    executable_path: str
    build_revision: str | None = None
    build_commit: str | None = None
    compiler: str | None = None
    architecture: str | None = None
    required_features: tuple[str, ...] = ()
    required_command_fragments: tuple[str, ...] = ()
    source_of_truth: str = "allowlisted-script"

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.executable_path.startswith("/"):
            raise ValueError("runtime name and absolute executable_path are required")
        if self.source_of_truth != "allowlisted-script":
            raise ValueError(
                "runtime flags must remain sourced from the allowlisted script"
            )


@dataclass(frozen=True)
class SafetyContract:
    """Gates that prevent unsafe model switches and false readiness."""

    require_allowlisted_script: bool = True
    require_script_readable: bool = True
    require_script_syntax_check: bool = True
    require_script_hash: bool = False
    stop_conflicts_before_start: bool = True
    verify_process_exit_after_stop: bool = True
    verify_endpoint_closed_after_stop: bool = True
    verify_alias_absent_after_stop: bool = True
    verify_memory_recovery_after_stop: bool = True
    minimum_free_memory_mib_before_start: int = 16_384
    minimum_free_memory_mib_after_stop: int = 16_384
    maximum_swap_used_mib: int = 1_024
    recovery_timeout_s: int = 120
    required_invariant_ids: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        # The controller always runs these checks; false would misstate them.
        if not all(
            (
                self.require_allowlisted_script,
                self.require_script_readable,
                self.require_script_syntax_check,
                self.stop_conflicts_before_start,
                self.verify_process_exit_after_stop,
                self.verify_endpoint_closed_after_stop,
                self.verify_alias_absent_after_stop,
                self.verify_memory_recovery_after_stop,
            )
        ):
            raise ValueError(
                "script, conflict and stop safety checks cannot be disabled"
            )
        for value in (
            self.minimum_free_memory_mib_before_start,
            self.minimum_free_memory_mib_after_stop,
            self.maximum_swap_used_mib,
            self.recovery_timeout_s,
        ):
            if value < 0:
                raise ValueError("resource guard values cannot be negative")


@dataclass(frozen=True)
class ModelProfile:
    """Complete reviewed recipe for one model deployment."""

    schema_version: int
    profile_id: str
    revision: int
    display_name: str
    artifact: ModelArtifact
    api_alias: str
    backend: Backend
    deployment_mode: DeploymentMode
    node_count: int
    node_selector: NodeSelector
    runner: ScriptRunner
    endpoint: EndpointContract
    readiness: ReadinessContract
    runtime: RuntimeRequirements
    safety: SafetyContract
    origin: ProfileOrigin = ProfileOrigin.REVIEWED
    provenance: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported profile schema version")
        if not self.profile_id.strip() or not self.display_name.strip():
            raise ValueError("profile_id and display_name are required")
        if self.revision <= 0 or self.node_count <= 0:
            raise ValueError("revision and node_count must be positive")
        if self.backend is Backend.LLAMA_CPP:
            if (
                self.deployment_mode is not DeploymentMode.INDEPENDENT
                or self.node_count != 1
            ):
                raise ValueError(
                    "llama.cpp script profiles must be single-node "
                    "independent deployments"
                )
        elif self.backend is Backend.VLLM:
            minimum_nodes = 1 if self.runner.launch_mode is LaunchMode.PER_NODE else 2
            if (
                self.deployment_mode is not DeploymentMode.SHARED_VLLM
                or self.node_count < minimum_nodes
            ):
                raise ValueError(
                    "vLLM profiles must be shared deployments, and head-script "
                    "recipes need at least two nodes"
                )
            if not all(
                (
                    self.runner.stop_script_path,
                    self.runner.expected_stop_sha256,
                    self.runner.working_directory,
                    self.runner.environment_path,
                    self.runner.required_environment,
                    self.runner.control_node_id,
                    self.runner.project_name,
                    self.runner.container_name,
                    self.runner.expected_recipe_commit,
                    self.runner.image_digest,
                )
            ):
                raise ValueError(
                    "shared vLLM profiles require a complete pinned cluster recipe"
                )
            if self.runner.control_node_id not in self.node_selector.allowed_node_ids:
                raise ValueError(
                    "shared vLLM control_node_id must be explicitly allowlisted"
                )
            if self.runner.launch_mode is LaunchMode.PER_NODE and set(
                self.runner.node_environment
            ) != set(self.node_selector.allowed_node_ids):
                raise ValueError(
                    "per-node launch needs exactly one environment overlay per "
                    "allowlisted node"
                )
        if (
            self.origin is ProfileOrigin.GENERATED
            and self.runner.launch_mode is not LaunchMode.PER_NODE
        ):
            raise ValueError("generated profiles must use the per-node launch contract")
        if self.readiness.expected_alias != self.api_alias:
            raise ValueError("readiness alias must equal api_alias")
        if self.endpoint != self.readiness.endpoint:
            raise ValueError("readiness and endpoint contracts must agree")
        if self.runtime.executable_path != self.readiness.process.executable_path:
            raise ValueError("runtime and process identity paths must agree")
        if self.safety.require_script_hash and (
            not self.runner.require_sha256 or not self.runner.expected_sha256
        ):
            raise ValueError("safety contract requires a pinned script SHA-256")


def profile_matches_node(profile: ModelProfile, node: NodeConfig) -> bool:
    """Whether a profile's node selector and endpoint port admit ``node``.

    An empty ``allowed_node_ids`` admits every node; labels and capabilities
    must all be present, and the profile port must lie in the node's range.
    """
    selector = profile.node_selector
    return (
        (not selector.allowed_node_ids or node.node_id in selector.allowed_node_ids)
        and selector.required_labels.issubset(node.labels)
        and selector.required_capabilities.issubset(node.capabilities)
        and node.endpoint_ports.start
        <= profile.endpoint.port
        <= node.endpoint_ports.end
    )
