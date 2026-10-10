# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Strict JSON configuration and profile loading.

Configuration is a reviewed input boundary. The parser rejects unknown keys,
wrong scalar types, unsupported schema versions, duplicate IDs, relative remote
paths, and unsafe profile combinations before later code can act on them. Optional
keys are defaulted here, in one place, so transports and orchestrators never need
to guess what an omitted value means.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from enum import Enum
from pathlib import Path
from typing import Any, NoReturn

from .domain import (
    ArtifactFormat,
    Backend,
    CompletionCanary,
    DeploymentMode,
    EndpointContract,
    ErrorCode,
    LaunchMode,
    ModelArtifact,
    ModelProfile,
    NodeConfig,
    NodeSelector,
    PortRange,
    ProcessIdentity,
    ProfileOrigin,
    ReadinessContract,
    RunnerKind,
    RuntimeRequirements,
    SafetyContract,
    ScriptRunner,
    SSHConfig,
)

NODE_SCHEMA_VERSION = 1
PROFILE_SCHEMA_VERSION = 1
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class ConfigError(ValueError):
    """A safe, user-facing configuration validation error."""

    def __init__(self, code: ErrorCode, path: str, message: str) -> None:
        self.code = code
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message} ({code.value})")


ConfigValidationError = ConfigError


def _fail(code: ErrorCode, path: str, message: str) -> NoReturn:
    raise ConfigError(code, path, message)


def _object(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, "expected a JSON object")
    return value


def _keys(
    value: Mapping[str, Any],
    path: str,
    *,
    required: Iterable[str],
    allowed: Iterable[str],
) -> None:
    allowed_set = set(allowed)
    unknown = sorted(set(value) - allowed_set)
    if unknown:
        _fail(
            ErrorCode.CONFIG_UNKNOWN_KEY,
            f"{path}.{unknown[0]}",
            "unknown key; configuration is fail-closed",
        )
    missing = sorted(set(required) - set(value))
    if missing:
        _fail(
            ErrorCode.CONFIG_MISSING_KEY,
            f"{path}.{missing[0]}",
            "required key is missing",
        )


def _string(value: Any, path: str, *, non_empty: bool = True) -> str:
    if not isinstance(value, str):
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, "expected a string")
    if non_empty and not value.strip():
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, "must not be empty")
    return value


def _optional_string(value: Any, path: str) -> str | None:
    if value is None:
        return None
    return _string(value, path)


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, "expected a boolean")
    return value


def _integer(
    value: Any, path: str, *, minimum: int | None = None, maximum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, "expected an integer")
    if minimum is not None and value < minimum:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, f"must be at least {minimum}")
    if maximum is not None and value > maximum:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, f"must be at most {maximum}")
    return value


def _list(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, "expected a JSON array")
    return value


def _strings(value: Any, path: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    return tuple(
        _string(item, f"{path}[{index}]", non_empty=not allow_empty)
        for index, item in enumerate(_list(value, path))
    )


def _string_mapping(value: Any, path: str) -> Mapping[str, str]:
    data = _object(value, path)
    return {
        _string(key, f"{path}.<key>"): _string(item, f"{path}.{key}")
        for key, item in data.items()
    }


def _node_environment(value: Any, path: str) -> Mapping[str, Mapping[str, str]]:
    data = _object(value, path)
    return {
        _id(node_id, f"{path}.<key>"): _string_mapping(overlay, f"{path}.{node_id}")
        for node_id, overlay in data.items()
    }


def _id(value: Any, path: str) -> str:
    result = _string(value, path)
    if not _ID_RE.fullmatch(result):
        _fail(
            ErrorCode.CONFIG_INVALID_VALUE,
            path,
            "must match lowercase id syntax [a-z0-9][a-z0-9._-]{0,127}",
        )
    return result


def _absolute_path(value: Any, path: str) -> str:
    result = _string(value, path)
    if not result.startswith("/"):
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, "remote paths must be absolute")
    return result


def _enum[E: Enum](value: Any, enum_type: type[E], path: str) -> E:
    raw = _string(value, path)
    try:
        return enum_type(raw)
    except ValueError:
        choices = ", ".join(repr(item.value) for item in enum_type)
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, f"must be one of {choices}")


def _hash(value: Any, path: str) -> str | None:
    result = _optional_string(value, path)
    if result is not None and not _SHA256_RE.fullmatch(result):
        _fail(
            ErrorCode.CONFIG_INVALID_VALUE,
            path,
            "must be a 64-character SHA-256 hex digest",
        )
    return result


def _schema_version(value: Mapping[str, Any], path: str, expected: int) -> int:
    if "schema_version" not in value:
        _fail(
            ErrorCode.CONFIG_MISSING_KEY,
            f"{path}.schema_version",
            "required key is missing",
        )
    version = _integer(value.get("schema_version"), f"{path}.schema_version", minimum=1)
    if version != expected:
        _fail(
            ErrorCode.CONFIG_SCHEMA_UNSUPPORTED,
            f"{path}.schema_version",
            f"only schema version {expected} is supported",
        )
    return version


def _parse_port_range(value: Any, path: str) -> PortRange:
    data = _object(value, path)
    _keys(data, path, required=("start", "end"), allowed=("start", "end"))
    return PortRange(
        _integer(data["start"], f"{path}.start", minimum=1, maximum=65535),
        _integer(data["end"], f"{path}.end", minimum=1, maximum=65535),
    )


def _parse_ssh(value: Any, path: str) -> SSHConfig:
    data = _object(value, path)
    _keys(
        data,
        path,
        required=("address", "user", "key_path"),
        allowed=("address", "user", "key_path", "connect_timeout_s"),
    )
    return SSHConfig(
        address=_string(data["address"], f"{path}.address"),
        user=_string(data["user"], f"{path}.user"),
        key_path=_string(data["key_path"], f"{path}.key_path"),
        connect_timeout_s=_integer(
            data.get("connect_timeout_s", 10), f"{path}.connect_timeout_s", minimum=1
        ),
    )


def parse_nodes_document(value: Any) -> tuple[NodeConfig, ...]:
    """Parse a versioned ``nodes`` document into immutable node records."""
    data = _object(value, "nodes")
    _schema_version(data, "nodes", NODE_SCHEMA_VERSION)
    _keys(
        data,
        "nodes",
        required=("schema_version", "nodes"),
        allowed=("schema_version", "nodes"),
    )
    nodes: list[NodeConfig] = []
    seen: set[str] = set()
    for index, raw_node in enumerate(_list(data["nodes"], "nodes.nodes")):
        path = f"nodes.nodes[{index}]"
        node = _object(raw_node, path)
        _keys(
            node,
            path,
            required=("id", "display_name", "ssh", "management_address", "model_roots"),
            allowed=(
                "id",
                "display_name",
                "ssh",
                "management_address",
                "cluster_addresses",
                "labels",
                "capabilities",
                "model_roots",
                "endpoint_ports",
            ),
        )
        node_id = _id(node["id"], f"{path}.id")
        if node_id in seen:
            _fail(
                ErrorCode.DUPLICATE_ID, f"{path}.id", f"duplicate node id {node_id!r}"
            )
        seen.add(node_id)
        roots = tuple(
            _absolute_path(item, f"{path}.model_roots[{i}]")
            for i, item in enumerate(_list(node["model_roots"], f"{path}.model_roots"))
        )
        if not roots:
            _fail(
                ErrorCode.CONFIG_INVALID_VALUE,
                f"{path}.model_roots",
                "at least one model root is required",
            )
        try:
            nodes.append(
                NodeConfig(
                    schema_version=NODE_SCHEMA_VERSION,
                    node_id=node_id,
                    display_name=_string(node["display_name"], f"{path}.display_name"),
                    ssh=_parse_ssh(node["ssh"], f"{path}.ssh"),
                    management_address=_string(
                        node["management_address"], f"{path}.management_address"
                    ),
                    cluster_addresses=_strings(
                        node.get("cluster_addresses", []), f"{path}.cluster_addresses"
                    ),
                    labels=frozenset(
                        _strings(node.get("labels", []), f"{path}.labels")
                    ),
                    capabilities=frozenset(
                        _strings(node.get("capabilities", []), f"{path}.capabilities")
                    ),
                    model_roots=roots,
                    endpoint_ports=_parse_port_range(
                        node.get("endpoint_ports", {"start": 11434, "end": 11434}),
                        f"{path}.endpoint_ports",
                    ),
                )
            )
        except ConfigError:
            raise
        except ValueError as exc:
            _fail(ErrorCode.CONFIG_INVALID_VALUE, path, str(exc))
    if not nodes:
        _fail(
            ErrorCode.CONFIG_INVALID_VALUE,
            "nodes.nodes",
            "at least one node is required",
        )
    return tuple(nodes)


def _parse_artifact(value: Any, path: str) -> ModelArtifact:
    data = _object(value, path)
    _keys(
        data,
        path,
        required=(
            "artifact_id",
            "display_name",
            "format",
            "primary_path",
            "required_files",
        ),
        allowed=(
            "artifact_id",
            "display_name",
            "format",
            "primary_path",
            "required_files",
            "quantization",
            "architecture",
            "size_bytes",
            "primary_size_bytes",
            "manifest_sha256",
        ),
    )
    required_files = tuple(
        _absolute_path(item, f"{path}.required_files[{i}]")
        for i, item in enumerate(
            _list(data["required_files"], f"{path}.required_files")
        )
    )
    try:
        return ModelArtifact(
            artifact_id=_id(data["artifact_id"], f"{path}.artifact_id"),
            display_name=_string(data["display_name"], f"{path}.display_name"),
            format=_enum(data["format"], ArtifactFormat, f"{path}.format"),
            primary_path=_absolute_path(data["primary_path"], f"{path}.primary_path"),
            required_files=required_files,
            quantization=_optional_string(
                data.get("quantization"), f"{path}.quantization"
            ),
            architecture=_optional_string(
                data.get("architecture"), f"{path}.architecture"
            ),
            size_bytes=None
            if data.get("size_bytes") is None
            else _integer(data["size_bytes"], f"{path}.size_bytes", minimum=1),
            primary_size_bytes=None
            if data.get("primary_size_bytes") is None
            else _integer(
                data["primary_size_bytes"], f"{path}.primary_size_bytes", minimum=1
            ),
            manifest_sha256=_hash(
                data.get("manifest_sha256"), f"{path}.manifest_sha256"
            ),
        )
    except ConfigError:
        raise
    except ValueError as exc:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, str(exc))


def _parse_selector(value: Any, path: str) -> NodeSelector:
    data = _object(value, path)
    _keys(
        data,
        path,
        required=(),
        allowed=("required_labels", "required_capabilities", "allowed_node_ids"),
    )
    return NodeSelector(
        required_labels=frozenset(
            _strings(data.get("required_labels", []), f"{path}.required_labels")
        ),
        required_capabilities=frozenset(
            _strings(
                data.get("required_capabilities", []), f"{path}.required_capabilities"
            )
        ),
        allowed_node_ids=frozenset(
            _id(item, f"{path}.allowed_node_ids[{i}]")
            for i, item in enumerate(
                _list(data.get("allowed_node_ids", []), f"{path}.allowed_node_ids")
            )
        ),
    )


def _parse_runner(value: Any, path: str) -> ScriptRunner:
    data = _object(value, path)
    _keys(
        data,
        path,
        required=("kind", "script_id", "script_path"),
        allowed=(
            "kind",
            "script_id",
            "script_path",
            "interpreter",
            "syntax_check",
            "expected_sha256",
            "require_sha256",
            "expected_mode",
            "expected_size_bytes",
            "log_path",
            "stop_script_path",
            "expected_stop_sha256",
            "working_directory",
            "environment_path",
            "required_environment",
            "control_node_id",
            "project_name",
            "container_name",
            "expected_recipe_commit",
            "image_digest",
            "launch_mode",
            "node_environment",
        ),
    )
    expected_sha256 = _hash(data.get("expected_sha256"), f"{path}.expected_sha256")
    try:
        return ScriptRunner(
            kind=_enum(data["kind"], RunnerKind, f"{path}.kind"),
            script_id=_id(data["script_id"], f"{path}.script_id"),
            script_path=_absolute_path(data["script_path"], f"{path}.script_path"),
            interpreter=_string(data.get("interpreter", "bash"), f"{path}.interpreter"),
            syntax_check=_boolean(
                data.get("syntax_check", True), f"{path}.syntax_check"
            ),
            expected_sha256=expected_sha256,
            require_sha256=_boolean(
                data.get("require_sha256", False), f"{path}.require_sha256"
            ),
            expected_mode=None
            if data.get("expected_mode") is None
            else _string(data["expected_mode"], f"{path}.expected_mode"),
            expected_size_bytes=None
            if data.get("expected_size_bytes") is None
            else _integer(
                data["expected_size_bytes"], f"{path}.expected_size_bytes", minimum=1
            ),
            log_path=None
            if data.get("log_path") is None
            else _absolute_path(data["log_path"], f"{path}.log_path"),
            stop_script_path=None
            if data.get("stop_script_path") is None
            else _absolute_path(data["stop_script_path"], f"{path}.stop_script_path"),
            expected_stop_sha256=_hash(
                data.get("expected_stop_sha256"), f"{path}.expected_stop_sha256"
            ),
            working_directory=None
            if data.get("working_directory") is None
            else _absolute_path(data["working_directory"], f"{path}.working_directory"),
            environment_path=None
            if data.get("environment_path") is None
            else _absolute_path(data["environment_path"], f"{path}.environment_path"),
            required_environment=_string_mapping(
                data.get("required_environment", {}), f"{path}.required_environment"
            ),
            control_node_id=None
            if data.get("control_node_id") is None
            else _id(data["control_node_id"], f"{path}.control_node_id"),
            project_name=_optional_string(
                data.get("project_name"), f"{path}.project_name"
            ),
            container_name=_optional_string(
                data.get("container_name"), f"{path}.container_name"
            ),
            expected_recipe_commit=_optional_string(
                data.get("expected_recipe_commit"), f"{path}.expected_recipe_commit"
            ),
            image_digest=_optional_string(
                data.get("image_digest"), f"{path}.image_digest"
            ),
            launch_mode=_enum(
                data.get("launch_mode", LaunchMode.HEAD_SCRIPT.value),
                LaunchMode,
                f"{path}.launch_mode",
            ),
            node_environment=_node_environment(
                data.get("node_environment", {}), f"{path}.node_environment"
            ),
        )
    except ConfigError:
        raise
    except ValueError as exc:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, str(exc))


def _parse_endpoint(value: Any, path: str) -> EndpointContract:
    data = _object(value, path)
    _keys(
        data,
        path,
        required=("port",),
        allowed=(
            "port",
            "health_path",
            "models_path",
            "completion_path",
            "host_source",
            "scheme",
        ),
    )
    try:
        return EndpointContract(
            port=_integer(data["port"], f"{path}.port", minimum=1, maximum=65535),
            health_path=_string(
                data.get("health_path", "/health"), f"{path}.health_path"
            ),
            models_path=_string(
                data.get("models_path", "/v1/models"), f"{path}.models_path"
            ),
            completion_path=_string(
                data.get("completion_path", "/v1/chat/completions"),
                f"{path}.completion_path",
            ),
            host_source=_string(
                data.get("host_source", "management-address"), f"{path}.host_source"
            ),
            scheme=_string(data.get("scheme", "http"), f"{path}.scheme"),
        )
    except ConfigError:
        raise
    except ValueError as exc:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, str(exc))


def _parse_readiness(
    value: Any, path: str, endpoint: EndpointContract, expected_alias: str
) -> ReadinessContract:
    data = _object(value, path)
    _keys(
        data,
        path,
        required=("process_identity",),
        allowed=(
            "process_identity",
            "expected_alias",
            "expected_health_status",
            "exact_alias_set",
            "startup_timeout_s",
            "poll_interval_s",
            "completion_canary",
        ),
    )
    process_data = _object(data["process_identity"], f"{path}.process_identity")
    _keys(
        process_data,
        f"{path}.process_identity",
        required=("executable_path", "required_command_fragments"),
        allowed=("executable_path", "required_command_fragments"),
    )
    canary_raw = data.get("completion_canary", {})
    canary_data = _object(canary_raw, f"{path}.completion_canary")
    _keys(
        canary_data,
        f"{path}.completion_canary",
        required=(),
        allowed=("enabled", "timeout_s", "max_tokens", "prompt"),
    )
    try:
        return ReadinessContract(
            process=ProcessIdentity(
                executable_path=_absolute_path(
                    process_data["executable_path"],
                    f"{path}.process_identity.executable_path",
                ),
                required_command_fragments=_strings(
                    process_data["required_command_fragments"],
                    f"{path}.process_identity.required_command_fragments",
                ),
            ),
            endpoint=endpoint,
            expected_alias=_string(
                data.get("expected_alias", expected_alias), f"{path}.expected_alias"
            ),
            expected_health_status=_integer(
                data.get("expected_health_status", 200),
                f"{path}.expected_health_status",
            ),
            exact_alias_set=_boolean(
                data.get("exact_alias_set", True), f"{path}.exact_alias_set"
            ),
            startup_timeout_s=_integer(
                data.get("startup_timeout_s", 420),
                f"{path}.startup_timeout_s",
                minimum=1,
            ),
            poll_interval_s=_integer(
                data.get("poll_interval_s", 2), f"{path}.poll_interval_s", minimum=1
            ),
            completion_canary=CompletionCanary(
                enabled=_boolean(
                    canary_data.get("enabled", False),
                    f"{path}.completion_canary.enabled",
                ),
                timeout_s=_integer(
                    canary_data.get("timeout_s", 30),
                    f"{path}.completion_canary.timeout_s",
                    minimum=1,
                ),
                max_tokens=_integer(
                    canary_data.get("max_tokens", 32),
                    f"{path}.completion_canary.max_tokens",
                    minimum=1,
                ),
                prompt=_string(
                    canary_data.get("prompt", "Say OK."),
                    f"{path}.completion_canary.prompt",
                ),
            ),
        )
    except ConfigError:
        raise
    except ValueError as exc:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, str(exc))


def _parse_runtime(value: Any, path: str) -> RuntimeRequirements:
    data = _object(value, path)
    _keys(
        data,
        path,
        required=("name", "executable_path"),
        allowed=(
            "name",
            "executable_path",
            "build_revision",
            "build_commit",
            "compiler",
            "architecture",
            "required_features",
            "required_command_fragments",
            "source_of_truth",
        ),
    )
    try:
        return RuntimeRequirements(
            name=_string(data["name"], f"{path}.name"),
            executable_path=_absolute_path(
                data["executable_path"], f"{path}.executable_path"
            ),
            build_revision=_optional_string(
                data.get("build_revision"), f"{path}.build_revision"
            ),
            build_commit=_optional_string(
                data.get("build_commit"), f"{path}.build_commit"
            ),
            compiler=_optional_string(data.get("compiler"), f"{path}.compiler"),
            architecture=_optional_string(
                data.get("architecture"), f"{path}.architecture"
            ),
            required_features=_strings(
                data.get("required_features", []), f"{path}.required_features"
            ),
            required_command_fragments=_strings(
                data.get("required_command_fragments", []),
                f"{path}.required_command_fragments",
            ),
            source_of_truth=_string(
                data.get("source_of_truth", "allowlisted-script"),
                f"{path}.source_of_truth",
            ),
        )
    except ConfigError:
        raise
    except ValueError as exc:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, str(exc))


def _parse_safety(value: Any, path: str) -> SafetyContract:
    data = _object(value, path)
    allowed = (
        "require_allowlisted_script",
        "require_script_readable",
        "require_script_syntax_check",
        "require_script_hash",
        "stop_conflicts_before_start",
        "verify_process_exit_after_stop",
        "verify_endpoint_closed_after_stop",
        "verify_alias_absent_after_stop",
        "verify_memory_recovery_after_stop",
        "minimum_free_memory_mib_before_start",
        "minimum_free_memory_mib_after_stop",
        "maximum_swap_used_mib",
        "recovery_timeout_s",
        "required_invariant_ids",
    )
    _keys(data, path, required=(), allowed=allowed)
    try:
        return SafetyContract(
            require_allowlisted_script=_boolean(
                data.get("require_allowlisted_script", True),
                f"{path}.require_allowlisted_script",
            ),
            require_script_readable=_boolean(
                data.get("require_script_readable", True),
                f"{path}.require_script_readable",
            ),
            require_script_syntax_check=_boolean(
                data.get("require_script_syntax_check", True),
                f"{path}.require_script_syntax_check",
            ),
            require_script_hash=_boolean(
                data.get("require_script_hash", False), f"{path}.require_script_hash"
            ),
            stop_conflicts_before_start=_boolean(
                data.get("stop_conflicts_before_start", True),
                f"{path}.stop_conflicts_before_start",
            ),
            verify_process_exit_after_stop=_boolean(
                data.get("verify_process_exit_after_stop", True),
                f"{path}.verify_process_exit_after_stop",
            ),
            verify_endpoint_closed_after_stop=_boolean(
                data.get("verify_endpoint_closed_after_stop", True),
                f"{path}.verify_endpoint_closed_after_stop",
            ),
            verify_alias_absent_after_stop=_boolean(
                data.get("verify_alias_absent_after_stop", True),
                f"{path}.verify_alias_absent_after_stop",
            ),
            verify_memory_recovery_after_stop=_boolean(
                data.get("verify_memory_recovery_after_stop", True),
                f"{path}.verify_memory_recovery_after_stop",
            ),
            minimum_free_memory_mib_before_start=_integer(
                data.get("minimum_free_memory_mib_before_start", 16_384),
                f"{path}.minimum_free_memory_mib_before_start",
                minimum=0,
            ),
            minimum_free_memory_mib_after_stop=_integer(
                data.get("minimum_free_memory_mib_after_stop", 16_384),
                f"{path}.minimum_free_memory_mib_after_stop",
                minimum=0,
            ),
            maximum_swap_used_mib=_integer(
                data.get("maximum_swap_used_mib", 1_024),
                f"{path}.maximum_swap_used_mib",
                minimum=0,
            ),
            recovery_timeout_s=_integer(
                data.get("recovery_timeout_s", 120),
                f"{path}.recovery_timeout_s",
                minimum=1,
            ),
            required_invariant_ids=frozenset(
                _strings(
                    data.get("required_invariant_ids", []),
                    f"{path}.required_invariant_ids",
                )
            ),
        )
    except ConfigError:
        raise
    except ValueError as exc:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, str(exc))


def parse_profile_document(value: Any) -> ModelProfile:
    """Parse one complete versioned model profile."""
    data = _object(value, "profile")
    _schema_version(data, "profile", PROFILE_SCHEMA_VERSION)
    _keys(
        data,
        "profile",
        required=(
            "schema_version",
            "profile_id",
            "display_name",
            "artifact",
            "api_alias",
            "backend",
            "deployment_mode",
            "node_count",
            "runner",
            "endpoint",
            "readiness",
            "runtime",
            "safety",
        ),
        allowed=(
            "schema_version",
            "profile_id",
            "revision",
            "display_name",
            "artifact",
            "api_alias",
            "backend",
            "deployment_mode",
            "node_count",
            "node_selector",
            "runner",
            "endpoint",
            "readiness",
            "runtime",
            "safety",
            "origin",
            "provenance",
        ),
    )
    profile_id = _id(data["profile_id"], "profile.profile_id")
    api_alias = _string(data["api_alias"], "profile.api_alias")
    endpoint = _parse_endpoint(data["endpoint"], "profile.endpoint")
    readiness = _parse_readiness(
        data["readiness"], "profile.readiness", endpoint, api_alias
    )
    try:
        return ModelProfile(
            schema_version=PROFILE_SCHEMA_VERSION,
            profile_id=profile_id,
            revision=_integer(data.get("revision", 1), "profile.revision", minimum=1),
            display_name=_string(data["display_name"], "profile.display_name"),
            artifact=_parse_artifact(data["artifact"], "profile.artifact"),
            api_alias=api_alias,
            backend=_enum(data["backend"], Backend, "profile.backend"),
            deployment_mode=_enum(
                data["deployment_mode"], DeploymentMode, "profile.deployment_mode"
            ),
            node_count=_integer(data["node_count"], "profile.node_count", minimum=1),
            node_selector=_parse_selector(
                data.get("node_selector", {}), "profile.node_selector"
            ),
            runner=_parse_runner(data["runner"], "profile.runner"),
            endpoint=endpoint,
            readiness=readiness,
            runtime=_parse_runtime(data["runtime"], "profile.runtime"),
            safety=_parse_safety(data["safety"], "profile.safety"),
            origin=_enum(
                data.get("origin", ProfileOrigin.REVIEWED.value),
                ProfileOrigin,
                "profile.origin",
            ),
            provenance=_string_mapping(
                data.get("provenance", {}), "profile.provenance"
            ),
        )
    except ConfigError:
        raise
    except ValueError as exc:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, "profile", str(exc))


def _load_json(path: Path) -> Any:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        _fail(
            ErrorCode.CONFIG_FILE_NOT_FOUND,
            str(path),
            "configuration file does not exist",
        )
    except OSError as exc:
        _fail(
            ErrorCode.CONFIG_FILE_NOT_FOUND,
            str(path),
            f"configuration file cannot be read: {exc}",
        )
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        _fail(
            ErrorCode.CONFIG_INVALID_JSON,
            str(path),
            f"invalid JSON at line {exc.lineno}, column {exc.colno}",
        )


def load_nodes(path: str | Path) -> tuple[NodeConfig, ...]:
    """Read and validate one nodes document."""
    return parse_nodes_document(_load_json(Path(path)))


def load_node_config(path: str | Path) -> tuple[NodeConfig, ...]:
    """Compatibility alias with an explicit return type for the nodes document."""
    return load_nodes(path)


def load_profile(path: str | Path) -> ModelProfile:
    """Read and validate one profile JSON file."""
    return parse_profile_document(_load_json(Path(path)))


def load_profiles(
    directory: str | Path, *more: str | Path, allow_empty: bool = False
) -> tuple[ModelProfile, ...]:
    """Load every JSON profile from one or more directories and reject duplicate IDs.

    Directories are read in the order given and each in lexical order. A missing
    directory is treated as empty only when ``allow_empty`` is true, which is how
    a freshly onboarded cluster starts before any recipe has been prepared.
    """
    paths: list[Path] = []
    for root in (Path(directory), *(Path(item) for item in more)):
        if allow_empty and not root.exists():
            continue
        try:
            paths.extend(sorted(root.glob("*.json")))
        except OSError as exc:
            _fail(
                ErrorCode.CONFIG_FILE_NOT_FOUND,
                str(root),
                f"profile directory cannot be read: {exc}",
            )
    if not paths and not allow_empty:
        _fail(
            ErrorCode.CONFIG_FILE_NOT_FOUND,
            str(directory),
            "profile directory contains no JSON profiles",
        )
    profiles: list[ModelProfile] = []
    seen: set[str] = set()
    for path in paths:
        profile = load_profile(path)
        if profile.profile_id in seen:
            _fail(
                ErrorCode.DUPLICATE_ID,
                str(path),
                f"duplicate profile id {profile.profile_id!r}",
            )
        seen.add(profile.profile_id)
        profiles.append(profile)
    return tuple(profiles)


def sha256_file(path: str | Path) -> str:
    """Hash a local file for an operator-controlled profile revision update."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
