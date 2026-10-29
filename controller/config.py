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

import json
import re
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, NoReturn

from .domain import (
    ID_RE,
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
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_REQUIRED: Any = object()  # sentinel: read the key without a default


class ConfigError(ValueError):
    """A safe, user-facing configuration validation error."""

    def __init__(self, code: ErrorCode, path: str, message: str) -> None:
        self.code = code
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message} ({code.value})")


def _fail(code: ErrorCode, path: str, message: str) -> NoReturn:
    raise ConfigError(code, path, message)


@contextmanager
def _domain_errors(path: str) -> Iterator[None]:
    """Report a domain record's own invariant failure as an invalid ``path``."""
    try:
        yield
    except ConfigError:
        raise
    except ValueError as exc:
        _fail(ErrorCode.CONFIG_INVALID_VALUE, path, str(exc))


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
            "unknown key, configuration is fail-closed",
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


def _items[T](value: Any, path: str, parse: Callable[[Any, str], T]) -> tuple[T, ...]:
    """Validate every element of a JSON array, reporting ``path[index]``."""
    return tuple(
        parse(item, f"{path}[{index}]") for index, item in enumerate(_list(value, path))
    )


def _strings(value: Any, path: str) -> tuple[str, ...]:
    return _items(value, path, _string)


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
    if not ID_RE.fullmatch(result):
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


def _sha256(value: Any, path: str) -> str:
    result = _string(value, path)
    if not _SHA256_RE.fullmatch(result):
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


class _Fields:
    """Typed reads from one JSON object whose keys were already checked.

    Each read reports errors at ``<object path>.<key>``. Reading without a
    default means the key was declared required. A default applies only when
    the key is absent, so an explicit ``null`` is still validated (and
    rejected); :meth:`nullable` is the one read that maps ``null`` to ``None``.
    """

    def __init__(self, data: Mapping[str, Any], path: str) -> None:
        self._data = data
        self._path = path

    def path(self, key: str) -> str:
        return f"{self._path}.{key}"

    def parse[T](
        self,
        key: str,
        parser: Callable[..., T],
        default: Any = _REQUIRED,
        **options: Any,
    ) -> T:
        value = (
            self._data[key] if default is _REQUIRED else self._data.get(key, default)
        )
        return parser(value, self.path(key), **options)

    def nullable[T](
        self, key: str, parser: Callable[..., T], **options: Any
    ) -> T | None:
        value = self._data.get(key)
        return None if value is None else parser(value, self.path(key), **options)

    def string(self, key: str, default: Any = _REQUIRED) -> str:
        return self.parse(key, _string, default)

    def boolean(self, key: str, default: Any = _REQUIRED) -> bool:
        return self.parse(key, _boolean, default)

    def integer(
        self,
        key: str,
        default: Any = _REQUIRED,
        *,
        minimum: int | None = None,
        maximum: int | None = None,
    ) -> int:
        return self.parse(key, _integer, default, minimum=minimum, maximum=maximum)

    def strings(self, key: str, default: Any = _REQUIRED) -> tuple[str, ...]:
        return self.parse(key, _strings, default)

    def enum[E: Enum](
        self, key: str, enum_type: type[E], default: Any = _REQUIRED
    ) -> E:
        return self.parse(
            key, lambda value, path: _enum(value, enum_type, path), default
        )


def _strict_object(
    value: Any,
    path: str,
    *,
    required: Iterable[str] = (),
    optional: Iterable[str] = (),
) -> _Fields:
    """Check a JSON object's keys (fail closed) and return typed reads of it."""
    required = tuple(required)
    data = _object(value, path)
    _keys(data, path, required=required, allowed=(*required, *optional))
    return _Fields(data, path)


def _parse_port_range(value: Any, path: str) -> PortRange:
    ports = _strict_object(value, path, required=("start", "end"))
    return PortRange(
        ports.integer("start", minimum=1, maximum=65535),
        ports.integer("end", minimum=1, maximum=65535),
    )


def _parse_ssh(value: Any, path: str) -> SSHConfig:
    ssh = _strict_object(
        value,
        path,
        required=("address", "user", "key_path"),
        optional=("connect_timeout_s",),
    )
    return SSHConfig(
        address=ssh.string("address"),
        user=ssh.string("user"),
        key_path=ssh.string("key_path"),
        connect_timeout_s=ssh.integer("connect_timeout_s", 10, minimum=1),
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
        node = _strict_object(
            raw_node,
            path,
            required=("id", "display_name", "ssh", "management_address", "model_roots"),
            optional=(
                "cluster_addresses",
                "labels",
                "capabilities",
                "endpoint_ports",
            ),
        )
        node_id = node.parse("id", _id)
        if node_id in seen:
            _fail(
                ErrorCode.DUPLICATE_ID, f"{path}.id", f"duplicate node id {node_id!r}"
            )
        seen.add(node_id)
        roots = node.parse("model_roots", _items, parse=_absolute_path)
        if not roots:
            _fail(
                ErrorCode.CONFIG_INVALID_VALUE,
                f"{path}.model_roots",
                "at least one model root is required",
            )
        with _domain_errors(path):
            nodes.append(
                NodeConfig(
                    schema_version=NODE_SCHEMA_VERSION,
                    node_id=node_id,
                    display_name=node.string("display_name"),
                    ssh=node.parse("ssh", _parse_ssh),
                    management_address=node.string("management_address"),
                    cluster_addresses=node.strings("cluster_addresses", []),
                    labels=frozenset(node.strings("labels", [])),
                    capabilities=frozenset(node.strings("capabilities", [])),
                    model_roots=roots,
                    endpoint_ports=node.parse(
                        "endpoint_ports",
                        _parse_port_range,
                        {"start": 11434, "end": 11434},
                    ),
                )
            )
    if not nodes:
        _fail(
            ErrorCode.CONFIG_INVALID_VALUE,
            "nodes.nodes",
            "at least one node is required",
        )
    return tuple(nodes)


def _parse_artifact(value: Any, path: str) -> ModelArtifact:
    artifact = _strict_object(
        value,
        path,
        required=(
            "artifact_id",
            "display_name",
            "format",
            "primary_path",
            "required_files",
        ),
        optional=(
            "quantization",
            "architecture",
            "size_bytes",
            "primary_size_bytes",
            "manifest_sha256",
        ),
    )
    required_files = artifact.parse("required_files", _items, parse=_absolute_path)
    with _domain_errors(path):
        return ModelArtifact(
            artifact_id=artifact.parse("artifact_id", _id),
            display_name=artifact.string("display_name"),
            format=artifact.enum("format", ArtifactFormat),
            primary_path=artifact.parse("primary_path", _absolute_path),
            required_files=required_files,
            quantization=artifact.nullable("quantization", _string),
            architecture=artifact.nullable("architecture", _string),
            size_bytes=artifact.nullable("size_bytes", _integer, minimum=1),
            primary_size_bytes=artifact.nullable(
                "primary_size_bytes", _integer, minimum=1
            ),
            manifest_sha256=artifact.nullable("manifest_sha256", _sha256),
        )


def _parse_selector(value: Any, path: str) -> NodeSelector:
    selector = _strict_object(
        value,
        path,
        optional=("required_labels", "required_capabilities", "allowed_node_ids"),
    )
    return NodeSelector(
        required_labels=frozenset(selector.strings("required_labels", [])),
        required_capabilities=frozenset(selector.strings("required_capabilities", [])),
        allowed_node_ids=frozenset(
            selector.parse("allowed_node_ids", _items, [], parse=_id)
        ),
    )


def _parse_runner(value: Any, path: str) -> ScriptRunner:
    runner = _strict_object(
        value,
        path,
        required=("kind", "script_id", "script_path"),
        optional=(
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
    expected_sha256 = runner.nullable("expected_sha256", _sha256)
    with _domain_errors(path):
        return ScriptRunner(
            kind=runner.enum("kind", RunnerKind),
            script_id=runner.parse("script_id", _id),
            script_path=runner.parse("script_path", _absolute_path),
            interpreter=runner.string("interpreter", "bash"),
            syntax_check=runner.boolean("syntax_check", True),
            expected_sha256=expected_sha256,
            require_sha256=runner.boolean("require_sha256", False),
            expected_mode=runner.nullable("expected_mode", _string),
            expected_size_bytes=runner.nullable(
                "expected_size_bytes", _integer, minimum=1
            ),
            log_path=runner.nullable("log_path", _absolute_path),
            stop_script_path=runner.nullable("stop_script_path", _absolute_path),
            expected_stop_sha256=runner.nullable("expected_stop_sha256", _sha256),
            working_directory=runner.nullable("working_directory", _absolute_path),
            environment_path=runner.nullable("environment_path", _absolute_path),
            required_environment=runner.parse(
                "required_environment", _string_mapping, {}
            ),
            control_node_id=runner.nullable("control_node_id", _id),
            project_name=runner.nullable("project_name", _string),
            container_name=runner.nullable("container_name", _string),
            expected_recipe_commit=runner.nullable("expected_recipe_commit", _string),
            image_digest=runner.nullable("image_digest", _string),
            launch_mode=runner.enum(
                "launch_mode", LaunchMode, LaunchMode.HEAD_SCRIPT.value
            ),
            node_environment=runner.parse("node_environment", _node_environment, {}),
        )


def _parse_endpoint(value: Any, path: str) -> EndpointContract:
    endpoint = _strict_object(
        value,
        path,
        required=("port",),
        optional=(
            "health_path",
            "models_path",
            "completion_path",
            "host_source",
            "scheme",
        ),
    )
    with _domain_errors(path):
        return EndpointContract(
            port=endpoint.integer("port", minimum=1, maximum=65535),
            health_path=endpoint.string("health_path", "/health"),
            models_path=endpoint.string("models_path", "/v1/models"),
            completion_path=endpoint.string("completion_path", "/v1/chat/completions"),
            host_source=endpoint.string("host_source", "management-address"),
            scheme=endpoint.string("scheme", "http"),
        )


def _parse_readiness(
    value: Any, path: str, endpoint: EndpointContract, expected_alias: str
) -> ReadinessContract:
    readiness = _strict_object(
        value,
        path,
        required=("process_identity",),
        optional=(
            "expected_alias",
            "expected_health_status",
            "exact_alias_set",
            "startup_timeout_s",
            "poll_interval_s",
            "completion_canary",
        ),
    )
    process = readiness.parse(
        "process_identity",
        _strict_object,
        required=("executable_path", "required_command_fragments"),
    )
    canary = readiness.parse(
        "completion_canary",
        _strict_object,
        {},
        optional=("enabled", "timeout_s", "max_tokens", "prompt"),
    )
    with _domain_errors(path):
        return ReadinessContract(
            process=ProcessIdentity(
                executable_path=process.parse("executable_path", _absolute_path),
                required_command_fragments=process.strings(
                    "required_command_fragments"
                ),
            ),
            endpoint=endpoint,
            expected_alias=readiness.string("expected_alias", expected_alias),
            expected_health_status=readiness.integer("expected_health_status", 200),
            exact_alias_set=readiness.boolean("exact_alias_set", True),
            startup_timeout_s=readiness.integer("startup_timeout_s", 420, minimum=1),
            poll_interval_s=readiness.integer("poll_interval_s", 2, minimum=1),
            completion_canary=CompletionCanary(
                enabled=canary.boolean("enabled", False),
                timeout_s=canary.integer("timeout_s", 30, minimum=1),
                max_tokens=canary.integer("max_tokens", 32, minimum=1),
                prompt=canary.string("prompt", "Say OK."),
            ),
        )


def _parse_runtime(value: Any, path: str) -> RuntimeRequirements:
    runtime = _strict_object(
        value,
        path,
        required=("name", "executable_path"),
        optional=(
            "build_revision",
            "build_commit",
            "compiler",
            "architecture",
            "required_features",
            "required_command_fragments",
            "source_of_truth",
        ),
    )
    with _domain_errors(path):
        return RuntimeRequirements(
            name=runtime.string("name"),
            executable_path=runtime.parse("executable_path", _absolute_path),
            build_revision=runtime.nullable("build_revision", _string),
            build_commit=runtime.nullable("build_commit", _string),
            compiler=runtime.nullable("compiler", _string),
            architecture=runtime.nullable("architecture", _string),
            required_features=runtime.strings("required_features", []),
            required_command_fragments=runtime.strings(
                "required_command_fragments", []
            ),
            source_of_truth=runtime.string("source_of_truth", "allowlisted-script"),
        )


def _parse_safety(value: Any, path: str) -> SafetyContract:
    safety = _strict_object(
        value,
        path,
        optional=(
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
        ),
    )
    with _domain_errors(path):
        return SafetyContract(
            require_allowlisted_script=safety.boolean(
                "require_allowlisted_script", True
            ),
            require_script_readable=safety.boolean("require_script_readable", True),
            require_script_syntax_check=safety.boolean(
                "require_script_syntax_check", True
            ),
            require_script_hash=safety.boolean("require_script_hash", False),
            stop_conflicts_before_start=safety.boolean(
                "stop_conflicts_before_start", True
            ),
            verify_process_exit_after_stop=safety.boolean(
                "verify_process_exit_after_stop", True
            ),
            verify_endpoint_closed_after_stop=safety.boolean(
                "verify_endpoint_closed_after_stop", True
            ),
            verify_alias_absent_after_stop=safety.boolean(
                "verify_alias_absent_after_stop", True
            ),
            verify_memory_recovery_after_stop=safety.boolean(
                "verify_memory_recovery_after_stop", True
            ),
            minimum_free_memory_mib_before_start=safety.integer(
                "minimum_free_memory_mib_before_start", 16_384, minimum=0
            ),
            minimum_free_memory_mib_after_stop=safety.integer(
                "minimum_free_memory_mib_after_stop", 16_384, minimum=0
            ),
            maximum_swap_used_mib=safety.integer(
                "maximum_swap_used_mib", 1_024, minimum=0
            ),
            recovery_timeout_s=safety.integer("recovery_timeout_s", 120, minimum=1),
            required_invariant_ids=frozenset(
                safety.strings("required_invariant_ids", [])
            ),
        )


def parse_profile_document(value: Any) -> ModelProfile:
    """Parse one complete versioned model profile."""
    data = _object(value, "profile")
    _schema_version(data, "profile", PROFILE_SCHEMA_VERSION)
    required = (
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
    )
    optional = ("revision", "node_selector", "origin", "provenance")
    _keys(data, "profile", required=required, allowed=(*required, *optional))
    profile = _Fields(data, "profile")
    profile_id = profile.parse("profile_id", _id)
    api_alias = profile.string("api_alias")
    endpoint = profile.parse("endpoint", _parse_endpoint)
    readiness = profile.parse(
        "readiness", _parse_readiness, endpoint=endpoint, expected_alias=api_alias
    )
    with _domain_errors("profile"):
        return ModelProfile(
            schema_version=PROFILE_SCHEMA_VERSION,
            profile_id=profile_id,
            revision=profile.integer("revision", 1, minimum=1),
            display_name=profile.string("display_name"),
            artifact=profile.parse("artifact", _parse_artifact),
            api_alias=api_alias,
            backend=profile.enum("backend", Backend),
            deployment_mode=profile.enum("deployment_mode", DeploymentMode),
            node_count=profile.integer("node_count", minimum=1),
            node_selector=profile.parse("node_selector", _parse_selector, {}),
            runner=profile.parse("runner", _parse_runner),
            endpoint=endpoint,
            readiness=readiness,
            runtime=profile.parse("runtime", _parse_runtime),
            safety=profile.parse("safety", _parse_safety),
            origin=profile.enum("origin", ProfileOrigin, ProfileOrigin.REVIEWED.value),
            provenance=profile.parse("provenance", _string_mapping, {}),
        )


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
