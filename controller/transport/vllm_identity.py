# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Exact identity checks of a recognized vLLM container's command and environment.

A cluster container is adopted only when its effective vLLM argv and its
environment carry the reviewed recipe values. Matching works on whole argv
tokens after the one reviewed executable, so a substring, a decoy ``echo`` or a
comment can never satisfy a check.
"""

from __future__ import annotations

import json
import shlex
from collections.abc import Mapping
from dataclasses import dataclass

from controller.transport.script_output import parse_key_values

VLLM_EXECUTABLE = "/usr/local/bin/vllm"
IDENTITY_ENVIRONMENT_KEYS = (
    "MAX_MODEL_LEN",
    "MAX_NUM_SEQS",
    "MAX_NUM_BATCHED_TOKENS",
    "LONG_PREFILL_TOKEN_THRESHOLD",
    "GPU_MEMORY_UTILIZATION",
    "KV_CACHE_MEMORY_BYTES",
    "KV_CACHE_DTYPE",
    "ATTENTION_BACKEND",
    "MTP_NUM_TOKENS",
    "ENABLE_LEGACY_DSV4_TOKENIZER_SHIM",
    "FLASHINFER_WORKSPACE_BASE",
    "VLLM_MEMORY_GUARD_EMERGENCY_AVAILABLE_MIB",
    "VLLM_MEMORY_GUARD_PRESSURE_AVAILABLE_MIB",
    "VLLM_MEMORY_GUARD_FULL_PSI_AVG10",
    "VLLM_MEMORY_GUARD_SWAP_GROWTH_MIB",
    "VLLM_MEMORY_GUARD_RAPID_DROP_MIB_PER_SECOND",
    "VLLM_MEMORY_GUARD_POLL_SECONDS",
    "VLLM_MEMORY_GUARD_TERMINATE_GRACE_SECONDS",
    "MOE_BACKEND",
    "ENFORCE_EAGER",
    "FLASHINFER_AUTOTUNE",
)

# Docker Compose can consume these recipe variables while rendering
# Config.Cmd, so they may be absent from Config.Env even though their
# effective values remain fixed in the container identity. Only fall back to
# the rendered command when the variable is absent; an explicitly conflicting
# environment value must still fail closed.
COMMAND_SUBSTITUTED_ENVIRONMENT_FLAGS = {
    "MAX_MODEL_LEN": "--max-model-len",
    "MAX_NUM_SEQS": "--max-num-seqs",
    "MAX_NUM_BATCHED_TOKENS": "--max-num-batched-tokens",
    "LONG_PREFILL_TOKEN_THRESHOLD": "--long-prefill-token-threshold",
    "GPU_MEMORY_UTILIZATION": "--gpu-memory-utilization",
    "KV_CACHE_MEMORY_BYTES": "--kv-cache-memory-bytes",
}


@dataclass(frozen=True)
class EnvironmentIdentity:
    """How a container's environment and argv compare with reviewed values."""

    command_substituted_keys: tuple[str, ...]
    mismatch_keys: tuple[str, ...]
    command_flag_mismatch_keys: tuple[str, ...]

    @property
    def matches(self) -> bool:
        return not self.mismatch_keys and not self.command_flag_mismatch_keys


def environment_identity(
    required_environment: Mapping[str, str],
    container_environment: Mapping[str, str],
    command: str,
) -> EnvironmentIdentity:
    """Compare the identity-relevant reviewed values with one container."""
    expected = {
        key: required_environment[key]
        for key in IDENTITY_ENVIRONMENT_KEYS
        if key in required_environment
    }
    command_substituted = tuple(
        sorted(
            key
            for key, value in expected.items()
            if key not in container_environment
            and (flag := COMMAND_SUBSTITUTED_ENVIRONMENT_FLAGS.get(key)) is not None
            and command_has_flag_value(command, flag, value)
        )
    )
    mismatches = tuple(
        sorted(
            key
            for key, value in expected.items()
            if container_environment.get(key) != value
            and key not in command_substituted
        )
    )
    command_flag_mismatches = tuple(
        sorted(
            key
            for key, value in expected.items()
            if (flag := COMMAND_SUBSTITUTED_ENVIRONMENT_FLAGS.get(key)) is not None
            and not command_has_flag_value(command, flag, value)
            and not (
                container_environment.get(key) == value
                and command_has_flag_environment_reference(command, flag, key)
            )
        )
    )
    return EnvironmentIdentity(command_substituted, mismatches, command_flag_mismatches)


def container_environment(payload: str) -> dict[str, str]:
    """Decode Docker's JSON environment list without retaining it in observations."""
    if not payload:
        return {}
    try:
        values = json.loads(payload)
    except json.JSONDecodeError:
        return {}
    if not isinstance(values, list):
        return {}
    return parse_key_values(item for item in values if isinstance(item, str))


def effective_vllm_arguments(payload: str) -> list[str] | None:
    """Return tokens after the one reviewed absolute vLLM executable."""
    if not payload:
        return None
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        decoded = payload
    parts = decoded if isinstance(decoded, list) else [decoded]
    tokens: list[str] = []
    for part in parts:
        if not isinstance(part, str):
            continue
        try:
            lexer = shlex.shlex(part, posix=True, punctuation_chars=";&|")
            lexer.whitespace_split = True
            lexer.commenters = "#"
            part_tokens = list(lexer)
        except ValueError:
            return None
        tokens.extend(part_tokens)

    # Ignore setup commands, diagnostics, and comments before the final
    # executable. The reviewed recipes invoke vLLM by absolute path.
    executable_positions = [
        index for index, token in enumerate(tokens) if token == VLLM_EXECUTABLE
    ]
    if len(executable_positions) != 1:
        return None
    arguments = tokens[executable_positions[0] + 1 :]
    shell_controls = {";", "&", "&&", "|", "||"}
    for index, token in enumerate(arguments):
        if token in shell_controls:
            return arguments[:index]
    return arguments


def command_has_exact_tokens(payload: str, required_tokens: tuple[str, ...]) -> bool:
    """Require every identity fragment as a complete effective argv token."""
    arguments = effective_vllm_arguments(payload)
    if arguments is None:
        return False
    required = [token for token in required_tokens if token != VLLM_EXECUTABLE]
    cursor = 0
    index = 0
    while index < len(required):
        token = required[index]
        try:
            position = arguments.index(token, cursor)
        except ValueError:
            return False
        if (
            token.startswith("--")
            and index + 1 < len(required)
            and not required[index + 1].startswith("--")
        ):
            value = required[index + 1]
            if position + 1 >= len(arguments) or arguments[position + 1] != value:
                return False
            if not command_has_flag_value(payload, token, value):
                return False
            cursor = position + 2
            index += 2
            continue
        cursor = position + 1
        index += 1
    return True


def command_has_flag_value(payload: str, flag: str, value: str) -> bool:
    """Match one exact option on the effective rendered vLLM invocation."""
    if not flag.startswith("--") or not value:
        return False
    arguments = effective_vllm_arguments(payload)
    if arguments is None:
        return False
    return _option_values(arguments, flag) == [value]


def command_has_flag_environment_reference(payload: str, flag: str, key: str) -> bool:
    """Match one option whose value is supplied by an exact container env key."""
    if not flag.startswith("--") or not key:
        return False
    arguments = effective_vllm_arguments(payload)
    if arguments is None:
        return False
    values = _option_values(arguments, flag)
    return (
        values is not None
        and len(values) == 1
        and values[0]
        in {
            f"${key}",
            f"${{{key}}}",
        }
    )


def _option_values(arguments: list[str], flag: str) -> list[str] | None:
    """Every value given to ``flag``; ``None`` when it ends the argv valueless."""
    values: list[str] = []
    for index, argument in enumerate(arguments):
        if argument == flag:
            if index + 1 >= len(arguments):
                return None
            values.append(arguments[index + 1])
        elif argument.startswith(flag + "="):
            values.append(argument[len(flag) + 1 :])
    return values
