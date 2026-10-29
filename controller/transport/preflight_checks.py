# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Read-only launch gates for single-unit and cluster-recipe profiles.

Each gate is one fixed shell test whose result the remote script prints as
``CHECK_<name>=ok|failed``. Every path, hash and pin comes from the reviewed
profile or cluster recipe. The memory gate is deferred while a resident
deployment still occupies the node; the orchestrator proves memory release
after its targeted stop instead.
"""

from __future__ import annotations

import shlex
from collections.abc import Callable, Mapping, Sequence

from controller.operations import DeploymentSpec, NodeAssignment
from controller.transport.base import NodeObservation, PreflightCheck
from controller.transport.managed_config import (
    ClusterScriptRecipe,
    required_mapping,
    required_string,
)
from controller.transport.script_output import decode_b64, parse_fields, parse_int
from controller.transport.ssh import SSHExecutionError, SSHExecutor

# "degraded" only means some unrelated user unit failed (for example a desktop
# snap notifier); the manager can still start and track transient units.
USER_MANAGER_CHECK = (
    'case "$(systemctl --user is-system-running 2>/dev/null)" in '
    "running|degraded) true ;; *) false ;; esac"
)
_SWAP_USED_REPORT = (
    "awk '/^SwapTotal:/ {total=$2} "
    '/^SwapFree:/ {print "SWAP_USED=" (total-$2) * 1024}\' /proc/meminfo'
)

Observe = Callable[[str], NodeObservation]
_Checks = list[tuple[str, str]]


def unit_preflight(
    executor: SSHExecutor, observe: Observe, node_id: str, spec: DeploymentSpec
) -> tuple[PreflightCheck, ...]:
    """Check the pinned launch script, runtime build and model files of a unit."""
    profile = dict(spec.profile)
    runner = required_mapping(profile, "runner")
    artifact = required_mapping(profile, "artifact")
    runtime = required_mapping(profile, "runtime")
    safety = required_mapping(profile, "safety")
    script_path = required_string(runner, "script_path")
    expected_hash = required_string(runner, "expected_sha256")
    expected_mode = str(runner.get("expected_mode") or "0644").lstrip("0") or "0"
    expected_size = int(runner.get("expected_size_bytes") or 0)
    runtime_path = required_string(runtime, "executable_path")
    primary_path = required_string(artifact, "primary_path")
    primary_size = int(artifact.get("primary_size_bytes") or 0)
    required_files = tuple(str(item) for item in artifact.get("required_files") or ())
    build_revision = str(runtime.get("build_revision") or "")
    build_commit = str(runtime.get("build_commit") or "")
    max_swap = int(safety.get("maximum_swap_used_bytes") or 0)
    checks: _Checks = [
        *_pinned_script_checks("script", script_path, expected_hash),
        (
            "script_mode",
            f'test "$(stat -c %a {shlex.quote(script_path)})" = '
            f"{shlex.quote(expected_mode)}",
        ),
        ("runtime_executable", f"test -x {shlex.quote(runtime_path)}"),
        ("primary_model", f"test -r {shlex.quote(primary_path)}"),
        ("user_manager", USER_MANAGER_CHECK),
    ]
    if expected_size:
        checks.append(
            (
                "script_size",
                f'test "$(stat -c %s {shlex.quote(script_path)})" = {expected_size}',
            )
        )
    if primary_size:
        checks.append(
            (
                "primary_model_size",
                f'test "$(stat -c %s {shlex.quote(primary_path)})" = {primary_size}',
            )
        )
    checks.extend(_readable_file_checks(required_files))
    version_b64_expression = "$(printf '%s' \"$runtime_version\" | base64 -w0)"
    fields = _run_checks(
        executor,
        node_id,
        [
            "set +e",
            f"runtime_version=$({shlex.quote(runtime_path)} --version 2>&1 | head -3)",
            *_check_lines(checks),
            f"printf '%s\\n' \"RUNTIME_VERSION_B64={version_b64_expression}\"",
            _SWAP_USED_REPORT,
        ],
        timeout_seconds=30,
    )
    if isinstance(fields, PreflightCheck):
        return (fields,)
    results = _check_results(
        checks,
        fields,
        "Correct the reviewed profile or remote installation before retrying.",
    )
    version_text = decode_b64(fields.get("RUNTIME_VERSION_B64", ""))
    if build_revision:
        results.append(
            PreflightCheck(
                "runtime_revision",
                build_revision in version_text,
                f"runtime reports {version_text or 'no version'}",
                "Install the profile-pinned llama.cpp revision or "
                "intentionally revise the profile.",
            )
        )
    if build_commit:
        results.append(
            PreflightCheck(
                "runtime_commit",
                build_commit in version_text,
                f"runtime reports {version_text or 'no version'}",
                "Install the profile-pinned llama.cpp commit or "
                "intentionally revise the profile.",
            )
        )
    results.append(
        _memory_check(
            observe(node_id),
            spec.minimum_free_memory_bytes,
            "Stop the resident deployment and verify memory recovery before launch.",
        )
    )
    swap_used = parse_int(fields.get("SWAP_USED"))
    if max_swap:
        results.append(
            PreflightCheck(
                "swap_bound",
                swap_used <= max_swap,
                f"swap used is {swap_used} bytes",
                "Allow the node to recover or reboot it before loading "
                "another large model.",
            )
        )
    return tuple(results)


def cluster_preflight(
    executor: SSHExecutor,
    observe: Observe,
    node_id: str,
    spec: DeploymentSpec,
    assignment: NodeAssignment,
    recipe: ClusterScriptRecipe,
) -> tuple[PreflightCheck, ...]:
    """Check a cluster rank's recipe revision, image pin and model files.

    A node that launches (the head, or every rank of a per-node recipe) also
    needs the pinned start and stop scripts, the operator environment and a
    running user manager.
    """
    artifact = required_mapping(spec.profile, "artifact")
    safety = required_mapping(spec.profile, "safety")
    recipe_revision_path = f"{recipe.working_directory}/.dgx-controller-recipe-commit"
    recipe_revision = (
        f"revision=$(git -C {shlex.quote(recipe.working_directory)} "
        "rev-parse HEAD 2>/dev/null || true); "
        'if [ -z "$revision" ] && '
        f"[ -r {shlex.quote(recipe_revision_path)} ]; then "
        f"revision=$(cat {shlex.quote(recipe_revision_path)}); fi; "
        f'test "$revision" = {shlex.quote(recipe.expected_recipe_commit)}'
    )
    required_files = tuple(str(path) for path in artifact.get("required_files") or ())
    checks: _Checks = [
        ("recipe_commit", recipe_revision),
        (
            "image_digest",
            f"docker image inspect {shlex.quote(recipe.image_digest)} >/dev/null",
        ),
        *_readable_file_checks(required_files),
    ]
    manifest_sha256 = str(artifact.get("manifest_sha256") or "")
    if manifest_sha256 and required_files:
        checks.append(
            ("manifest_hash", _sha256_check(required_files[-1], manifest_sha256))
        )
    if assignment.role == "head" or recipe.per_node:
        checks.extend(
            [
                *_pinned_script_checks(
                    "start_script", recipe.start_script_path, recipe.start_sha256
                ),
                *_pinned_script_checks(
                    "stop_script", recipe.stop_script_path, recipe.stop_sha256
                ),
                (
                    "environment_source",
                    f"test -r {shlex.quote(recipe.environment_path)}",
                ),
                (
                    "environment_overlay_directory",
                    f"test -w {shlex.quote(recipe.working_directory)}",
                ),
                ("user_manager", USER_MANAGER_CHECK),
            ]
        )
    fields = _run_checks(
        executor,
        node_id,
        [
            "set +e",
            *_check_lines(checks),
            "printf 'RECIPE_COMMIT_B64=%s\\n' "
            "\"$(printf '%s' \"${revision:-}\" | base64 | tr -d '\\n')\"",
            _SWAP_USED_REPORT,
        ],
        timeout_seconds=45,
    )
    if isinstance(fields, PreflightCheck):
        return (fields,)
    observed_recipe_commit = (
        decode_b64(fields.get("RECIPE_COMMIT_B64", "")) or "unavailable"
    )
    results = _check_results(
        checks,
        fields,
        "Restore the exact reviewed recipe, model files, environment, "
        "or image pin before retrying.",
        messages={
            "recipe_commit": f"recipe commit observed {observed_recipe_commit}, "
            f"expected {recipe.expected_recipe_commit}"
        },
    )
    results.append(
        _memory_check(
            observe(node_id),
            spec.minimum_free_memory_bytes,
            "Recover the profile-required memory on both nodes before launch.",
        )
    )
    max_swap = int(safety.get("maximum_swap_used_bytes") or 0)
    swap_used = parse_int(fields.get("SWAP_USED"))
    results.append(
        PreflightCheck(
            "swap_bound",
            swap_used <= max_swap,
            f"swap used is {swap_used} bytes",
            "Allow swap to recover before launch.",
        )
    )
    return tuple(results)


def _sha256_check(path: str, expected: str) -> str:
    return (
        f'test "$(sha256sum {shlex.quote(path)} | '
        f"awk '{{print $1}}')\" = {shlex.quote(expected)}"
    )


def _pinned_script_checks(prefix: str, path: str, sha256: str) -> _Checks:
    """A script must be readable, parse as Bash and carry its reviewed hash."""
    return [
        (f"{prefix}_readable", f"test -r {shlex.quote(path)}"),
        (f"{prefix}_syntax", f"bash -n {shlex.quote(path)}"),
        (f"{prefix}_hash", _sha256_check(path, sha256)),
    ]


def _readable_file_checks(paths: Sequence[str]) -> _Checks:
    return [
        (f"model_file_{index + 1}", f"test -r {shlex.quote(path)}")
        for index, path in enumerate(paths)
    ]


def _check_lines(checks: _Checks) -> list[str]:
    lines: list[str] = []
    for name, command in checks:
        lines.extend(
            [
                f"if {command}; then",
                f"  printf '%s\\n' {shlex.quote('CHECK_' + name + '=ok')}",
                "else",
                f"  printf '%s\\n' {shlex.quote('CHECK_' + name + '=failed')}",
                "fi",
            ]
        )
    return lines


def _run_checks(
    executor: SSHExecutor,
    node_id: str,
    lines: Sequence[str],
    *,
    timeout_seconds: float,
) -> dict[str, str] | PreflightCheck:
    """Run the gate script; an SSH failure becomes the one failed gate."""
    try:
        result = executor.run_script(
            node_id,
            "\n".join(lines) + "\n",
            timeout_seconds=timeout_seconds,
            check=True,
        )
    except SSHExecutionError as exc:
        return PreflightCheck(
            "ssh_reachable",
            False,
            str(exc),
            "Restore SSH connectivity before changing the active model.",
        )
    return parse_fields(result.stdout)


def _check_results(
    checks: _Checks,
    fields: Mapping[str, str],
    remediation: str,
    *,
    messages: Mapping[str, str] | None = None,
) -> list[PreflightCheck]:
    results: list[PreflightCheck] = []
    for name, _command in checks:
        passed = fields.get("CHECK_" + name) == "ok"
        default = f"{name.replace('_', ' ')} {'verified' if passed else 'failed'}"
        results.append(
            PreflightCheck(
                name, passed, (messages or {}).get(name, default), remediation
            )
        )
    return results


def _memory_check(
    observation: NodeObservation, required_free_bytes: int, remediation: str
) -> PreflightCheck:
    deferred = observation.is_process_running and (
        observation.managed or bool(observation.active_alias)
    )
    return PreflightCheck(
        "memory",
        deferred or observation.free_memory_bytes >= required_free_bytes,
        "memory check deferred until resident deployment stops"
        if deferred
        else f"available memory is {observation.free_memory_bytes} bytes",
        remediation,
    )
