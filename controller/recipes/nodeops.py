# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Remote primitives used to prepare a generated recipe on DGX Spark nodes.

Preparation never touches a running model: it installs controller-owned files
into a per-recipe directory, starts the pinned ``prepare.sh`` in a named
``systemd --user`` unit, and reads back a ``KEY=value`` status file. All remote
scripts below are fixed templates; every interpolated value is shell-quoted and
was produced by :mod:`controller.recipes.render` from a validated draft.
"""

from __future__ import annotations

import base64
import hashlib
import re
import shlex
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

from controller.transport.base import CommandResult
from controller.transport.ssh import SSHExecutionError, SSHExecutor

PREPARE_ID_RE = re.compile(r"[0-9a-f]{10}\Z")
FILE_NAME_RE = re.compile(r"[A-Za-z0-9._-]{1,80}\Z")
STATUS_KEYS = frozenset(
    {
        "STATE",
        "STEP",
        "MESSAGE",
        "MEM_TOTAL",
        "HOME_DIR",
        "DISK_AVAILABLE",
        "IMAGE_ID",
        "VLLM_VERSION",
        "MULTINODE_SUPPORTED",
        "MANIFEST_PATH",
        "MANIFEST_SHA256",
    }
)


@dataclass(frozen=True)
class PrepareStatus:
    """Latest reported state of one node's prepare unit."""

    state: str  # running | succeeded | failed | absent
    step: str = ""
    message: str = ""
    facts: Mapping[str, str] = field(default_factory=dict)
    downloaded_bytes: int | None = None
    unit_active: bool = False


class RecipeNodeOps(Protocol):
    def remote_home(self, node_id: str) -> str: ...

    def install_files(
        self, node_id: str, directory: str, files: Mapping[str, tuple[bytes, int]]
    ) -> CommandResult: ...

    def start_prepare(
        self, node_id: str, directory: str, prepare_id: str
    ) -> CommandResult: ...

    def prepare_status(
        self, node_id: str, directory: str, prepare_id: str, model_path: str
    ) -> PrepareStatus: ...

    def stop_prepare(self, node_id: str, prepare_id: str) -> CommandResult: ...


def prepare_unit_name(prepare_id: str) -> str:
    if not PREPARE_ID_RE.fullmatch(prepare_id):
        raise ValueError("invalid prepare id")
    return f"dgx-prepare-{prepare_id}.service"


def parse_status(text: str) -> tuple[dict[str, str], str]:
    """Return the last value per allowlisted key and the raw last state."""
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if separator and key in STATUS_KEYS:
            values[key] = value[:500]
    return values, values.get("STATE", "")


class SSHRecipeNodeOps:
    """Execute the fixed prepare primitives over the configured SSH executor."""

    def __init__(self, executor: SSHExecutor) -> None:
        self._executor = executor

    def remote_home(self, node_id: str) -> str:
        result = self._executor.run_script(
            node_id, 'printf "%s\\n" "$HOME"\n', timeout_seconds=15, check=True
        )
        home = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else ""
        if not home.startswith("/") or not re.fullmatch(
            r"/[A-Za-z0-9._/-]{0,200}", home
        ):
            raise SSHExecutionError(
                f"node {node_id} reported an unsupported home directory"
            )
        return home

    def install_files(
        self, node_id: str, directory: str, files: Mapping[str, tuple[bytes, int]]
    ) -> CommandResult:
        _require_directory(directory)
        lines = [
            "set -eu",
            "umask 077",
            f"directory={shlex.quote(directory)}",
            'install -d -m 700 "$directory"',
        ]
        for name, (content, mode) in sorted(files.items()):
            if not FILE_NAME_RE.fullmatch(name) or name in {".", ".."}:
                raise ValueError(f"invalid recipe file name {name!r}")
            digest = hashlib.sha256(content).hexdigest()
            payload = base64.b64encode(content).decode("ascii")
            lines.extend(
                [
                    f'tmp=$(mktemp "$directory/.{name}.XXXXXX")',
                    f"printf '%s' {shlex.quote(payload)} | base64 -d > \"$tmp\"",
                    'test "$(sha256sum "$tmp" | awk \'{print $1}\')" = '
                    f"{shlex.quote(digest)}",
                    f'chmod {mode:o} "$tmp"',
                    f'mv -f "$tmp" "$directory/{name}"',
                ]
            )
        try:
            result = self._executor.run_script(
                node_id, "\n".join(lines) + "\n", timeout_seconds=60, check=False
            )
        except (SSHExecutionError, ValueError) as error:
            return CommandResult(False, str(error), "recipe_install_failed")
        if not result.ok:
            return CommandResult(
                False,
                result.stderr.strip() or "recipe files could not be installed",
                "recipe_install_failed",
            )
        return CommandResult(True, f"installed {len(files)} recipe files")

    def start_prepare(
        self, node_id: str, directory: str, prepare_id: str
    ) -> CommandResult:
        _require_directory(directory)
        unit = prepare_unit_name(prepare_id)
        script = f"""set -eu
directory={shlex.quote(directory)}
unit={shlex.quote(unit)}
test -r "$directory/prepare.sh"
bash -n "$directory/prepare.sh"
test -r "$directory/prepare-{prepare_id}.env"
systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true
systemd-run --user --unit "$unit" --collect \\
  --setenv=PREPARE_ENV="$directory/prepare-{prepare_id}.env" \\
  /bin/bash "$directory/prepare.sh"
"""
        try:
            result = self._executor.run_script(
                node_id, script, timeout_seconds=30, check=False
            )
        except SSHExecutionError as error:
            return CommandResult(False, str(error), "prepare_start_failed")
        if not result.ok:
            return CommandResult(
                False,
                result.stderr.strip() or "prepare unit did not start",
                "prepare_start_failed",
            )
        return CommandResult(True, "prepare unit started")

    def prepare_status(
        self, node_id: str, directory: str, prepare_id: str, model_path: str
    ) -> PrepareStatus:
        _require_directory(directory)
        _require_directory(model_path)
        unit = prepare_unit_name(prepare_id)
        script = f"""set -u
status={shlex.quote(f"{directory}/prepare-{prepare_id}.status")}
active=$(systemctl --user show {shlex.quote(unit)} -p ActiveState --value \\
  2>/dev/null || printf inactive)
printf 'UNIT_ACTIVE=%s\\n' "$active"
printf 'DOWNLOADED=%s\\n' \\
  "$(du -sb {shlex.quote(model_path)} 2>/dev/null | awk '{{print $1}}')"
if [ -r "$status" ]; then
  printf 'STATUS_B64=%s\\n' "$(tail -c 16384 "$status" | base64 -w0)"
fi
"""
        try:
            result = self._executor.run_script(
                node_id, script, timeout_seconds=30, check=True
            )
        except SSHExecutionError as error:
            return PrepareStatus("unknown", message=str(error))
        fields = dict(
            line.partition("=")[::2]
            for line in result.stdout.splitlines()
            if "=" in line
        )
        try:
            status_text = base64.b64decode(
                fields.get("STATUS_B64", ""), validate=True
            ).decode("utf-8", "replace")
        except ValueError:
            status_text = ""
        values, state = parse_status(status_text)
        unit_active = fields.get("UNIT_ACTIVE") in {"active", "activating"}
        if not status_text:
            state = "running" if unit_active else "absent"
        elif state == "running" and not unit_active:
            state = "failed"
            values.setdefault(
                "MESSAGE", "prepare unit exited without reporting a result"
            )
        downloaded = fields.get("DOWNLOADED", "")
        return PrepareStatus(
            state=state or "unknown",
            step=values.get("STEP", ""),
            message=values.get("MESSAGE", ""),
            facts=values,
            downloaded_bytes=int(downloaded) if downloaded.isdigit() else None,
            unit_active=unit_active,
        )

    def stop_prepare(self, node_id: str, prepare_id: str) -> CommandResult:
        unit = prepare_unit_name(prepare_id)
        script = f"""set -u
systemctl --user stop {shlex.quote(unit)} >/dev/null 2>&1 || true
container={shlex.quote(f"dgxc-prepare-{prepare_id}")}
if docker container inspect "$container" >/dev/null 2>&1; then
  docker rm --force "$container" >/dev/null 2>&1 || true
fi
"""
        try:
            self._executor.run_script(node_id, script, timeout_seconds=60, check=False)
        except SSHExecutionError as error:
            return CommandResult(False, str(error), "prepare_stop_failed")
        return CommandResult(True, "prepare unit stopped")


def _require_directory(path: str) -> None:
    if not re.fullmatch(r"/[A-Za-z0-9._/-]{1,400}", path) or "/../" in f"{path}/":
        raise ValueError("recipe directories must be absolute, plain paths")


class FakeRecipeNodeOps:
    """Deterministic in-memory prepare simulation for fake mode and tests."""

    def __init__(
        self,
        node_ids: tuple[str, ...] | list[str],
        *,
        home: str = "/home/spark",
        memory_total_bytes: int = 128 * 1024**3,
        polls_per_step: int = 1,
        fail_nodes: Mapping[str, str] | None = None,
    ) -> None:
        self.node_ids = tuple(node_ids)
        self.home = home
        self.memory_total_bytes = memory_total_bytes
        self.polls_per_step = max(1, polls_per_step)
        self.fail_nodes = dict(fail_nodes or {})
        self.files: dict[tuple[str, str], dict[str, bytes]] = {}
        self._runs: dict[tuple[str, str], int] = {}
        self.stopped: list[tuple[str, str]] = []

    def _node(self, node_id: str) -> None:
        if node_id not in self.node_ids:
            raise SSHExecutionError(f"unknown configured node: {node_id}")

    def remote_home(self, node_id: str) -> str:
        self._node(node_id)
        return self.home

    def install_files(
        self, node_id: str, directory: str, files: Mapping[str, tuple[bytes, int]]
    ) -> CommandResult:
        self._node(node_id)
        _require_directory(directory)
        stored = self.files.setdefault((node_id, directory), {})
        for name, (content, _mode) in files.items():
            stored[name] = content
        return CommandResult(True, f"installed {len(files)} recipe files")

    def start_prepare(
        self, node_id: str, directory: str, prepare_id: str
    ) -> CommandResult:
        self._node(node_id)
        prepare_unit_name(prepare_id)
        if "prepare.sh" not in self.files.get((node_id, directory), {}):
            return CommandResult(
                False, "prepare script missing", "prepare_start_failed"
            )
        self._runs[(node_id, prepare_id)] = 0
        return CommandResult(True, "prepare unit started")

    def prepare_status(
        self, node_id: str, directory: str, prepare_id: str, model_path: str
    ) -> PrepareStatus:
        self._node(node_id)
        key = (node_id, prepare_id)
        if key not in self._runs:
            return PrepareStatus("absent")
        self._runs[key] += 1
        polls = self._runs[key]
        steps = ("pulling", "downloading", "verifying")
        index = (polls - 1) // self.polls_per_step
        environment = self._prepare_environment(node_id, directory, prepare_id)
        if node_id in self.fail_nodes and index >= 1:
            return PrepareStatus(
                "failed", step="downloading", message=self.fail_nodes[node_id]
            )
        if index < len(steps):
            return PrepareStatus(
                "running",
                step=steps[index],
                downloaded_bytes=polls * 1024**3,
                unit_active=True,
            )
        image = environment.get("VLLM_IMAGE", "vllm/vllm-openai@sha256:" + "0" * 64)
        manifest = hashlib.sha256(
            f"{environment.get('HF_REPO')}@{environment.get('HF_REVISION')}".encode()
        ).hexdigest()
        facts = {
            "STATE": "succeeded",
            "STEP": "done",
            "HOME_DIR": self.home,
            "MEM_TOTAL": str(self.memory_total_bytes),
            "IMAGE_ID": "sha256:" + hashlib.sha256(image.encode()).hexdigest(),
            "VLLM_VERSION": "0.0.0+fake",
            "MANIFEST_PATH": "model.safetensors.index.json",
            "MANIFEST_SHA256": manifest,
        }
        return PrepareStatus(
            "succeeded", step="done", facts=facts, downloaded_bytes=polls * 1024**3
        )

    def stop_prepare(self, node_id: str, prepare_id: str) -> CommandResult:
        self.stopped.append((node_id, prepare_id))
        self._runs.pop((node_id, prepare_id), None)
        return CommandResult(True, "prepare unit stopped")

    def _prepare_environment(
        self, node_id: str, directory: str, prepare_id: str
    ) -> dict[str, str]:
        content = self.files.get((node_id, directory), {}).get(
            f"prepare-{prepare_id}.env", b""
        )
        values: dict[str, str] = {}
        for line in content.decode("utf-8", "replace").splitlines():
            key, separator, value = line.partition("=")
            if separator:
                values[key] = value
        return values
