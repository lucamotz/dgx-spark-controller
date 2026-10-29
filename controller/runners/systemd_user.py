# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Exact process ownership through transient ``systemd --user`` services."""

from __future__ import annotations

import re
import shlex
from collections.abc import Mapping
from dataclasses import dataclass

from controller.transport.base import CommandResult, LogChunk
from controller.transport.cluster_scripts import verified_script_prologue
from controller.transport.ssh import SSHExecutionError, SSHExecutor

UNIT_RE = re.compile(r"dgx-model-[a-z0-9][a-z0-9-]{0,62}\Z")
RECIPE_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,95}\Z")


@dataclass(frozen=True)
class ScriptUnitRecipe:
    """Reviewed mapping from a recipe ID to one remote launch script."""

    id: str
    script_path: str
    sha256: str
    timeout_stop_seconds: int = 45

    def __post_init__(self) -> None:
        if not RECIPE_RE.fullmatch(self.id):
            raise ValueError(f"invalid recipe id: {self.id!r}")
        if not self.script_path.startswith("/"):
            raise ValueError("remote script path must be absolute")
        if not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
            raise ValueError(
                "recipe sha256 must be 64 lowercase hexadecimal characters"
            )
        if self.timeout_stop_seconds <= 0:
            raise ValueError("recipe stop timeout must be positive")


class SystemdUserRunner:
    """Start and stop only named, allowlisted transient user services."""

    def __init__(
        self, executor: SSHExecutor, recipes: Mapping[str, ScriptUnitRecipe]
    ) -> None:
        self._executor = executor
        self._recipes = dict(recipes)
        if any(key != recipe.id for key, recipe in self._recipes.items()):
            raise ValueError("recipe registry keys must equal ScriptUnitRecipe.id")

    @staticmethod
    def _unit(unit_name: str) -> str:
        if not UNIT_RE.fullmatch(unit_name):
            raise ValueError(f"invalid controller unit name: {unit_name!r}")
        return unit_name

    @classmethod
    def _service(cls, unit_name: str) -> str:
        return cls._unit(unit_name) + ".service"

    def _recipe(self, recipe_id: str) -> ScriptUnitRecipe:
        try:
            return self._recipes[recipe_id]
        except KeyError as exc:
            raise ValueError(f"unknown allowlisted recipe: {recipe_id}") from exc

    def start_unit(self, node_id: str, unit_name: str, recipe_id: str) -> CommandResult:
        unit = self._unit(unit_name)
        recipe = self._recipe(recipe_id)
        q_service = shlex.quote(unit + ".service")
        q_run_unit = shlex.quote(unit)
        prologue = verified_script_prologue(recipe.script_path, recipe.sha256)
        script = f"""{prologue}unit={q_service}
if systemctl --user is-active --quiet "$unit"; then
  printf '%s\\n' 'already-running'
  exit 0
fi
systemctl --user reset-failed "$unit" 2>/dev/null || true
systemd-run --user --unit={q_run_unit} --collect \\
  --property=KillMode=control-group \\
  --property=Restart=no \\
  --property=TimeoutStopSec={recipe.timeout_stop_seconds} \\
  /bin/bash "$script_path"
"""
        try:
            result = self._executor.run_script(
                node_id, script, timeout_seconds=30, check=False
            )
        except SSHExecutionError as exc:
            return CommandResult(False, str(exc), "ssh_execution_failed")
        if not result.ok:
            detail = result.stderr.strip() or result.stdout.strip()
            return CommandResult(False, detail, "unit_start_failed")
        return CommandResult(True, result.stdout.strip() or "unit start accepted")

    def stop_unit(
        self,
        node_id: str,
        unit_name: str,
        *,
        timeout_seconds: float,
        force: bool = False,
    ) -> CommandResult:
        service = self._service(unit_name)
        q_service = shlex.quote(service)
        force_lines = ""
        if force:
            force_lines = (
                f"systemctl --user kill --kill-whom=all --signal=KILL {q_service} "
                "2>/dev/null || true\n"
            )
        script = f"""set -eu
if ! systemctl --user show {q_service} >/dev/null 2>&1; then
  printf '%s\\n' 'already-stopped'
  exit 0
fi
systemctl --user stop --no-block {q_service} || true
{force_lines}deadline=$((SECONDS + {max(1, int(timeout_seconds))}))
while systemctl --user is-active --quiet {q_service}; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    printf '%s\\n' 'stop-timeout' >&2
    exit 75
  fi
  sleep 1
done
printf '%s\\n' 'stopped'
"""
        try:
            result = self._executor.run_script(
                node_id,
                script,
                timeout_seconds=max(10, timeout_seconds + 5),
                check=False,
            )
        except SSHExecutionError as exc:
            return CommandResult(False, str(exc), "ssh_execution_failed")
        if not result.ok:
            detail = result.stderr.strip() or result.stdout.strip()
            code = (
                "unit_stop_timeout" if result.returncode == 75 else "unit_stop_failed"
            )
            return CommandResult(False, detail, code)
        return CommandResult(True, result.stdout.strip() or "unit stopped")

    def unit_logs(
        self,
        node_id: str,
        unit_name: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> LogChunk:
        del cursor
        service = self._service(unit_name)
        bounded_limit = max(1, min(int(limit), 500))
        script = (
            "set -eu\n"
            f"journalctl --user --unit={shlex.quote(service)} --no-pager "
            f"--output=short-iso --lines={bounded_limit} 2>/dev/null || true\n"
        )
        try:
            result = self._executor.run_script(
                node_id, script, timeout_seconds=15, check=False
            )
        except SSHExecutionError as exc:
            return LogChunk((f"log retrieval failed: {exc}",), None)
        return LogChunk(tuple(result.stdout.splitlines()), None)
