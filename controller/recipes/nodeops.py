# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Remote primitives used to prepare and delete generated recipes on nodes.

Preparation never touches a running model: it installs controller-owned files
into a per-recipe directory, starts the pinned ``prepare.sh`` in a named
``systemd --user`` unit, and reads back a ``KEY=value`` status file. All remote
scripts below are fixed templates; every interpolated value is shell-quoted and
was produced by :mod:`controller.recipes.render` from a validated draft.

Deletion is bounded the same way. A recipe directory is addressed only as the
node user's home plus the fixed controller subdirectory and a validated
generated profile id, weights only as a configured model root plus a name of
the exact shape :func:`~controller.recipes.render.model_dir_for` produces, and
an image only by its pinned ``vllm/vllm-openai@sha256:<digest>`` reference.
The removal target is always a literal, quoted path; the remote script refuses
symlinks, requires the physical path to match, and refuses while a prepare
unit or a container could still use the target.
"""

from __future__ import annotations

import base64
import hashlib
import posixpath
import re
import shlex
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from typing import Protocol

from controller.recipes.render import (
    CACHE_VOLUME,
    MODEL_DIR_RE,
    REMOTE_RECIPES_SUBDIR,
    container_name_for,
    is_generated_profile_id,
    working_directory_for,
)
from controller.recipes.spec import IMAGE_REFERENCE_RE
from controller.transport.base import CommandResult
from controller.transport.ssh import SSHExecutionError, SSHExecutor

PREPARE_ID_RE = re.compile(r"[0-9a-f]{10}\Z")
PREPARE_CONTAINER_WAIT_SECONDS = 30
USER_RE = re.compile(r"[a-z_][a-z0-9_.-]{0,31}\Z")
REMOVAL_STATES = frozenset({"removed", "absent", "refused", "failed"})
_REASON_RE = re.compile(r"[a-z][a-z-]{0,31}\Z")
_PLAIN_ABSOLUTE_RE = re.compile(r"(?:/[A-Za-z0-9._-]+)+\Z")
# Shared shell helpers for the fixed removal scripts. ``result`` ends a script
# with one machine-readable outcome; ``check_target`` refuses a symlink and
# requires the target's physical path to equal the exact expected location.
_REMOVAL_PRELUDE = r"""set -u
result() { printf 'RESULT=%s\nREASON=%s\n' "$1" "${2:-}"; exit 0; }
physical() { (cd -P -- "$1" 2>/dev/null && pwd -P); }
check_target() {
  if [ -L "$1" ]; then result refused symlink; fi
  if [ ! -e "$1" ]; then result absent; fi
  if [ ! -d "$1" ]; then result refused not-a-directory; fi
  if [ "$(physical "$1")" != "$2" ]; then result refused symlink; fi
}
no_container() {
  command -v docker >/dev/null 2>&1 || return 0
  names=$(docker ps --all --format '{{.Names}}') || result refused docker-unavailable
  for name in "$@"; do
    if printf '%s\n' "$names" | grep -Fxq -- "$name"; then
      result refused container-exists
    fi
  done
}
docker_ready() {
  command -v docker >/dev/null 2>&1 || result absent no-docker
  docker version --format '{{.Server.Version}}' >/dev/null 2>&1 \
    || result refused docker-unavailable
}
"""
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

    state: str  # running | succeeded | failed | absent | unknown
    step: str = ""
    message: str = ""
    facts: Mapping[str, str] = field(default_factory=dict)
    downloaded_bytes: int | None = None
    unit_active: bool = False


@dataclass(frozen=True)
class LingerStatus:
    """Whether a node's ``systemd --user`` manager outlives SSH sessions."""

    user: str
    value: str  # "yes", "no", or "" when loginctl could not tell

    @property
    def enabled(self) -> bool:
        return self.value == "yes"


@dataclass(frozen=True)
class RemovalResult:
    """Outcome of one bounded remote removal."""

    state: str  # removed | absent | refused | failed; delete reports add in-use
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.state in {"removed", "absent"}

    @property
    def detail(self) -> str:
        """The reason if the node gave one, otherwise the state."""
        return self.reason or self.state


class RecipeNodeOps(Protocol):
    def remote_home(self, node_id: str) -> str: ...

    def check_linger(self, node_id: str) -> LingerStatus: ...

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

    def remove_recipe_directory(
        self,
        node_id: str,
        home: str,
        profile_id: str,
        *,
        prepare_id: str | None = None,
    ) -> RemovalResult: ...

    def remove_model_directory(
        self, node_id: str, model_root: str, model_dir: str
    ) -> RemovalResult: ...

    def remove_image(self, node_id: str, image: str) -> RemovalResult: ...

    def remove_cache_volume(self, node_id: str) -> RemovalResult: ...


def prepare_unit_name(prepare_id: str) -> str:
    if not PREPARE_ID_RE.fullmatch(prepare_id):
        raise ValueError("invalid prepare id")
    return f"dgx-prepare-{prepare_id}.service"


def require_removal_target(
    *,
    home: str | None = None,
    profile_id: str | None = None,
    model_root: str | None = None,
    model_dir: str | None = None,
    image: str | None = None,
) -> None:
    """Validate deletion targets before any remote script is rendered.

    Raises:
        ValueError: A value does not have the exact shape the recipe builder
            produces, so it must never reach ``rm`` or ``docker image rm``.

    """
    for path in (home, model_root):
        if path is not None and (
            len(path) > 400
            or not _PLAIN_ABSOLUTE_RE.fullmatch(path)
            or any(part in {".", ".."} for part in path.split("/"))
        ):
            raise ValueError("home and model roots must be absolute, plain paths")
    if profile_id is not None and not is_generated_profile_id(profile_id):
        raise ValueError("only generated recipe directories can be removed")
    if model_dir is not None and (
        not MODEL_DIR_RE.fullmatch(model_dir) or ".." in model_dir
    ):
        raise ValueError("only generated model directories can be removed")
    if image is not None and not IMAGE_REFERENCE_RE.fullmatch(image):
        raise ValueError("only digest-pinned vLLM images can be removed")


def _key_values(text: str, keys: Collection[str] | None = None) -> dict[str, str]:
    """Return the last raw value per ``KEY=value`` line, optionally allowlisted."""
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if separator and (keys is None or key in keys):
            values[key] = value
    return values


def parse_removal(text: str) -> RemovalResult:
    """Read the one outcome a removal script reports; anything else is a failure."""
    values = _key_values(text, {"RESULT", "REASON"})
    state = values.get("RESULT", "").strip()
    reason = values.get("REASON", "").strip()
    if state not in REMOVAL_STATES:
        return RemovalResult("failed", "no-result")
    return RemovalResult(state, reason if _REASON_RE.fullmatch(reason) else "")


def parse_linger(text: str) -> LingerStatus:
    """Read ``USER=`` and ``LINGER=`` lines; unknown values become empty."""
    values = _key_values(text)
    user = values.get("USER", "").strip()
    linger = values.get("LINGER", "").strip().lower()
    return LingerStatus(
        user=user if USER_RE.fullmatch(user) else "",
        value=linger if linger in {"yes", "no"} else "",
    )


def parse_status(text: str) -> tuple[dict[str, str], str]:
    """Return the last value per allowlisted key and the raw last state."""
    values = {key: value[:500] for key, value in _key_values(text, STATUS_KEYS).items()}
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
        if not re.fullmatch(r"/[A-Za-z0-9._/-]{0,200}", home):
            raise SSHExecutionError(
                f"node {node_id} reported an unsupported home directory"
            )
        return home

    def check_linger(self, node_id: str) -> LingerStatus:
        script = """set -u
user=$(id -un)
printf 'USER=%s\\n' "$user"
printf 'LINGER=%s\\n' "$(loginctl show-user "$user" -p Linger --value 2>/dev/null)"
"""
        result = self._executor.run_script(
            node_id, script, timeout_seconds=15, check=True
        )
        return parse_linger(result.stdout)

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
        return self._command(
            node_id,
            "\n".join(lines) + "\n",
            timeout_seconds=60,
            code="recipe_install_failed",
            failure="recipe files could not be installed",
            success=f"installed {len(files)} recipe files",
            errors=(SSHExecutionError, ValueError),
        )

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
        return self._command(
            node_id,
            script,
            timeout_seconds=30,
            code="prepare_start_failed",
            failure="prepare unit did not start",
            success="prepare unit started",
        )

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
        fields = _key_values(result.stdout)
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
        # Docker removes a stopped "docker run --rm" container asynchronously,
        # so wait until it is gone: cleanup refuses while it still exists.
        script = f"""set -u
systemctl --user stop {shlex.quote(unit)} >/dev/null 2>&1 || true
container={shlex.quote(f"dgxc-prepare-{prepare_id}")}
for _ in $(seq 1 {PREPARE_CONTAINER_WAIT_SECONDS}); do
  docker container inspect "$container" >/dev/null 2>&1 || exit 0
  docker rm --force "$container" >/dev/null 2>&1 || true
  sleep 1
done
exit 1
"""
        return self._command(
            node_id,
            script,
            timeout_seconds=60,
            code="prepare_stop_failed",
            failure="prepare container is still present",
            success="prepare unit stopped",
            report_stderr=False,
        )

    def remove_recipe_directory(
        self,
        node_id: str,
        home: str,
        profile_id: str,
        *,
        prepare_id: str | None = None,
    ) -> RemovalResult:
        require_removal_target(home=home, profile_id=profile_id)
        target = shlex.quote(working_directory_for(home, profile_id))
        relative = shlex.quote(f"{REMOTE_RECIPES_SUBDIR}/{profile_id}")
        containers = [container_name_for(profile_id)]
        lines = [
            'case "$HOME" in /?*) ;; *) result refused home ;; esac',
            'home=$(physical "$HOME")',
            f'if [ -z "$home" ] || [ "$home" != "$(physical {shlex.quote(home)})" ]; '
            "then result refused home-mismatch; fi",
            f'check_target {target} "$home"/{relative}',
        ]
        if prepare_id:
            unit = shlex.quote(prepare_unit_name(prepare_id))
            containers.append(f"dgxc-prepare-{prepare_id}")
            lines.append(
                f"if systemctl --user is-active --quiet {unit}; then "
                "result refused prepare-active; fi"
            )
        recipes_root = posixpath.dirname(working_directory_for(home, profile_id))
        lines += [
            "no_container " + " ".join(shlex.quote(name) for name in containers),
            f"rm -rf -- {target} || result failed rm",
            # Leave no empty controller directories behind the last recipe.
            f"rmdir -- {shlex.quote(recipes_root)} "
            f"{shlex.quote(posixpath.dirname(recipes_root))} 2>/dev/null || true",
            "result removed",
        ]
        return self._remove(node_id, lines, timeout_seconds=120)

    def remove_model_directory(
        self, node_id: str, model_root: str, model_dir: str
    ) -> RemovalResult:
        require_removal_target(model_root=model_root, model_dir=model_dir)
        target = shlex.quote(f"{model_root}/{model_dir}")
        name = shlex.quote(model_dir)
        argument = shlex.quote(f"/{model_dir}")
        lines = [
            f"root=$(physical {shlex.quote(model_root)})",
            'if [ -z "$root" ]; then result absent; fi',
            f'check_target {target} "$root"/{name}',
            # A prepare may be downloading into this model root right now.
            "if systemctl --user list-units --all --no-legend --plain "
            "--state=active,activating 'dgx-prepare-*.service' 2>/dev/null "
            "| grep -q .; then result refused prepare-active; fi",
            "if command -v docker >/dev/null 2>&1; then",
            "  running=$(docker ps --no-trunc --format '{{.Names}} {{.Command}}') "
            "|| result refused docker-unavailable",
            "  if printf '%s\\n' \"$running\" | grep -q '^dgxc-prepare-'; then "
            "result refused prepare-active; fi",
            # A hand-started container serving these weights also blocks removal.
            f"  if printf '%s\\n' \"$running\" | grep -Fq -- {argument}; then "
            "result refused in-use; fi",
            "fi",
            f"rm -rf -- {target} || result failed rm",
            "result removed",
        ]
        return self._remove(node_id, lines, timeout_seconds=600)

    def remove_image(self, node_id: str, image: str) -> RemovalResult:
        require_removal_target(image=image)
        reference = shlex.quote(image)
        lines = [
            "docker_ready",
            f"docker image inspect {reference} >/dev/null 2>&1 || result absent",
            f"id=$(docker image inspect --format '{{{{.Id}}}}' {reference}) "
            "|| result refused docker-unavailable",
            'users=$(docker ps --all --quiet --filter "ancestor=$id") '
            "|| result refused docker-unavailable",
            'if [ -n "$users" ]; then result refused in-use; fi',
            f"docker image rm -- {reference} >/dev/null 2>&1 || result failed image-rm",
            "result removed",
        ]
        return self._remove(node_id, lines, timeout_seconds=300)

    def remove_cache_volume(self, node_id: str) -> RemovalResult:
        volume = shlex.quote(CACHE_VOLUME)
        lines = [
            "docker_ready",
            f"docker volume inspect {volume} >/dev/null 2>&1 || result absent",
            f"users=$(docker ps --all --quiet --filter volume={volume}) "
            "|| result refused docker-unavailable",
            'if [ -n "$users" ]; then result refused in-use; fi',
            f"docker volume rm -- {volume} >/dev/null 2>&1 || result failed volume-rm",
            "result removed",
        ]
        return self._remove(node_id, lines, timeout_seconds=60)

    def _command(
        self,
        node_id: str,
        script: str,
        *,
        timeout_seconds: float,
        code: str,
        failure: str,
        success: str,
        errors: tuple[type[Exception], ...] = (SSHExecutionError,),
        report_stderr: bool = True,
    ) -> CommandResult:
        """Run one fixed script and reduce its outcome to a redacted result."""
        try:
            result = self._executor.run_script(
                node_id, script, timeout_seconds=timeout_seconds, check=False
            )
        except errors as error:
            return CommandResult(False, str(error), code)
        if not result.ok:
            message = result.stderr.strip() if report_stderr else ""
            return CommandResult(False, message or failure, code)
        return CommandResult(True, success)

    def _remove(
        self, node_id: str, lines: list[str], *, timeout_seconds: float
    ) -> RemovalResult:
        script = _REMOVAL_PRELUDE + "\n".join(lines) + "\n"
        try:
            result = self._executor.run_script(
                node_id, script, timeout_seconds=timeout_seconds, check=False
            )
        except (SSHExecutionError, ValueError):
            return RemovalResult("failed", "unreachable")
        if not result.ok:
            return RemovalResult("failed", "script-failed")
        return parse_removal(result.stdout)


def _require_directory(path: str) -> None:
    if not re.fullmatch(r"/[A-Za-z0-9._/-]{1,400}", path) or "/../" in f"{path}/":
        raise ValueError("recipe directories must be absolute, plain paths")


class FakeRecipeNodeOps:
    """Deterministic in-memory prepare simulation for fake mode and tests.

    Besides files and prepare runs, the fake tracks the artifacts a prepare
    leaves on each node (model directories, pulled images, the cache volume)
    so deletion can be exercised. Tests flip ``linger``, ``unreachable``,
    ``refuse_removal`` and the ``*_in_use`` sets to simulate node conditions.
    """

    def __init__(
        self,
        node_ids: tuple[str, ...] | list[str],
        *,
        home: str = "/home/spark",
        user: str = "spark",
        memory_total_bytes: int = 128 * 1024**3,
        polls_per_step: int = 1,
        fail_nodes: Mapping[str, str] | None = None,
    ) -> None:
        self.node_ids = tuple(node_ids)
        self.home = home
        self.user = user
        self.memory_total_bytes = memory_total_bytes
        self.polls_per_step = max(1, polls_per_step)
        self.fail_nodes = dict(fail_nodes or {})
        self.linger = {node_id: "yes" for node_id in self.node_ids}
        self.unreachable: set[str] = set()
        self.refuse_removal: dict[str, str] = {}
        self.files: dict[tuple[str, str], dict[str, bytes]] = {}
        self._runs: dict[tuple[str, str], int] = {}
        self._finished: set[tuple[str, str]] = set()
        self.stopped: list[tuple[str, str]] = []
        self.model_dirs: set[tuple[str, str]] = set()
        self.images: set[tuple[str, str]] = set()
        self.volumes: set[str] = set()
        self.images_in_use: set[tuple[str, str]] = set()
        self.volumes_in_use: set[str] = set()
        self.removals: list[tuple[str, str, str]] = []

    def _node(self, node_id: str) -> None:
        if node_id not in self.node_ids:
            raise SSHExecutionError(f"unknown configured node: {node_id}")
        if node_id in self.unreachable:
            raise SSHExecutionError(f"node {node_id} is unreachable")

    def remote_home(self, node_id: str) -> str:
        self._node(node_id)
        return self.home

    def check_linger(self, node_id: str) -> LingerStatus:
        self._node(node_id)
        return LingerStatus(self.user, self.linger.get(node_id, ""))

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
        environment = self._prepare_environment(node_id, directory, prepare_id)
        if environment.get("MODEL_ROOT") and environment.get("MODEL_DIR"):
            self.model_dirs.add(
                (node_id, f"{environment['MODEL_ROOT']}/{environment['MODEL_DIR']}")
            )
        if environment.get("VLLM_IMAGE"):
            self.images.add((node_id, environment["VLLM_IMAGE"]))
        self.volumes.add(node_id)
        return CommandResult(True, "prepare unit started")

    def prepare_status(
        self, node_id: str, directory: str, prepare_id: str, model_path: str
    ) -> PrepareStatus:
        if node_id in self.unreachable:
            return PrepareStatus("unknown", message=f"node {node_id} is unreachable")
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
            self._finished.add(key)
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
        self._finished.add(key)
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

    def remove_recipe_directory(
        self,
        node_id: str,
        home: str,
        profile_id: str,
        *,
        prepare_id: str | None = None,
    ) -> RemovalResult:
        self._node(node_id)
        require_removal_target(home=home, profile_id=profile_id)
        directory = working_directory_for(home, profile_id)
        if home != self.home:
            return RemovalResult("refused", "home-mismatch")
        if node_id in self.refuse_removal:
            return RemovalResult("refused", self.refuse_removal[node_id])
        key = (node_id, prepare_id or "")
        if key in self._runs and key not in self._finished:
            return RemovalResult("refused", "prepare-active")
        self.removals.append(("directory", node_id, directory))
        if self.files.pop((node_id, directory), None) is None:
            return RemovalResult("absent")
        return RemovalResult("removed")

    def remove_model_directory(
        self, node_id: str, model_root: str, model_dir: str
    ) -> RemovalResult:
        self._node(node_id)
        require_removal_target(model_root=model_root, model_dir=model_dir)
        target = (node_id, f"{model_root}/{model_dir}")
        if target not in self.model_dirs:
            return RemovalResult("absent")
        self.model_dirs.discard(target)
        self.removals.append(("weights", *target))
        return RemovalResult("removed")

    def remove_image(self, node_id: str, image: str) -> RemovalResult:
        self._node(node_id)
        require_removal_target(image=image)
        if (node_id, image) in self.images_in_use:
            return RemovalResult("refused", "in-use")
        if (node_id, image) not in self.images:
            return RemovalResult("absent")
        self.images.discard((node_id, image))
        self.removals.append(("image", node_id, image))
        return RemovalResult("removed")

    def remove_cache_volume(self, node_id: str) -> RemovalResult:
        self._node(node_id)
        if node_id in self.volumes_in_use:
            return RemovalResult("refused", "in-use")
        if node_id not in self.volumes:
            return RemovalResult("absent")
        self.volumes.discard(node_id)
        self.removals.append(("cache-volume", node_id, CACHE_VOLUME))
        return RemovalResult("removed")

    def _prepare_environment(
        self, node_id: str, directory: str, prepare_id: str
    ) -> dict[str, str]:
        content = self.files.get((node_id, directory), {}).get(
            f"prepare-{prepare_id}.env", b""
        )
        return _key_values(content.decode("utf-8", "replace"))
