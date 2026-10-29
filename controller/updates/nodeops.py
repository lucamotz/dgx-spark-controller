# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Remote primitives for system updates of the configured nodes.

All privileged work on a node goes through one root-owned helper,
``templates/dgx-spark-controller-update.sh``, installed at
:data:`HELPER_PATH`. A sudoers rule lets the node user run exactly its
``check``, ``download``, ``apply``, ``status`` and ``reboot`` subcommands
without a password. Every script here verifies the installed helper's
SHA-256 against the packaged copy before it calls ``sudo``, so an outdated
or modified helper is never run.

Installing the helper is the one step that needs the node user's sudo
password. :meth:`SSHUpdateNodeOps.install_helper` stages the packaged helper
in the user's home with BatchMode SSH, then runs it once as
``sudo -S ... install``; the password reaches ``sudo`` on the SSH session's
stdin and is never written anywhere.

Nodes need no Internet access: while a check, download or apply runs, the
controller forwards the node's ``127.0.0.1:18877`` to its own update relay
(:mod:`controller.updates.relay`) over an SSH reverse tunnel.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import re
import shlex
import subprocess
import threading
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from importlib import resources
from typing import Protocol

from controller.transport.script_output import decode_b64, parse_fields
from controller.transport.ssh import SSHExecutionError, SSHExecutor
from controller.updates.relay import HOST_RE, RelaySession

HELPER_PATH = "/usr/local/sbin/dgx-spark-controller-update"
# Relative to the node user's home directory.
HELPER_STAGING_PATH = (
    ".local/share/dgx-spark-controller/update-helper/dgx-spark-controller-update"
)
RELAY_REMOTE_PORT = 18877
RUN_MODES = frozenset({"download", "apply"})
RUN_STATES = frozenset({"idle", "running", "succeeded", "failed", "interrupted"})
_STEP_RE = re.compile(r"[a-z][a-z-]{0,23}\Z")
_BOOT_ID_RE = re.compile(r"[0-9a-f-]{36}\Z")
_ERROR_RE = re.compile(r"[a-z][a-z-]{0,39}\Z")
_VERSION_TEXT_RE = re.compile(r"[^A-Za-z0-9 ._:()+~-]")
_SUDO_PASSWORD_MAX = 256


def helper_bytes() -> bytes:
    """The packaged node helper, byte for byte as it is installed."""
    return (
        resources.files("controller.updates")
        .joinpath("templates/dgx-spark-controller-update.sh")
        .read_bytes()
    )


def helper_sha256() -> str:
    return hashlib.sha256(helper_bytes()).hexdigest()


class UpdateNodeError(RuntimeError):
    """A node-side update step failed with a stable code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class RunStatus:
    """State of a node's latest helper ``download`` or ``apply`` run."""

    state: str = "idle"
    mode: str = ""
    step: str = ""
    packages: int | None = None
    firmware: int | None = None
    reboot_required: bool = False
    failed_step: str = ""
    log_tail: tuple[str, ...] = ()

    @property
    def finished(self) -> bool:
        return self.state in {"succeeded", "failed", "interrupted"}


@dataclass(frozen=True)
class UpdateCheck:
    """Pending updates a node reported after refreshing through the relay."""

    packages: int
    firmware: int
    reboot_required: bool

    @property
    def pending(self) -> int:
        return self.packages + self.firmware


@dataclass(frozen=True)
class NodeFacts:
    """Facts read from a node without root."""

    node_id: str
    reachable: bool
    helper: str = "unreachable"
    message: str = ""
    boot_id: str = ""
    system_state: str = ""
    # Boot jobs still running, apart from the splash wait of a headless node;
    # None when the node could not list them.
    boot_jobs: int | None = None
    user_manager: str = ""
    docker_ready: bool = False
    gpu_ready: bool = False
    links_expected: int = 0
    links_present: int = 0
    kernel: str = ""
    driver: str = ""
    os_version: str = ""
    dgx_version: str = ""
    # Repository host -> upstream scheme, from the node's apt sources.
    source_hosts: Mapping[str, str] = field(default_factory=dict)
    run: RunStatus | None = None

    @property
    def booted(self) -> bool:
        """The node finished booting and can host a model again.

        A headless node whose display manager never takes over from the boot
        splash stays ``starting`` for good, so a node whose only running boot
        job is that splash wait counts as booted too.
        """
        finished = self.system_state in {"running", "degraded"} or (
            self.system_state == "starting" and self.boot_jobs == 0
        )
        return (
            self.reachable
            and finished
            and self.user_manager in {"running", "degraded"}
            and self.docker_ready
            and self.gpu_ready
            and self.links_present >= self.links_expected
        )


class RelayTunnels(Protocol):
    def close(self) -> None: ...


class UpdateNodeOps(Protocol):
    def facts(self, node_id: str) -> NodeFacts: ...

    def check(self, node_id: str) -> UpdateCheck: ...

    def start_run(self, node_id: str, mode: str) -> RunStatus: ...

    def run_status(self, node_id: str) -> RunStatus: ...

    def reboot(self, node_id: str) -> str: ...

    def install_helper(self, node_id: str, password: str) -> None: ...

    def open_relay(
        self, node_ids: Sequence[str], relay: RelaySession
    ) -> RelayTunnels: ...


def relay_hosts(facts: Iterable[NodeFacts]) -> dict[str, str]:
    """Repository hosts the relay may serve, merged over nodes; https wins."""
    hosts: dict[str, str] = {}
    for node in facts:
        for host, scheme in node.source_hosts.items():
            if hosts.get(host) != "https":
                hosts[host] = scheme
    return hosts


def require_sudo_password(value: object) -> str:
    """Validate a one-time sudo password without ever echoing it."""
    if (
        not isinstance(value, str)
        or not value
        or len(value) > _SUDO_PASSWORD_MAX
        or any(char in value for char in "\n\r\0")
    ):
        raise ValueError("invalid sudo password")
    return value


def parse_run_status(text: str) -> RunStatus:
    """Read a helper ``status``/``download``/``apply`` report leniently."""
    fields = parse_fields(text)
    state = fields.get("STATE", "idle")
    tail = decode_b64(fields.get("LOG_TAIL", ""))
    return RunStatus(
        state=state if state in RUN_STATES else "idle",
        mode=fields.get("MODE", "") if fields.get("MODE") in RUN_MODES else "",
        step=_step(fields.get("STEP", "")),
        packages=_optional_int(fields.get("PACKAGES")),
        firmware=_optional_int(fields.get("FIRMWARE")),
        reboot_required=fields.get("REBOOT_REQUIRED") == "1",
        failed_step=_step(fields.get("FAILED_STEP", "")),
        log_tail=tuple(line[:300] for line in tail.splitlines()[-30:]),
    )


def parse_check(text: str) -> UpdateCheck:
    fields = parse_fields(text)
    if "PACKAGES" not in fields or "FIRMWARE" not in fields:
        raise UpdateNodeError(
            _error_code(fields, "update_check_failed"),
            "The node could not check for updates through the controller relay.",
        )
    return UpdateCheck(
        packages=_optional_int(fields.get("PACKAGES")) or 0,
        firmware=_optional_int(fields.get("FIRMWARE")) or 0,
        reboot_required=fields.get("REBOOT_REQUIRED") == "1",
    )


def parse_facts(node_id: str, text: str, expected_sha256: str) -> NodeFacts:
    """Read the non-root probe; the helper state is decided here, not remotely."""
    fields = parse_fields(text)
    sha = fields.get("HELPER_SHA256", "")
    owner = fields.get("HELPER_OWNER", "")
    if not sha:
        helper = "missing"
    elif sha != expected_sha256 or not _root_owned(owner):
        helper = "outdated"
    elif fields.get("SUDO_OK") != "1":
        helper = "no-sudo"
    else:
        helper = "ready"
    run = None
    if helper == "ready":
        run = parse_run_status(
            "\n".join(
                f"{key[4:]}={value}"
                for key, value in fields.items()
                if key.startswith("RUN_")
            )
        )
    hosts: dict[str, str] = {}
    for item in fields.get("SOURCES", "").split():
        scheme, _, host = item.partition("://")
        host = host.lower()
        # One https source is enough to fetch that host over https.
        if (
            scheme in {"http", "https"}
            and HOST_RE.fullmatch(host)
            and hosts.get(host) != "https"
        ):
            hosts[host] = scheme
    return NodeFacts(
        node_id=node_id,
        reachable=True,
        helper=helper,
        boot_id=_boot_id(fields.get("BOOT_ID", "")),
        system_state=_step(fields.get("SYSTEM_STATE", "")),
        boot_jobs=_optional_int(fields.get("BOOT_JOBS")),
        user_manager=_step(fields.get("USER_MANAGER", "")),
        docker_ready=fields.get("DOCKER_READY") == "1",
        gpu_ready=fields.get("GPU_READY") == "1",
        links_expected=_optional_int(fields.get("LINKS_EXPECTED")) or 0,
        links_present=_optional_int(fields.get("LINKS_PRESENT")) or 0,
        kernel=_version_text(fields.get("KERNEL", "")),
        driver=_version_text(fields.get("DRIVER", "")),
        os_version=_version_text(fields.get("OS_VERSION", "")),
        dgx_version=_version_text(fields.get("DGX_VERSION", "")),
        source_hosts=dict(sorted(hosts.items())),
        run=run,
    )


def _root_owned(owner: str) -> bool:
    """``root:root:<mode>`` with no group or other write permission."""
    user, _, rest = owner.partition(":")
    group, _, mode = rest.partition(":")
    return (
        user == group == "root"
        and re.fullmatch(r"[0-7]{3,4}", mode) is not None
        and not int(mode, 8) & 0o022
    )


def _optional_int(value: str | None) -> int | None:
    value = (value or "").strip()
    return int(value) if value.isdecimal() and len(value) <= 18 else None


def _step(value: str) -> str:
    value = value.strip()
    return value if _STEP_RE.fullmatch(value) else ""


def _boot_id(value: str) -> str:
    value = value.strip()
    return value if _BOOT_ID_RE.fullmatch(value) else ""


def _version_text(value: str) -> str:
    return _VERSION_TEXT_RE.sub("", value.strip())[:80]


def _error_code(fields: Mapping[str, str], fallback: str) -> str:
    error = fields.get("ERROR", "").strip()
    return error.replace("-", "_") if _ERROR_RE.fullmatch(error) else fallback


class _Tunnels:
    """Reverse tunnels that forward each node's relay port to the controller."""

    def __init__(self, processes: Sequence[subprocess.Popen[bytes]]) -> None:
        self._processes = list(processes)
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            processes, self._processes = self._processes, []
        for process in processes:
            if process.stdin is not None:
                process.stdin.close()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


class SSHUpdateNodeOps:
    """Execute the fixed update primitives over the configured SSH executor."""

    def __init__(
        self,
        executor: SSHExecutor,
        cluster_addresses: Mapping[str, Sequence[str]],
        *,
        expected_sha256: str | None = None,
    ) -> None:
        self._executor = executor
        self._addresses = {
            node_id: tuple(addresses)
            for node_id, addresses in cluster_addresses.items()
        }
        self._sha256 = expected_sha256 or helper_sha256()

    def facts(self, node_id: str) -> NodeFacts:
        addresses = " ".join(
            shlex.quote(address) for address in self._addresses.get(node_id, ())
        )
        script = f"""set -u
helper={shlex.quote(HELPER_PATH)}
expected={shlex.quote(self._sha256)}
printf 'BOOT_ID=%s\\n' "$(cat /proc/sys/kernel/random/boot_id)"
printf 'SYSTEM_STATE=%s\\n' "$(systemctl is-system-running 2>/dev/null)"
printf 'USER_MANAGER=%s\\n' "$(systemctl --user is-system-running 2>/dev/null)"
if jobs=$(systemctl list-jobs --no-legend 2>/dev/null); then
  printf 'BOOT_JOBS=%s\\n' "$(printf '%s\\n' "$jobs" \\
    | awk '$4 == "running" && $2 != "plymouth-quit-wait.service"' | wc -l)"
fi
if docker info --format '{{{{.ServerVersion}}}}' >/dev/null 2>&1; then
  echo DOCKER_READY=1
fi
if nvidia-smi -L >/dev/null 2>&1; then echo GPU_READY=1; fi
printf 'DRIVER=%s\\n' "$(nvidia-smi --query-gpu=driver_version \\
  --format=csv,noheader 2>/dev/null | head -n 1)"
printf 'KERNEL=%s\\n' "$(uname -r)"
printf 'OS_VERSION=%s\\n' "$(sed -n 's/^PRETTY_NAME=//p' /etc/os-release | tr -d '"')"
printf 'DGX_VERSION=%s\\n' "$(sed -n 's/^DGX_OTA_VERSION=//p' /etc/dgx-release \\
  2>/dev/null | tail -n 1 | tr -d '"')"
expected_links=0
present_links=0
for address in {addresses}; do
  expected_links=$((expected_links + 1))
  if ip -o -4 addr show 2>/dev/null | grep -Fq " $address/"; then
    present_links=$((present_links + 1))
  fi
done
printf 'LINKS_EXPECTED=%s\\nLINKS_PRESENT=%s\\n' "$expected_links" "$present_links"
printf 'SOURCES=%s\\n' "$(cat /etc/apt/sources.list /etc/apt/sources.list.d/*.list \\
  /etc/apt/sources.list.d/*.sources 2>/dev/null | grep -v '^[[:space:]]*#' \\
  | grep -oE 'https?://[A-Za-z0-9.-]+' | sort -u | tr '\\n' ' ')"
if [ -f "$helper" ] && [ ! -L "$helper" ]; then
  actual=$(sha256sum "$helper" | cut -d' ' -f1)
  printf 'HELPER_SHA256=%s\\n' "$actual"
  printf 'HELPER_OWNER=%s\\n' "$(stat -c '%U:%G:%a' "$helper")"
  if [ "$actual" = "$expected" ] && status=$(sudo -n "$helper" status 2>/dev/null); then
    echo SUDO_OK=1
    printf '%s\\n' "$status" | sed 's/^/RUN_/'
  fi
fi
"""
        try:
            result = self._executor.run_script(node_id, script, timeout_seconds=30)
        except SSHExecutionError as error:
            return NodeFacts(node_id=node_id, reachable=False, message=str(error))
        if not result.ok and "BOOT_ID=" not in result.stdout:
            return NodeFacts(
                node_id=node_id,
                reachable=False,
                message=result.stderr.strip()[:300] or "probe failed",
            )
        return parse_facts(node_id, result.stdout, self._sha256)

    def check(self, node_id: str) -> UpdateCheck:
        return parse_check(self._helper(node_id, "check", timeout_seconds=1500).stdout)

    def start_run(self, node_id: str, mode: str) -> RunStatus:
        if mode not in RUN_MODES:
            raise ValueError("invalid helper run mode")
        result = self._helper(node_id, mode, timeout_seconds=120)
        fields = parse_fields(result.stdout)
        if not result.ok or "STATE" not in fields:
            raise UpdateNodeError(
                _error_code(fields, "update_start_failed"),
                f"The update helper on {node_id} could not start its {mode} run.",
            )
        return parse_run_status(result.stdout)

    def run_status(self, node_id: str) -> RunStatus:
        result = self._helper(node_id, "status", timeout_seconds=30)
        if not result.ok:
            raise UpdateNodeError(
                _error_code(parse_fields(result.stdout), "update_status_failed"),
                f"The update helper on {node_id} did not report its state.",
            )
        return parse_run_status(result.stdout)

    def reboot(self, node_id: str) -> str:
        result = self._helper(node_id, "reboot", timeout_seconds=60)
        fields = parse_fields(result.stdout)
        boot_id = _boot_id(fields.get("BOOT_ID", ""))
        if not result.ok or fields.get("REBOOT_SCHEDULED") != "1" or not boot_id:
            raise UpdateNodeError(
                _error_code(fields, "reboot_failed"),
                f"Node {node_id} did not accept the reboot request.",
            )
        return boot_id

    def install_helper(self, node_id: str, password: str) -> None:
        require_sudo_password(password)
        content = helper_bytes()
        payload = base64.b64encode(content).decode("ascii")
        stage = f"""set -eu
umask 077
target={shlex.quote(HELPER_STAGING_PATH)}
mkdir -p "$(dirname "$target")"
tmp=$(mktemp "$target.XXXXXX")
trap 'rm -f "$tmp"' EXIT
printf '%s' {shlex.quote(payload)} | base64 -d > "$tmp"
test "$(sha256sum "$tmp" | cut -d' ' -f1)" = {shlex.quote(self._sha256)}
mv -f "$tmp" "$target"
echo STAGED=1
"""
        try:
            staged = self._executor.run_script(node_id, stage, timeout_seconds=60)
            if "STAGED=1" not in staged.stdout:
                raise UpdateNodeError(
                    "helper_stage_failed",
                    f"The update helper could not be copied to {node_id}.",
                )
            result = self._executor.run_argv(
                node_id,
                (
                    "/usr/bin/sudo",
                    "-S",
                    "-k",
                    "-p",
                    "",
                    "--",
                    "/bin/bash",
                    HELPER_STAGING_PATH,
                    "install",
                ),
                stdin=password + "\n",
                timeout_seconds=120,
            )
        except SSHExecutionError as error:
            raise UpdateNodeError("node_unreachable", str(error)) from None
        finally:
            with contextlib.suppress(SSHExecutionError):
                self._executor.run_script(
                    node_id,
                    f"rm -f -- {shlex.quote(HELPER_STAGING_PATH)}\n",
                    timeout_seconds=30,
                )
        self._require_installed(node_id, result.returncode, result.stdout, self._sha256)

    def open_relay(self, node_ids: Sequence[str], relay: RelaySession) -> RelayTunnels:
        processes: list[subprocess.Popen[bytes]] = []
        try:
            for node_id in node_ids:
                processes.append(
                    subprocess.Popen(  # noqa: S603 - fixed argv from configuration
                        list(
                            self._executor.reverse_tunnel_command(
                                node_id, RELAY_REMOTE_PORT, relay.port
                            )
                        ),
                        stdin=subprocess.PIPE,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                )
            for node_id, process in zip(node_ids, processes, strict=True):
                self._await_relay(node_id, process, relay.token)
        except BaseException:
            _Tunnels(processes).close()
            raise
        return _Tunnels(processes)

    def _await_relay(
        self, node_id: str, process: subprocess.Popen[bytes], token: str
    ) -> None:
        """Prove the node's forwarded port reaches this relay session."""
        probe = (
            "curl -fsS --connect-timeout 3 --max-time 5 "
            f"--proxy http://127.0.0.1:{RELAY_REMOTE_PORT} "
            "http://dgx-spark-controller.invalid/token\n"
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise UpdateNodeError(
                    "relay_tunnel_failed",
                    f"The update relay tunnel to {node_id} closed. Port "
                    f"{RELAY_REMOTE_PORT} may be in use on the node.",
                )
            with contextlib.suppress(SSHExecutionError):
                result = self._executor.run_script(node_id, probe, timeout_seconds=15)
                if result.ok and result.stdout.strip() == token:
                    return
            time.sleep(1)
        raise UpdateNodeError(
            "relay_tunnel_failed",
            f"Node {node_id} could not reach the controller's update relay. "
            f"another program may hold port {RELAY_REMOTE_PORT} there.",
        )

    def _helper(
        self, node_id: str, subcommand: str, *, timeout_seconds: float
    ) -> _Execution:
        script = f"""set -u
helper={shlex.quote(HELPER_PATH)}
actual=$(sha256sum "$helper" 2>/dev/null | cut -d' ' -f1)
if [ "$actual" != {shlex.quote(self._sha256)} ]; then
  echo ERROR=helper-mismatch
  exit 3
fi
exec sudo -n "$helper" {shlex.quote(subcommand)}
"""
        try:
            result = self._executor.run_script(
                node_id, script, timeout_seconds=timeout_seconds
            )
        except SSHExecutionError as error:
            raise UpdateNodeError("node_unreachable", str(error)) from None
        return _Execution(result.returncode, result.stdout)

    @staticmethod
    def _require_installed(
        node_id: str, returncode: int, stdout: str, expected_sha256: str
    ) -> None:
        fields = parse_fields(stdout)
        if returncode == 0 and fields.get("INSTALLED") == "1":
            if fields.get("SHA256") != expected_sha256:
                # The staged copy changed between staging and install. The
                # hash gate keeps the controller from ever running it.
                raise UpdateNodeError(
                    "helper_mismatch",
                    f"The helper installed on {node_id} is not the packaged one.",
                )
            return
        if "ERROR" in fields:
            raise UpdateNodeError(
                _error_code(fields, "helper_install_failed"),
                f"The update helper on {node_id} reported {fields['ERROR'][:40]!r}.",
            )
        # sudo itself refused: a wrong password or a user without sudo rights.
        raise UpdateNodeError(
            "sudo_authentication_failed",
            f"sudo on {node_id} did not accept the password, or the node user "
            "may not use sudo.",
        )


@dataclass(frozen=True)
class _Execution:
    returncode: int
    stdout: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclass
class FakeUpdateNode:
    """One simulated node for :class:`FakeUpdateNodeOps`."""

    helper: str = "missing"
    boot_id: str = "00000000-0000-4000-8000-000000000001"
    packages: int = 12
    firmware: int = 1
    reboot_required: bool = False
    kernel: str = "6.17.0-1000-nvidia"
    driver: str = "580.0"
    run: RunStatus = field(default_factory=RunStatus)
    run_polls: int = 0
    rebooting_polls: int = 0
    unreachable: bool = False
    fail_step: dict[str, str] = field(default_factory=dict)


class FakeUpdateNodeOps:
    """Deterministic in-memory update simulation for fake mode and tests.

    A run finishes after ``polls_per_run`` status reads; a reboot changes the
    boot id at once and leaves the node unreachable for ``reboot_polls`` fact
    reads. Tests set ``nodes[...].fail_step[mode]`` to fail a run at that step
    and ``password`` to the accepted sudo password.
    """

    def __init__(
        self,
        node_ids: Sequence[str],
        *,
        helper: str = "missing",
        polls_per_run: int = 1,
        reboot_polls: int = 0,
        password: str | None = None,
    ) -> None:
        self.nodes = {node_id: FakeUpdateNode(helper=helper) for node_id in node_ids}
        self.polls_per_run = max(1, polls_per_run)
        self.reboot_polls = max(0, reboot_polls)
        self.password = password
        self.relay_ports: list[int] = []
        self.calls: list[tuple[str, str]] = []
        self.relay_open = False

    def _node(self, node_id: str) -> FakeUpdateNode:
        node = self.nodes[node_id]
        if node.unreachable:
            raise UpdateNodeError("node_unreachable", f"{node_id} is unreachable")
        return node

    def facts(self, node_id: str) -> NodeFacts:
        node = self.nodes[node_id]
        if node.rebooting_polls > 0:
            node.rebooting_polls -= 1
            return NodeFacts(node_id=node_id, reachable=False, message="rebooting")
        if node.unreachable:
            return NodeFacts(node_id=node_id, reachable=False, message="unreachable")
        return NodeFacts(
            node_id=node_id,
            reachable=True,
            helper=node.helper,
            boot_id=node.boot_id,
            system_state="running",
            user_manager="running",
            docker_ready=True,
            gpu_ready=True,
            kernel=node.kernel,
            driver=node.driver,
            os_version="Ubuntu 24.04.5 LTS",
            dgx_version="7.5.0",
            source_hosts={"ports.ubuntu.com": "http"},
            run=node.run if node.helper == "ready" else None,
        )

    def check(self, node_id: str) -> UpdateCheck:
        node = self._ready(node_id, "check")
        if not self.relay_open:
            raise UpdateNodeError("relay_unreachable", "relay is not open")
        return UpdateCheck(node.packages, node.firmware, node.reboot_required)

    def start_run(self, node_id: str, mode: str) -> RunStatus:
        if mode not in RUN_MODES:
            raise ValueError("invalid helper run mode")
        node = self._ready(node_id, mode)
        if not self.relay_open:
            raise UpdateNodeError("relay_unreachable", "relay is not open")
        node.run = RunStatus(state="running", mode=mode, step="apt-update")
        node.run_polls = 0
        return node.run

    def run_status(self, node_id: str) -> RunStatus:
        node = self._ready(node_id, "status")
        if node.run.state == "running":
            node.run_polls += 1
            if node.run_polls >= self.polls_per_run:
                failed = node.fail_step.get(node.run.mode, "")
                applied = node.run.mode == "apply" and not failed
                node.run = replace(
                    node.run,
                    state="failed" if failed else "succeeded",
                    step=failed or "complete",
                    failed_step=failed,
                    packages=node.packages,
                    firmware=node.firmware,
                    reboot_required=applied
                    and bool(node.packages or node.firmware or node.reboot_required),
                    log_tail=(
                        "E: Unable to fetch some archives, maybe run "
                        "apt-get update or try with --fix-missing?",
                        "E: simulated failure",
                    )
                    if failed
                    else (),
                )
                if applied:
                    node.packages = node.firmware = 0
                    node.reboot_required = False
        return node.run

    def reboot(self, node_id: str) -> str:
        node = self._ready(node_id, "reboot")
        previous = node.boot_id
        node.boot_id = _next_boot_id(previous)
        node.kernel = node.kernel.replace("1000", "1001")
        node.rebooting_polls = self.reboot_polls
        return previous

    def install_helper(self, node_id: str, password: str) -> None:
        require_sudo_password(password)
        node = self._node(node_id)
        self.calls.append((node_id, "install"))
        if self.password is not None and password != self.password:
            raise UpdateNodeError("sudo_authentication_failed", "wrong password")
        node.helper = "ready"

    def open_relay(self, node_ids: Sequence[str], relay: RelaySession) -> RelayTunnels:
        for node_id in node_ids:
            self._node(node_id)
        self.relay_ports.append(relay.port)
        self.relay_open = True
        return _FakeTunnels(self)

    def _ready(self, node_id: str, call: str) -> FakeUpdateNode:
        node = self._node(node_id)
        self.calls.append((node_id, call))
        if node.helper != "ready":
            raise UpdateNodeError("helper_mismatch", "helper not installed")
        return node


class _FakeTunnels:
    def __init__(self, ops: FakeUpdateNodeOps) -> None:
        self._ops = ops

    def close(self) -> None:
        self._ops.relay_open = False


def _next_boot_id(boot_id: str) -> str:
    return boot_id[:-12] + f"{(int(boot_id[-12:], 16) + 1) % 16**12:012x}"


@contextlib.contextmanager
def relay_tunnels(
    ops: UpdateNodeOps, node_ids: Sequence[str], relay: RelaySession
) -> Iterator[None]:
    """Hold reverse tunnels to the relay for the body's duration."""
    tunnels = ops.open_relay(node_ids, relay)
    try:
        yield
    finally:
        tunnels.close()
