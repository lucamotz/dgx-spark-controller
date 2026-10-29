# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Low-level, bounded SSH execution for configured DGX Spark nodes.

This module is deliberately smaller than the lifecycle transport. It knows how
to execute a reviewed script over SSH, but it does not accept requests from the
HTTP layer and it does not decide which model may run. Concrete runners and
runtime adapters provide those allowlists.
"""

from __future__ import annotations

import os
import shlex
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

MAX_REMOTE_SCRIPT_BYTES = 64 * 1024


class SSHExecutionError(RuntimeError):
    """Raised when a configured SSH command cannot be executed successfully."""


@dataclass(frozen=True)
class SSHNode:
    """Connection facts for one allowlisted node."""

    id: str
    host: str
    user: str
    key_path: str
    connect_timeout_seconds: int = 10

    def __post_init__(self) -> None:
        if not self.id or not self.host or not self.user or not self.key_path:
            raise ValueError("SSH node id, host, user, and key_path are required")
        if self.connect_timeout_seconds <= 0:
            raise ValueError("SSH connect timeout must be positive")

    @property
    def destination(self) -> str:
        return f"{self.user}@{self.host}"

    @property
    def expanded_key_path(self) -> str:
        return os.path.expanduser(self.key_path)


@dataclass(frozen=True)
class SSHExecution:
    """Captured result from one bounded remote script."""

    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


class SSHExecutor:
    """Execute fixed scripts on nodes from an immutable registry."""

    def __init__(
        self, nodes: Mapping[str, SSHNode], *, extra_options: tuple[str, ...] = ()
    ) -> None:
        """``extra_options`` are fixed ``-o`` pairs chosen by the controller itself
        (for example the onboarding-pinned known_hosts file), never request data."""
        if (
            len(extra_options) % 2
            or any(flag != "-o" for flag in extra_options[0::2])
            or any(not value or value.startswith("-") for value in extra_options[1::2])
        ):
            raise ValueError("extra SSH options must be '-o' NAME=VALUE pairs")
        self._extra_options = tuple(extra_options)
        self._nodes = dict(nodes)
        if not self._nodes:
            raise ValueError("at least one SSH node must be configured")
        if any(key != node.id for key, node in self._nodes.items()):
            raise ValueError("SSH node registry keys must equal SSHNode.id")

    def node(self, node_id: str) -> SSHNode:
        try:
            return self._nodes[node_id]
        except KeyError as exc:
            raise SSHExecutionError(f"unknown configured node: {node_id}") from exc

    def command_for(
        self, node_id: str, remote_command: str = "bash -s"
    ) -> tuple[str, ...]:
        """Return the exact SSH argv used for a node, primarily for auditing/tests."""
        node = self.node(node_id)
        return (
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={node.connect_timeout_seconds}",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=2",
            *self._extra_options,
            "-i",
            node.expanded_key_path,
            node.destination,
            remote_command,
        )

    def reverse_tunnel_command(
        self, node_id: str, remote_port: int, local_port: int
    ) -> tuple[str, ...]:
        """SSH argv that forwards the node's loopback ``remote_port`` here.

        The forward binds 127.0.0.1 on the node only and the session exits if
        the node refuses it. Its remote command waits for stdin to close, so
        the tunnel ends with the controller process that holds the pipe.
        """
        if not (1024 <= remote_port <= 65535 and 1 <= local_port <= 65535):
            raise ValueError("tunnel ports are out of range")
        return (
            *self.command_for(node_id, "cat >/dev/null")[:-2],
            "-o",
            "ExitOnForwardFailure=yes",
            # A shared master connection would keep the forward alive.
            "-o",
            "ControlMaster=no",
            "-o",
            "ControlPath=none",
            "-R",
            f"127.0.0.1:{remote_port}:127.0.0.1:{local_port}",
            self.node(node_id).destination,
            "cat >/dev/null",
        )

    def run_script(
        self,
        node_id: str,
        script: str,
        *,
        timeout_seconds: float,
        check: bool = False,
    ) -> SSHExecution:
        """Send one reviewed script over stdin and capture its result."""
        if timeout_seconds <= 0:
            raise ValueError("SSH command timeout must be positive")
        if not script.strip():
            raise ValueError("remote script must not be empty")
        if len(script.encode("utf-8")) > MAX_REMOTE_SCRIPT_BYTES:
            raise ValueError("remote script exceeds the 64 KiB safety limit")

        node = self.node(node_id)
        if not Path(node.expanded_key_path).is_file():
            raise SSHExecutionError(
                f"configured SSH key does not exist for node {node_id}; check its "
                "local path"
            )
        try:
            completed = subprocess.run(
                list(self.command_for(node_id)),
                input=script,
                text=True,
                capture_output=True,
                timeout=timeout_seconds,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SSHExecutionError(
                f"SSH execution failed for node {node_id}: {exc}"
            ) from exc

        result = SSHExecution(completed.returncode, completed.stdout, completed.stderr)
        if check and not result.ok:
            detail = (
                result.stderr.strip() or result.stdout.strip() or "no remote detail"
            )
            raise SSHExecutionError(
                f"SSH command failed for node {node_id} with exit {result.returncode}: "
                f"{detail}"
            )
        return result

    def run_argv(
        self,
        node_id: str,
        argv: tuple[str, ...],
        *,
        stdin: str,
        timeout_seconds: float,
    ) -> SSHExecution:
        """Run one fixed remote argv with ``stdin`` and capture its result.

        The argv comes from controller constants and is shell-quoted for the
        remote login shell. ``stdin`` may hold a secret (the one-time sudo
        password of a helper install); it is passed on the pipe only and never
        appears in an argv, an exception or a log.
        """
        if timeout_seconds <= 0:
            raise ValueError("SSH command timeout must be positive")
        if not argv or any("\n" in part or "\0" in part for part in argv):
            raise ValueError("remote argv must be single-line words")
        node = self.node(node_id)
        if not Path(node.expanded_key_path).is_file():
            raise SSHExecutionError(
                f"configured SSH key does not exist for node {node_id}; check its "
                "local path"
            )
        try:
            completed = subprocess.run(
                list(self.command_for(node_id, shlex.join(argv))),
                input=stdin,
                text=True,
                capture_output=True,
                timeout=timeout_seconds,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SSHExecutionError(
                f"SSH execution failed for node {node_id}: {type(exc).__name__}"
            ) from None
        return SSHExecution(completed.returncode, completed.stdout, completed.stderr)
