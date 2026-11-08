# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Shared test helpers: polling, inline background work, local remote scripts."""

from __future__ import annotations

import subprocess
import time
import unittest
from collections.abc import Callable, Mapping
from concurrent.futures import Executor, Future
from pathlib import Path
from typing import Any

from controller.transport.ssh import SSHExecution, SSHExecutionError


def wait_until(
    test: unittest.TestCase,
    predicate: Callable[[], Any],
    *,
    timeout: float = 10.0,
    message: str = "condition not reached",
) -> Any:
    """Return the first truthy ``predicate()``; fail ``test`` after ``timeout``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    test.fail(message)


class InlineExecutor(Executor):
    """Run submitted work at once, so its future is done when returned."""

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        future: Future = Future()
        future.set_result(fn(*args, **kwargs))
        return future


class HeldExecutor(Executor):
    """Accept work and never run it, like a request queued behind others.

    A test finishes a held request by setting its future's result.
    """

    def __init__(self) -> None:
        self.futures: list[Future] = []

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        future: Future = Future()
        self.futures.append(future)
        return future


class BashExecutor:
    """Run each remote script locally with ``bash -s``, as SSH would."""

    def __init__(self, environment: Mapping[str, str]) -> None:
        self.environment = dict(environment)
        self.calls: list[tuple[str, str, float]] = []

    def run_script(
        self, node_id: str, script: str, *, timeout_seconds: float, check: bool = False
    ) -> SSHExecution:
        self.calls.append((node_id, script, timeout_seconds))
        completed = subprocess.run(
            ["bash", "-s"],
            input=script,
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if check and completed.returncode:
            raise SSHExecutionError(completed.stderr.strip() or "script failed")
        return SSHExecution(completed.returncode, completed.stdout, completed.stderr)


def install_stubs(bin_dir: Path, stubs: Mapping[str, str]) -> None:
    """Write executable Bash stubs that shadow node binaries on ``PATH``."""
    for name, body in stubs.items():
        path = bin_dir / name
        path.write_text("#!/usr/bin/env bash\n" + body + "\n")
        path.chmod(0o755)
