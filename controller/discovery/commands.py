# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""The one way the onboarding wizard runs a local command.

Every discovery command is a fixed argv list built from validated values and
is never passed through a shell. It runs with a timeout, captures text output,
and never raises for a non-zero exit, so each caller maps exit codes and
failures to its own safe error. A command without ``input`` gets
``stdin=DEVNULL``, so a tool that would prompt (for example ``ssh-keygen``
before overwriting a file) reads EOF instead of waiting on a terminal.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping, Sequence
from typing import Any

Runner = Callable[..., Any]
"""``subprocess.run`` compatible callable, injectable for tests."""


def run_captured(
    runner: Runner,
    argv: Sequence[str],
    *,
    timeout: float,
    input: str | None = None,
    env: Mapping[str, str] | None = None,
) -> Any:
    """Run ``argv`` with captured text output and return the completed process.

    Args:
        runner: ``subprocess.run`` or a test double with the same signature.
        argv: Program and arguments; never interpreted by a shell.
        timeout: Seconds before ``subprocess.TimeoutExpired`` is raised.
        input: Text written to stdin; without it stdin is ``DEVNULL``.
        env: Complete child environment; the parent's when omitted.

    Returns:
        The runner's result, normally a ``subprocess.CompletedProcess``.

    """
    options: dict[str, Any] = {
        "capture_output": True,
        "text": True,
        "timeout": timeout,
        "check": False,
    }
    if input is None:
        options["stdin"] = subprocess.DEVNULL
    else:
        options["input"] = input
    if env is not None:
        options["env"] = env
    return runner(list(argv), **options)
