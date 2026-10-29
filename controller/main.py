# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Command-line entry point for the loopback-only DGX Spark Controller."""

from __future__ import annotations

import argparse
import os
import signal
import threading
import webbrowser
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from controller.host import ServiceHost
from controller.paths import resolve_app_paths
from controller.server import create_server


def start_parent_watchdog(
    server: Any,
    shutdown_started: threading.Event,
    *,
    enabled: bool | None = None,
    getppid: Callable[[], int] = os.getppid,
    interval_seconds: float = 1.0,
) -> threading.Thread | None:
    """Stop only the local service if its desktop-launcher parent disappears.

    The launcher opts in through ``DGX_CONTROLLER_EXIT_WITH_PARENT=1``. Remote
    inference is deliberately untouched; shutdown only ends the loopback HTTP
    service and its local workers.
    """
    requested = (
        os.environ.get("DGX_CONTROLLER_EXIT_WITH_PARENT") == "1"
        if enabled is None
        else enabled
    )
    if not requested:
        return None
    if interval_seconds <= 0:
        raise ValueError("parent watchdog interval must be positive")
    expected_parent = getppid()
    if expected_parent <= 1:
        return None

    def watch() -> None:
        while not shutdown_started.wait(interval_seconds):
            if getppid() == expected_parent:
                continue
            if not shutdown_started.is_set():
                shutdown_started.set()
                # A dedicated thread, so BaseServer.shutdown cannot deadlock
                # the serve_forever thread.
                server.shutdown()
            return

    thread = threading.Thread(
        target=watch, name="dgx-controller-parent-watchdog", daemon=True
    )
    thread.start()
    return thread


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    paths = resolve_app_paths()
    parser = argparse.ArgumentParser(
        prog="dgx-spark-controller",
        description="Run the local DGX Spark Controller API and browser UI.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="loopback bind address (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=18876,
        help="loopback TCP port (default: %(default)s)",
    )
    parser.add_argument(
        "--mode",
        choices=("live", "fake"),
        default="live",
        help="transport mode (default: %(default)s)",
    )
    parser.add_argument(
        "--nodes",
        type=Path,
        default=paths.nodes_file,
        help="nodes JSON written by the setup wizard (default: %(default)s)",
    )
    parser.add_argument(
        "--profiles",
        type=Path,
        action="append",
        help="directory of reviewed profile JSON files; repeatable "
        f"(default: {paths.profiles_dir} and {paths.generated_profiles_dir})",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=paths.database_path,
        help="local SQLite journal path (default: %(default)s)",
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="open the UI in the default browser once the server is listening",
    )
    parser.add_argument(
        "--no-startup-reconcile",
        action="store_true",
        help="skip the read-only startup reconciliation pass (diagnostics only)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    paths = resolve_app_paths().ensure()
    host = ServiceHost(
        paths=paths,
        mode=args.mode,
        nodes_path=args.nodes,
        profile_dirs=tuple(args.profiles) if args.profiles else None,
        database_path=args.database,
        start_reconciliation=not args.no_startup_reconcile,
    )
    try:
        server = create_server(host, host=args.host, port=args.port)
    except Exception:
        host.close()
        raise
    shutdown_started = threading.Event()

    def request_shutdown(_signum: object, _frame: object) -> None:
        if shutdown_started.is_set():
            return
        shutdown_started.set()
        threading.Thread(
            target=server.shutdown, name="dgx-controller-shutdown", daemon=True
        ).start()

    previous_handlers = {}
    for name in ("SIGINT", "SIGTERM"):
        signal_number = getattr(signal, name, None)
        if signal_number is not None:
            previous_handlers[signal_number] = signal.signal(
                signal_number, request_shutdown
            )
    start_parent_watchdog(server, shutdown_started)
    if args.open_browser:
        webbrowser.open(f"http://{args.host}:{server.server_address[1]}/")
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        request_shutdown(None, None)
    finally:
        for signal_number, handler in previous_handlers.items():
            signal.signal(signal_number, handler)
        server.server_close()
        # Local shutdown never unloads a remote model; interrupted lifecycle
        # work is resolved by startup reconciliation on the next run.
        host.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
