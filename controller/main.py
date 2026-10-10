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

from controller.domain import ModelProfile, NodeConfig
from controller.host import ServiceHost
from controller.paths import resolve_app_paths
from controller.runners.systemd_user import ScriptUnitRecipe, SystemdUserRunner
from controller.server import create_server
from controller.transport.managed_ssh import (
    ClusterScriptRecipe,
    ManagedSSHNode,
    ManagedSSHTransport,
)
from controller.transport.ssh import SSHExecutor, SSHNode


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


def build_live_transport(
    nodes: Sequence[NodeConfig],
    profiles: Sequence[ModelProfile],
    *,
    extra_ssh_options: tuple[str, ...] = (),
) -> tuple[ManagedSSHTransport, SSHExecutor]:
    """Create the fixed SSH/systemd transport from reviewed configuration only."""
    endpoint_by_node: dict[str, tuple[int, str, str]] = {}
    for node in nodes:
        matching = [
            profile for profile in profiles if _profile_matches_node(profile, node)
        ]
        independent_endpoints = {
            (
                profile.endpoint.port,
                profile.endpoint.health_path,
                profile.endpoint.models_path,
            )
            for profile in matching
            if profile.deployment_mode.value == "independent"
        }
        if len(independent_endpoints) > 1:
            raise ValueError(
                "a node may have only one independent observed endpoint contract; "
                f"{node.node_id!r} has multiple"
            )
        if independent_endpoints:
            endpoint_by_node[node.node_id] = next(iter(independent_endpoints))
        else:
            ports = node.endpoint_ports
            observation_port = (
                11434 if ports.start <= 11434 <= ports.end else ports.start
            )
            endpoint_by_node[node.node_id] = (observation_port, "/health", "/v1/models")
    recipes: dict[str, ScriptUnitRecipe] = {}
    for profile in profiles:
        if profile.deployment_mode.value != "independent":
            continue
        expected_hash = profile.runner.expected_sha256
        if not expected_hash:
            raise ValueError(
                "live systemd runner requires a pinned script SHA-256 for "
                f"{profile.profile_id!r}"
            )
        recipe = ScriptUnitRecipe(
            id=profile.runner.script_id,
            script_path=profile.runner.script_path,
            sha256=expected_hash.lower(),
            timeout_stop_seconds=profile.safety.recovery_timeout_s,
        )
        existing = recipes.get(recipe.id)
        if existing is not None and existing != recipe:
            raise ValueError(
                f"allowlisted script id {recipe.id!r} maps to conflicting recipes"
            )
        recipes[recipe.id] = recipe
    executor = SSHExecutor(
        {
            node.node_id: SSHNode(
                id=node.node_id,
                host=node.ssh.address,
                user=node.ssh.user,
                key_path=node.ssh.key_path,
                connect_timeout_seconds=node.ssh.connect_timeout_s,
            )
            for node in nodes
        },
        extra_options=extra_ssh_options,
    )
    runner = SystemdUserRunner(executor, recipes)
    managed_nodes = {
        node.node_id: ManagedSSHNode(
            id=node.node_id,
            management_address=node.management_address,
            endpoint_port=endpoint_by_node[node.node_id][0],
            health_path=endpoint_by_node[node.node_id][1],
            models_path=endpoint_by_node[node.node_id][2],
            probe_port_range=(node.endpoint_ports.start, node.endpoint_ports.end),
        )
        for node in nodes
    }
    cluster_recipes = [
        cluster
        for profile in profiles
        if (cluster := _cluster_recipe(profile, nodes)) is not None
    ]
    transport = ManagedSSHTransport(executor, runner, managed_nodes, cluster_recipes)
    return transport, executor


def _cluster_recipe(
    profile: ModelProfile, nodes: Sequence[NodeConfig]
) -> ClusterScriptRecipe | None:
    if profile.deployment_mode.value != "shared-vllm":
        return None
    selected_node_ids = tuple(
        node.node_id
        for node in nodes
        if node.node_id in profile.node_selector.allowed_node_ids
    )
    if len(selected_node_ids) != profile.node_count:
        return None
    runner = profile.runner
    return ClusterScriptRecipe(
        profile_id=profile.profile_id,
        deployment_id="dep-" + profile.profile_id,
        node_ids=selected_node_ids,
        control_node_id=runner.control_node_id or "",
        start_script_path=runner.script_path,
        stop_script_path=runner.stop_script_path or "",
        start_sha256=runner.expected_sha256 or "",
        stop_sha256=runner.expected_stop_sha256 or "",
        working_directory=runner.working_directory or "",
        environment_path=runner.environment_path or "",
        required_environment=runner.required_environment,
        expected_recipe_commit=runner.expected_recipe_commit or "",
        image_digest=runner.image_digest or "",
        container_name=runner.container_name or "",
        endpoint_port=profile.endpoint.port,
        health_path=profile.endpoint.health_path,
        models_path=profile.endpoint.models_path,
        expected_alias=profile.api_alias,
        required_command_fragments=profile.readiness.process.required_command_fragments,
        launch_mode=runner.launch_mode.value,
        node_environment=runner.node_environment,
    )


def _profile_matches_node(profile: ModelProfile, node: NodeConfig) -> bool:
    selector = profile.node_selector
    return (
        (not selector.allowed_node_ids or node.node_id in selector.allowed_node_ids)
        and selector.required_labels.issubset(node.labels)
        and selector.required_capabilities.issubset(node.capabilities)
        and node.endpoint_ports.start
        <= profile.endpoint.port
        <= node.endpoint_ports.end
    )


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
