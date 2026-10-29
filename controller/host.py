# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Process-level owner of the controller services.

The host lets the controller start before any node is configured (setup mode),
rebuilds the lifecycle application when onboarding saves ``nodes.json`` or a
recipe registers a generated profile, and keeps the onboarding wizard and the
recipe builder alive across those rebuilds. Rebuilds happen only while no
load, unload, recover, or reconcile is queued or running, and never run the
startup reconciliation again, so an in-flight recipe prepare is never mistaken
for interrupted work. After a restart the recipe service, not reconciliation,
reattaches to or settles recipe operations.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple, NoReturn

from controller.app import ControllerApplication
from controller.config import ConfigError, load_nodes, load_profiles
from controller.discovery.hostkeys import ssh_known_hosts_options
from controller.discovery.setup import (
    DiscoveryBackend,
    FakeDiscoveryBackend,
    RealDiscoveryBackend,
    SetupService,
)
from controller.domain import ModelProfile, NodeConfig, profile_matches_node
from controller.paths import AppPaths
from controller.persistence import OperationJournal
from controller.recipes.catalog_client import CatalogClient
from controller.recipes.nodeops import (
    FakeRecipeNodeOps,
    RecipeNodeOps,
    SSHRecipeNodeOps,
)
from controller.recipes.service import RecipeService
from controller.recipes.store import RecipeStore
from controller.runners.systemd_user import ScriptUnitRecipe, SystemdUserRunner
from controller.transport.base import NodeTransport
from controller.transport.managed_ssh import (
    ClusterScriptRecipe,
    ManagedSSHNode,
    ManagedSSHTransport,
)
from controller.transport.ssh import SSHExecutor, SSHNode
from controller.updates.nodeops import (
    FakeUpdateNodeOps,
    SSHUpdateNodeOps,
    UpdateNodeOps,
)
from controller.updates.relay import UpdateRelay
from controller.updates.service import UpdateService
from controller.updates.settings import SettingsStore

logger = logging.getLogger(__name__)

TransportFactory = Callable[
    [tuple[NodeConfig, ...], tuple[ModelProfile, ...], AppPaths],
    tuple[NodeTransport, RecipeNodeOps, UpdateNodeOps],
]


class ServiceHost:
    """Owns the journal, setup wizard, recipe builder, and current application."""

    def __init__(
        self,
        *,
        paths: AppPaths,
        mode: str,
        nodes_path: Path | None = None,
        profile_dirs: tuple[Path, ...] | None = None,
        database_path: Path | None = None,
        start_reconciliation: bool = True,
        transport_factory: TransportFactory | None = None,
        discovery_backend: DiscoveryBackend | None = None,
        catalog: CatalogClient | None = None,
        recipe_poll_interval_seconds: float = 5.0,
        journal: OperationJournal | None = None,
    ) -> None:
        if mode not in {"live", "fake"}:
            raise ValueError("mode must be either 'live' or 'fake'")
        self.paths = paths
        self.mode = mode
        self.nodes_path = nodes_path or paths.nodes_file
        self.profile_dirs = profile_dirs or (
            paths.profiles_dir,
            paths.generated_profiles_dir,
        )
        self._transport_factory = transport_factory or (
            fake_transport_factory if mode == "fake" else live_transport_factory
        )
        self.journal = journal or OperationJournal(
            str(database_path or paths.database_path)
        )
        self._mutex = threading.RLock()
        self._app: ControllerApplication | None = None
        self._node_ops: RecipeNodeOps | None = None
        self._update_ops: UpdateNodeOps | None = None
        self._config_error: dict[str, str] | None = None
        self._reload_pending = False
        self._closed = False
        self.setup = SetupService(
            paths=paths,
            backend=discovery_backend
            or (FakeDiscoveryBackend() if mode == "fake" else RealDiscoveryBackend()),
            on_nodes_saved=self.request_reload,
        )
        self.recipes = RecipeService(
            store=RecipeStore(paths.recipe_drafts_dir),
            journal=self.journal,
            catalog=catalog or CatalogClient(),
            nodes_provider=self.current_nodes,
            node_ops_provider=self.current_node_ops,
            generated_profiles_dir=paths.generated_profiles_dir,
            profiles_provider=self.current_profiles,
            on_profile_generated=lambda _profile_id: self.request_reload(),
            on_profiles_removed=lambda _profile_ids: self.request_reload(),
            poll_interval_seconds=recipe_poll_interval_seconds,
        )
        self.update_relay = UpdateRelay(paths.data_dir / "update-cache")
        self.updates = UpdateService(
            store=SettingsStore(paths.data_dir / "update-settings.json"),
            relay=self.update_relay,
            app=lambda: self.app,
            node_ops=self.current_update_ops,
        )
        self._build(start_reconciliation=start_reconciliation)
        self.recipes.recover_interrupted()
        self.updates.start()

    @classmethod
    def wrap(
        cls,
        application: ControllerApplication,
        *,
        paths: AppPaths,
        catalog: CatalogClient | None = None,
    ) -> ServiceHost:
        """Serve an already-built application (tests and embedding).

        The wrapped application is never rebuilt from configuration files.
        """
        host = cls(
            paths=paths,
            mode="fake",
            nodes_path=paths.config_dir / "wrapped-nodes-absent.json",
            journal=application.journal,
            catalog=catalog,
            transport_factory=_refuse_rebuild,
        )
        host._app = application
        return host

    @property
    def app(self) -> ControllerApplication | None:
        self.maybe_reload()
        with self._mutex:
            return self._app

    def current_nodes(self) -> Mapping[str, NodeConfig]:
        with self._mutex:
            return dict(self._app.nodes) if self._app is not None else {}

    def current_profiles(self) -> Mapping[str, ModelProfile]:
        with self._mutex:
            return dict(self._app.profiles) if self._app is not None else {}

    def current_node_ops(self) -> RecipeNodeOps | None:
        with self._mutex:
            return self._node_ops

    def current_update_ops(self) -> UpdateNodeOps | None:
        with self._mutex:
            return self._update_ops

    def status(self) -> dict[str, Any]:
        with self._mutex:
            configured = self._app is not None
            return {
                "mode": self.mode,
                "configured": configured,
                "setup_required": not configured,
                "reload_pending": self._reload_pending,
                "config_error": self._config_error,
                "nodes_path": str(self.nodes_path),
            }

    def request_reload(self) -> None:
        """Rebuild now if the lifecycle worker is idle, otherwise when it is."""
        with self._mutex:
            self._reload_pending = True
        self.maybe_reload()

    def maybe_reload(self) -> None:
        with self._mutex:
            if not self._reload_pending or self._closed:
                return
            if self._app is not None and self._app.has_active_lifecycle_operation():
                return
            self._reload_pending = False
            previous, self._app = self._app, None
            self._node_ops = self._update_ops = None
        if previous is not None:
            previous.close(wait=True)
        self._build(start_reconciliation=False)

    def close(self) -> None:
        """Stop local workers; remote deployments are intentionally left running."""
        with self._mutex:
            if self._closed:
                return
            self._closed = True
            app, self._app = self._app, None
        self.recipes.close(wait=False)
        self.updates.close()
        if app is not None:
            app.close(wait=True)
        self.journal.close()

    def _build(self, *, start_reconciliation: bool) -> None:
        if not self.nodes_path.exists():
            with self._mutex:
                self._config_error = None
            return
        try:
            nodes = load_nodes(self.nodes_path)
            profiles = load_profiles(*self.profile_dirs, allow_empty=True)
            transport, node_ops, update_ops = self._transport_factory(
                nodes, profiles, self.paths
            )
            app = ControllerApplication(
                nodes=nodes,
                profiles=profiles,
                journal=self.journal,
                transport=transport,
                start_reconciliation=start_reconciliation,
                live_operations=self.recipes.live_operation_ids,
                owns_journal=False,
                update_ops=update_ops,
                update_relay=self.update_relay,
                update_settings=self.updates.settings,
            )
        except (ConfigError, ValueError) as error:
            logger.warning("controller configuration is invalid: %s", error)
            with self._mutex:
                self._config_error = {
                    "code": getattr(
                        getattr(error, "code", None), "value", "config_invalid"
                    ),
                    "message": str(error),
                }
            return
        with self._mutex:
            self._app, self._node_ops, self._config_error = app, node_ops, None
            self._update_ops = update_ops
        if not start_reconciliation:
            # A rebuild skips reconciliation; refresh the read-only view instead.
            threading.Thread(
                target=app.refresh_observations,
                name="dgx-controller-refresh",
                daemon=True,
            ).start()


def _refuse_rebuild(*_args: object) -> NoReturn:
    raise RuntimeError("wrapped host cannot rebuild")


def fake_transport_factory(
    nodes: tuple[NodeConfig, ...],
    profiles: tuple[ModelProfile, ...],
    paths: AppPaths,
) -> tuple[NodeTransport, RecipeNodeOps, UpdateNodeOps]:
    from controller.transport.fake import FakeNode, FakeNodeTransport

    del profiles, paths
    transport = FakeNodeTransport(
        {
            node.node_id: FakeNode(
                node.node_id, baseline_free_memory_bytes=128 * 1024 * 1024 * 1024
            )
            for node in nodes
        }
    )
    node_ids = [node.node_id for node in nodes]
    return transport, FakeRecipeNodeOps(node_ids), FakeUpdateNodeOps(node_ids)


def live_transport_factory(
    nodes: tuple[NodeConfig, ...],
    profiles: tuple[ModelProfile, ...],
    paths: AppPaths,
) -> tuple[NodeTransport, RecipeNodeOps, UpdateNodeOps]:
    known_hosts = ssh_known_hosts_options(paths.known_hosts_file)
    transport, executor = build_live_transport(
        nodes, profiles, extra_ssh_options=known_hosts
    )
    update_ops = SSHUpdateNodeOps(
        executor, {node.node_id: node.cluster_addresses for node in nodes}
    )
    return transport, SSHRecipeNodeOps(executor), update_ops


def build_live_transport(
    nodes: Sequence[NodeConfig],
    profiles: Sequence[ModelProfile],
    *,
    extra_ssh_options: tuple[str, ...] = (),
) -> tuple[ManagedSSHTransport, SSHExecutor]:
    """Create the fixed SSH/systemd transport from reviewed configuration only."""
    endpoints = {node.node_id: _observed_endpoint(node, profiles) for node in nodes}
    recipes = _script_unit_recipes(profiles)
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
            endpoint_port=endpoints[node.node_id].port,
            health_path=endpoints[node.node_id].health_path,
            models_path=endpoints[node.node_id].models_path,
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


class _ObservedEndpoint(NamedTuple):
    port: int
    health_path: str
    models_path: str


def _observed_endpoint(
    node: NodeConfig, profiles: Sequence[ModelProfile]
) -> _ObservedEndpoint:
    """The endpoint observed on ``node``: its independent profile's, if any.

    Without one, the node is observed on port 11434 when its range allows it
    and otherwise on the first port of its range.
    """
    independent = {
        _ObservedEndpoint(
            profile.endpoint.port,
            profile.endpoint.health_path,
            profile.endpoint.models_path,
        )
        for profile in profiles
        if profile_matches_node(profile, node)
        and profile.deployment_mode.value == "independent"
    }
    if len(independent) > 1:
        raise ValueError(
            "a node may have only one independent observed endpoint contract; "
            f"{node.node_id!r} has multiple"
        )
    if independent:
        return next(iter(independent))
    ports = node.endpoint_ports
    port = 11434 if ports.start <= 11434 <= ports.end else ports.start
    return _ObservedEndpoint(port, "/health", "/v1/models")


def _script_unit_recipes(
    profiles: Sequence[ModelProfile],
) -> dict[str, ScriptUnitRecipe]:
    """Pinned systemd unit recipes for the independent (single-node) profiles."""
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
    return recipes


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
