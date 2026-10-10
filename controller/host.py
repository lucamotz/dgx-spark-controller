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
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, NoReturn

from controller.app import ControllerApplication
from controller.config import ConfigError, load_nodes, load_profiles
from controller.discovery.setup import (
    DiscoveryBackend,
    FakeDiscoveryBackend,
    RealDiscoveryBackend,
    SetupService,
)
from controller.domain import ModelProfile, NodeConfig
from controller.paths import AppPaths
from controller.persistence import OperationJournal
from controller.recipes.catalog_client import CatalogClient
from controller.recipes.nodeops import FakeRecipeNodeOps, RecipeNodeOps
from controller.recipes.service import RecipeService
from controller.recipes.store import RecipeStore
from controller.transport.base import NodeTransport

logger = logging.getLogger(__name__)

TransportFactory = Callable[
    [tuple[NodeConfig, ...], tuple[ModelProfile, ...], AppPaths],
    tuple[NodeTransport, RecipeNodeOps],
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
        self._build(start_reconciliation=start_reconciliation)
        self.recipes.recover_interrupted()

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
            previous, self._app, self._node_ops = self._app, None, None
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
            transport, node_ops = self._transport_factory(nodes, profiles, self.paths)
            app = ControllerApplication(
                nodes=nodes,
                profiles=profiles,
                journal=self.journal,
                transport=transport,
                start_reconciliation=start_reconciliation,
                live_operations=self.recipes.live_operation_ids,
                owns_journal=False,
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
) -> tuple[NodeTransport, RecipeNodeOps]:
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
    return transport, FakeRecipeNodeOps([node.node_id for node in nodes])


def live_transport_factory(
    nodes: tuple[NodeConfig, ...],
    profiles: tuple[ModelProfile, ...],
    paths: AppPaths,
) -> tuple[NodeTransport, RecipeNodeOps]:
    from controller.discovery.hostkeys import ssh_known_hosts_options
    from controller.main import build_live_transport
    from controller.recipes.nodeops import SSHRecipeNodeOps

    known_hosts = ssh_known_hosts_options(paths.known_hosts_file)
    transport, executor = build_live_transport(
        nodes, profiles, extra_ssh_options=known_hosts
    )
    return transport, SSHRecipeNodeOps(executor)
