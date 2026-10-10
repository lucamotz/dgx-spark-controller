# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Validated node and cluster-recipe settings of the managed SSH transport.

Both records are built from reviewed configuration at startup; every value
that reaches a remote script is validated here first.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

DEPLOYMENT_ID_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,52}\Z")
PROJECT_NAME_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}\Z")
HEAD_SCRIPT_LAUNCH = "head-script"
PER_NODE_LAUNCH = "per-node"


@dataclass(frozen=True)
class ClusterScriptRecipe:
    """Pinned Docker recipe of one shared vLLM placement.

    A head-script recipe's launcher on the control node starts every rank; a
    per-node recipe has the controller start each rank on its own node.
    """

    profile_id: str
    deployment_id: str
    node_ids: tuple[str, ...]
    control_node_id: str
    start_script_path: str
    stop_script_path: str
    start_sha256: str
    stop_sha256: str
    working_directory: str
    environment_path: str
    required_environment: Mapping[str, str]
    expected_recipe_commit: str
    image_digest: str
    container_name: str
    endpoint_port: int
    health_path: str
    models_path: str
    expected_alias: str
    required_command_fragments: tuple[str, ...]
    launch_mode: str = HEAD_SCRIPT_LAUNCH
    node_environment: Mapping[str, Mapping[str, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not DEPLOYMENT_ID_RE.fullmatch(self.deployment_id):
            raise ValueError("invalid fixed cluster deployment id")
        if self.launch_mode not in {HEAD_SCRIPT_LAUNCH, PER_NODE_LAUNCH}:
            raise ValueError("unsupported cluster launch mode")
        minimum_nodes = 1 if self.per_node else 2
        if (
            self.control_node_id not in self.node_ids
            or len(self.node_ids) < minimum_nodes
        ):
            raise ValueError(
                "cluster recipe requires its control node and enough nodes "
                "for its launch mode"
            )
        if self.per_node and set(self.node_environment) != set(self.node_ids):
            raise ValueError("per-node recipes need one environment overlay per node")
        if not PROJECT_NAME_RE.fullmatch(self.container_name):
            raise ValueError("invalid cluster container name")

    @property
    def per_node(self) -> bool:
        """Whether the controller starts every rank itself (generated recipes)."""
        return self.launch_mode == PER_NODE_LAUNCH

    def environment_for(self, node_id: str) -> dict[str, str]:
        """Profile-wide values plus this node's rank overlay, as rendered remotely."""
        values = dict(self.required_environment)
        values.update(self.node_environment.get(node_id, {}))
        return values

    @property
    def marker_name(self) -> str:
        return f"{self.deployment_id}.owner"

    @property
    def managed_environment_path(self) -> str:
        """Private recipe environment rendered from the reviewed profile."""
        return f"{self.working_directory}/.env.dgx-controller-{self.profile_id}"

    @property
    def launcher_unit_name(self) -> str:
        """Stable remote unit that owns one in-flight cluster launcher."""
        return f"dgx-cluster-{self.deployment_id}.service"


@dataclass(frozen=True)
class ManagedSSHNode:
    """Observed-service settings for one configured Spark."""

    id: str
    management_address: str
    endpoint_port: int = 11434
    health_path: str = "/health"
    models_path: str = "/v1/models"
    legacy_process_pattern: str = "build/bin/llama-server -m"
    # Listening ports in this inclusive range are asked for /v1/models so an
    # inference server the controller did not start is named, not just counted.
    probe_port_range: tuple[int, int] | None = None

    def __post_init__(self) -> None:
        if not self.id or not self.management_address:
            raise ValueError("managed node id and management address are required")
        if not 1 <= self.endpoint_port <= 65535:
            raise ValueError("endpoint port must be between 1 and 65535")
        if not self.health_path.startswith("/") or not self.models_path.startswith("/"):
            raise ValueError("endpoint paths must be absolute URL paths")
        if not self.legacy_process_pattern.strip():
            raise ValueError("legacy process pattern must not be empty")
        if self.probe_port_range is not None:
            start, end = self.probe_port_range
            if not 1 <= start <= end <= 65535:
                raise ValueError("probe port range must be ordered within 1-65535")

    @property
    def endpoint_root(self) -> str:
        return f"http://{self.management_address}:{self.endpoint_port}"


def required_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    """Return one object of a projected profile, failing closed if absent."""
    result = value.get(key)
    if not isinstance(result, Mapping):
        raise ValueError(f"deployment profile is missing object {key!r}")
    return result


def required_string(value: Mapping[str, Any], key: str) -> str:
    """Return one non-empty string of a projected profile, failing closed."""
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ValueError(f"deployment profile is missing string {key!r}")
    return result
