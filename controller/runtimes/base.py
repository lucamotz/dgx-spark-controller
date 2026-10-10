# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Runtime adapter contract for allowlisted model execution recipes."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from controller.operations import DeploymentSpec, NodeAssignment
from controller.transport.base import CommandResult, PreflightCheck


@dataclass(frozen=True)
class RuntimeRecipe:
    """A named, reviewed runtime recipe; never an arbitrary shell command."""

    recipe_id: str
    runtime_name: str
    profile_id: str


class RuntimeAdapter(Protocol):
    """Profile-specific recipe adapter used below the orchestration boundary."""

    def preflight(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> Sequence[PreflightCheck]:
        """Validate script/runtime/model invariants without changing node state."""

    def start(
        self, node_id: str, spec: DeploymentSpec, assignment: NodeAssignment
    ) -> CommandResult:
        """Start an allowlisted recipe through a runner-owned unit."""

    def stop(
        self,
        node_id: str,
        deployment_id: str,
        *,
        timeout_seconds: float,
        force: bool = False,
    ) -> CommandResult:
        """Stop exactly the recipe's managed unit for this deployment."""
