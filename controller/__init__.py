# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Public contracts for DGXSparkController."""

from .domain import (
    Backend,
    DeploymentMode,
    ErrorCode,
    ModelProfile,
    NodeConfig,
)
from .operations import (
    DeploymentSpec,
    NodeAssignment,
    OperationAction,
    OperationPhase,
    OperationState,
    ensure_state_transition,
)
from .orchestrator import OperationOrchestrator
from .persistence import OperationJournal
from .reconcile import Reconciler

__all__ = [
    "Backend",
    "DeploymentMode",
    "DeploymentSpec",
    "ErrorCode",
    "ModelProfile",
    "NodeAssignment",
    "NodeConfig",
    "OperationAction",
    "OperationJournal",
    "OperationOrchestrator",
    "OperationPhase",
    "OperationState",
    "Reconciler",
    "ensure_state_transition",
]
