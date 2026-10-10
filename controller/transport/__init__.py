# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Node transport interfaces and deterministic test implementation."""

from .base import (
    CanaryRequest,
    CanaryResult,
    CommandResult,
    LogChunk,
    NodeObservation,
    NodeTransport,
    PreflightCheck,
    UnitState,
)
from .fake import FakeNode, FakeNodeTransport

__all__ = [
    "CanaryRequest",
    "CanaryResult",
    "CommandResult",
    "FakeNode",
    "FakeNodeTransport",
    "LogChunk",
    "NodeObservation",
    "NodeTransport",
    "PreflightCheck",
    "UnitState",
]
