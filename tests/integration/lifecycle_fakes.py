# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Deployment builders, fault transports and run helpers for lifecycle tests."""

from __future__ import annotations

from dataclasses import replace

from controller.operations import DeploymentSpec, NodeAssignment, OperationRecord
from controller.orchestrator import OperationOrchestrator
from controller.transport.base import CommandResult, NodeObservation, UnitState
from controller.transport.fake import FakeNodeTransport

CANARY = {
    "enabled": True,
    "timeout_s": 120,
    "max_tokens": 16,
    "prompt": "Reply with OK.",
    "completion_path": "/v1/chat/completions",
}


def spec(
    deployment_id: str,
    alias: str,
    *assignments: NodeAssignment,
    required_memory: int = 8,
    minimum_free: int = 0,
    post_stop_free: int = 0,
    startup_timeout: float = 0,
    stop_timeout: float = 0,
    readiness_poll: float = 0,
    canary: dict | None = None,
) -> DeploymentSpec:
    profile: dict = {"required_memory_bytes": required_memory}
    if canary is not None:
        profile["completion_canary"] = canary
    return DeploymentSpec(
        deployment_id=deployment_id,
        profile_id=f"profile-{deployment_id}",
        expected_alias=alias,
        assignments=tuple(assignments),
        profile=profile,
        minimum_free_memory_bytes=minimum_free,
        post_stop_free_memory_bytes=post_stop_free,
        startup_timeout_seconds=startup_timeout,
        stop_timeout_seconds=stop_timeout,
        readiness_poll_seconds=readiness_poll,
    )


def execute_load(
    engine: OperationOrchestrator, deployment: DeploymentSpec, *, idempotency_key: str
) -> OperationRecord:
    """Persist a load, then run it unless the key replays earlier work."""
    operation, created = engine.submit_load(deployment, idempotency_key=idempotency_key)
    return engine.run(operation.id) if created else operation


def execute_unload(
    engine: OperationOrchestrator, deployment_id: str, *, idempotency_key: str
) -> OperationRecord:
    """Persist an unload, then run it unless the key replays earlier work."""
    operation, created = engine.submit_unload(
        deployment_id, idempotency_key=idempotency_key
    )
    return engine.run(operation.id) if created else operation


class FailingNewStart(FakeNodeTransport):
    """Refuses every start of deployment ``single``."""

    def start(self, node_id, spec, assignment):
        if spec.deployment_id == "single":
            self.calls.append(("start", node_id, spec.deployment_id))
            return CommandResult(False, "simulated start failure", "boom")
        return super().start(node_id, spec, assignment)


class ExitedContainer(FakeNodeTransport):
    """Reports deployment ``exited`` as a container that exited after launch."""

    def observe(self, node_id) -> NodeObservation:
        observed = super().observe(node_id)
        if observed.deployment_id == "exited":
            return replace(
                observed,
                unit_state=UnitState.STOPPED,
                managed=False,
                deployment_id=None,
                endpoint_healthy=False,
                active_alias=None,
                served_aliases=(),
                facts={"exited_deployment_id": "exited"},
            )
        return observed
