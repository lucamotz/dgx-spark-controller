# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Explicit projections from reviewed configuration into orchestration records.

The HTTP application deals only in profile and node identifiers.  This module is
the one place where the immutable configuration dataclasses become the smaller,
durable :class:`~controller.operations.DeploymentSpec` consumed by the
orchestrator.  Keeping this projection separate makes it impossible for a
browser request to supply a script, a path, flags, or an SSH invocation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from typing import Any

from controller.domain import DeploymentMode, ModelProfile, NodeConfig
from controller.operations import DeploymentSpec, NodeAssignment

MIB = 1024 * 1024


class ProjectionError(ValueError):
    """A safe request/configuration mismatch found before an operation is queued."""

    def __init__(self, code: str, message: str, remediation: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = remediation


def validate_placement(profile: ModelProfile, nodes: Sequence[NodeConfig]) -> None:
    """Prove a requested placement satisfies the reviewed profile selector."""
    if len(nodes) != profile.node_count:
        raise ProjectionError(
            "invalid_placement",
            f"Profile {profile.profile_id!r} requires exactly "
            f"{profile.node_count} node(s).",
            "Select the reviewed number of compatible nodes for this profile.",
        )
    node_ids = [node.node_id for node in nodes]
    if len(node_ids) != len(set(node_ids)):
        raise ProjectionError(
            "invalid_placement",
            "A deployment cannot select the same node more than once.",
            "Select each configured node at most once.",
        )
    for node in nodes:
        selector = profile.node_selector
        if selector.allowed_node_ids and node.node_id not in selector.allowed_node_ids:
            raise ProjectionError(
                "node_not_allowed",
                f"Node {node.node_id!r} is not allowlisted by profile "
                f"{profile.profile_id!r}.",
                "Choose a node explicitly allowed by the reviewed profile.",
            )
        missing_labels = selector.required_labels.difference(node.labels)
        if missing_labels:
            raise ProjectionError(
                "node_incompatible",
                f"Node {node.node_id!r} is missing required labels: "
                f"{', '.join(sorted(missing_labels))}.",
                "Choose a node matching the reviewed profile labels.",
            )
        missing_capabilities = selector.required_capabilities.difference(
            node.capabilities
        )
        if missing_capabilities:
            raise ProjectionError(
                "node_incompatible",
                f"Node {node.node_id!r} is missing required capabilities: "
                f"{', '.join(sorted(missing_capabilities))}.",
                "Choose a node matching the reviewed profile capabilities.",
            )
        if (
            not node.endpoint_ports.start
            <= profile.endpoint.port
            <= node.endpoint_ports.end
        ):
            raise ProjectionError(
                "endpoint_not_allowed",
                f"Profile endpoint port {profile.endpoint.port} is not allowed on node "
                f"{node.node_id!r}.",
                "Review the node port range and profile together before retrying.",
            )


def project_deployment(
    profile: ModelProfile,
    nodes: Sequence[NodeConfig],
    *,
    deployment_id: str,
) -> DeploymentSpec:
    """Build a complete allowlisted deployment spec from reviewed input records."""
    validate_placement(profile, nodes)
    primary_node = next(
        (node for node in nodes if node.node_id == profile.runner.control_node_id),
        nodes[0],
    )
    endpoint = f"{profile.endpoint.scheme}://{primary_node.management_address}:{profile.endpoint.port}"
    minimum_before = profile.safety.minimum_free_memory_mib_before_start * MIB
    minimum_after = profile.safety.minimum_free_memory_mib_after_stop * MIB
    required_memory = (
        profile.artifact.size_bytes or profile.artifact.primary_size_bytes or 0
    )
    if profile.deployment_mode is DeploymentMode.SHARED_VLLM:
        required_memory = (
            required_memory + profile.node_count - 1
        ) // profile.node_count
    profile_mapping = _profile_mapping(profile, required_memory)
    return DeploymentSpec(
        deployment_id=deployment_id,
        profile_id=profile.profile_id,
        expected_alias=profile.api_alias,
        assignments=tuple(
            NodeAssignment(
                node.node_id,
                "head"
                if profile.deployment_mode is DeploymentMode.SHARED_VLLM
                and node.node_id == primary_node.node_id
                else "worker"
                if profile.deployment_mode is DeploymentMode.SHARED_VLLM
                else "single",
            )
            for node in nodes
        ),
        profile=profile_mapping,
        endpoint=endpoint,
        startup_timeout_seconds=float(profile.readiness.startup_timeout_s),
        stop_timeout_seconds=float(profile.safety.recovery_timeout_s),
        minimum_free_memory_bytes=minimum_before,
        post_stop_free_memory_bytes=minimum_after,
    )


def _profile_mapping(profile: ModelProfile, required_memory: int) -> Mapping[str, Any]:
    """Return only serialisable, reviewed execution facts needed by transports."""
    runner = asdict(profile.runner)
    # Enums are dataclass-safe but are normalised here for JSON durability and
    # future transports that do not import the domain enum types.
    runner["kind"] = profile.runner.kind.value
    runner["launch_mode"] = profile.runner.launch_mode.value
    runner["node_environment"] = {
        node_id: dict(overlay)
        for node_id, overlay in profile.runner.node_environment.items()
    }
    artifact = asdict(profile.artifact)
    artifact["format"] = profile.artifact.format.value
    runtime = asdict(profile.runtime)
    safety = asdict(profile.safety)
    safety["required_invariant_ids"] = sorted(profile.safety.required_invariant_ids)
    safety["maximum_swap_used_bytes"] = profile.safety.maximum_swap_used_mib * MIB
    canary = profile.readiness.completion_canary
    return {
        "profile_id": profile.profile_id,
        "revision": profile.revision,
        "origin": profile.origin.value,
        "backend": profile.backend.value,
        "deployment_mode": profile.deployment_mode.value,
        "runner": runner,
        "artifact": artifact,
        "runtime": runtime,
        "safety": safety,
        # Reviewed request values only; canary evidence never records the prompt
        # or the generated text.
        "completion_canary": {
            "enabled": canary.enabled,
            "timeout_s": canary.timeout_s,
            "max_tokens": canary.max_tokens,
            "prompt": canary.prompt,
            "completion_path": profile.endpoint.completion_path,
        },
        "required_memory_bytes": required_memory,
        "script_valid": True,
        "model_present": True,
        "cluster_lock": True,
        "cluster_id": "default",
    }
