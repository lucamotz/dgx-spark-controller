# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Pinned journal traces of the lifecycle flows against fake nodes.

Event codes, phases, messages, results and the order of transport calls are an
operator-visible contract. A deliberate change updates the fixture in the same
commit; regenerate it with::

    DGX_UPDATE_TRACE_GOLDENS=1 python -m pytest \\
        tests/integration/test_operation_traces.py
"""

from __future__ import annotations

import json
import os
import re
import unittest
from collections.abc import Callable
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

from lifecycle_fakes import (
    CANARY,
    ExitedContainer,
    FailingNewStart,
    execute_load,
    execute_unload,
)
from lifecycle_fakes import spec as lifecycle_spec

from controller.operations import (
    DeploymentSpec,
    NodeAssignment,
    OperationAction,
    OperationRecord,
)
from controller.orchestrator import OperationOrchestrator
from controller.persistence import OperationJournal
from controller.reconcile import Reconciler
from controller.transport.base import NodeObservation, UnitState
from controller.transport.fake import FakeNode, FakeNodeTransport

GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "lifecycle" / "traces.json"
UPDATE = os.environ.get("DGX_UPDATE_TRACE_GOLDENS") == "1"
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_TIMESTAMP_KEYS = frozenset({"created_at", "updated_at"})


def spec(
    deployment_id: str, *assignments: NodeAssignment, **options: Any
) -> DeploymentSpec:
    """A traced deployment: one-second waits poll each node three times."""
    return lifecycle_spec(
        deployment_id,
        f"{deployment_id}-alias",
        *assignments,
        **{"post_stop_free": 32, "startup_timeout": 1, "stop_timeout": 1, **options},
    )


class Harness:
    """One journal, fake nodes and a deterministic clock for a scenario."""

    def __init__(self, transport: FakeNodeTransport | None = None) -> None:
        self.journal = OperationJournal()
        self.transport = transport or FakeNodeTransport(
            {
                node_id: FakeNode(node_id, baseline_free_memory_bytes=32)
                for node_id in ("node-a", "node-b")
            }
        )
        self.clock = 0.0
        self.engine = OperationOrchestrator(
            self.journal,
            self.transport,
            poll_interval_seconds=0.5,
            sleeper=self._sleep,
            monotonic=lambda: self.clock,
        )
        self.operations: list[OperationRecord] = []
        self.reports: list[dict[str, Any]] = []

    def _sleep(self, seconds: float) -> None:
        self.clock += seconds

    def load(self, deployment: DeploymentSpec, key: str) -> OperationRecord:
        return self._track(execute_load(self.engine, deployment, idempotency_key=key))

    def unload(self, deployment_id: str, key: str) -> OperationRecord:
        return self._track(
            execute_unload(self.engine, deployment_id, idempotency_key=key)
        )

    def recover(self, deployment: DeploymentSpec, key: str) -> OperationRecord:
        queued, _ = self.engine.submit_recover(deployment, idempotency_key=key)
        return self._track(self.engine.run(queued.id))

    def _track(self, operation: OperationRecord) -> OperationRecord:
        self.operations.append(operation)
        return operation

    def trace(self) -> dict[str, Any]:
        operations = []
        for operation in self.operations:
            current = self.journal.get_operation(operation.id)
            assert current is not None
            operations.append(
                {
                    "action": current.action.value,
                    "state": current.state.value,
                    "phase": current.phase.value,
                    "error_code": current.error_code,
                    "error_message": current.error_message,
                    "remediation": current.remediation,
                    "result": current.result,
                    "previous_placement": current.previous_placement,
                    "events": [
                        [event.phase.value, event.code, event.message, event.details]
                        for event in self.journal.events(operation.id)
                    ],
                }
            )
        trace = {
            "operations": operations,
            "deployments": [
                [record.id, record.desired_state, record.observed_state]
                for record in sorted(
                    self.journal.list_deployments(), key=lambda item: item.id
                )
            ],
            "calls": [list(call) for call in self.transport.calls],
            "locks": self.journal.active_locks(),
            "reconciliation_reports": self.reports,
        }
        self.journal.close()
        return _normalise(trace)


def _normalise(trace: dict[str, Any]) -> dict[str, Any]:
    def strip(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: "<timestamp>" if key in _TIMESTAMP_KEYS else strip(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [strip(item) for item in value]
        return value

    text = json.dumps(strip(trace), sort_keys=True)
    labels: dict[str, str] = {}
    text = _UUID_RE.sub(
        lambda match: labels.setdefault(match.group(0), f"<op-{len(labels) + 1}>"),
        text,
    )
    return json.loads(text)


def _idle_load_and_unload() -> Harness:
    harness = Harness()
    harness.load(spec("dep-a", NodeAssignment("node-a")), "load")
    harness.unload("dep-a", "unload")
    return harness


def _already_ready_load() -> Harness:
    harness = Harness()
    deployment = spec("ready", NodeAssignment("node-a"))
    harness.transport.seed_deployment(
        "node-a", deployment_id="ready", alias="ready-alias", memory_bytes=8
    )
    harness.load(deployment, "load")
    return harness


def _single_node_switch() -> Harness:
    harness = Harness()
    harness.load(spec("old", NodeAssignment("node-a"), required_memory=19), "old")
    harness.load(
        spec("new", NodeAssignment("node-a"), required_memory=16, minimum_free=16),
        "switch",
    )
    return harness


def _cluster_to_single_switch() -> Harness:
    harness = Harness()
    harness.load(
        spec(
            "tp", NodeAssignment("node-a", "head"), NodeAssignment("node-b", "worker")
        ),
        "tp",
    )
    harness.load(spec("single", NodeAssignment("node-a")), "switch")
    return harness


def _failed_start_restores_prior() -> Harness:
    harness = Harness(
        FailingNewStart(
            {
                node_id: FakeNode(node_id, baseline_free_memory_bytes=32)
                for node_id in ("node-a", "node-b")
            }
        )
    )
    harness.load(
        spec(
            "tp", NodeAssignment("node-a", "head"), NodeAssignment("node-b", "worker")
        ),
        "tp",
    )
    harness.load(spec("single", NodeAssignment("node-a")), "switch")
    return harness


def _wrong_alias_rolls_back() -> Harness:
    harness = Harness()
    harness.transport.node("node-a").alias_override = "wrong-model"
    harness.load(spec("wrong", NodeAssignment("node-a")), "load")
    return harness


def _rollback_cannot_stop() -> Harness:
    harness = Harness()
    harness.transport.node("node-a").alias_override = "wrong-model"
    harness.transport.node("node-a").fail_stop = True
    harness.load(spec("stuck", NodeAssignment("node-a")), "load")
    return harness


def _cluster_head_start_fails() -> Harness:
    harness = Harness()
    harness.transport.node("node-a").fail_start = True
    harness.load(
        spec(
            "tp", NodeAssignment("node-a", "head"), NodeAssignment("node-b", "worker")
        ),
        "load",
    )
    return harness


def _exited_container_fails_readiness() -> Harness:
    harness = Harness(
        ExitedContainer({"node-a": FakeNode("node-a", baseline_free_memory_bytes=32)})
    )
    harness.load(spec("exited", NodeAssignment("node-a", "head")), "load")
    return harness


def _launcher_failure_fails_readiness() -> Harness:
    class FailedLauncher(FakeNodeTransport):
        def observe(self, node_id):
            observed = super().observe(node_id)
            if observed.deployment_id == "launcher":
                return replace(
                    observed,
                    unit_state=UnitState.FAILED,
                    endpoint_healthy=False,
                    facts={"launcher_failed": True},
                )
            return observed

    harness = Harness(
        FailedLauncher({"node-a": FakeNode("node-a", baseline_free_memory_bytes=32)})
    )
    harness.load(spec("launcher", NodeAssignment("node-a", "head")), "load")
    return harness


def _unmanaged_conflict_refused() -> Harness:
    harness = Harness()
    harness.transport.seed_deployment(
        "node-a", deployment_id="foreign", alias="foreign", managed=False
    )
    harness.load(spec("wanted", NodeAssignment("node-a")), "load")
    return harness


def _lock_conflict_refused() -> Harness:
    harness = Harness()
    other, _ = harness.journal.create_or_get_operation(
        action=OperationAction.RECONCILE,
        idempotency_key="holder",
        request={"kind": "reconcile"},
        node_ids=["node-a"],
    )
    harness.journal.acquire_locks(other.id, ["node-a"], cluster=False)
    harness.load(spec("blocked", NodeAssignment("node-a")), "load")
    return harness


def _preflight_failure_refused() -> Harness:
    harness = Harness()
    harness.transport.node("node-a").preflight_failures = {"script_hash": "mismatch"}
    harness.load(spec("dep-a", NodeAssignment("node-a")), "load")
    harness.load(
        spec("dep-b", NodeAssignment("node-b"), canary={**CANARY, "max_tokens": "1"}),
        "bad-canary",
    )
    return harness


def _unreachable_node_refused() -> Harness:
    harness = Harness()
    harness.transport.node("node-a").reachable = False
    harness.load(spec("dep-a", NodeAssignment("node-a")), "load")
    return harness


def _partial_unload_needs_attention() -> Harness:
    harness = Harness()
    harness.load(spec("dep-a", NodeAssignment("node-a")), "load")
    harness.transport.node("node-a").fail_stop = True
    harness.unload("dep-a", "unload")
    return harness


def _absent_unload_checks_memory() -> Harness:
    harness = Harness()
    harness.load(spec("dep-a", NodeAssignment("node-a")), "load")
    harness.transport.stop("node-a", "dep-a", timeout_seconds=0)
    harness.transport.node("node-a").active_memory_bytes = 31
    harness.unload("dep-a", "low-memory")
    harness.transport.node("node-a").active_memory_bytes = 0
    harness.unload("dep-a", "recovered")
    return harness


def _unload_ownership_refused() -> Harness:
    harness = Harness()
    harness.load(spec("dep-a", NodeAssignment("node-a")), "load")
    harness.transport.stop("node-a", "dep-a", timeout_seconds=0)
    harness.transport.seed_deployment(
        "node-a", deployment_id="other", alias="other", managed=True
    )
    harness.unload("dep-a", "owned-by-other")
    lost_owner = NodeObservation("node-a", True, 32, unit_state=UnitState.RUNNING)
    with patch.object(harness.transport, "observe", return_value=lost_owner):
        harness.unload("dep-a", "unmanaged")
    return harness


def _cluster_unload_stops_head_first() -> Harness:
    harness = Harness()
    harness.load(
        spec(
            "tp", NodeAssignment("node-b", "worker"), NodeAssignment("node-a", "head")
        ),
        "load",
    )
    harness.unload("tp", "unload")
    return harness


def _recovery_resets_and_refuses() -> Harness:
    harness = Harness()
    orphan = spec(
        "orphan", NodeAssignment("node-a", "head"), NodeAssignment("node-b", "worker")
    )
    for node_id in ("node-a", "node-b"):
        harness.transport.seed_deployment(
            node_id,
            deployment_id="orphan",
            alias="orphan-alias",
            memory_bytes=8,
            managed=False,
            endpoint_healthy=False,
        )
    harness.recover(orphan, "recover")
    harness.transport.seed_deployment(
        "node-a", deployment_id="foreign", alias="foreign", managed=False
    )
    harness.recover(orphan, "refused")
    return harness


def _canary_pass_and_failing_switch() -> Harness:
    harness = Harness()
    harness.transport.node("node-a").canary_finish_reason = "length"
    harness.load(spec("first", NodeAssignment("node-a"), canary=dict(CANARY)), "ok")
    harness.transport.node("node-a").canary_completion_tokens = 0
    harness.load(spec("second", NodeAssignment("node-a"), canary=dict(CANARY)), "bad")
    return harness


def _cluster_canary_targets_head() -> Harness:
    harness = Harness()
    harness.load(
        spec(
            "tp",
            NodeAssignment("node-b", "worker"),
            NodeAssignment("node-a", "head"),
            canary=dict(CANARY),
        ),
        "load",
    )
    return harness


def _reconcile_resolves_interrupted_work() -> Harness:
    harness = Harness()
    harness.load(spec("old", NodeAssignment("node-a")), "old")
    harness.load(spec("new", NodeAssignment("node-a")), "switch")
    stale = harness.journal.get_deployment("old")
    assert stale is not None
    harness.journal.save_deployment(
        replace(stale, desired_state="ready", observed_state="ready")
    )
    interrupted_load, _ = harness.engine.submit_load(
        spec("tp", NodeAssignment("node-b")), idempotency_key="interrupted-load"
    )
    interrupted_unload, _ = harness.engine.submit_unload(
        "new", idempotency_key="interrupted-unload"
    )
    for operation in (interrupted_load, interrupted_unload):
        harness.journal.claim_operation(operation.id)
        harness.operations.append(operation)
    harness.transport.seed_deployment("node-b", deployment_id="tp", alias="tp-alias")
    before = len(harness.transport.calls)
    report = Reconciler(harness.journal, harness.transport).reconcile(
        tuple(harness.transport.nodes)
    )
    # Reconciliation observes a set of nodes, so only its call set is stable.
    harness.transport.calls[before:] = sorted(harness.transport.calls[before:])
    harness.reports.append(asdict(report))
    return harness


SCENARIOS: dict[str, Callable[[], Harness]] = {
    "idle_load_and_unload": _idle_load_and_unload,
    "already_ready_load": _already_ready_load,
    "single_node_switch": _single_node_switch,
    "cluster_to_single_switch": _cluster_to_single_switch,
    "failed_start_restores_prior": _failed_start_restores_prior,
    "wrong_alias_rolls_back": _wrong_alias_rolls_back,
    "rollback_cannot_stop": _rollback_cannot_stop,
    "cluster_head_start_fails": _cluster_head_start_fails,
    "exited_container_fails_readiness": _exited_container_fails_readiness,
    "launcher_failure_fails_readiness": _launcher_failure_fails_readiness,
    "unmanaged_conflict_refused": _unmanaged_conflict_refused,
    "lock_conflict_refused": _lock_conflict_refused,
    "preflight_failure_refused": _preflight_failure_refused,
    "unreachable_node_refused": _unreachable_node_refused,
    "partial_unload_needs_attention": _partial_unload_needs_attention,
    "absent_unload_checks_memory": _absent_unload_checks_memory,
    "unload_ownership_refused": _unload_ownership_refused,
    "cluster_unload_stops_head_first": _cluster_unload_stops_head_first,
    "recovery_resets_and_refuses": _recovery_resets_and_refuses,
    "canary_pass_and_failing_switch": _canary_pass_and_failing_switch,
    "cluster_canary_targets_head": _cluster_canary_targets_head,
    "reconcile_resolves_interrupted_work": _reconcile_resolves_interrupted_work,
}


class OperationTraceTests(unittest.TestCase):
    def test_lifecycle_traces_match_their_reviewed_fixture(self) -> None:
        traces = {name: scenario().trace() for name, scenario in SCENARIOS.items()}
        if UPDATE:
            GOLDEN.parent.mkdir(parents=True, exist_ok=True)
            GOLDEN.write_text(json.dumps(traces, indent=2, sort_keys=True) + "\n")
        expected = json.loads(GOLDEN.read_text())
        self.assertEqual(sorted(expected), sorted(traces))
        for name, trace in traces.items():
            with self.subTest(name):
                self.assertEqual(expected[name], trace)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
