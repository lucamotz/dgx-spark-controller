# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import unittest
from dataclasses import replace
from types import SimpleNamespace

from controller.config import load_nodes, load_profile
from controller.domain import NodeSelector, PortRange, profile_matches_node
from controller.operations import (
    OperationError,
    OperationState,
    ensure_state_transition,
)

PROFILE_PATH = (
    "config/examples/profiles/mimo-v26-flash-rl-bf16-884736-c2-shared-vllm.json"
)
CLUSTER_NODES_PATH = "config/examples/nodes.cluster.example.json"


class DomainContractTests(unittest.TestCase):
    def test_operation_state_catalog_allows_only_documented_edges(self):
        ensure_state_transition(OperationState.QUEUED, OperationState.RUNNING)
        ensure_state_transition(OperationState.RUNNING, OperationState.NEEDS_ATTENTION)
        with self.assertRaises(OperationError) as raised:
            ensure_state_transition(OperationState.SUCCEEDED, OperationState.RUNNING)
        self.assertEqual("illegal_state_transition", raised.exception.code)

    def test_profile_selector_and_port_range_decide_node_compatibility(self):
        profile = load_profile(PROFILE_PATH)
        node = load_nodes(CLUSTER_NODES_PATH)[0]
        self.assertTrue(profile_matches_node(profile, node))

        port = profile.endpoint.port
        for incompatible in (
            replace(node, node_id="spark-z"),
            replace(node, labels=frozenset()),
            replace(node, capabilities=frozenset()),
            replace(node, endpoint_ports=PortRange(port + 1, port + 2)),
        ):
            with self.subTest(node=incompatible):
                self.assertFalse(profile_matches_node(profile, incompatible))

        # Shared profiles must allowlist nodes; an empty allowlist admits any
        # node, and the predicate reads only the selector and the endpoint.
        unrestricted = SimpleNamespace(
            node_selector=NodeSelector(), endpoint=profile.endpoint
        )
        self.assertTrue(
            profile_matches_node(unrestricted, replace(node, node_id="spark-z"))
        )


if __name__ == "__main__":
    unittest.main()
