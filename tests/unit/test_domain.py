# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import unittest
from dataclasses import FrozenInstanceError

from controller.config import load_profile
from controller.domain import (
    DeploymentState,
    OperationState,
    can_transition_deployment,
    can_transition_operation,
)

PROFILE_PATH = (
    "config/examples/profiles/mimo-v26-flash-rl-bf16-884736-c2-shared-vllm.json"
)


class DomainContractTests(unittest.TestCase):
    def test_committed_mimo_profile_is_an_immutable_typed_record(self):
        profile = load_profile(PROFILE_PATH)

        self.assertEqual(
            profile.profile_id, "mimo-v26-flash-rl-bf16-884736-c2-shared-vllm"
        )
        self.assertEqual(
            profile.artifact.required_files[0], profile.artifact.primary_path
        )
        self.assertEqual(
            profile.runner.script_path,
            "/home/spark/mimo-v26-experiment/start-spark-tp2.sh",
        )
        with self.assertRaises(FrozenInstanceError):
            profile.api_alias = "wrong-alias"

    def test_state_transition_catalog_allows_only_documented_edges(self):
        self.assertTrue(
            can_transition_deployment(
                DeploymentState.PLANNED, DeploymentState.PREFLIGHTING
            )
        )
        self.assertTrue(
            can_transition_deployment(DeploymentState.LOADING, DeploymentState.READY)
        )
        self.assertTrue(
            can_transition_deployment(DeploymentState.READY, DeploymentState.DRAINING)
        )
        self.assertFalse(
            can_transition_deployment(
                DeploymentState.READY, DeploymentState.STARTING_HEAD
            )
        )
        self.assertFalse(
            can_transition_deployment(DeploymentState.STOPPED, DeploymentState.READY)
        )

        self.assertTrue(
            can_transition_operation(OperationState.QUEUED, OperationState.RUNNING)
        )
        self.assertTrue(
            can_transition_operation(
                OperationState.RUNNING, OperationState.NEEDS_ATTENTION
            )
        )
        self.assertFalse(
            can_transition_operation(OperationState.SUCCEEDED, OperationState.RUNNING)
        )

    def test_mimo_profile_contracts_are_consistent(self):
        profile = load_profile(PROFILE_PATH)

        self.assertEqual(profile.endpoint, profile.readiness.endpoint)
        self.assertEqual(profile.api_alias, profile.readiness.expected_alias)
        self.assertEqual(
            profile.runtime.executable_path, profile.readiness.process.executable_path
        )
        self.assertEqual(profile.runtime.source_of_truth, "allowlisted-script")
        self.assertEqual(profile.deployment_mode.value, "shared-vllm")


if __name__ == "__main__":
    unittest.main()
