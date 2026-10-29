# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Byte-for-byte pins of every remote script the controller renders.

The remote scripts, their order, target node, timeout and ``check`` flag are
part of the node safety contract. A deliberate change updates the fixture in
the same commit; regenerate it with::

    DGX_UPDATE_SCRIPT_GOLDENS=1 python -m pytest \\
        tests/unit/test_remote_script_goldens.py
"""

from __future__ import annotations

import os
import unittest
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

from controller.operations import DeploymentSpec, NodeAssignment
from controller.recipes.nodeops import SSHRecipeNodeOps
from controller.runners.systemd_user import ScriptUnitRecipe, SystemdUserRunner
from controller.transport.base import CanaryRequest
from controller.transport.managed_ssh import (
    ClusterScriptRecipe,
    ManagedSSHNode,
    ManagedSSHTransport,
)
from controller.transport.ssh import SSHExecution

GOLDENS = Path(__file__).resolve().parents[1] / "fixtures" / "remote_scripts"
UPDATE = os.environ.get("DGX_UPDATE_SCRIPT_GOLDENS") == "1"


class RecordingExecutor:
    """Record every remote script and answer with canned results."""

    def __init__(self, results: dict[int, SSHExecution] | None = None) -> None:
        self.results = results or {}
        self.calls: list[tuple[str, float, bool, str]] = []

    def run_script(self, node_id, script, *, timeout_seconds, check=False):
        self.calls.append((node_id, timeout_seconds, check, script))
        return self.results.get(len(self.calls), SSHExecution(0, "", ""))

    def transcript(self) -> str:
        parts = [
            f"### call {index}: node={node_id} timeout={timeout} check={check}\n"
            f"{script}"
            for index, (node_id, timeout, check, script) in enumerate(
                self.calls, start=1
            )
        ]
        return "".join(parts) + "### end\n"


def _cluster_recipe(**changes) -> ClusterScriptRecipe:
    recipe = ClusterScriptRecipe(
        profile_id="cluster-profile",
        deployment_id="dep-cluster-profile",
        node_ids=("spark-1", "spark-2"),
        control_node_id="spark-1",
        start_script_path="/recipes/start vllm.sh",
        stop_script_path="/recipes/stop.sh",
        start_sha256="a" * 64,
        stop_sha256="b" * 64,
        working_directory="/recipes",
        environment_path="/recipes/.env",
        required_environment={
            "MAX_MODEL_LEN": "1048576",
            "EXTRA_ARGS": "--note 'quoted $value'",
            "VLLM_HEAD_IMAGE_ID": "sha256:" + "1" * 64,
        },
        expected_recipe_commit="abc123",
        image_digest="registry/image@sha256:" + "c" * 64,
        container_name="cluster-vllm-1",
        endpoint_port=8888,
        health_path="/health",
        models_path="/v1/models",
        expected_alias="cluster-model",
        required_command_fragments=("/usr/local/bin/vllm", "--max-model-len"),
    )
    return replace(recipe, **changes) if changes else recipe


def _per_node_recipe() -> ClusterScriptRecipe:
    return _cluster_recipe(
        launch_mode="per-node",
        node_environment={
            "spark-1": {"NODE_RANK": "0", "VLLM_HOST_IP": "198.51.100.1"},
            "spark-2": {"NODE_RANK": "1", "VLLM_HOST_IP": "198.51.100.2"},
        },
    )


def _cluster_spec() -> DeploymentSpec:
    return DeploymentSpec(
        deployment_id="dep-cluster-profile",
        profile_id="cluster-profile",
        expected_alias="cluster-model",
        assignments=(
            NodeAssignment("spark-1", "head"),
            NodeAssignment("spark-2", "worker"),
        ),
        profile={
            "deployment_mode": "shared-vllm",
            "artifact": {
                "required_files": ["/models/a b.safetensors", "/models/index.json"],
                "manifest_sha256": "d" * 64,
            },
            "safety": {"maximum_swap_used_bytes": 1024},
        },
        startup_timeout_seconds=900,
        stop_timeout_seconds=120,
    )


def _unit_spec() -> DeploymentSpec:
    return DeploymentSpec(
        deployment_id="production",
        profile_id="example-profile",
        expected_alias="example-model",
        assignments=(NodeAssignment("spark-1"),),
        minimum_free_memory_bytes=16 * 1024**3,
        profile={
            "runner": {
                "script_id": "example",
                "script_path": "/home/operator/load example.sh",
                "expected_sha256": "e" * 64,
                "expected_mode": "0644",
                "expected_size_bytes": 10,
            },
            "artifact": {
                "primary_path": "/models/example-00001.gguf",
                "primary_size_bytes": 20,
                "required_files": ["/models/example-00001.gguf", "/models/x'y.gguf"],
            },
            "runtime": {
                "executable_path": "/opt/llama-server",
                "build_revision": "10036",
                "build_commit": "3dafb585f",
            },
            "safety": {"maximum_swap_used_bytes": 1024 * 1024**2},
        },
    )


def _transport(
    executor: RecordingExecutor,
    *recipes: ClusterScriptRecipe,
    runner: SystemdUserRunner | None = None,
) -> ManagedSSHTransport:
    return ManagedSSHTransport(
        executor,  # type: ignore[arg-type]
        runner or _runner(executor),
        {
            "spark-1": ManagedSSHNode(
                "spark-1", "192.0.2.10", probe_port_range=(8000, 8999)
            ),
            "spark-2": ManagedSSHNode(
                "spark-2",
                "192.0.2.11",
                endpoint_port=8080,
                legacy_process_pattern="llama-server --port 8080",
            ),
        },
        recipes,
    )


def _runner(executor: RecordingExecutor) -> SystemdUserRunner:
    return SystemdUserRunner(
        executor,  # type: ignore[arg-type]
        {
            "example": ScriptUnitRecipe(
                "example", "/home/operator/load example.sh", "e" * 64, 60
            )
        },
    )


_FAILED = SSHExecution(1, "", "launch refused")


def _scenarios() -> dict[str, Callable[[RecordingExecutor], object]]:
    canary = CanaryRequest(120, 16, 'Reply with OK. It\'s "$(true)"; `x`')
    return {
        "observe_plain_node": lambda ex: _transport(ex).observe("spark-2"),
        "observe_head_script_head": lambda ex: _transport(
            ex, _cluster_recipe()
        ).observe("spark-1"),
        "observe_head_script_worker": lambda ex: _transport(
            ex, _cluster_recipe()
        ).observe("spark-2"),
        "observe_per_node_worker": lambda ex: _transport(
            ex, _per_node_recipe()
        ).observe("spark-2"),
        "preflight_unit": lambda ex: _transport(ex).preflight(
            "spark-1", _unit_spec(), NodeAssignment("spark-1")
        ),
        "preflight_cluster_head": lambda ex: _transport(
            ex, _cluster_recipe()
        ).preflight("spark-1", _cluster_spec(), NodeAssignment("spark-1", "head")),
        "preflight_cluster_worker": lambda ex: _transport(
            ex, _cluster_recipe()
        ).preflight("spark-2", _cluster_spec(), NodeAssignment("spark-2", "worker")),
        "preflight_per_node_worker": lambda ex: _transport(
            ex, _per_node_recipe()
        ).preflight("spark-2", _cluster_spec(), NodeAssignment("spark-2", "worker")),
        "start_unit": lambda ex: _transport(ex).start(
            "spark-1", _unit_spec(), NodeAssignment("spark-1")
        ),
        "start_head_script": lambda ex: [
            _transport(ex, _cluster_recipe()).start(
                "spark-2", _cluster_spec(), NodeAssignment("spark-2", "worker")
            ),
            _transport(ex, _cluster_recipe()).start(
                "spark-1", _cluster_spec(), NodeAssignment("spark-1", "head")
            ),
        ],
        "start_head_script_launch_fails": lambda ex: _transport(
            ex, _cluster_recipe()
        ).start("spark-1", _cluster_spec(), NodeAssignment("spark-1", "head")),
        "start_per_node_worker": lambda ex: _transport(ex, _per_node_recipe()).start(
            "spark-2", _cluster_spec(), NodeAssignment("spark-2", "worker")
        ),
        "start_per_node_launch_fails": lambda ex: _transport(
            ex, _per_node_recipe()
        ).start("spark-2", _cluster_spec(), NodeAssignment("spark-2", "worker")),
        "stop_unit": lambda ex: [
            _transport(ex).stop("spark-1", "production", timeout_seconds=30),
            _transport(ex).stop(
                "spark-1", "production", timeout_seconds=7.5, force=True
            ),
        ],
        "stop_head_script": lambda ex: [
            _transport(ex, _cluster_recipe()).stop(
                "spark-2", "dep-cluster-profile", timeout_seconds=120
            ),
            _transport(ex, _cluster_recipe()).stop(
                "spark-1", "dep-cluster-profile", timeout_seconds=120
            ),
        ],
        "stop_per_node": lambda ex: _transport(ex, _per_node_recipe()).stop(
            "spark-2", "dep-cluster-profile", timeout_seconds=5
        ),
        "recover_head_script": lambda ex: _transport(ex, _cluster_recipe()).recover(
            _cluster_spec()
        ),
        "recover_per_node": lambda ex: _transport(ex, _per_node_recipe()).recover(
            _cluster_spec()
        ),
        "canary_cluster": lambda ex: _transport(
            ex, _cluster_recipe()
        ).completion_canary("spark-1", _cluster_spec(), canary),
        "canary_unit": lambda ex: _transport(ex).completion_canary(
            "spark-1", _unit_spec(), canary
        ),
        "logs": lambda ex: [
            _transport(ex, _cluster_recipe()).logs(
                "spark-2", "dep-cluster-profile", limit=900
            ),
            _transport(ex).logs("spark-1", "production", cursor="x", limit=0),
        ],
    }


_RESULTS = {
    "start_head_script_launch_fails": {4: _FAILED},
    "start_per_node_launch_fails": {3: _FAILED},
    "recipe_prepare": {1: SSHExecution(0, "/home/spark\n", "")},
}


def _recipe_scenarios() -> dict[str, Callable[[RecordingExecutor], object]]:
    directory = (
        "/home/spark/.local/share/dgx-spark-controller/recipes/g-qwen3-8b-00aa11"
    )
    model_dir = "Qwen--Qwen3-8B-b968826d"

    def prepare(ex: RecordingExecutor) -> None:
        ops = SSHRecipeNodeOps(ex)  # type: ignore[arg-type]
        ops.remote_home("spark-1")
        ops.check_linger("spark-1")
        ops.install_files(
            "spark-1",
            directory,
            {
                "prepare.sh": (b"#!/bin/bash\necho ok\n", 0o755),
                "prepare-00aa11bb22.env": (b"A=1\n", 0o600),
            },
        )
        ops.start_prepare("spark-1", directory, "00aa11bb22")
        ops.prepare_status(
            "spark-1", directory, "00aa11bb22", f"/home/spark/models/{model_dir}"
        )
        ops.stop_prepare("spark-1", "00aa11bb22")

    def remove(ex: RecordingExecutor) -> None:
        ops = SSHRecipeNodeOps(ex)  # type: ignore[arg-type]
        ops.remove_recipe_directory("spark-1", "/home/spark", "g-qwen3-8b-00aa11")
        ops.remove_recipe_directory(
            "spark-1", "/home/spark", "g-qwen3-8b-00aa11", prepare_id="00aa11bb22"
        )
        ops.remove_model_directory("spark-1", "/home/spark/models", model_dir)
        ops.remove_image("spark-1", "vllm/vllm-openai@sha256:" + "a" * 64)
        ops.remove_cache_volume("spark-1")

    return {"recipe_prepare": prepare, "recipe_remove": remove}


class RemoteScriptGoldenTests(unittest.TestCase):
    def test_rendered_scripts_match_their_reviewed_fixtures(self) -> None:
        for name, scenario in {**_scenarios(), **_recipe_scenarios()}.items():
            with self.subTest(name):
                executor = RecordingExecutor(_RESULTS.get(name))
                scenario(executor)
                rendered = executor.transcript()
                path = GOLDENS / f"{name}.txt"
                if UPDATE:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(rendered)
                self.assertEqual(path.read_text(), rendered)


if __name__ == "__main__":
    unittest.main()
