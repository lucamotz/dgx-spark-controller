# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from controller.runners.systemd_user import ScriptUnitRecipe, SystemdUserRunner
from controller.transport.base import UnitState
from controller.transport.ssh import SSHExecutor, SSHNode

HASH = "a" * 64


class SystemdUserRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        key = root / "key"
        key.write_text("x")
        executor = SSHExecutor(
            {"spark-1": SSHNode("spark-1", "192.0.2.10", "operator", str(key))}
        )
        recipe = ScriptUnitRecipe("step", "/home/operator/load-step.sh", HASH)
        self.runner = SystemdUserRunner(executor, {"step": recipe})

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_start_validates_script_and_uses_named_transient_unit(self) -> None:
        with patch(
            "controller.transport.ssh.subprocess.run",
            return_value=CompletedProcess([], 0, "Running as unit\n", ""),
        ) as run:
            result = self.runner.start_unit("spark-1", "dgx-model-production", "step")
        self.assertTrue(result.ok)
        remote = run.call_args.kwargs["input"]
        self.assertIn('bash -n "$script_path"', remote)
        self.assertIn('sha256sum "$script_path"', remote)
        self.assertIn(
            "systemd-run --user --unit=dgx-model-production --collect", remote
        )
        self.assertIn("KillMode=control-group", remote)

    def test_status_maps_systemd_state_and_main_pid(self) -> None:
        output = "LoadState=loaded\nActiveState=active\nSubState=running\nMainPID=42\n"
        with patch(
            "controller.transport.ssh.subprocess.run",
            return_value=CompletedProcess([], 0, output, ""),
        ):
            status = self.runner.unit_status("spark-1", "dgx-model-production")
        self.assertEqual(status.state, UnitState.RUNNING)
        self.assertEqual(status.process_identity, "pid:42")

    def test_stop_targets_only_named_unit(self) -> None:
        with patch(
            "controller.transport.ssh.subprocess.run",
            return_value=CompletedProcess([], 0, "stopped\n", ""),
        ) as run:
            result = self.runner.stop_unit(
                "spark-1", "dgx-model-production", timeout_seconds=10, force=False
            )
        self.assertTrue(result.ok)
        remote = run.call_args.kwargs["input"]
        self.assertIn(
            "systemctl --user stop --no-block dgx-model-production.service", remote
        )
        self.assertNotIn("pkill", remote)


if __name__ == "__main__":
    unittest.main()
