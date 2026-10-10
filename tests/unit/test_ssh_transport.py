# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from controller.transport.ssh import SSHExecutionError, SSHExecutor, SSHNode


class SSHTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def node(self, key_path: str) -> SSHNode:
        return SSHNode("spark-1", "192.0.2.10", "operator", key_path, 7)

    def test_command_is_argument_array_and_remote_script_is_stdin(self) -> None:
        key = self.root / "spark key"
        key.write_text("not-a-real-private-key")
        executor = SSHExecutor({"spark-1": self.node(str(key))})
        with patch(
            "controller.transport.ssh.subprocess.run",
            return_value=CompletedProcess([], 0, "ready\n", ""),
        ) as run:
            result = executor.run_script(
                "spark-1", "printf '%s\\n' ready\n", timeout_seconds=5, check=True
            )
        self.assertTrue(result.ok)
        command = run.call_args.args[0]
        self.assertEqual(command[-2:], ["operator@192.0.2.10", "bash -s"])
        self.assertEqual(command[command.index("-i") + 1], str(key))
        self.assertEqual(run.call_args.kwargs["input"], "printf '%s\\n' ready\n")

    def test_unknown_node_and_missing_key_fail_before_subprocess(self) -> None:
        executor = SSHExecutor({"spark-1": self.node(str(self.root / "missing"))})
        with patch("controller.transport.ssh.subprocess.run") as run:
            with self.assertRaisesRegex(SSHExecutionError, "unknown configured node"):
                executor.run_script("spark-2", "true\n", timeout_seconds=1)
            with self.assertRaisesRegex(SSHExecutionError, "key does not exist"):
                executor.run_script("spark-1", "true\n", timeout_seconds=1)
        run.assert_not_called()

    def test_remote_failure_is_actionable_when_checking(self) -> None:
        key = self.root / "key"
        key.write_text("x")
        executor = SSHExecutor({"spark-1": self.node(str(key))})
        with (
            patch(
                "controller.transport.ssh.subprocess.run",
                return_value=CompletedProcess([], 23, "", "script hash mismatch\n"),
            ),
            self.assertRaisesRegex(SSHExecutionError, "script hash mismatch"),
        ):
            executor.run_script("spark-1", "false\n", timeout_seconds=5, check=True)


if __name__ == "__main__":
    unittest.main()
