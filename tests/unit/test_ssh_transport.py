# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import shlex
import tempfile
import traceback
import unittest
from pathlib import Path
from subprocess import CompletedProcess, TimeoutExpired
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
            with self.assertRaisesRegex(
                SSHExecutionError, "key file for node .* was not found"
            ):
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

    def executor(self) -> SSHExecutor:
        key = self.root / "key"
        key.write_text("x")
        return SSHExecutor({"spark-1": self.node(str(key))})

    def test_reverse_tunnel_binds_node_loopback_and_ends_with_stdin(self) -> None:
        executor = self.executor()
        self.assertEqual(
            (
                *executor.command_for("spark-1")[:-2],
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "ControlMaster=no",
                "-o",
                "ControlPath=none",
                "-R",
                "127.0.0.1:18877:127.0.0.1:45678",
                "operator@192.0.2.10",
                "cat >/dev/null",
            ),
            executor.reverse_tunnel_command("spark-1", 18877, 45678),
        )

    def test_reverse_tunnel_rejects_out_of_range_ports(self) -> None:
        executor = self.executor()
        for remote, local in (
            (1023, 45678),
            (65536, 45678),
            (18877, 0),
            (18877, 65536),
        ):
            with (
                self.subTest(remote=remote, local=local),
                self.assertRaises(ValueError),
            ):
                executor.reverse_tunnel_command("spark-1", remote, local)

    def test_run_argv_quotes_each_word_and_sends_stdin_on_the_pipe(self) -> None:
        argv = ("sudo", "-S", "--", "/bin/echo", "a b; reboot", "$(id)")
        with patch(
            "controller.transport.ssh.subprocess.run",
            return_value=CompletedProcess([], 0, "ok\n", ""),
        ) as run:
            result = self.executor().run_argv(
                "spark-1", argv, stdin="hunter2\n", timeout_seconds=5
            )
        self.assertTrue(result.ok)
        command = run.call_args.args[0]
        self.assertEqual(["operator@192.0.2.10", shlex.join(argv)], command[-2:])
        self.assertEqual(list(argv), shlex.split(command[-1]))
        self.assertEqual("hunter2\n", run.call_args.kwargs["input"])
        self.assertNotIn("hunter2", " ".join(command))

    def test_run_argv_rejects_missing_and_unsafe_words_before_ssh(self) -> None:
        executor = self.executor()
        with patch("controller.transport.ssh.subprocess.run") as run:
            for argv in ((), ("sudo", "true\nreboot"), ("sudo", "true\0")):
                with self.subTest(argv=argv), self.assertRaises(ValueError):
                    executor.run_argv("spark-1", argv, stdin="", timeout_seconds=5)
        run.assert_not_called()

    def test_run_argv_timeout_does_not_reveal_stdin(self) -> None:
        def time_out(command: list[str], **kwargs: object) -> None:
            raise TimeoutExpired(command, 5)

        with (
            patch("controller.transport.ssh.subprocess.run", side_effect=time_out),
            self.assertRaises(SSHExecutionError) as caught,
        ):
            self.executor().run_argv(
                "spark-1", ("sudo", "-S", "true"), stdin="hunter2\n", timeout_seconds=5
            )
        rendered = "".join(traceback.format_exception(caught.exception))
        self.assertIn("TimeoutExpired", rendered)
        self.assertNotIn("hunter2", rendered)


if __name__ == "__main__":
    unittest.main()
