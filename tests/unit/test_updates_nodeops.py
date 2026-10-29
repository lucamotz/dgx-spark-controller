# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Update node primitives: probe parsers, the helper hash gate and the fake."""

from __future__ import annotations

import base64
import hashlib
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import Mock, patch

from controller.transport.ssh import (
    SSHExecution,
    SSHExecutionError,
    SSHExecutor,
    SSHNode,
)
from controller.updates.nodeops import (
    HELPER_PATH,
    HELPER_STAGING_PATH,
    RELAY_REMOTE_PORT,
    FakeUpdateNodeOps,
    NodeFacts,
    RunStatus,
    SSHUpdateNodeOps,
    UpdateCheck,
    UpdateNodeError,
    helper_bytes,
    helper_sha256,
    parse_check,
    parse_facts,
    parse_run_status,
    relay_hosts,
    relay_tunnels,
)
from controller.updates.relay import RelaySession, RelayStats

SHA = helper_sha256()
BOOT_ID = "6f0c1d2e-3a4b-4c5d-8e9f-001122334455"
PASSWORD = "correct horse;$(id)"
INSTALL_ARGV = (
    "/usr/bin/sudo",
    "-S",
    "-k",
    "-p",
    "",
    "--",
    "/bin/bash",
    HELPER_STAGING_PATH,
    "install",
)
CLEANUP = f"rm -f -- {HELPER_STAGING_PATH}\n"
RELAY = RelaySession(1234, "5e55" * 8, RelayStats())


class RecordingExecutor:
    """Record every SSH call and answer from queued results."""

    def __init__(self, *results: SSHExecution | Exception) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, str | tuple[str, ...], str | None]] = []

    def _answer(self) -> SSHExecution:
        result = self.results.pop(0) if self.results else SSHExecution(0, "", "")
        if isinstance(result, Exception):
            raise result
        return result

    def run_script(
        self, node_id: str, script: str, *, timeout_seconds: float, check=False
    ) -> SSHExecution:
        self.calls.append(("script", script, None))
        return self._answer()

    def run_argv(
        self, node_id: str, argv: tuple[str, ...], *, stdin: str, timeout_seconds: float
    ) -> SSHExecution:
        self.calls.append(("argv", argv, stdin))
        return self._answer()

    def reverse_tunnel_command(
        self, node_id: str, remote_port: int, local_port: int
    ) -> tuple[str, ...]:
        return ("ssh", "-N", "-R", f"{remote_port}:{local_port}", node_id)


class LocalExecutor:
    """Run the fixed remote scripts with ``bash -s`` in a home directory."""

    def __init__(self, home: Path, environment: dict[str, str]) -> None:
        self.home = home
        self.environment = environment
        self.installed: list[tuple[bytes, int]] = []

    def run_script(
        self, node_id: str, script: str, *, timeout_seconds: float, check=False
    ) -> SSHExecution:
        completed = subprocess.run(
            ["bash", "-s"],
            input=script,
            cwd=self.home,
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        return SSHExecution(completed.returncode, completed.stdout, completed.stderr)

    def run_argv(
        self, node_id: str, argv: tuple[str, ...], *, stdin: str, timeout_seconds: float
    ) -> SSHExecution:
        staged = self.home / HELPER_STAGING_PATH
        content = staged.read_bytes()
        self.installed.append((content, stat.S_IMODE(staged.stat().st_mode)))
        digest = hashlib.sha256(content).hexdigest()
        return SSHExecution(0, f"INSTALLED=1\nSHA256={digest}\n", "")


def probe(sha: str = SHA, owner: str = "root:root:755", sudo: str = "1") -> str:
    return (
        f"BOOT_ID={BOOT_ID}\nHELPER_SHA256={sha}\nHELPER_OWNER={owner}\n"
        f"SUDO_OK={sudo}\n"
    )


class ParserTests(unittest.TestCase):
    def test_helper_state_is_decided_from_the_probe(self) -> None:
        for text, state in (
            (f"BOOT_ID={BOOT_ID}\n", "missing"),
            (probe(sha="0" * 64), "outdated"),
            (probe(owner="spark:spark:755"), "outdated"),
            (probe(owner="root:root:757"), "outdated"),
            (probe(sudo=""), "no-sudo"),
            (probe(), "ready"),
        ):
            with self.subTest(state=state):
                self.assertEqual(state, parse_facts("spark-a", text, SHA).helper)

    def test_a_node_is_back_when_it_can_host_a_model(self) -> None:
        # A headless node's boot splash can keep systemd "starting" forever.
        ready = (
            f"BOOT_ID={BOOT_ID}\nSYSTEM_STATE=starting\nUSER_MANAGER=running\n"
            "DOCKER_READY=1\nGPU_READY=1\nLINKS_EXPECTED=2\nLINKS_PRESENT=2\n"
        )
        self.assertTrue(parse_facts("spark-a", ready, SHA).booted)
        still_booting = ready + "BOOT_JOBS=3\n"
        self.assertFalse(parse_facts("spark-a", still_booting, SHA).booted)
        for missing in ("USER_MANAGER=running", "DOCKER_READY=1", "LINKS_PRESENT=2"):
            with self.subTest(missing=missing):
                facts = parse_facts("spark-a", ready.replace(missing, ""), SHA)
                self.assertFalse(facts.booted)

    def test_run_is_read_only_from_prefixed_lines_of_a_ready_helper(self) -> None:
        status = "STATE=failed\nRUN_STATE=running\nRUN_MODE=apply\n"

        run = parse_facts("spark-a", probe() + status, SHA).run
        assert run is not None
        self.assertEqual(("running", "apply"), (run.state, run.mode))
        self.assertIsNone(parse_facts("spark-a", probe(sudo="0") + status, SHA).run)

    def test_source_hosts_keep_valid_names_and_prefer_https(self) -> None:
        sources = (
            "http://ports.ubuntu.com https://ports.ubuntu.com "
            "https://security.ubuntu.com http://security.ubuntu.com "
            "http://PPA.launchpadcontent.net http://repo.example.com:8080 "
            "http://user@evil.example http://10.0.0.1 http://localhost "
            "ftp://ftp.example.org http://.."
        )
        facts = parse_facts("spark-a", f"SOURCES={sources}\n", SHA)
        self.assertEqual(
            {
                "ports.ubuntu.com": "https",
                "ppa.launchpadcontent.net": "http",
                "security.ubuntu.com": "https",
            },
            facts.source_hosts,
        )

    def test_relay_hosts_merge_nodes_and_prefer_https(self) -> None:
        nodes = [
            NodeFacts("spark-a", True, source_hosts={"ports.ubuntu.com": "http"}),
            NodeFacts("spark-b", True, source_hosts={"ports.ubuntu.com": "https"}),
            NodeFacts("spark-c", True, source_hosts={"ports.ubuntu.com": "http"}),
        ]
        self.assertEqual({"ports.ubuntu.com": "https"}, relay_hosts(nodes))

    def test_hostile_run_status_values_are_dropped(self) -> None:
        text = (
            "STATE=pwned\nMODE=install\nSTEP=apt;reboot\nPACKAGES=1e9\n"
            "FIRMWARE=-1\nFAILED_STEP=../../etc/shadow\nLOG_TAIL=%%%\n"
        )
        self.assertEqual(RunStatus(), parse_run_status(text))

    def test_log_tail_is_bounded(self) -> None:
        tail = "\n".join([f"line {number}" for number in range(40)] + ["x" * 400])
        encoded = base64.b64encode(tail.encode()).decode()

        log_tail = parse_run_status(f"STATE=failed\nLOG_TAIL={encoded}\n").log_tail
        self.assertEqual(30, len(log_tail))
        self.assertEqual(("line 11", "x" * 300), (log_tail[0], log_tail[-1]))

    def test_oversized_numbers_are_dropped(self) -> None:
        self.assertIsNone(parse_run_status("PACKAGES=" + "9" * 5000).packages)

    def test_check_reports_pending_updates(self) -> None:
        text = "PACKAGES=12\nFIRMWARE=1\nREBOOT_REQUIRED=0\n"
        self.assertEqual(UpdateCheck(12, 1, False), parse_check(text))

    def test_failed_check_carries_a_safe_error_code(self) -> None:
        for text, code in (
            ("ERROR=relay-unreachable\n", "relay_unreachable"),
            ("ERROR=$(reboot)\n", "update_check_failed"),
            ("PACKAGES=3\n", "update_check_failed"),
        ):
            with self.subTest(text=text), self.assertRaises(UpdateNodeError) as caught:
                parse_check(text)
            self.assertEqual(code, caught.exception.code)


class HelperCallTests(unittest.TestCase):
    def test_every_helper_call_checks_the_hash_before_sudo(self) -> None:
        calls = {
            "check": lambda ops: ops.check("spark-a"),
            "download": lambda ops: ops.start_run("spark-a", "download"),
            "apply": lambda ops: ops.start_run("spark-a", "apply"),
            "status": lambda ops: ops.run_status("spark-a"),
            "reboot": lambda ops: ops.reboot("spark-a"),
        }
        for subcommand, call in calls.items():
            mismatch = SSHExecution(3, "ERROR=helper-mismatch\n", "")
            executor = RecordingExecutor(mismatch)
            with self.subTest(subcommand), self.assertRaises(UpdateNodeError) as caught:
                call(SSHUpdateNodeOps(executor, {}))
            self.assertEqual("helper_mismatch", caught.exception.code)
            [(_kind, script, _stdin)] = executor.calls
            assert isinstance(script, str)
            self.assertLess(script.index(f"!= {SHA}"), script.index("sudo"))
            self.assertTrue(script.endswith(f'exec sudo -n "$helper" {subcommand}\n'))

    def test_start_run_accepts_only_download_and_apply(self) -> None:
        executor = RecordingExecutor()
        for mode in ("check", "install", "run-apply", "apply; reboot"):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                SSHUpdateNodeOps(executor, {}).start_run("spark-a", mode)
        self.assertEqual([], executor.calls)

    def test_probe_asks_sudo_for_status_only_after_the_hash_matches(self) -> None:
        executor = RecordingExecutor(SSHExecution(0, f"BOOT_ID={BOOT_ID}\n", ""))
        SSHUpdateNodeOps(executor, {}).facts("spark-a")

        [(_kind, script, _stdin)] = executor.calls
        assert isinstance(script, str)
        self.assertIn(f"expected={SHA}\n", script)
        self.assertEqual(1, script.count("sudo"))
        self.assertLess(
            script.index('[ "$actual" = "$expected" ] && '), script.index("sudo -n")
        )

    def test_closing_the_relay_ends_every_tunnel(self) -> None:
        processes = [FakeTunnelProcess(), FakeTunnelProcess()]
        token = SSHExecution(0, f"{RELAY.token}\n", "")
        executor = RecordingExecutor(token, token)
        with patch(
            "controller.updates.nodeops.subprocess.Popen", side_effect=processes
        ) as popen:
            tunnels = SSHUpdateNodeOps(executor, {}).open_relay(["a", "b"], RELAY)
        self.assertEqual(
            [
                list(executor.reverse_tunnel_command(node_id, RELAY_REMOTE_PORT, 1234))
                for node_id in ("a", "b")
            ],
            [call.args[0] for call in popen.call_args_list],
        )
        self.assertEqual(subprocess.PIPE, popen.call_args.kwargs["stdin"])

        tunnels.close()
        for process in processes:
            process.stdin.close.assert_called_once_with()
            self.assertTrue(process.terminated)

    def test_tunnel_must_reach_this_relay_session(self) -> None:
        process = FakeTunnelProcess()
        clock = Mock(monotonic=Mock(side_effect=[0, 0, 31]))
        executor = RecordingExecutor(SSHExecution(0, "online\n", ""))
        with (
            patch("controller.updates.nodeops.subprocess.Popen", return_value=process),
            patch("controller.updates.nodeops.time", clock),
            self.assertRaises(UpdateNodeError) as caught,
        ):
            SSHUpdateNodeOps(executor, {}).open_relay(["spark-a"], RELAY)

        self.assertEqual("relay_tunnel_failed", caught.exception.code)
        self.assertTrue(process.terminated)

    def test_open_relay_closes_every_tunnel_when_one_fails(self) -> None:
        alive, closed = FakeTunnelProcess(), FakeTunnelProcess(returncode=255)
        executor = RecordingExecutor(SSHExecution(0, f"{RELAY.token}\n", ""))
        with (
            patch(
                "controller.updates.nodeops.subprocess.Popen",
                side_effect=[alive, closed],
            ),
            self.assertRaises(UpdateNodeError) as caught,
        ):
            SSHUpdateNodeOps(executor, {}).open_relay(["spark-a", "spark-b"], RELAY)

        self.assertEqual("relay_tunnel_failed", caught.exception.code)
        self.assertTrue(alive.terminated)


class FakeTunnelProcess:
    def __init__(self, returncode: int | None = None) -> None:
        self.returncode = returncode
        self.terminated = False
        self.stdin = Mock()

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def wait(self, timeout: float | None = None) -> int | None:
        return self.returncode


@unittest.skipUnless(os.access("/bin/bash", os.X_OK), "bash is required")
class HelperGateScriptTests(unittest.TestCase):
    """Run the helper call script under stub ``sha256sum`` and ``sudo``."""

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.sudo_log = self.root / "sudo.log"
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        for name, body in (
            ("sha256sum", 'printf "%s  %s\\n" "$INSTALLED" "$1"\n'),
            ("sudo", 'printf "%s\\n" "$*" >> "$SUDO_LOG"\necho STATE=idle\n'),
        ):
            (bin_dir / name).write_text(f"#!/bin/sh\n{body}")
            (bin_dir / name).chmod(0o755)
        self.path = f"{bin_dir}:{os.environ['PATH']}"

    def ops(self, installed: str) -> SSHUpdateNodeOps:
        environment = {
            "PATH": self.path,
            "INSTALLED": installed,
            "SUDO_LOG": str(self.sudo_log),
        }
        return SSHUpdateNodeOps(LocalExecutor(self.root, environment), {})

    def test_runs_sudo_when_the_installed_hash_matches(self) -> None:
        self.assertEqual("idle", self.ops(SHA).run_status("spark-a").state)
        self.assertEqual(f"-n {HELPER_PATH} status\n", self.sudo_log.read_text())

    def test_never_runs_sudo_for_another_hash(self) -> None:
        with self.assertRaises(UpdateNodeError) as caught:
            self.ops("0" * 64).run_status("spark-a")
        self.assertEqual("helper_mismatch", caught.exception.code)
        self.assertFalse(self.sudo_log.exists())


class InstallHelperTests(unittest.TestCase):
    def test_stages_then_sends_the_password_only_on_stdin(self) -> None:
        executor = RecordingExecutor(
            SSHExecution(0, "STAGED=1\n", ""),
            SSHExecution(0, f"INSTALLED=1\nSHA256={SHA}\n", ""),
        )
        SSHUpdateNodeOps(executor, {}).install_helper("spark-a", PASSWORD)

        stage, install, cleanup = executor.calls
        self.assertIn(base64.b64encode(helper_bytes()).decode(), stage[1])
        self.assertEqual(("argv", INSTALL_ARGV, f"{PASSWORD}\n"), install)
        self.assertEqual(("script", CLEANUP, None), cleanup)
        for _kind, command, _stdin in executor.calls:
            words = command if isinstance(command, str) else " ".join(command)
            self.assertNotIn(PASSWORD, words)

    def test_failures_map_to_codes_and_still_remove_the_staged_copy(self) -> None:
        staged = SSHExecution(0, "STAGED=1\n", "")
        refused = SSHExecution(1, "", "Sorry, try again.\n")
        helper_error = SSHExecution(1, "ERROR=invalid-sudo-user\n", "")
        other_helper = SSHExecution(0, f"INSTALLED=1\nSHA256={'0' * 64}\n", "")
        for code, results in (
            ("helper_stage_failed", [SSHExecution(1, "", "mktemp: failed\n")]),
            ("node_unreachable", [staged, SSHExecutionError("timed out")]),
            ("sudo_authentication_failed", [staged, refused]),
            ("invalid_sudo_user", [staged, helper_error]),
            ("helper_mismatch", [staged, other_helper]),
        ):
            executor = RecordingExecutor(*results)
            with self.subTest(code=code), self.assertRaises(UpdateNodeError) as caught:
                SSHUpdateNodeOps(executor, {}).install_helper("spark-a", PASSWORD)
            self.assertEqual(code, caught.exception.code)
            self.assertEqual(("script", CLEANUP, None), executor.calls[-1])

    def test_rejects_an_unusable_password_before_any_ssh(self) -> None:
        executor = RecordingExecutor()
        for password in ("", "secret\nreboot", "x" * 257, None):
            with self.subTest(password=password), self.assertRaises(ValueError):
                SSHUpdateNodeOps(executor, {}).install_helper("spark-a", password)
        self.assertEqual([], executor.calls)

    def test_installs_through_the_ssh_executor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory) / "key"
            key.write_text("not-a-real-private-key")
            node = SSHNode("spark-a", "192.0.2.10", "spark", str(key))
            completed = [
                CompletedProcess([], 0, "STAGED=1\n", ""),
                CompletedProcess([], 0, f"INSTALLED=1\nSHA256={SHA}\n", ""),
                CompletedProcess([], 0, "", ""),
            ]
            with patch(
                "controller.transport.ssh.subprocess.run", side_effect=completed
            ):
                SSHUpdateNodeOps(SSHExecutor({"spark-a": node}), {}).install_helper(
                    "spark-a", PASSWORD
                )


@unittest.skipUnless(os.access("/bin/bash", os.X_OK), "bash is required")
class StageScriptTests(unittest.TestCase):
    """Run the staging script in a temporary home with the real tools."""

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.executor = LocalExecutor(self.home, {"PATH": os.environ["PATH"]})

    def test_stages_the_packaged_helper_privately_and_removes_it(self) -> None:
        SSHUpdateNodeOps(self.executor, {}).install_helper("spark-a", PASSWORD)

        self.assertEqual([(helper_bytes(), 0o600)], self.executor.installed)
        self.assertEqual([], [path for path in self.home.rglob("*") if path.is_file()])

    def test_never_runs_sudo_for_a_staged_copy_with_another_hash(self) -> None:
        ops = SSHUpdateNodeOps(self.executor, {}, expected_sha256="0" * 64)
        with self.assertRaises(UpdateNodeError) as caught:
            ops.install_helper("spark-a", PASSWORD)
        self.assertEqual("helper_stage_failed", caught.exception.code)
        self.assertEqual([], self.executor.installed)
        self.assertEqual([], [path for path in self.home.rglob("*") if path.is_file()])


class ProbeScriptTests(unittest.TestCase):
    """The real probe script, run against stub commands, feeds the parser."""

    def test_headless_node_waiting_only_on_the_splash_is_booted(self) -> None:
        with tempfile.TemporaryDirectory() as work:
            stubs = {
                "systemctl": """case "$*" in
  is-system-running) echo starting ;;
  "--user is-system-running") echo running ;;
  "list-jobs --no-legend")
    echo "212 plymouth-quit-wait.service start running"
    echo "2 multi-user.target start waiting" ;;
esac""",
                "docker": "exit 0",
                "nvidia-smi": "exit 0",
                "ip": "echo '4: eth inet 10.0.0.1/24 brd 10.0.0.255 scope global'",
            }
            bin_dir = Path(work, "bin")
            bin_dir.mkdir()
            for name, body in stubs.items():
                stub = bin_dir / name
                stub.write_text(f"#!/bin/sh\n{body}\n")
                stub.chmod(0o755)
            executor = LocalExecutor(
                Path(work), {"PATH": f"{bin_dir}:{os.environ['PATH']}"}
            )
            facts = SSHUpdateNodeOps(executor, {"spark-a": ("10.0.0.1",)}).facts(
                "spark-a"
            )
        self.assertEqual(
            ("starting", 0, "running"),
            (facts.system_state, facts.boot_jobs, facts.user_manager),
        )
        self.assertTrue(facts.booted)


class FakeUpdateNodeOpsTests(unittest.TestCase):
    def test_apply_run_then_reboot_changes_the_boot_id(self) -> None:
        ops = FakeUpdateNodeOps(["spark-a"], polls_per_run=2, reboot_polls=1)
        ops.install_helper("spark-a", PASSWORD)
        with relay_tunnels(ops, ["spark-a"], RELAY):
            ops.start_run("spark-a", "apply")
            self.assertEqual("running", ops.run_status("spark-a").state)
            finished = ops.run_status("spark-a")
        self.assertEqual(
            ("succeeded", True), (finished.state, finished.reboot_required)
        )

        previous = ops.reboot("spark-a")
        self.assertFalse(ops.facts("spark-a").reachable)
        facts = ops.facts("spark-a")
        self.assertTrue(facts.booted)
        self.assertNotEqual(previous, facts.boot_id)

    def test_wrong_password_leaves_the_helper_missing(self) -> None:
        ops = FakeUpdateNodeOps(["spark-a"], password=PASSWORD)
        with self.assertRaises(UpdateNodeError) as caught:
            ops.install_helper("spark-a", "wrong")
        self.assertEqual("sudo_authentication_failed", caught.exception.code)
        self.assertEqual("missing", ops.facts("spark-a").helper)

    def test_downloads_need_an_open_relay(self) -> None:
        ops = FakeUpdateNodeOps(["spark-a"], helper="ready")
        with relay_tunnels(ops, ["spark-a"], RELAY):
            ops.check("spark-a")
        for call in (ops.check, lambda node_id: ops.start_run(node_id, "download")):
            with self.assertRaises(UpdateNodeError) as caught:
                call("spark-a")
            self.assertEqual("relay_unreachable", caught.exception.code)


if __name__ == "__main__":
    unittest.main()
