# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""The root update helper, exercised only as a non-root user and as text."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from controller.updates import nodeops

SCRIPT = (
    Path(nodeops.__file__).with_name("templates") / "dgx-spark-controller-update.sh"
)
SOURCE = SCRIPT.read_text(encoding="utf-8")
BASH = shutil.which("bash") or ""
HELPER = "/usr/local/sbin/dgx-spark-controller-update"


@unittest.skipUnless(BASH, "bash is required")
class HelperScriptTests(unittest.TestCase):
    def run_bash(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [BASH, *args],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env={"PATH": "/usr/bin:/bin"},
        )

    def test_script_parses(self) -> None:
        result = self.run_bash("-n", str(SCRIPT))
        self.assertEqual(0, result.returncode, result.stderr)

    def test_version_needs_no_privileges(self) -> None:
        result = self.run_bash(str(SCRIPT), "version")
        self.assertEqual((0, "HELPER_VERSION=1\n"), (result.returncode, result.stdout))

    def test_anything_but_one_known_subcommand_is_a_usage_error(self) -> None:
        for args in ((), ("status", "extra"), ("shell",), ("",), ("--help",)):
            with self.subTest(args=args):
                result = self.run_bash(str(SCRIPT), *args)
                self.assertEqual(
                    (64, "ERROR=usage\n"), (result.returncode, result.stdout)
                )

    @unittest.skipIf(os.geteuid() == 0, "the helper must not run as root here")
    def test_privileged_subcommands_refuse_a_non_root_user(self) -> None:
        for subcommand in (
            "check",
            "download",
            "apply",
            "status",
            "reboot",
            "install",
            "uninstall",
            "run-download",
            "run-apply",
        ):
            with self.subTest(subcommand=subcommand):
                result = self.run_bash(str(SCRIPT), subcommand)
                self.assertEqual(
                    (77, "ERROR=not-root\n"), (result.returncode, result.stdout)
                )

    def test_sudoers_rule_allows_only_the_unprivileged_entry_points(self) -> None:
        block = re.search(r"\n  \{\n(.*?)\n  \} >\"\$tmp_sudoers\"", SOURCE, re.S)
        assert block is not None
        result = self.run_bash("-c", f"HELPER={HELPER}\nuser=spark\n{block[1]}")

        rule = result.stdout.splitlines()[-1]
        allowed = ", ".join(
            f"{HELPER} {command}"
            for command in ("check", "download", "apply", "status", "reboot")
        )
        self.assertEqual(f"spark ALL=(root) NOPASSWD: {allowed}", rule)


class HelperSourceTests(unittest.TestCase):
    def test_packaged_hash_is_the_hash_of_the_file(self) -> None:
        self.assertEqual(
            hashlib.sha256(SCRIPT.read_bytes()).hexdigest(), nodeops.helper_sha256()
        )

    def test_never_sources_or_evaluates_anything(self) -> None:
        command = re.compile(
            r"(?:^|[;&|(]|\b(?:then|do|else)\b)\s*(?:source|\.|eval)\s"
        )
        self.assertEqual(
            [], [line for line in SOURCE.splitlines() if command.search(line)]
        )
        self.assertNotRegex(SOURCE, r"\beval\b")

    def test_path_is_fixed_before_any_command_runs(self) -> None:
        lines = SOURCE.splitlines()
        export = lines.index("export PATH=/usr/sbin:/usr/bin:/sbin:/bin")
        preamble = [line for line in lines[:export] if line and line[0] != "#"]
        self.assertEqual(["set -euo pipefail", "umask 022"], preamble)

    def test_apt_refuses_insecure_repositories(self) -> None:
        for line in (
            'Acquire::AllowInsecureRepositories "false";',
            'Acquire::AllowDowngradeToInsecureRepositories "false";',
        ):
            self.assertIn(f"\n{line}\n", SOURCE)


class AptSourcesRewriteTests(unittest.TestCase):
    """Run the helper's embedded source rewrite against a temporary /etc/apt."""

    SIGNED = {
        "sources.list": "deb http://ports.ubuntu.com/ubuntu-ports noble main\n",
        "sources.list.d/nvidia.list": "deb https://repo.download.nvidia.com/x r main\n",
    }
    END_TO_END = {
        "sources.list.d/pro.sources": (
            "Types: deb\nURIs: https://esm.ubuntu.com/apps/ubuntu\n"
            "Suites: noble-apps-security\nComponents: main\n"
        ),
        "sources.list.d/local.list": "deb [trusted=yes] https://unsigned.example/ ./\n",
        "sources.list.d/weak.sources": (
            "Types: deb\nURIs: https://weak.example/\nSuites: ./\nAllow-Weak: yes\n"
        ),
    }

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        auth = self.root / "etc/apt/auth.conf.d/90ubuntu-advantage"
        auth.parent.mkdir(parents=True)
        auth.write_text("machine esm.ubuntu.com/apps/ubuntu/ login bearer password x\n")

    def rewrite(self, mode: str, sources: dict[str, str]) -> dict[str, str]:
        etc = self.root / "etc/apt"
        (etc / "sources.list.d").mkdir(exist_ok=True)
        for name, text in sources.items():
            (etc / name).write_text(text)
        target = self.root / mode
        (target / "sources.list.d").mkdir(parents=True)
        program = re.search(r"<<'PY'[^\n]*\n(.*?)\nPY\n", SOURCE, re.S)
        assert program is not None
        subprocess.run(
            [sys.executable, "-I", "-", mode, str(target)],
            input=program[1].replace('"/etc/apt/', f'"{etc}/'),
            text=True,
            check=True,
            timeout=30,
        )
        return {
            str(path.relative_to(target)): path.read_text()
            for path in target.rglob("*")
            if path.is_file()
        }

    def test_signed_sources_reach_the_relay_over_plain_http(self) -> None:
        nvidia = "deb http://repo.download.nvidia.com/x r main\n"
        self.assertEqual(
            {**self.SIGNED, "sources.list.d/nvidia.list": nvidia},
            self.rewrite("online", self.SIGNED),
        )

    def test_credentialed_and_unsigned_sources_stay_end_to_end(self) -> None:
        self.assertEqual(
            {"sources.list": "", **self.END_TO_END},
            self.rewrite("online", self.END_TO_END),
        )
        self.assertEqual(
            {"sources.list": ""}, self.rewrite("directory", self.END_TO_END)
        )

    def test_unsigned_sources_are_recognised_by_any_true_value(self) -> None:
        unsigned = {
            "sources.list.d/true.list": "deb [trusted=true] https://u.example/ ./\n",
            "sources.list.d/one.sources": (
                "Types: deb\nURIs: https://weak.example/\nSuites: ./\nTrusted: 1\n"
            ),
        }
        self.assertEqual({"sources.list": ""}, self.rewrite("directory", unsigned))


if __name__ == "__main__":
    unittest.main()
