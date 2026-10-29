# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Tests for host validation, host-key scanning, and the trusted store.

Public-key fixtures are throwaway public halves (their private halves were
discarded); expected fingerprints were produced by ``ssh-keygen -lf``.
"""

import base64
import hashlib
import hmac
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

from controller.discovery.hostkeys import (
    HostKey,
    KnownHostEntry,
    KnownHostsStore,
    fingerprint,
    keyscan_command,
    known_hosts_host,
    known_hosts_lookup_command,
    known_hosts_status,
    parse_keyscan_output,
    parse_known_hosts_lookup,
    scan_host_keys,
    ssh_config_path_value,
    ssh_known_hosts_options,
    user_known_host_keys,
    validate_host,
)
from controller.discovery.network import DiscoveryError

ED25519 = "AAAAC3NzaC1lZDI1NTE5AAAAIAABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4f"
ED25519_FP = "SHA256:ZkAslGjFiUHdGf/WUL8rQvkib4PTvQatUV0OUQSncCA"
ECDSA = (
    "AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBO2mQpW7u/7dSbrvt7X+"
    "B6cXRYHHOzckh+db/GOPsGKHK91DuCzPdXrqiHUdyVj3t/LL3Imb3BmMJ5kingsn2+Q="
)
ECDSA_FP = "SHA256:fcnX8bTy2fadY1720RFvPERxO/I/Edqh2SAA8+K0OBs"
RSA = (
    "AAAAB3NzaC1yc2EAAAADAQABAAABAQDL+7Dd+J4fit0QMWF9lMbKrJbbcVKyfvJ9f+UIHn7+"
    "a0lXA/jZrDxb2e41ijx6+C/QGRkg3qJEv2tSpQ6UsDG3KRkTYP5bvZMlGnWxBRK8gukG7BwD"
    "JszcdsWQYR/Nu0bd/MNlzPlzUKOMn6tHeaEE8LlE1O0zN1Jij3qYoDX7IkuvFU9Wg4KxfzT1"
    "J48mdWx+pNPPv7oJkSVDr/ShPxpkwZ2b0oHVjWyMlcDvyGalwsgHxVZ8npeqL6JBb3LBVzXV"
    "z0YhTxrsXCsBNg49uGBgZ9XG3mQO45ivVXPjbBq96u1ypooS/rvLfyxZuoSW1OGpkPvPV6w5"
    "UwUH5IZiKoEZ"
)
RSA_FP = "SHA256:TabztiQrWJmAw5SZMv3Dd+xRME/F9Hx/4hq6EyUFdks"

KEYSCAN_OUTPUT = f"""\
# 192.168.1.20:22 SSH-2.0-OpenSSH_9.6p1 Ubuntu-3ubuntu13.5
192.168.1.20 ssh-rsa {RSA}
192.168.1.20 ecdsa-sha2-nistp256 {ECDSA}
192.168.1.20 ssh-ed25519 {ED25519}
192.168.1.20 ssh-ed25519 {ED25519}
192.168.1.20 ssh-dss AAAAB3NzaC1kc3MAAACBAP
192.168.1.20 ssh-ed25519 !!!notbase64!!!
192.168.1.20 ssh-rsa {ED25519}
garbage
"""


class FakeRunner:
    """Record one subprocess call and return or raise a scripted result."""

    def __init__(self, result: Any, returncode: int = 0) -> None:
        """Script stdout text (with an exit status) or an exception."""
        self.result = result
        self.returncode = returncode
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def __call__(self, argv: list[str], **kwargs: Any) -> Any:
        """Return the scripted result."""
        self.calls.append((argv, kwargs))
        if isinstance(self.result, BaseException):
            raise self.result
        return subprocess.CompletedProcess(
            argv, self.returncode, self.result, "# banner\n"
        )


class ValidateHostTests(unittest.TestCase):
    """Host canonicalization and rejection."""

    def test_valid_hosts(self) -> None:
        """Names are lowercased without trailing dot; IPs are compressed."""
        cases = {
            "spark-1a2b.local": "spark-1a2b.local",
            "Spark-1A2B.Local.": "spark-1a2b.local",
            "gx10-0042": "gx10-0042",
            "xn--bcher-kva.example": "xn--bcher-kva.example",
            "192.168.1.5": "192.168.1.5",
            "[fe80::1]": "fe80::1",
            "2001:DB8:0:0::0001": "2001:db8::1",
            "a" * 63 + ".local": "a" * 63 + ".local",
            "host.a1": "host.a1",
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(validate_host(value), expected)
        long_name = ".".join(["a" * 63] * 3 + ["b" * 61])
        self.assertEqual(len(long_name), 253)
        self.assertEqual(validate_host(long_name), long_name)

    def test_invalid_hosts(self) -> None:
        """Option/line injection, bad labels, numeric names, and zones fail."""
        for value in (
            "",
            " spark",
            "spark ",
            "spark 1",
            "spark\n",
            "spark\tx",
            "-oProxyCommand=evil",
            "-spark",
            "spark-.local",
            "user@spark",
            "spark:22",
            "spark_1",
            "spark..local",
            ".spark",
            "a" * 64 + ".local",
            ".".join(["a" * 63] * 4),
            "1.2.3",
            "999.1.1.1",
            "192.168.001.1",
            "host.123",
            "10.1",
            "1234",
            "host.0x7f",
            "fe80::1%en0",
            "[spark.local]",
            "[192.168.1.5",
            "spark,other",
            "sp\u00e4r",
            None,
            42,
        ):
            with self.subTest(value=value), self.assertRaises(DiscoveryError) as caught:
                validate_host(value)  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "invalid_host")

    def test_known_hosts_host(self) -> None:
        """Port 22 is bare; other ports use the bracket form."""
        self.assertEqual(known_hosts_host("Spark.local"), "spark.local")
        self.assertEqual(known_hosts_host("spark.local", 2222), "[spark.local]:2222")
        self.assertEqual(known_hosts_host("fe80::1", 2222), "[fe80::1]:2222")
        for port in (0, 65536, True, "22"):
            with self.subTest(port=port), self.assertRaises(DiscoveryError):
                known_hosts_host("spark", port)  # type: ignore[arg-type]


class FingerprintTests(unittest.TestCase):
    """SHA-256 fingerprints and key objects."""

    def test_matches_ssh_keygen(self) -> None:
        """Fingerprints equal what ``ssh-keygen -lf`` printed for the fixtures."""
        self.assertEqual(fingerprint(ED25519), ED25519_FP)
        self.assertEqual(fingerprint(ECDSA), ECDSA_FP)
        self.assertEqual(fingerprint(RSA), RSA_FP)

    def test_rejects_bad_blobs(self) -> None:
        """Bad base64 or truncated blobs fail."""
        for blob in (
            "",
            "!!!",
            "AAAA",
            "AAAAC3Nza",
            "QUJD",
            "A" * 30000,
            ED25519 + "\n",
        ):
            with (
                self.subTest(blob=blob[:20]),
                self.assertRaises(DiscoveryError) as caught,
            ):
                fingerprint(blob)
            self.assertEqual(caught.exception.code, "invalid_host_key")

    def test_host_key_validation(self) -> None:
        """Keys canonicalize the host and reject inconsistent data."""
        key = HostKey.create("Spark-1A2B.local.", 22, "ssh-ed25519", ED25519)
        self.assertEqual(key.host, "spark-1a2b.local")
        self.assertEqual(key.fingerprint_sha256, ED25519_FP)
        self.assertEqual(
            key.known_hosts_line(), f"spark-1a2b.local ssh-ed25519 {ED25519}"
        )
        other_port = HostKey.create("fe80::1", 2222, "ecdsa-sha2-nistp256", ECDSA)
        self.assertEqual(
            other_port.known_hosts_line(), f"[fe80::1]:2222 ecdsa-sha2-nistp256 {ECDSA}"
        )
        bad = (
            lambda: HostKey.create("spark", 22, "ssh-rsa", ED25519),
            lambda: HostKey.create("spark", 22, "ssh-dss", ED25519),
            lambda: HostKey("spark", 22, "ssh-ed25519", ED25519, RSA_FP),
            lambda: HostKey.create("bad host", 22, "ssh-ed25519", ED25519),
            lambda: HostKey.create("spark", 0, "ssh-ed25519", ED25519),
        )
        for index, call in enumerate(bad):
            with self.subTest(index=index), self.assertRaises(DiscoveryError):
                call()


class KeyscanTests(unittest.TestCase):
    """ssh-keyscan argv, parsing, and failure handling."""

    def test_parse_output(self) -> None:
        """Comments, unsupported types, garbage, mismatches, and dups are dropped."""
        keys = parse_keyscan_output(KEYSCAN_OUTPUT, "192.168.1.20", 22)
        self.assertEqual(
            [(key.key_type, key.fingerprint_sha256) for key in keys],
            [
                ("ssh-ed25519", ED25519_FP),
                ("ecdsa-sha2-nistp256", ECDSA_FP),
                ("ssh-rsa", RSA_FP),
            ],
        )
        self.assertTrue(all(key.host == "192.168.1.20" for key in keys))
        self.assertEqual(parse_keyscan_output("", "spark", 22), [])
        bracketed = parse_keyscan_output(
            f"[spark]:2222 ssh-ed25519 {ED25519}", "spark", 2222
        )
        self.assertEqual(bracketed[0].port, 2222)

    def test_command_is_fixed_argv(self) -> None:
        """The host is validated and placed after ``--``."""
        self.assertEqual(
            keyscan_command("Spark-1A2B.local", port=2222, timeout=7),
            [
                "ssh-keyscan",
                "-T",
                "7",
                "-p",
                "2222",
                "-t",
                "ed25519,ecdsa,rsa",
                "--",
                "spark-1a2b.local",
            ],
        )
        for kwargs in ({"timeout": 0}, {"timeout": 61}, {"timeout": 1.5}, {"port": 0}):
            with self.subTest(kwargs=kwargs), self.assertRaises(DiscoveryError):
                keyscan_command("spark", **kwargs)  # type: ignore[arg-type]

    def test_scan_host_keys_success(self) -> None:
        """The runner gets the argv, a bounded timeout, and no stdin."""
        runner = FakeRunner(KEYSCAN_OUTPUT)
        keys = scan_host_keys("192.168.1.20", timeout=5, runner=runner)
        self.assertEqual(len(keys), 3)
        argv, kwargs = runner.calls[0]
        self.assertEqual(argv[-2:], ["--", "192.168.1.20"])
        self.assertEqual(kwargs["timeout"], 25)
        self.assertIs(kwargs["stdin"], subprocess.DEVNULL)
        self.assertIs(kwargs["check"], False)

    def test_scan_host_keys_failures(self) -> None:
        """Missing binary, timeout, and empty output are ``host_key_scan_failed``."""
        for result in (
            FileNotFoundError("ssh-keyscan"),
            subprocess.TimeoutExpired("ssh-keyscan", 25),
            PermissionError("denied"),
            "",
            "# only a comment\n",
        ):
            with (
                self.subTest(result=result),
                self.assertRaises(DiscoveryError) as caught,
            ):
                scan_host_keys("192.168.1.20", runner=FakeRunner(result))
            self.assertEqual(caught.exception.code, "host_key_scan_failed")
            self.assertNotIn("banner", caught.exception.message)

    def test_invalid_host_never_runs(self) -> None:
        """Validation happens before the subprocess."""
        runner = FakeRunner(KEYSCAN_OUTPUT)
        with self.assertRaises(DiscoveryError):
            scan_host_keys("-oProxyCommand=touch /tmp/pwned", runner=runner)
        self.assertEqual(runner.calls, [])


def hashed_host(host: str, salt: bytes = bytes(range(20))) -> str:
    """Hash a host name like OpenSSH ``HashKnownHosts``."""
    digest = hmac.new(salt, host.encode(), hashlib.sha1).digest()
    return f"|1|{base64.b64encode(salt).decode()}|{base64.b64encode(digest).decode()}"


LOOKUP_OUTPUT = f"""\
# Host 192.168.1.20 found: line 3
192.168.1.20 ssh-ed25519 {ED25519}
# Host 192.168.1.20 found: line 7
{hashed_host("192.168.1.20")} ecdsa-sha2-nistp256 {ECDSA}
# Host 192.168.1.20 found: line 9 REVOKED
@revoked 192.168.1.20 ssh-rsa {RSA}
# Host 192.168.1.20 found: line 11 CA
@cert-authority 192.168.1.20 ssh-ed25519 {ED25519}
192.168.1.20 ssh-dss AAAAB3NzaC1kc3MAAACBAP
192.168.1.20 ssh-ed25519 !!!notbase64!!!
192.168.1.20 ssh-ed25519 {ED25519}
"""


class UserKnownHostsTests(unittest.TestCase):
    """Read-only comparison with the operator's own known_hosts."""

    def setUp(self) -> None:
        """Create a temporary known_hosts; the real ~/.ssh is never read."""
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "known_hosts"
        self.path.write_text("", encoding="utf-8")

    def tearDown(self) -> None:
        """Remove the temporary directory."""
        self.temporary.cleanup()

    def test_command_is_fixed_argv(self) -> None:
        """The validated host token is the single argument of ``-F``."""
        self.assertEqual(
            known_hosts_lookup_command("Spark-1A2B.local", 22, self.path),
            ["ssh-keygen", "-F", "spark-1a2b.local", "-f", str(self.path)],
        )
        self.assertEqual(
            known_hosts_lookup_command("192.168.1.20", 2222, self.path)[2],
            "[192.168.1.20]:2222",
        )
        with self.assertRaises(DiscoveryError):
            known_hosts_lookup_command("spark", 22, Path("relative/known_hosts"))
        runner = FakeRunner(LOOKUP_OUTPUT)
        with self.assertRaises(DiscoveryError):
            user_known_host_keys(
                "-oProxyCommand=x", known_hosts_path=self.path, runner=runner
            )
        self.assertEqual(runner.calls, [])

    def test_parse_lookup_output(self) -> None:
        """Hashed and revoked entries count; CA lines and junk are skipped."""
        self.assertEqual(
            parse_known_hosts_lookup(LOOKUP_OUTPUT),
            [
                KnownHostEntry("ssh-ed25519", ED25519_FP),
                KnownHostEntry("ecdsa-sha2-nistp256", ECDSA_FP),
                KnownHostEntry("ssh-rsa", RSA_FP, revoked=True),
            ],
        )

    def test_lookup_results(self) -> None:
        """Found, not found, missing file, and failures that hide the answer."""
        runner = FakeRunner(LOOKUP_OUTPUT)
        entries = user_known_host_keys(
            "192.168.1.20", known_hosts_path=self.path, runner=runner
        )
        self.assertEqual(len(entries or []), 3)
        argv, kwargs = runner.calls[0]
        self.assertEqual(argv[:3], ["ssh-keygen", "-F", "192.168.1.20"])
        self.assertIs(kwargs["stdin"], subprocess.DEVNULL)
        self.assertEqual(kwargs["timeout"], 5)
        self.assertEqual(
            user_known_host_keys(
                "spark", known_hosts_path=self.path, runner=FakeRunner("", 1)
            ),
            [],
        )
        missing = FakeRunner(LOOKUP_OUTPUT)
        self.assertEqual(
            user_known_host_keys(
                "spark", known_hosts_path=self.path.with_name("absent"), runner=missing
            ),
            [],
        )
        self.assertEqual(missing.calls, [])
        for runner in (
            FakeRunner("", 255),
            FakeRunner(FileNotFoundError("ssh-keygen")),
            FakeRunner(subprocess.TimeoutExpired("ssh-keygen", 5)),
        ):
            with self.subTest(runner=runner.result):
                self.assertIsNone(
                    user_known_host_keys(
                        "spark", known_hosts_path=self.path, runner=runner
                    )
                )

    def test_status(self) -> None:
        """Each scanned key is compared with recorded keys of the same type."""
        ed = HostKey.create("spark", 22, "ssh-ed25519", ED25519)
        rsa = HostKey.create("spark", 22, "ssh-rsa", RSA)
        recorded = [KnownHostEntry("ssh-ed25519", ED25519_FP)]
        self.assertEqual(known_hosts_status(ed, recorded), "match")
        self.assertEqual(known_hosts_status(rsa, recorded), "absent")
        self.assertEqual(known_hosts_status(ed, []), "absent")
        self.assertEqual(known_hosts_status(ed, None), "unknown")
        other = [KnownHostEntry("ssh-ed25519", RSA_FP)]
        self.assertEqual(known_hosts_status(ed, other), "mismatch")
        revoked = [KnownHostEntry("ssh-ed25519", ED25519_FP, revoked=True)]
        self.assertEqual(known_hosts_status(ed, revoked), "revoked")

    @unittest.skipUnless(shutil.which("ssh-keygen"), "OpenSSH tools not installed")
    def test_real_ssh_keygen_finds_plain_and_hashed_entries(self) -> None:
        """``ssh-keygen -F`` against a temporary file matches hashed names too."""
        self.path.write_text(
            f"192.0.2.10 ssh-ed25519 {ED25519}\n"
            f"{hashed_host('spark-1a2b.local')} ecdsa-sha2-nistp256 {ECDSA}\n"
            f"[192.0.2.10]:2222 ssh-rsa {RSA}\n",
            encoding="utf-8",
        )
        self.assertEqual(
            user_known_host_keys("192.0.2.10", known_hosts_path=self.path),
            [KnownHostEntry("ssh-ed25519", ED25519_FP)],
        )
        self.assertEqual(
            user_known_host_keys("spark-1a2b.local", known_hosts_path=self.path),
            [KnownHostEntry("ecdsa-sha2-nistp256", ECDSA_FP)],
        )
        self.assertEqual(
            user_known_host_keys("192.0.2.10", 2222, known_hosts_path=self.path),
            [KnownHostEntry("ssh-rsa", RSA_FP)],
        )
        self.assertEqual(
            user_known_host_keys("192.0.2.99", known_hosts_path=self.path), []
        )


class KnownHostsStoreTests(unittest.TestCase):
    """The controller-owned known_hosts file."""

    def setUp(self) -> None:
        """Create a private temporary directory."""
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.path = self.root / "config dir" / "known_hosts"
        self.store = KnownHostsStore(self.path)
        self.ed = HostKey.create("spark-1a2b.local", 22, "ssh-ed25519", ED25519)
        self.ecdsa = HostKey.create(
            "spark-1a2b.local", 22, "ecdsa-sha2-nistp256", ECDSA
        )
        self.alt = HostKey.create("spark-1a2b.local", 2222, "ssh-ed25519", ED25519)

    def tearDown(self) -> None:
        """Remove the temporary directory."""
        self.temporary.cleanup()

    def test_trust_creates_private_files_without_duplicates(self) -> None:
        """Parent 0700, file 0600, and repeated trust is idempotent."""
        self.assertEqual(self.store.trusted_keys("spark-1a2b.local"), [])
        self.store.trust([self.ed, self.ecdsa, self.ed])
        self.store.trust([self.ed])
        self.assertEqual(stat.S_IMODE(self.path.parent.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(
            self.path.read_text().splitlines(),
            [self.ed.known_hosts_line(), self.ecdsa.known_hosts_line()],
        )
        self.assertEqual(os.listdir(self.path.parent), ["known_hosts"])

    def test_lookup_is_exact_per_host_and_port(self) -> None:
        """Ports are distinct; host lookups are case-insensitive."""
        self.store.trust([self.ed, self.alt])
        self.assertEqual(self.store.trusted_keys("SPARK-1A2B.local."), [self.ed])
        self.assertEqual(self.store.trusted_keys("spark-1a2b.local", 2222), [self.alt])
        self.assertEqual(self.store.trusted_keys("spark-1a2b"), [])
        self.assertTrue(self.store.is_trusted(self.ed))
        self.assertFalse(self.store.is_trusted(self.ecdsa))
        rsa = HostKey.create("spark-1a2b.local", 22, "ssh-rsa", RSA)
        self.assertFalse(self.store.is_trusted(rsa))

    def test_preserves_foreign_lines_and_reads_multi_host_entries(self) -> None:
        """Comments, markers, hashed and unknown lines survive rewrites."""
        self.path.parent.mkdir(mode=0o700)
        foreign = [
            "# operator comment",
            "@cert-authority *.lan ssh-ed25519 " + ED25519,
            "|1|abc=|def= ssh-ed25519 " + ED25519,
            "other.lan,spark-1a2b.local ecdsa-sha2-nistp256 " + ECDSA,
            "broken line",
            "spark-1a2b.local ssh-ed25519 !!!",
        ]
        self.path.write_text("\n".join(foreign) + "\n")
        self.assertEqual(self.store.trusted_keys("spark-1a2b.local"), [self.ecdsa])
        self.store.trust([self.ed])
        self.assertEqual(
            self.path.read_text().splitlines(), [*foreign, self.ed.known_hosts_line()]
        )
        self.store.trust([self.ecdsa])
        self.assertEqual(len(self.path.read_text().splitlines()), len(foreign) + 1)

    def test_trust_edge_cases(self) -> None:
        """Empty input writes nothing; huge files fail."""
        self.store.trust([])
        self.assertFalse(self.path.exists())
        self.path.parent.mkdir(mode=0o700)
        self.path.write_text("#" * (1024 * 1024 + 1))
        with self.assertRaises(DiscoveryError) as caught:
            self.store.trusted_keys("spark")
        self.assertEqual(caught.exception.code, "known_hosts_too_large")


class SshOptionTests(unittest.TestCase):
    """OpenSSH option strings for strict host-key checking."""

    def test_plain_and_quoted_paths(self) -> None:
        """Whitespace forces quotes; %, backslash, and quote are escaped."""
        self.assertEqual(
            ssh_config_path_value("/home/u/.config/kh"), "/home/u/.config/kh"
        )
        self.assertEqual(
            ssh_config_path_value("/Users/u/Library/Application Support/X/known_hosts"),
            '"/Users/u/Library/Application Support/X/known_hosts"',
        )
        self.assertEqual(ssh_config_path_value("/a/100%/kh"), '"/a/100%%/kh"')
        self.assertEqual(ssh_config_path_value('/a"b\\c'), '"/a\\"b\\\\c"')
        self.assertEqual(ssh_config_path_value("/a#b/$x"), '"/a#b/$x"')
        for bad in ("", "/a/${HOME}/kh", "/a\nb", "/a\x00b", "/a\x7fb"):
            with self.subTest(bad=bad), self.assertRaises(DiscoveryError) as caught:
                ssh_config_path_value(bad)
            self.assertEqual(caught.exception.code, "invalid_known_hosts_path")

    def test_options(self) -> None:
        """User known_hosts first, store second, strict checking always on."""
        store = Path(
            "/Users/u/Library/Application Support/DGXSparkController/config/known_hosts"
        )
        self.assertEqual(
            ssh_known_hosts_options(store),
            (
                "-o",
                f'UserKnownHostsFile=~/.ssh/known_hosts "{store}"',
                "-o",
                "StrictHostKeyChecking=yes",
            ),
        )
        with self.assertRaises(DiscoveryError):
            ssh_known_hosts_options(Path("relative/known_hosts"))

    @unittest.skipUnless(shutil.which("ssh"), "OpenSSH client not installed")
    def test_openssh_parses_the_encoding(self) -> None:
        """``ssh -G`` (prints config, never connects) accepts the encoded paths.

        Twenty quoted paths containing spaces stay below OpenSSH's 32-file limit
        only if each quoted path is one entry, and ``%%`` must decode to ``%``.
        """
        paths = [f"/tmp/App Support {index}/100%/known_hosts" for index in range(20)]
        value = " ".join(ssh_config_path_value(path) for path in paths)
        completed = subprocess.run(
            [
                "ssh",
                "-G",
                "-F",
                "/dev/null",
                "-o",
                f"UserKnownHostsFile={value}",
                "-o",
                "StrictHostKeyChecking=yes",
                "localhost",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            stdin=subprocess.DEVNULL,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        lines = {
            line.split(" ", 1)[0]: line.split(" ", 1)[1]
            for line in completed.stdout.splitlines()
            if " " in line
        }
        self.assertEqual(lines["userknownhostsfile"], " ".join(paths))
        self.assertEqual(lines["stricthostkeychecking"], "true")


if __name__ == "__main__":
    unittest.main()
