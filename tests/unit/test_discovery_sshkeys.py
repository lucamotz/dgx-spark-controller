# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Tests for SSH client-key discovery, validation, and generation.

Every test uses a temporary HOME; ``~/.ssh`` of the real user is never touched.
"Private keys" here are placeholder text, never real key material.
"""

import hashlib
import os
import shlex
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

from controller.discovery.network import DiscoveryError
from controller.discovery.sshkeys import (
    KeyCandidate,
    copy_id_command,
    find_key_candidates,
    generate_key,
    nvidia_sync_key_paths,
    public_key_text,
    resolve_key_path,
    validate_username,
)

ED25519 = "AAAAC3NzaC1lZDI1NTE5AAAAIAABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4f"
PUBLIC_LINE = f"ssh-ed25519 {ED25519} operator@laptop"
PLACEHOLDER = "placeholder, not a private key\n"
SYNC_SUFFIX = Path("NVIDIA", "Sync", "config", "nvsync.key")


def write(path: Path, text: str, mode: int = 0o600) -> Path:
    """Write a file with an explicit mode, creating parents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    os.chmod(path, mode)
    return path


class TempHomeTestCase(unittest.TestCase):
    """Provide an isolated home directory and a second, outside directory."""

    def setUp(self) -> None:
        """Create the temporary directories."""
        self.temporary = tempfile.TemporaryDirectory()
        self.outside_dir = tempfile.TemporaryDirectory()
        self.home = Path(self.temporary.name) / "home"
        self.home.mkdir()
        self.ssh = self.home / ".ssh"
        self.ssh.mkdir(mode=0o700)
        self.outside = Path(self.outside_dir.name)

    def tearDown(self) -> None:
        """Remove the temporary directories."""
        for root in (self.home, self.outside):
            for path in root.rglob("*"):
                if not path.is_symlink():
                    os.chmod(path, 0o700 if path.is_dir() else 0o600)
        self.temporary.cleanup()
        self.outside_dir.cleanup()


class NvidiaSyncPathTests(TempHomeTestCase):
    """Platform-specific NVIDIA Sync key locations."""

    def test_macos(self) -> None:
        """MacOS uses Application Support."""
        self.assertEqual(
            nvidia_sync_key_paths(home=self.home, platform="darwin", env={}),
            [self.home / "Library" / "Application Support" / SYNC_SUFFIX],
        )

    def test_linux_candidates(self) -> None:
        """Linux lists XDG config, ~/.config, XDG data, ~/.local/share."""
        xdg_config = self.outside / "cfg"
        xdg_data = self.outside / "data"
        self.assertEqual(
            nvidia_sync_key_paths(
                home=self.home,
                platform="linux",
                env={
                    "XDG_CONFIG_HOME": str(xdg_config),
                    "XDG_DATA_HOME": str(xdg_data),
                },
            ),
            [
                xdg_config / SYNC_SUFFIX,
                self.home / ".config" / SYNC_SUFFIX,
                xdg_data / SYNC_SUFFIX,
                self.home / ".local" / "share" / SYNC_SUFFIX,
            ],
        )

    def test_linux_ignores_relative_or_duplicate_xdg(self) -> None:
        """Relative XDG values are ignored and duplicates collapse."""
        self.assertEqual(
            nvidia_sync_key_paths(
                home=self.home,
                platform="linux",
                env={
                    "XDG_CONFIG_HOME": "relative",
                    "XDG_DATA_HOME": str(self.home / ".local/share"),
                },
            ),
            [
                self.home / ".config" / SYNC_SUFFIX,
                self.home / ".local" / "share" / SYNC_SUFFIX,
            ],
        )


class FindKeyCandidatesTests(TempHomeTestCase):
    """Candidate discovery without reading private keys."""

    def populate(self) -> None:
        """Create a realistic mix of files."""
        write(self.home / "Library" / "Application Support" / SYNC_SUFFIX, PLACEHOLDER)
        write(self.ssh / "id_ed25519", PLACEHOLDER, 0o000)
        write(self.ssh / "id_ed25519.pub", PUBLIC_LINE + "\n", 0o644)
        write(self.ssh / "work", PLACEHOLDER)
        write(self.ssh / "work.pub", "garbage-type!! AAAA\n", 0o644)
        write(self.ssh / "id_rsa", PLACEHOLDER)
        write(self.ssh / "orphan.pub", PUBLIC_LINE, 0o644)
        for name in ("known_hosts", "known_hosts.old", "config", "authorized_keys"):
            write(self.ssh / name, "x", 0o644)
            write(self.ssh / f"{name}.pub", "x", 0o644)
        (self.ssh / "subdir").mkdir()
        write(self.ssh / "subdir.pub", "x", 0o644)
        outside_key = write(self.outside / "escaped", PLACEHOLDER)
        (self.ssh / "escaped").symlink_to(outside_key)
        write(self.ssh / "escaped.pub", PUBLIC_LINE, 0o644)
        inside_key = write(self.home / "keys" / "real", PLACEHOLDER)
        (self.ssh / "linked").symlink_to(inside_key)
        write(self.ssh / "linked.pub", PUBLIC_LINE, 0o644)
        (self.ssh / "dangling").symlink_to(self.home / "missing")
        write(self.ssh / "dangling.pub", PUBLIC_LINE, 0o644)

    def test_order_filters_and_fields(self) -> None:
        """NVIDIA Sync first, then ~/.ssh pairs by name, then the controller key."""
        self.populate()
        controller_dir = self.home / "controller-ssh"
        write(controller_dir / "id_ed25519", PLACEHOLDER)
        candidates = find_key_candidates(
            home=self.home, platform="darwin", env={}, controller_key_dir=controller_dir
        )
        self.assertEqual(
            [(c.display_path, c.source) for c in candidates],
            [
                (
                    "~/Library/Application Support/NVIDIA/Sync/config/nvsync.key",
                    "nvidia-sync",
                ),
                ("~/.ssh/id_ed25519", "ssh-dir"),
                ("~/.ssh/linked", "ssh-dir"),
                ("~/.ssh/work", "ssh-dir"),
                ("~/controller-ssh/id_ed25519", "controller"),
            ],
        )
        sync, ed25519, linked, work, controller = candidates
        self.assertFalse(sync.has_public_key)
        self.assertIsNone(sync.key_type)
        self.assertEqual(ed25519.key_type, "ssh-ed25519")
        self.assertTrue(ed25519.has_public_key)
        self.assertEqual(ed25519.path, str(self.ssh / "id_ed25519"))
        self.assertTrue(Path(ed25519.path).is_absolute())
        resolved = str((self.ssh / "id_ed25519").resolve())
        self.assertEqual(ed25519.id, hashlib.sha256(resolved.encode()).hexdigest()[:12])
        self.assertEqual(linked.key_type, "ssh-ed25519")
        self.assertIsNone(work.key_type)
        self.assertTrue(work.has_public_key)
        self.assertFalse(controller.has_public_key)
        self.assertEqual(len({c.id for c in candidates}), len(candidates))

    def test_deduplicates_by_resolved_path(self) -> None:
        """A controller dir equal to ~/.ssh does not list a key twice."""
        write(self.ssh / "id_ed25519", PLACEHOLDER)
        write(self.ssh / "id_ed25519.pub", PUBLIC_LINE, 0o644)
        candidates = find_key_candidates(
            home=self.home, platform="linux", env={}, controller_key_dir=self.ssh
        )
        self.assertEqual([c.source for c in candidates], ["ssh-dir"])

    def test_missing_ssh_dir_and_serialization(self) -> None:
        """No keys yields an empty list; candidates serialize."""
        shutil.rmtree(self.ssh)
        self.assertEqual(
            find_key_candidates(home=self.home, platform="linux", env={}), []
        )
        candidate = KeyCandidate(
            "abc", "/h/.ssh/k", "~/.ssh/k", "ssh-dir", True, "ssh-ed25519"
        )
        self.assertEqual(
            candidate.to_dict(),
            {
                "id": "abc",
                "path": "/h/.ssh/k",
                "display_path": "~/.ssh/k",
                "source": "ssh-dir",
                "has_public_key": True,
                "key_type": "ssh-ed25519",
            },
        )


class ResolveKeyPathTests(TempHomeTestCase):
    """Validation of operator-typed key paths."""

    def assert_code(self, raw: Any, code: str) -> DiscoveryError:
        """Assert ``resolve_key_path`` fails with ``code``."""
        with self.assertRaises(DiscoveryError) as caught:
            resolve_key_path(raw, home=self.home)
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def test_accepts_private_key_inside_home(self) -> None:
        """Tilde and absolute spellings resolve to the same file."""
        key = write(self.ssh / "id_ed25519", PLACEHOLDER)
        expected = key.resolve()
        for raw in ("~/.ssh/id_ed25519", str(key), "  ~/.ssh/id_ed25519 "):
            with self.subTest(raw=raw):
                self.assertEqual(resolve_key_path(raw, home=self.home), expected)

    def test_rejects_bad_input(self) -> None:
        """Empty, relative, control characters, and .pub paths are invalid."""
        write(self.ssh / "id_ed25519.pub", PUBLIC_LINE, 0o644)
        for raw in (
            "",
            "   ",
            None,
            "id_ed25519",
            ".ssh/key",
            "~/.ssh/k\x00",
            "x" * 5000,
        ):
            with self.subTest(raw=raw):
                self.assert_code(raw, "invalid_ssh_key_path")
        self.assert_code("~/.ssh/id_ed25519.pub", "invalid_ssh_key_path")

    def test_rejects_paths_outside_home(self) -> None:
        """Other users, absolute outside paths, traversal, and symlinks fail."""
        outside_key = write(self.outside / "key", PLACEHOLDER)
        (self.ssh / "escape").symlink_to(outside_key)
        for raw in (
            "~root/.ssh/id_rsa",
            str(outside_key),
            "~/.ssh/../../outside",
            "~/.ssh/escape",
            "/",
        ):
            with self.subTest(raw=raw):
                self.assert_code(raw, "ssh_key_outside_home")

    def test_missing_or_not_a_file(self) -> None:
        """Missing paths and directories are ``ssh_key_not_found``."""
        self.assert_code("~/.ssh/missing", "ssh_key_not_found")
        self.assert_code("~/.ssh", "ssh_key_not_found")
        self.assert_code("~", "ssh_key_not_found")

    def test_permissions(self) -> None:
        """Group- or world-accessible keys are refused with a chmod hint."""
        for mode in (0o640, 0o604, 0o644, 0o660):
            with self.subTest(mode=oct(mode)):
                key = write(self.ssh / "loose", PLACEHOLDER, mode)
                error = self.assert_code(str(key), "ssh_key_permissions")
                self.assertIn("chmod 600", error.remediation)
        write(self.ssh / "loose", PLACEHOLDER, 0o400)
        self.assertEqual(resolve_key_path("~/.ssh/loose", home=self.home).name, "loose")


class FakeKeygen:
    """Imitate ``ssh-keygen`` by writing placeholder files."""

    def __init__(
        self,
        *,
        mode: int = 0o600,
        write_public: bool = True,
        write_private: bool = True,
        returncode: int = 0,
        error: BaseException | None = None,
    ) -> None:
        """Script the outcome."""
        self.mode = mode
        self.write_public = write_public
        self.write_private = write_private
        self.returncode = returncode
        self.error = error
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def __call__(
        self, argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        """Create the key files named by ``-f``."""
        self.calls.append((argv, kwargs))
        if self.error is not None:
            raise self.error
        path = Path(argv[argv.index("-f") + 1])
        if self.write_private:
            write(path, PLACEHOLDER, self.mode)
        if self.write_public:
            write(Path(f"{path}.pub"), PUBLIC_LINE + "\n", 0o644)
        return subprocess.CompletedProcess(argv, self.returncode, "", "")


class GenerateKeyTests(TempHomeTestCase):
    """Key generation through an injected runner."""

    def setUp(self) -> None:
        """Choose a not-yet-existing controller key directory."""
        super().setUp()
        self.directory = self.home / "Library" / "Application Support" / "Ctl" / "ssh"

    def test_success(self) -> None:
        """The directory is created 0700 and the exact argv is used."""
        runner = FakeKeygen()
        candidate = generate_key(self.directory, runner=runner, home=self.home)
        private = self.directory / "id_ed25519"
        argv, kwargs = runner.calls[0]
        self.assertEqual(
            argv,
            [
                "ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-C",
                "dgx-spark-controller",
                "-f",
                str(private),
            ],
        )
        self.assertIs(kwargs["stdin"], subprocess.DEVNULL)
        self.assertEqual(kwargs["timeout"], 30)
        self.assertEqual(stat.S_IMODE(self.directory.stat().st_mode), 0o700)
        self.assertEqual(candidate.source, "controller")
        self.assertEqual(candidate.path, str(private))
        self.assertEqual(
            candidate.display_path, "~/Library/Application Support/Ctl/ssh/id_ed25519"
        )
        self.assertEqual(candidate.key_type, "ssh-ed25519")
        self.assertTrue(candidate.has_public_key)

    def test_never_overwrites(self) -> None:
        """An existing private or public key stops before running ssh-keygen."""
        for existing in ("id_ed25519", "id_ed25519.pub"):
            with self.subTest(existing=existing):
                shutil.rmtree(self.directory, ignore_errors=True)
                write(self.directory / existing, "keep me")
                runner = FakeKeygen()
                with self.assertRaises(DiscoveryError) as caught:
                    generate_key(self.directory, runner=runner)
                self.assertEqual(caught.exception.code, "ssh_key_exists")
                self.assertEqual(runner.calls, [])
                self.assertEqual((self.directory / existing).read_text(), "keep me")

    def test_failures(self) -> None:
        """Runner errors, bad exit codes, missing files, and bad modes fail."""
        cases = (
            (
                FakeKeygen(error=FileNotFoundError("ssh-keygen")),
                "ssh_key_generation_failed",
            ),
            (
                FakeKeygen(error=subprocess.TimeoutExpired("ssh-keygen", 30)),
                "ssh_key_generation_failed",
            ),
            (FakeKeygen(returncode=1), "ssh_key_generation_failed"),
            (
                FakeKeygen(write_private=False, write_public=False),
                "ssh_key_generation_failed",
            ),
            (FakeKeygen(write_public=False), "ssh_key_generation_failed"),
            (FakeKeygen(mode=0o644), "ssh_key_permissions"),
        )
        for index, (runner, code) in enumerate(cases):
            with self.subTest(index=index):
                shutil.rmtree(self.directory, ignore_errors=True)
                with self.assertRaises(DiscoveryError) as caught:
                    generate_key(self.directory, runner=runner)
                self.assertEqual(caught.exception.code, code)

    def test_invalid_arguments(self) -> None:
        """Bad names, comments, and directories are rejected before running."""
        runner = FakeKeygen()
        cases: tuple[tuple[dict[str, Any], str], ...] = (
            ({"name": "../evil"}, "invalid_ssh_key_name"),
            ({"name": "-x"}, "invalid_ssh_key_name"),
            ({"name": ".hidden"}, "invalid_ssh_key_name"),
            ({"name": "a/b"}, "invalid_ssh_key_name"),
            ({"name": "key.pub"}, "invalid_ssh_key_name"),
            ({"name": ""}, "invalid_ssh_key_name"),
            ({"comment": "a\nb"}, "invalid_ssh_key_comment"),
            ({"comment": "comment \u00e9"}, "invalid_ssh_key_comment"),
            ({"comment": "x" * 129}, "invalid_ssh_key_comment"),
        )
        for kwargs, code in cases:
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaises(DiscoveryError) as caught,
            ):
                generate_key(self.directory, runner=runner, **kwargs)
            self.assertEqual(caught.exception.code, code)
        with self.assertRaises(DiscoveryError) as caught:
            generate_key(Path("relative/dir"), runner=runner)
        self.assertEqual(caught.exception.code, "invalid_ssh_key_path")
        not_a_dir = write(self.home / "file", "x")
        with self.assertRaises(DiscoveryError) as caught:
            generate_key(not_a_dir, runner=runner)
        self.assertEqual(caught.exception.code, "invalid_ssh_key_path")
        self.assertEqual(runner.calls, [])

    @unittest.skipUnless(shutil.which("ssh-keygen"), "OpenSSH ssh-keygen not installed")
    def test_real_ssh_keygen_in_temporary_directory(self) -> None:
        """The real tool accepts the argv; the result is a 0600 key pair."""
        candidate = generate_key(self.directory, comment="unit-test", home=self.home)
        private = self.directory / "id_ed25519"
        self.assertEqual(stat.S_IMODE(private.stat().st_mode), 0o600)
        self.assertEqual(candidate.key_type, "ssh-ed25519")
        line = public_key_text(private)
        self.assertTrue(line.startswith("ssh-ed25519 "))
        self.assertTrue(line.endswith(" unit-test"))
        with self.assertRaises(DiscoveryError):
            generate_key(self.directory, home=self.home)


class PublicKeyTextTests(TempHomeTestCase):
    """Reading only the ``.pub`` sibling."""

    def test_valid_lines(self) -> None:
        """A single key line is returned stripped, with or without comment."""
        key = self.ssh / "id_ed25519"
        for text in (
            PUBLIC_LINE + "\n",
            f"ssh-ed25519 {ED25519}",
            f"  {PUBLIC_LINE}  \n",
        ):
            with self.subTest(text=text):
                write(Path(f"{key}.pub"), text, 0o644)
                self.assertEqual(public_key_text(key), text.strip())

    def test_missing(self) -> None:
        """No .pub file, or a directory, is ``public_key_missing``."""
        key = self.ssh / "id_ed25519"
        for setup in (lambda: None, lambda: Path(f"{key}.pub").mkdir()):
            setup()
            with self.assertRaises(DiscoveryError) as caught:
                public_key_text(key)
            self.assertEqual(caught.exception.code, "public_key_missing")

    def test_invalid_content(self) -> None:
        """Anything but one valid key line is ``public_key_invalid``."""
        key = self.ssh / "id_ed25519"
        for content in (
            b"",
            b"garbage",
            f"{PUBLIC_LINE}\n{PUBLIC_LINE}\n".encode(),
            b"ssh-ed25519 !!!notbase64",
            f"ssh-rsa {ED25519}".encode(),
            f"ssh-ed25519 {ED25519} bad\x01comment".encode(),
            b"ssh-ed25519 " + ED25519.encode() + b" \xff\xfe",
            b"ssh-ed25519 " + b"A" * 20000,
        ):
            with self.subTest(content=content[:30]):
                Path(f"{key}.pub").write_bytes(content)
                with self.assertRaises(DiscoveryError) as caught:
                    public_key_text(key)
                self.assertEqual(caught.exception.code, "public_key_invalid")


class CopyIdAndUserTests(unittest.TestCase):
    """The operator-run ssh-copy-id command and user validation."""

    def test_copy_id_command(self) -> None:
        """The command is shell-quoted and targets the .pub file."""
        key = Path("/Users/luca/Library/Application Support/Ctl/ssh/id_ed25519")
        command = copy_id_command("luca", "Spark-1A2B.local", key)
        self.assertEqual(
            command,
            "ssh-copy-id -i "
            "'/Users/luca/Library/Application Support/Ctl/ssh/id_ed25519.pub' "
            "luca@spark-1a2b.local",
        )
        self.assertEqual(
            shlex.split(copy_id_command("svc_1", "192.168.1.20", key, port=2222)),
            ["ssh-copy-id", "-i", f"{key}.pub", "-p", "2222", "svc_1@192.168.1.20"],
        )

    def test_copy_id_validation(self) -> None:
        """Bad users, hosts, and ports never produce a command."""
        key = Path("/home/u/.ssh/id_ed25519")
        for args, kwargs, code in (
            (("Root", "spark"), {}, "invalid_ssh_user"),
            (("luca", "-oProxyCommand=x"), {}, "invalid_host"),
            (("luca", "spark; rm -rf ~"), {}, "invalid_host"),
            (("luca", "spark"), {"port": 0}, "invalid_port"),
            (("luca", "spark"), {"port": True}, "invalid_port"),
        ):
            with (
                self.subTest(args=args, kwargs=kwargs),
                self.assertRaises(DiscoveryError) as caught,
            ):
                copy_id_command(*args, key, **kwargs)
            self.assertEqual(caught.exception.code, code)

    def test_validate_username(self) -> None:
        """POSIX-style names pass; anything else is ``invalid_ssh_user``."""
        for name in ("luca", "_svc", "a-b_c1", "a" * 32):
            with self.subTest(name=name):
                self.assertEqual(validate_username(name), name)
        for bad in (
            "",
            "Luca",
            "1abc",
            "-x",
            "a" * 33,
            "root$",
            "a b",
            "a@b",
            "luca\n",
            None,
        ):
            with self.subTest(bad=bad), self.assertRaises(DiscoveryError) as caught:
                validate_username(bad)  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "invalid_ssh_user")


if __name__ == "__main__":
    unittest.main()
