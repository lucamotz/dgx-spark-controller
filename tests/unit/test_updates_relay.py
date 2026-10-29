# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""The update relay: allowlisted hosts, contained paths and a safe cache."""

from __future__ import annotations

import http.client
import socket
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from controller.updates.relay import RelaySession, RelaySource, UpdateRelay

HOSTS = {"ports.ubuntu.com": "https"}
INRELEASE = "http://ports.ubuntu.com/ubuntu-ports/dists/noble/InRelease"
PACKAGE = "http://ports.ubuntu.com/ubuntu-ports/pool/main/h/hello/hello_2.10_arm64.deb"
CONTROL = "http://dgx-spark-controller.invalid"
MODE = f"{CONTROL}/mode"


class FakeUpstream:
    """An upstream response that yields its body in two chunks."""

    def __init__(
        self,
        status: int = 200,
        body: bytes = b"",
        *,
        headers: dict[str, str] | None = None,
        error: OSError | None = None,
    ) -> None:
        self.status = status
        self.headers = (
            {"Content-Length": str(len(body))} if headers is None else headers
        )
        self._chunks = [body[: len(body) // 2], body[len(body) // 2 :]]
        self._error = error

    def getheader(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name, default)

    def read(self, _amount: int) -> bytes:
        if self._error is not None and len(self._chunks) == 1:
            raise self._error
        return self._chunks.pop(0) if self._chunks else b""


class PeerSocket(socket.socket):
    """A real socket that reports a chosen upstream peer address."""

    def __init__(self, address: str, fileno: int) -> None:
        super().__init__(fileno=fileno)
        self.address = address

    def getpeername(self) -> tuple[str, int]:
        return self.address, 443


class UpdateRelayTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cache = self.root / "cache"

    def serve(self, source: RelaySource | None = None) -> RelaySession:
        return self.enterContext(
            UpdateRelay(self.cache).serving(source or RelaySource("online"), HOSTS)
        )

    def upstream(self, *responses: FakeUpstream) -> Mock:
        return self.enterContext(
            patch(
                "controller.updates.relay._open",
                side_effect=[(response, Mock()) for response in responses],
            )
        )

    def get(self, port: int, url: str) -> tuple[int, bytes]:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        self.addCleanup(connection.close)
        connection.request("GET", url)
        response = connection.getresponse()
        result = response.status, response.read()
        # The relay answers one connection's requests in order, so this reply
        # means it has finished the first request, cache write included.
        connection.request("GET", MODE)
        connection.getresponse().read()
        return result

    def cached(self, url: str) -> Path:
        return self.cache / url.removeprefix("http://")

    def cached_files(self) -> list[Path]:
        return [path for path in self.cache.rglob("*") if path.is_file()]

    def test_fetches_over_the_source_scheme_and_caches_privately(self) -> None:
        opened = self.upstream(FakeUpstream(body=b"signed index"))
        port = self.serve().port

        self.assertEqual((200, b"signed index"), self.get(port, INRELEASE))
        self.assertEqual(
            "https://ports.ubuntu.com/ubuntu-ports/dists/noble/InRelease",
            opened.call_args.args[0],
        )
        cached = self.cached(INRELEASE)
        self.assertEqual(b"signed index", cached.read_bytes())
        self.assertEqual(0o600, stat.S_IMODE(cached.stat().st_mode))

    def test_packages_come_from_the_cache_and_indexes_are_refetched(self) -> None:
        for url, content in ((INRELEASE, b"stale index"), (PACKAGE, b"package")):
            self.cached(url).parent.mkdir(parents=True, exist_ok=True)
            self.cached(url).write_bytes(content)
        opened = self.upstream(FakeUpstream(body=b"fresh index"))
        port = self.serve().port

        self.assertEqual((200, b"package"), self.get(port, PACKAGE))
        self.assertEqual((200, b"fresh index"), self.get(port, INRELEASE))
        self.assertEqual(1, opened.call_count)

    def test_refuses_hosts_outside_the_allowlist(self) -> None:
        opened = self.upstream()
        port = self.serve().port
        for url in (
            "http://evil.example/ubuntu-ports/dists/noble/InRelease",
            "http://ports.ubuntu.com.evil.example/ubuntu-ports/dists/noble/InRelease",
            "http://localhost/server-status",
            "http://127.0.0.1/server-status",
        ):
            with self.subTest(url=url):
                self.assertEqual(403, self.get(port, url)[0])
        opened.assert_not_called()

    def test_rejects_paths_that_could_leave_the_host_directory(self) -> None:
        opened = self.upstream()
        port = self.serve().port
        for path in (
            "/ubuntu-ports/../../etc/passwd",
            "/ubuntu-ports/%2e%2e/%2E%2E/etc/passwd",
            "/ubuntu-ports/..%2f..%2fetc%2fpasswd",
            "/ubuntu-ports/%252e%252e/etc/passwd",
            "/ubuntu-ports/..%5c..%5cetc%5cpasswd",
            "//etc/passwd",
            "/ubuntu-ports/InRelease%00.deb",
        ):
            with self.subTest(path=path):
                self.assertEqual(
                    400, self.get(port, f"http://ports.ubuntu.com{path}")[0]
                )
        opened.assert_not_called()

    def test_rejects_requests_that_are_not_plain_absolute_http(self) -> None:
        opened = self.upstream()
        port = self.serve().port
        for url in (
            "/ubuntu-ports/dists/noble/InRelease",
            "https://ports.ubuntu.com/ubuntu-ports/dists/noble/InRelease",
            "http://user@ports.ubuntu.com/ubuntu-ports/dists/noble/InRelease",
            "http://ports.ubuntu.com:8080/ubuntu-ports/dists/noble/InRelease",
            "http://ports.ubuntu.com:abc/ubuntu-ports/dists/noble/InRelease",
            f"{INRELEASE}?cache=bust",
        ):
            with self.subTest(url=url):
                self.assertEqual(400, self.get(port, url)[0])
        opened.assert_not_called()

    def test_control_host_answers_only_the_mode_and_the_session_token(self) -> None:
        relay = self.serve()
        self.assertEqual((200, b"online"), self.get(relay.port, MODE))
        self.assertEqual(
            (200, relay.token.encode()), self.get(relay.port, f"{CONTROL}/token")
        )
        for path in ("/stats", "/token/mode", "/mode/x"):
            with self.subTest(path=path):
                self.assertEqual(404, self.get(relay.port, f"{CONTROL}{path}")[0])

    def test_directory_source_serves_its_tree_without_the_network(self) -> None:
        mirror = self.root / "mirror"
        index = mirror / INRELEASE.removeprefix("http://")
        index.parent.mkdir(parents=True)
        index.write_bytes(b"mirrored index")
        opened = self.upstream()
        port = self.serve(RelaySource("directory", mirror)).port

        self.assertEqual((200, b"directory"), self.get(port, MODE))
        self.assertEqual((200, b"mirrored index"), self.get(port, INRELEASE))
        self.assertEqual(404, self.get(port, f"{INRELEASE}.gpg")[0])
        opened.assert_not_called()

    def test_directory_source_refuses_symlinks_that_leave_it(self) -> None:
        mirror = self.root / "mirror"
        dists = mirror / "ports.ubuntu.com/ubuntu-ports/dists"
        dists.mkdir(parents=True)
        (self.root / "secret").write_bytes(b"secret")
        (dists / "leak").symlink_to(self.root / "secret")
        (mirror / "cdn.fwupd.org").symlink_to(self.root)
        port = self.serve(RelaySource("directory", mirror)).port

        for url in (
            "http://ports.ubuntu.com/ubuntu-ports/dists/leak",
            "http://cdn.fwupd.org/secret",
        ):
            with self.subTest(url=url):
                self.assertEqual((403, b"outside the source"), self.get(port, url))

    def test_follows_a_redirect_within_the_same_site(self) -> None:
        moved = "http://security.ubuntu.com/ubuntu-ports/dists/noble/InRelease"
        opened = self.upstream(
            FakeUpstream(302, headers={"Location": moved}),
            FakeUpstream(body=b"index"),
        )
        port = self.serve().port

        self.assertEqual((200, b"index"), self.get(port, INRELEASE))
        self.assertEqual(moved, opened.call_args.args[0])
        self.assertEqual(b"index", self.cached(INRELEASE).read_bytes())

    def test_refuses_a_redirect_to_another_site(self) -> None:
        port = self.serve().port
        for location in (
            "https://evil.example/InRelease",
            "//evil.example/InRelease",
            "https://ubuntu.com.evil.example/InRelease",
            "https://evilubuntu.com/InRelease",
            "https://security.ubuntu.com:8443/InRelease",
            "ftp://ports.ubuntu.com/InRelease",
            "file:///etc/passwd",
        ):
            redirect = FakeUpstream(302, headers={"Location": location})
            with (
                self.subTest(location=location),
                patch(
                    "controller.updates.relay._open", return_value=(redirect, Mock())
                ) as opened,
            ):
                self.assertEqual(502, self.get(port, INRELEASE)[0])
                self.assertEqual(1, opened.call_count)
        self.assertEqual([], self.cached_files())

    def test_directory_listing_is_served_but_never_cached(self) -> None:
        listing = "http://security.ubuntu.com/ubuntu-ports/pool/main/h/hello/"
        self.upstream(
            FakeUpstream(302, headers={"Location": listing}),
            FakeUpstream(body=b"<html>index of</html>"),
        )
        port = self.serve().port

        self.assertEqual((200, b"<html>index of</html>"), self.get(port, PACKAGE))
        self.assertEqual([], self.cached_files())

    def test_serves_without_caching_when_the_cache_path_is_blocked(self) -> None:
        (self.cache / "ports.ubuntu.com").mkdir(parents=True)
        (self.cache / "ports.ubuntu.com/ubuntu-ports").write_bytes(b"not a directory")
        self.upstream(FakeUpstream(body=b"index"))
        port = self.serve().port

        self.assertEqual((200, b"index"), self.get(port, INRELEASE))
        self.assertEqual(
            [self.cache / "ports.ubuntu.com/ubuntu-ports"], self.cached_files()
        )

    def test_refuses_upstream_peers_on_this_host_or_its_link(self) -> None:
        port = self.serve().port
        for address in (
            "127.0.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "169.254.169.254",
            "fe80::1",
            "0.0.0.0",
        ):
            connection = Mock()
            connection.sock.getpeername.return_value = (address, 443)
            with (
                self.subTest(address=address),
                patch(
                    "controller.updates.relay.http.client.HTTPSConnection",
                    return_value=connection,
                ),
            ):
                self.assertEqual(403, self.get(port, INRELEASE)[0])
            connection.request.assert_not_called()

    def test_fetches_from_a_public_upstream_peer(self) -> None:
        connection = Mock()
        connection.sock.getpeername.return_value = ("192.0.2.10", 443)
        connection.getresponse.return_value = FakeUpstream(body=b"index")
        port = self.serve().port
        with patch(
            "controller.updates.relay.http.client.HTTPSConnection",
            return_value=connection,
        ) as https:
            self.assertEqual((200, b"index"), self.get(port, INRELEASE))

        self.assertEqual(("ports.ubuntu.com", 443), https.call_args.args)
        self.assertEqual(
            "/ubuntu-ports/dists/noble/InRelease", connection.request.call_args.args[1]
        )

    def test_incomplete_upstream_download_is_never_cached(self) -> None:
        port = self.serve().port
        for upstream in (
            FakeUpstream(body=b"x" * 40, headers={"Content-Length": "100"}),
            FakeUpstream(body=b"x" * 40, error=ConnectionResetError()),
        ):
            with (
                self.subTest(headers=upstream.headers),
                patch(
                    "controller.updates.relay._open", return_value=(upstream, Mock())
                ),
            ):
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                self.addCleanup(connection.close)
                connection.request("GET", PACKAGE)
                with self.assertRaises(http.client.IncompleteRead):
                    connection.getresponse().read()
                self.assertEqual([], self.cached_files())

    def connect_status(self, port: int, target: str) -> int:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        self.addCleanup(connection.close)
        connection.connect()
        with patch("controller.updates.relay.socket.create_connection") as upstream:
            connection.request("CONNECT", target)
            status = connection.getresponse().status
        upstream.assert_not_called()
        return status

    def test_refuses_tunnels_except_to_allowlisted_hosts_on_443_online(self) -> None:
        online = self.serve().port
        directory = self.serve(RelaySource("directory", self.root)).port
        for port, target in (
            (online, "evil.example:443"),
            (online, "ports.ubuntu.com:22"),
            (online, "ports.ubuntu.com:0443"),
            (directory, "ports.ubuntu.com:443"),
        ):
            with self.subTest(target=target, port=port):
                self.assertEqual(403, self.connect_status(port, target))

    def tunnel(self, peer: str) -> tuple[bytes, socket.socket, socket.socket]:
        """CONNECT to the allowlisted host; its upstream socket has ``peer``."""
        port = self.serve().port
        upstream, remote = socket.socketpair()
        self.addCleanup(remote.close)
        remote.settimeout(5)
        client = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.addCleanup(client.close)
        with patch(
            "controller.updates.relay.socket.create_connection",
            return_value=PeerSocket(peer, upstream.detach()),
        ) as create:
            client.sendall(b"CONNECT PORTS.ubuntu.com:443 HTTP/1.1\r\n\r\n")
            reply = b""
            while b"\r\n\r\n" not in reply:
                reply += client.recv(1024)
        create.assert_called_once_with(("ports.ubuntu.com", 443), timeout=30)
        return reply, client, remote

    def test_refuses_a_tunnel_whose_upstream_peer_is_loopback(self) -> None:
        reply, _client, remote = self.tunnel("127.0.0.1")
        self.assertTrue(reply.startswith(b"HTTP/1.1 403 "))
        self.assertEqual(b"", remote.recv(64))

    def test_tunnels_bytes_to_an_allowlisted_host(self) -> None:
        reply, client, remote = self.tunnel("192.0.2.10")
        self.assertTrue(reply.startswith(b"HTTP/1.1 200 "))

        client.sendall(b"client hello")
        self.assertEqual(b"client hello", remote.recv(64))
        remote.sendall(b"server hello")
        self.assertEqual(b"server hello", client.recv(64))

    def test_stops_listening_when_the_session_ends(self) -> None:
        with UpdateRelay(self.cache).serving(RelaySource("online"), HOSTS) as relay:
            port = relay.port
        with self.assertRaises(ConnectionRefusedError):
            socket.create_connection(("127.0.0.1", port), timeout=1).close()


if __name__ == "__main__":
    unittest.main()
