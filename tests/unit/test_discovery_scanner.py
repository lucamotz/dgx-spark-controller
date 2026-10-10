# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Tests for candidate classification, bounded probes, and scan orchestration.

Every scan uses injected fakes. The only real sockets are loopback servers on
127.0.0.1 that exercise the default banner and HTTP probes offline.
"""

import contextlib
import ipaddress
import json
import socket
import threading
import time
import unittest
from collections.abc import Callable
from typing import Any
from unittest import mock

from controller.discovery import scanner
from controller.discovery.mdns import MdnsService
from controller.discovery.network import DiscoveryError
from controller.discovery.scanner import (
    Candidate,
    classify,
    expand_targets,
    fetch_json,
    model_ids,
    read_ssh_banner,
    safe_text,
    sanitize_banner,
    scan,
    tcp_connect,
)

UBUNTU_BANNER = "SSH-2.0-OpenSSH_9.6p1 Ubuntu-3ubuntu13.5"
NET = ipaddress.IPv4Network("192.168.1.0/29")


def candidate(**overrides: Any) -> Candidate:
    """Classify a host with neutral defaults."""
    arguments: dict[str, Any] = {
        "hostname": None,
        "ssh_banner": None,
        "open_ports": (22,),
        "served_models": (),
    }
    arguments.update(overrides)
    return classify("192.168.1.10", **arguments)


class ClassifyTests(unittest.TestCase):
    """Heuristic scoring."""

    def test_factory_and_oem_hostnames_are_high(self) -> None:
        """Factory/OEM patterns with SSH give high confidence."""
        for name in (
            "spark-1a2b",
            "SPARK-1A2B.local.",
            "spark-beef-2.lan",
            "gx10-0042.lan",
            "lab-gb10-01",
            "my-ascent",
            "promax-gb10",
        ):
            with self.subTest(name=name):
                result = candidate(hostname=name)
                self.assertEqual(result.confidence, "high")
                self.assertTrue(result.likely_spark)
                self.assertEqual(result.hostname, name.rstrip("."))

    def test_generic_names_are_medium(self) -> None:
        """A generic 'dgx'/'spark' name, or 'spark-' without 4 hex, is medium."""
        for name in ("dgx-lab", "sparky", "spark-1a2bc", "spark-zzzz", "my-spark"):
            with self.subTest(name=name):
                result = candidate(hostname=name)
                self.assertEqual(result.confidence, "medium")
                self.assertTrue(result.likely_spark)

    def test_ubuntu_banner_plus_model_is_medium(self) -> None:
        """Two weak signals together are medium; either alone is low."""
        both = candidate(
            ssh_banner=UBUNTU_BANNER, open_ports=(22, 8888), served_models=["mimo"]
        )
        self.assertEqual(both.confidence, "medium")
        self.assertEqual(both.served_models, ("mimo",))
        self.assertIn(
            "SSH banner reports Ubuntu (DGX OS is Ubuntu-based)", both.reasons
        )
        self.assertEqual(candidate(ssh_banner=UBUNTU_BANNER).confidence, "low")
        self.assertEqual(candidate(served_models=["mimo"]).confidence, "low")
        self.assertFalse(candidate(hostname="nas.lan").likely_spark)

    def test_ssh_is_required(self) -> None:
        """Without SSH, even a factory hostname is low."""
        result = candidate(hostname="spark-1a2b", open_ports=())
        self.assertFalse(result.ssh_open)
        self.assertEqual(result.confidence, "low")
        self.assertFalse(result.likely_spark)
        self.assertIn("onboarding needs SSH", result.reasons[-1])
        forced = candidate(hostname="spark-1a2b", open_ports=(22,), ssh_open=False)
        self.assertEqual(forced.confidence, "low")
        from_banner = candidate(
            hostname="spark-1a2b", open_ports=(), ssh_banner="SSH-2.0-x"
        )
        self.assertTrue(from_banner.ssh_open)

    def test_mdns_name_can_supply_the_match(self) -> None:
        """The strongest name wins, whether from reverse DNS or mDNS."""
        result = candidate(
            hostname="192-168-1-10.isp.example.",
            mdns_names=["spark-c0de.local.", "spark-c0de.local"],
        )
        self.assertEqual(result.hostname, "spark-c0de.local")
        self.assertEqual(result.confidence, "high")
        plain = candidate(hostname=None, mdns_names=["printer.local."])
        self.assertEqual(plain.hostname, "printer.local")
        rdns_first = candidate(hostname="nas.lan", mdns_names=["printer.local."])
        self.assertEqual(rdns_first.hostname, "nas.lan")

    def test_untrusted_text_is_sanitized(self) -> None:
        """Reasons strip control characters and cap remote text at 120 chars."""
        hostile = "spark-1a2b\x1b[31m\u202e" + "x" * 200
        result = candidate(hostname=hostile, served_models=["m\x00" + "y" * 300])
        for reason in result.reasons:
            self.assertNotIn("\x1b", reason)
            self.assertNotIn("\u202e", reason)
            self.assertNotIn("\x00", reason)
        hostname_reason = result.reasons[0]
        quoted = hostname_reason.split("'")[1]
        self.assertLessEqual(len(quoted), 120)
        self.assertTrue(quoted.endswith("..."))
        self.assertLessEqual(len(result.served_models[0]), scanner.MAX_MODEL_ID_CHARS)

    def test_ports_and_address_are_normalized(self) -> None:
        """Ports are deduplicated and sorted; invalid addresses raise."""
        result = classify(
            " 192.168.1.10 ",
            hostname=None,
            ssh_banner=None,
            open_ports=[8888, 22, 22, 0, 70000, True],  # type: ignore[list-item]
            served_models=["a", "a", 7],  # type: ignore[list-item]
        )
        self.assertEqual(result.address, "192.168.1.10")
        self.assertEqual(result.open_ports, (22, 8888))
        self.assertEqual(result.served_models, ("a",))
        for bad in ("spark.local", "fe80::1", ""):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                classify(
                    bad, hostname=None, ssh_banner=None, open_ports=(), served_models=()
                )

    def test_to_dict(self) -> None:
        """Serialization uses lists for sequences."""
        data = candidate(hostname="spark-1a2b", ssh_banner=UBUNTU_BANNER).to_dict()
        self.assertEqual(data["address"], "192.168.1.10")
        self.assertEqual(data["open_ports"], [22])
        self.assertEqual(data["confidence"], "high")
        self.assertIsInstance(data["reasons"], list)
        json.dumps(data)


class TextHygieneTests(unittest.TestCase):
    """Banner and text sanitizers."""

    def test_safe_text(self) -> None:
        """Control and format characters are removed and length is capped."""
        self.assertEqual(safe_text("a\x00b\u200bc\n"), "abc")
        self.assertEqual(safe_text("x" * 10, 5), "xx...")
        self.assertEqual(len(safe_text("y" * 500)), 120)

    def test_sanitize_banner(self) -> None:
        """The SSH line wins, output is printable ASCII, capped at 255."""
        self.assertIsNone(sanitize_banner(None))
        self.assertIsNone(sanitize_banner(b""))
        self.assertIsNone(sanitize_banner("\r\n\x00\x01"))
        self.assertEqual(
            sanitize_banner(UBUNTU_BANNER.encode() + b"\r\n"), UBUNTU_BANNER
        )
        self.assertEqual(
            sanitize_banner(b"Welcome!\r\nSSH-2.0-dropbear\r\n"), "SSH-2.0-dropbear"
        )
        self.assertEqual(sanitize_banner(b"hello\xff\x1b[0m"), "hello[0m")
        self.assertEqual(len(sanitize_banner("SSH-2.0-" + "z" * 400) or ""), 255)


class ModelIdsTests(unittest.TestCase):
    """Parsing ``/v1/models`` documents."""

    def test_valid_and_invalid_documents(self) -> None:
        """Only ``data[].id`` strings are used, capped per port."""
        document = {"object": "list", "data": [{"id": "mimo"}, {"id": "glm"}, {"x": 1}]}
        self.assertEqual(model_ids(document), ("mimo", "glm"))
        bad_documents: tuple[Any, ...] = (
            None,
            [],
            "x",
            {"data": "x"},
            {"data": [1, None]},
            {},
        )
        for bad in bad_documents:
            with self.subTest(bad=bad):
                self.assertEqual(model_ids(bad), ())
        many = {"data": [{"id": f"m{index}"} for index in range(100)]}
        self.assertEqual(len(model_ids(many)), scanner.MAX_MODELS_PER_PORT)


class ExpandTargetsTests(unittest.TestCase):
    """Target expansion and safety caps."""

    def test_host_enumeration(self) -> None:
        """Network/broadcast are skipped below /31; excludes and overlaps apply."""
        hosts = expand_targets([ipaddress.IPv4Network("192.168.1.0/24")])
        self.assertEqual(len(hosts), 254)
        self.assertEqual((hosts[0], hosts[-1]), ("192.168.1.1", "192.168.1.254"))
        self.assertEqual(
            expand_targets([ipaddress.IPv4Network("10.0.0.0/31")]),
            ["10.0.0.0", "10.0.0.1"],
        )
        self.assertEqual(expand_targets(["10.0.0.9"]), ["10.0.0.9"])
        self.assertEqual(
            expand_targets(
                ["192.168.1.0/30", "192.168.1.0/29"],
                exclude={"192.168.1.2", "fe80::1", "junk"},
            ),
            ["192.168.1.1", "192.168.1.3", "192.168.1.4", "192.168.1.5", "192.168.1.6"],
        )

    def test_targets_are_revalidated(self) -> None:
        """Public or oversized targets are refused."""
        for target, code in (
            (ipaddress.IPv4Network("8.8.8.0/24"), "scan_target_not_private"),
            (ipaddress.IPv4Network("10.0.0.0/16"), "scan_target_too_large"),
            ("not a network", "invalid_scan_target"),
        ):
            with (
                self.subTest(target=target),
                self.assertRaises(DiscoveryError) as caught,
            ):
                expand_targets([target])  # type: ignore[list-item]
            self.assertEqual(caught.exception.code, code)

    def test_total_host_cap(self) -> None:
        """More than four /22s in total is refused."""
        four = [f"10.0.{block * 4}.0/22" for block in range(4)]
        self.assertEqual(len(expand_targets(four)), 4 * 1022)
        with self.assertRaises(DiscoveryError) as caught:
            expand_targets([*four, "10.0.16.0/22"])
        self.assertEqual(caught.exception.code, "scan_target_too_large")


class FakeNetwork:
    """Thread-safe scripted probes for ``scan``."""

    def __init__(self) -> None:
        """Script a small LAN."""
        self.lock = threading.Lock()
        self.connects: list[tuple[str, int]] = []
        self.urls: list[str] = []
        self.banner_calls: list[str] = []
        self.open = {
            ("192.168.1.2", 22),
            ("192.168.1.3", 22),
            ("192.168.1.3", 8000),
            ("192.168.1.4", 22),
            ("192.168.1.4", 8888),
            ("192.168.1.5", 22),
            ("192.168.1.6", 22),
        }

    def connect(self, address: str, port: int, timeout: float) -> bool:
        """Report scripted open ports."""
        with self.lock:
            self.connects.append((address, port))
        if address == "192.168.1.6" and port == 8000:
            raise OSError("host unreachable")
        return (address, port) in self.open

    def read_banner(
        self, address: str, port: int, timeout: float
    ) -> str | bytes | None:
        """Return scripted banners."""
        with self.lock:
            self.banner_calls.append(address)
        if address == "192.168.1.5":
            raise OSError("reset")
        return {"192.168.1.4": UBUNTU_BANNER.encode() + b"\r\n"}.get(
            address, "SSH-2.0-OpenSSH_9.2"
        )

    def reverse_dns(self, address: str) -> str | None:
        """Return scripted PTR names."""
        if address == "192.168.1.4":
            raise OSError("no PTR")
        return {"192.168.1.2": "spark-1a2b.lan", "192.168.1.6": "nas.lan"}.get(address)

    def http_get_json(self, url: str, timeout: float) -> Any:
        """Return scripted ``/v1/models`` documents."""
        with self.lock:
            self.urls.append(url)
        if url == "http://192.168.1.4:8888/v1/models":
            return {"data": [{"id": "mimo-v2.6-flash"}]}
        raise ValueError("bad json")

    def mdns_browse(self) -> list[MdnsService]:
        """Return one service that names .3 and an out-of-scope address."""
        return [
            MdnsService(
                "gx10-abcd._ssh._tcp.local.",
                "_ssh._tcp.local.",
                "gx10-abcd.local.",
                22,
                ("192.168.1.3", "192.168.1.200", "fe80::1", "junk"),
                {},
            ),
            MdnsService("x._ssh._tcp.local.", "_ssh._tcp.local.", None, None, (), {}),
        ]

    def kwargs(self) -> dict[str, Any]:
        """Return the injectable probe arguments."""
        return {
            "connect": self.connect,
            "read_banner": self.read_banner,
            "reverse_dns": self.reverse_dns,
            "http_get_json": self.http_get_json,
            "mdns_browse": self.mdns_browse,
        }


class ScanTests(unittest.TestCase):
    """End-to-end orchestration with fakes."""

    def test_full_scan(self) -> None:
        """Responders are probed, merged with mDNS, classified, and ranked."""
        fake = FakeNetwork()
        progress: list[tuple[int, int]] = []
        results = scan(
            [NET],
            exclude=["192.168.1.1"],
            workers=4,
            progress=lambda done, total: progress.append((done, total)),
            **fake.kwargs(),
        )
        self.assertEqual(
            [(c.address, c.confidence) for c in results],
            [
                ("192.168.1.2", "high"),
                ("192.168.1.3", "high"),
                ("192.168.1.4", "medium"),
                ("192.168.1.5", "low"),
                ("192.168.1.6", "low"),
            ],
        )
        by_address = {c.address: c for c in results}
        self.assertEqual(by_address["192.168.1.2"].hostname, "spark-1a2b.lan")
        self.assertEqual(by_address["192.168.1.3"].hostname, "gx10-abcd.local")
        self.assertEqual(by_address["192.168.1.3"].open_ports, (22, 8000))
        self.assertEqual(by_address["192.168.1.3"].served_models, ())
        self.assertEqual(by_address["192.168.1.4"].served_models, ("mimo-v2.6-flash",))
        self.assertEqual(by_address["192.168.1.4"].ssh_banner, UBUNTU_BANNER)
        self.assertIsNone(by_address["192.168.1.4"].hostname)
        self.assertIsNone(by_address["192.168.1.5"].ssh_banner)
        self.assertTrue(all(c.ssh_open for c in results))
        scanned = {address for address, port in fake.connects if port == 22}
        self.assertEqual(scanned, {f"192.168.1.{i}" for i in range(2, 7)})
        self.assertEqual(
            sorted(fake.urls),
            ["http://192.168.1.3:8000/v1/models", "http://192.168.1.4:8888/v1/models"],
        )
        self.assertEqual(progress[0], (0, 5))
        self.assertEqual(progress[-1], (5, 5))
        self.assertEqual([done for done, _ in progress], list(range(6)))

    def test_no_responders(self) -> None:
        """A silent network returns nothing and skips phase 2."""
        fake = FakeNetwork()
        fake.open = set()
        self.assertEqual(scan([NET], **fake.kwargs()), [])
        self.assertEqual(fake.banner_calls, [])

    def test_sorting_is_numeric(self) -> None:
        """Likely sparks first, then numeric address order."""
        open_hosts = {"10.0.0.9", "10.0.0.10", "10.0.0.100"}
        results = scan(
            ["10.0.0.0/25"],  # type: ignore[list-item]
            connect=lambda address, port, timeout: port == 22 and address in open_hosts,
            read_banner=lambda *args: None,
            reverse_dns=lambda address: "dgx-b" if address == "10.0.0.100" else None,
            http_get_json=lambda url, timeout: None,
            mdns_browse=list,
        )
        self.assertEqual(
            [c.address for c in results], ["10.0.0.100", "10.0.0.9", "10.0.0.10"]
        )

    def test_invalid_input_fails_before_probing(self) -> None:
        """Bad targets or options raise before any connection attempt."""
        fake = FakeNetwork()
        for kwargs, code in (
            (
                {"targets": [ipaddress.IPv4Network("1.1.1.0/24")]},
                "scan_target_not_private",
            ),
            ({"targets": [NET], "timeout": 0}, "invalid_scan_options"),
            ({"targets": [NET], "timeout": 6}, "invalid_scan_options"),
            ({"targets": [NET], "workers": 0}, "invalid_scan_options"),
            ({"targets": [NET], "workers": True}, "invalid_scan_options"),
        ):
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaises(DiscoveryError) as caught,
            ):
                scan(**kwargs, **fake.kwargs())
            self.assertEqual(caught.exception.code, code)
        self.assertEqual(fake.connects, [])

    def test_pre_cancelled_scan_does_nothing(self) -> None:
        """A cancel set before starting returns immediately."""
        fake = FakeNetwork()
        cancel = threading.Event()
        cancel.set()
        self.assertEqual(scan([NET], cancel=cancel, **fake.kwargs()), [])
        self.assertEqual(fake.connects, [])

    def test_cancel_during_phase_one(self) -> None:
        """Cancelling stops probing and skips phase 2."""
        fake = FakeNetwork()
        cancel = threading.Event()

        def slow_connect(address: str, port: int, timeout: float) -> bool:
            time.sleep(0.01)
            return fake.connect(address, port, timeout)

        kwargs = fake.kwargs()
        kwargs["connect"] = slow_connect
        results = scan(
            [ipaddress.IPv4Network("192.168.1.0/24")],
            workers=1,
            cancel=cancel,
            progress=lambda done, total: cancel.set() if done >= 3 else None,
            **kwargs,
        )
        self.assertLess(len(fake.connects), 20)
        self.assertEqual(fake.banner_calls, [])
        self.assertTrue(all(c.ssh_banner is None for c in results))

    def test_mdns_failure_is_ignored(self) -> None:
        """A failing mDNS browse does not break the scan."""
        fake = FakeNetwork()

        def broken() -> list[MdnsService]:
            raise OSError("multicast blocked")

        kwargs = fake.kwargs()
        kwargs["mdns_browse"] = broken
        results = scan([NET], **kwargs)
        self.assertEqual(len(results), 5)
        self.assertIsNone({c.address: c for c in results}["192.168.1.3"].hostname)

    def test_slow_reverse_dns_and_mdns_are_bounded(self) -> None:
        """Stuck resolver or mDNS calls are abandoned after their deadlines."""
        release = threading.Event()
        self.addCleanup(release.set)

        def stuck_dns(address: str) -> str | None:
            release.wait(10)
            return "spark-1a2b"

        def stuck_mdns() -> list[MdnsService]:
            release.wait(10)
            return []

        started = time.monotonic()
        with (
            mock.patch.object(scanner, "REVERSE_DNS_TIMEOUT_SECONDS", 0.05),
            mock.patch.object(scanner, "MDNS_WAIT_SECONDS", 0.05),
        ):
            results = scan(
                ["192.168.1.2"],  # type: ignore[list-item]
                connect=lambda address, port, timeout: port == 22,
                read_banner=lambda *args: None,
                reverse_dns=stuck_dns,
                http_get_json=lambda url, timeout: None,
                mdns_browse=stuck_mdns,
            )
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertEqual(len(results), 1)
        self.assertIsNone(results[0].hostname)

    def test_worker_limit(self) -> None:
        """Workers are clamped to the maximum and the descriptor budget."""
        self.assertEqual(scanner._worker_limit(1), 1)
        self.assertLessEqual(scanner._worker_limit(10_000), scanner.MAX_WORKERS)
        if scanner._resource is not None:
            with mock.patch.object(
                scanner._resource, "getrlimit", return_value=(256, 1024)
            ):
                self.assertEqual(scanner._worker_limit(128), 96)


class LoopbackServer:
    """A one-connection TCP server on 127.0.0.1 driven by a handler."""

    def __init__(self, handler: Callable[[socket.socket], None]) -> None:
        """Start listening and serve one connection in a thread."""
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.port = self.listener.getsockname()[1]
        self.handler = handler
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        try:
            conn, _ = self.listener.accept()
        except OSError:
            return
        with conn, contextlib.suppress(OSError):
            self.handler(conn)

    def close(self) -> None:
        """Stop the server."""
        self.listener.close()
        self.thread.join(5)


def read_request(conn: socket.socket) -> bytes:
    """Read an HTTP request head."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data += chunk
    return data


class LoopbackProbeTests(unittest.TestCase):
    """Default probes against local loopback servers only."""

    def serve(self, handler: Callable[[socket.socket], None]) -> int:
        """Start a server and return its port."""
        server = LoopbackServer(handler)
        self.addCleanup(server.close)
        return server.port

    def http(self, response: bytes) -> tuple[int, list[bytes]]:
        """Serve one HTTP response and capture the request."""
        requests: list[bytes] = []

        def handler(conn: socket.socket) -> None:
            requests.append(read_request(conn))
            conn.sendall(response)

        return self.serve(handler), requests

    def test_tcp_connect(self) -> None:
        """Open ports connect; closed ports do not."""
        port = self.serve(lambda conn: None)
        self.assertTrue(tcp_connect("127.0.0.1", port, 1.0))
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
        probe.close()
        self.assertFalse(tcp_connect("127.0.0.1", closed_port, 0.5))

    def test_read_ssh_banner(self) -> None:
        """The banner is read without sending anything."""
        received: list[bytes] = []

        def handler(conn: socket.socket) -> None:
            conn.sendall(UBUNTU_BANNER.encode() + b"\r\n")
            conn.settimeout(2)
            received.append(conn.recv(10))

        port = self.serve(handler)
        banner = read_ssh_banner("127.0.0.1", port, 1.0)
        self.assertEqual(sanitize_banner(banner), UBUNTU_BANNER)

    def test_silent_banner_times_out(self) -> None:
        """A server that never speaks yields ``None`` within the deadline."""
        done = threading.Event()

        def hold_open(conn: socket.socket) -> None:
            done.wait(5)

        port = self.serve(hold_open)
        self.addCleanup(done.set)
        started = time.monotonic()
        self.assertIsNone(read_ssh_banner("127.0.0.1", port, 0.2))
        self.assertLess(time.monotonic() - started, 2.0)

    def test_fetch_json_content_length(self) -> None:
        """A 200 JSON response is parsed; the request is a fixed HTTP/1.0 GET."""
        body = json.dumps({"data": [{"id": "mimo"}]}).encode()
        port, requests = self.http(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            + f"Content-Length: {len(body)}\r\n\r\n".encode()
            + body
        )
        document = fetch_json(f"http://127.0.0.1:{port}/v1/models", 1.5)
        self.assertEqual(model_ids(document), ("mimo",))
        self.assertTrue(requests[0].startswith(b"GET /v1/models HTTP/1.0\r\n"))
        self.assertIn(b"Connection: close", requests[0])

    def test_fetch_json_chunked(self) -> None:
        """Chunked transfer encoding is decoded."""
        port, _ = self.http(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
            b'5\r\n{"dat\r\n10\r\na": [{"id": "x"}\r\n2\r\n]}\r\n0\r\n\r\n'
        )
        self.assertEqual(
            fetch_json(f"http://127.0.0.1:{port}/v1/models", 1.5),
            {"data": [{"id": "x"}]},
        )

    def test_fetch_json_failures(self) -> None:
        """Non-200, bad JSON, truncation, and oversize bodies give ``None``."""
        oversized = b"x" * (scanner.HTTP_MAX_BODY_BYTES + 10)
        responses = (
            b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 2\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nnot",
            b"HTTP/1.1 200 OK\r\nContent-Length: 50\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\n\r\n" + oversized,
            b"garbage\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
        )
        for index, response in enumerate(responses):
            with self.subTest(index=index):
                port, _ = self.http(response)
                self.assertIsNone(fetch_json(f"http://127.0.0.1:{port}/v1/models", 1.5))

    def test_slow_drip_is_cut_off(self) -> None:
        """One overall deadline bounds a server that trickles bytes."""
        stop = threading.Event()

        def handler(conn: socket.socket) -> None:
            read_request(conn)
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n")
            while not stop.is_set():
                conn.sendall(b"x")
                time.sleep(0.02)

        port = self.serve(handler)
        self.addCleanup(stop.set)
        started = time.monotonic()
        self.assertIsNone(fetch_json(f"http://127.0.0.1:{port}/v1/models", 0.3))
        self.assertLess(time.monotonic() - started, 2.0)

    def test_fetch_json_refuses_non_literal_or_non_http_urls(self) -> None:
        """Names (DNS), other schemes, and missing ports are refused offline."""
        for url in (
            "http://localhost:8000/v1/models",
            "https://127.0.0.1:8000/v1/models",
            "http://127.0.0.1/v1/models",
            "http://127.0.0.1:99999/v1/models",
            "http://127.0.0.1:8000/v1/models with space",
            "not a url",
        ):
            with self.subTest(url=url):
                self.assertIsNone(fetch_json(url, 0.2))


if __name__ == "__main__":
    unittest.main()
