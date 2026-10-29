# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Tests for local interface enumeration and scan-target validation."""

import ipaddress
import json
import subprocess
import unittest
from typing import Any

from controller.discovery.commands import run_captured
from controller.discovery.network import (
    MAX_SCAN_HOSTS,
    DiscoveryError,
    LocalNetwork,
    default_scan_targets,
    local_ipv4_networks,
    parse_ifconfig,
    parse_ip_json,
    validate_scan_target,
)

IP_JSON = json.dumps(
    [
        {
            "ifindex": 1,
            "ifname": "lo",
            "flags": ["LOOPBACK", "UP", "LOWER_UP"],
            "addr_info": [{"family": "inet", "local": "127.0.0.1", "prefixlen": 8}],
        },
        {
            "ifindex": 2,
            "ifname": "enP7s7",
            "flags": ["BROADCAST", "MULTICAST", "UP", "LOWER_UP"],
            "addr_info": [
                {"family": "inet", "local": "192.168.1.20", "prefixlen": 24},
                {"family": "inet", "local": "192.168.1.20", "prefixlen": 24},
            ],
        },
        {
            "ifindex": 3,
            "ifname": "enp1s0f0np0",
            "flags": ["BROADCAST", "UP"],
            "addr_info": [{"family": "inet", "local": "169.254.10.5", "prefixlen": 16}],
        },
        {
            "ifindex": 4,
            "ifname": "docker0",
            "flags": ["UP"],
            "addr_info": [{"family": "inet", "local": "172.17.0.1", "prefixlen": 16}],
        },
        {
            "ifindex": 5,
            "ifname": "wlan0",
            "flags": ["BROADCAST", "MULTICAST"],
            "addr_info": [{"family": "inet", "local": "10.0.0.7", "prefixlen": 24}],
        },
        {
            "ifindex": 6,
            "ifname": "eth9",
            "flags": ["UP"],
            "addr_info": [{"family": "inet", "local": "10.9.9.9", "prefixlen": 32}],
        },
        {
            "ifindex": 7,
            "ifname": "eth8",
            "flags": ["UP"],
            "addr_info": [
                {"family": "inet", "local": "10.8.0.1", "prefixlen": True},
                {"family": "inet", "local": "not-an-ip", "prefixlen": 24},
                "garbage",
            ],
        },
        {},
        "garbage",
    ]
)

MACOS_IFCONFIG = """\
lo0: flags=8049<UP,LOOPBACK,RUNNING,MULTICAST> mtu 16384
\toptions=1203<RXCSUM,TXCSUM,TXSTATUS,SW_TIMESTAMP>
\tinet 127.0.0.1 netmask 0xff000000
\tinet6 ::1 prefixlen 128
en0: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500
\tether 3c:a6:f6:00:00:01
\tinet6 fe80::1c%en0 prefixlen 64 secured scopeid 0xb
\tinet 192.168.1.5 netmask 0xffffff00 broadcast 192.168.1.255
\tstatus: active
en7: flags=8822<BROADCAST,SMART,SIMPLEX,MULTICAST> mtu 1500
\tinet 10.20.30.40 netmask 0xffffff00 broadcast 10.20.30.255
en5: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500
\tinet 169.254.3.4 netmask 0xffff0000 broadcast 169.254.255.255
utun3: flags=8051<UP,POINTOPOINT,RUNNING,MULTICAST> mtu 1380
\tinet 10.8.0.2 --> 10.8.0.1 netmask 0xffffff00
en9: flags=8863<UP,BROADCAST,RUNNING> mtu 1500
\tinet 10.1.1.1 netmask 0xffff00ff
"""

NET_TOOLS_2 = """\
eth0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 10.0.5.9  netmask 255.255.0.0  broadcast 10.0.255.255
        ether 00:11:22:33:44:55  txqueuelen 1000  (Ethernet)

veth1234: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 172.18.0.2  netmask 255.255.0.0  broadcast 172.18.255.255
"""

NET_TOOLS_1 = """\
eth1      Link encap:Ethernet  HWaddr 00:11:22:33:44:66
          inet addr:192.168.50.3  Bcast:192.168.50.255  Mask:255.255.255.0
          UP BROADCAST RUNNING MULTICAST  MTU:1500  Metric:1

eth1:1    Link encap:Ethernet  HWaddr 00:11:22:33:44:66
          inet addr:192.168.51.3  Bcast:192.168.51.255  Mask:255.255.255.128
"""


class FakeRunner:
    """Record subprocess calls and answer from a command-name table."""

    def __init__(self, answers: dict[str, Any]) -> None:
        """Map a command basename to stdout text, a return code, or an exception."""
        self.answers = answers
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def __call__(self, argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        """Return the scripted result for ``argv``."""
        self.calls.append((argv, kwargs))
        answer = self.answers.get(argv[0].rsplit("/", 1)[-1])
        if isinstance(answer, BaseException):
            raise answer
        if isinstance(answer, int):
            return subprocess.CompletedProcess(argv, answer, "", "boom")
        if answer is None:
            raise FileNotFoundError(argv[0])
        return subprocess.CompletedProcess(argv, 0, answer, "")


class RunCapturedTests(unittest.TestCase):
    """The shared argv-only command runner."""

    def test_command_gets_no_shell_no_check_and_no_stdin(self) -> None:
        """Without input, the child reads EOF and the result is never checked."""
        runner = FakeRunner({"tool": "out"})
        completed = run_captured(runner, ("tool", "--flag"), timeout=2)
        self.assertEqual(completed.stdout, "out")
        argv, kwargs = runner.calls[0]
        self.assertEqual(argv, ["tool", "--flag"])
        self.assertEqual(
            kwargs,
            {
                "capture_output": True,
                "text": True,
                "timeout": 2,
                "check": False,
                "stdin": subprocess.DEVNULL,
            },
        )

    def test_input_replaces_devnull_and_env_passes_through(self) -> None:
        """A probe script goes to stdin; an explicit environment is kept."""
        runner = FakeRunner({"ssh": "ok"})
        run_captured(runner, ["ssh"], timeout=5, input="script", env={"LC_ALL": "C"})
        _argv, kwargs = runner.calls[0]
        self.assertEqual(
            kwargs,
            {
                "capture_output": True,
                "text": True,
                "timeout": 5,
                "check": False,
                "input": "script",
                "env": {"LC_ALL": "C"},
            },
        )


class ParseIpJsonTests(unittest.TestCase):
    """Parsing ``ip -j -4 addr show``."""

    def test_keeps_lan_and_link_local_and_drops_the_rest(self) -> None:
        """Loopback, down, docker, /32, and malformed entries are dropped."""
        self.assertEqual(
            parse_ip_json(IP_JSON),
            [
                LocalNetwork("enP7s7", "192.168.1.20", "192.168.1.0/24"),
                LocalNetwork("enp1s0f0np0", "169.254.10.5", "169.254.0.0/16"),
            ],
        )

    def test_malformed_documents_yield_nothing(self) -> None:
        """Garbage never raises."""
        for text in ("", "not json", "{}", "42", '[{"ifname": 3}]', "[null]"):
            with self.subTest(text=text):
                self.assertEqual(parse_ip_json(text), [])


class ParseIfconfigTests(unittest.TestCase):
    """Parsing BSD/macOS and net-tools ``ifconfig`` output."""

    def test_macos_output(self) -> None:
        """Hex netmasks work; down, tunnel, and non-contiguous masks are skipped."""
        self.assertEqual(
            parse_ifconfig(MACOS_IFCONFIG),
            [
                LocalNetwork("en0", "192.168.1.5", "192.168.1.0/24"),
                LocalNetwork("en5", "169.254.3.4", "169.254.0.0/16"),
            ],
        )

    def test_net_tools_formats(self) -> None:
        """Dotted masks, ``inet addr:``/``Mask:``, and alias names are parsed."""
        self.assertEqual(
            parse_ifconfig(NET_TOOLS_2),
            [LocalNetwork("eth0", "10.0.5.9", "10.0.0.0/16")],
        )
        self.assertEqual(
            parse_ifconfig(NET_TOOLS_1),
            [
                LocalNetwork("eth1", "192.168.50.3", "192.168.50.0/24"),
                LocalNetwork("eth1:1", "192.168.51.3", "192.168.51.0/25"),
            ],
        )

    def test_garbage_lines_are_ignored(self) -> None:
        """Lines outside an interface block or with bad values are skipped."""
        text = (
            "\tinet 10.0.0.1 netmask 0xffffff00\n"
            "??? weird header\n"
            "\tinet 10.0.0.2 netmask 0xffffff00\n"
            "en0: flags=8863<UP> mtu 1500\n"
            "\tinet 999.1.1.1 netmask 0xffffff00\n"
            "\tinet 10.0.0.3\n"
        )
        self.assertEqual(parse_ifconfig(text), [])


class LocalNetworksTests(unittest.TestCase):
    """Command selection and fallbacks of ``local_ipv4_networks``."""

    def test_linux_uses_ip_json_with_bounded_timeout(self) -> None:
        """Linux runs ``ip -j -4 addr show`` with a 3 s timeout and C locale."""
        runner = FakeRunner({"ip": IP_JSON})
        networks = local_ipv4_networks(runner=runner, platform="linux")
        self.assertEqual(networks[0].network, "192.168.1.0/24")
        self.assertEqual(len(runner.calls), 1)
        argv, kwargs = runner.calls[0]
        self.assertEqual(argv[1:], ["-j", "-4", "addr", "show"])
        self.assertEqual(kwargs["timeout"], 3.0)
        self.assertEqual(kwargs["env"]["LC_ALL"], "C")
        self.assertIs(kwargs["check"], False)
        self.assertIs(kwargs["stdin"], subprocess.DEVNULL)

    def test_linux_falls_back_to_ifconfig(self) -> None:
        """A failing ``ip`` falls back to ``ifconfig``."""
        runner = FakeRunner({"ip": 1, "ifconfig": NET_TOOLS_2})
        networks = local_ipv4_networks(runner=runner, platform="linux")
        self.assertEqual(networks, [LocalNetwork("eth0", "10.0.5.9", "10.0.0.0/16")])
        self.assertEqual(
            [call[0][0].rsplit("/", 1)[-1] for call in runner.calls], ["ip", "ifconfig"]
        )

    def test_macos_uses_ifconfig_only(self) -> None:
        """Non-Linux platforms only run ``ifconfig``."""
        runner = FakeRunner({"ifconfig": MACOS_IFCONFIG})
        networks = local_ipv4_networks(runner=runner, platform="darwin")
        self.assertEqual(
            networks[0], LocalNetwork("en0", "192.168.1.5", "192.168.1.0/24")
        )
        self.assertEqual(len(runner.calls), 1)

    def test_command_failures_use_udp_fallback(self) -> None:
        """Missing tools and timeouts never raise; the route address is a /24."""
        for answers in (
            {},
            {"ip": subprocess.TimeoutExpired("ip", 3), "ifconfig": OSError("denied")},
            {"ip": "not json", "ifconfig": ""},
        ):
            with self.subTest(answers=answers):
                networks = local_ipv4_networks(
                    runner=FakeRunner(answers),
                    platform="linux",
                    udp_probe=lambda: "10.1.2.3",
                )
                self.assertEqual(
                    networks, [LocalNetwork("default", "10.1.2.3", "10.1.2.0/24")]
                )

    def test_fallback_without_route_or_with_loopback_is_empty(self) -> None:
        """No usable route yields an empty list."""
        for probe_result in (None, "", "127.0.0.1", "0.0.0.0"):

            def probe(value: str | None = probe_result) -> str | None:
                return value

            with self.subTest(probe_result=probe_result):
                result = local_ipv4_networks(
                    runner=FakeRunner({}), platform="darwin", udp_probe=probe
                )
                self.assertEqual(result, [])


class ValidateScanTargetTests(unittest.TestCase):
    """Strict private-only target validation."""

    def test_accepts_private_networks(self) -> None:
        """RFC 1918, CGNAT, link-local, host bits, and single addresses pass."""
        cases = {
            "192.168.1.0/24": "192.168.1.0/24",
            " 192.168.1.77/24 ": "192.168.1.0/24",
            "192.168.1.77": "192.168.1.77/32",
            "10.4.0.0/22": "10.4.0.0/22",
            "172.31.255.0/24": "172.31.255.0/24",
            "100.64.0.0/22": "100.64.0.0/22",
            "169.254.1.0/24": "169.254.1.0/24",
            "192.168.1.0/255.255.255.0": "192.168.1.0/24",
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(
                    validate_scan_target(text), ipaddress.IPv4Network(expected)
                )

    def test_size_boundary(self) -> None:
        """A /22 (1024 addresses) is the largest accepted network."""
        self.assertEqual(
            validate_scan_target("10.0.0.0/22").num_addresses, MAX_SCAN_HOSTS
        )
        for text in ("10.0.0.0/21", "10.0.0.0/8", "169.254.0.0/16"):
            with self.subTest(text=text), self.assertRaises(DiscoveryError) as caught:
                validate_scan_target(text)
            self.assertEqual(caught.exception.code, "scan_target_too_large")

    def test_rejects_non_private_networks(self) -> None:
        """Public, loopback, multicast, reserved, and straddling ranges fail."""
        for text in (
            "8.8.8.0/24",
            "8.8.8.8",
            "0.0.0.0/0",
            "0.0.0.0",
            "127.0.0.0/24",
            "224.0.0.0/24",
            "240.0.0.0/24",
            "255.255.255.255",
            "172.32.0.0/24",
            "192.168.0.0/15",
            "100.128.0.0/24",
        ):
            with self.subTest(text=text), self.assertRaises(DiscoveryError) as caught:
                validate_scan_target(text)
            self.assertEqual(caught.exception.code, "scan_target_not_private")

    def test_rejects_garbage(self) -> None:
        """Unparsable or non-IPv4 input is ``invalid_scan_target``."""
        for value in (
            "",
            "   ",
            "spark.local",
            "192.168.1.0/33",
            "192.168.1.0/-1",
            "192.168.1.1 /24",
            "192.168.001.1",
            "fe80::/64",
            "::1",
            "1" * 100,
            12345,
            None,
        ):
            with self.subTest(value=value), self.assertRaises(DiscoveryError) as caught:
                validate_scan_target(value)  # type: ignore[arg-type]
            self.assertEqual(caught.exception.code, "invalid_scan_target")


class DefaultScanTargetsTests(unittest.TestCase):
    """Default targets derived from local networks."""

    def test_clamps_large_networks_and_skips_public_ones(self) -> None:
        """/22 and smaller stay; larger become the host's /24; public is skipped."""
        networks = [
            LocalNetwork("en0", "192.168.1.5", "192.168.1.0/24"),
            LocalNetwork("en0", "192.168.1.6", "192.168.1.0/24"),
            LocalNetwork("en1", "10.0.5.9", "10.0.0.0/16"),
            LocalNetwork("en5", "169.254.3.4", "169.254.0.0/16"),
            LocalNetwork("en6", "192.168.2.9", "192.168.2.0/23"),
            LocalNetwork("en7", "10.20.0.1", "10.20.0.0/22"),
            LocalNetwork("en8", "203.0.113.5", "203.0.113.0/24"),
            LocalNetwork("bad", "not-an-ip", "nonsense"),
        ]
        self.assertEqual(
            default_scan_targets(networks),
            [
                ipaddress.IPv4Network("192.168.1.0/24"),
                ipaddress.IPv4Network("10.0.5.0/24"),
                ipaddress.IPv4Network("169.254.3.0/24"),
                ipaddress.IPv4Network("192.168.2.0/23"),
                ipaddress.IPv4Network("10.20.0.0/22"),
            ],
        )

    def test_empty_input(self) -> None:
        """No networks means no targets."""
        self.assertEqual(default_scan_targets([]), [])

    def test_local_network_serializes(self) -> None:
        """``LocalNetwork.to_dict`` mirrors its fields."""
        self.assertEqual(
            LocalNetwork("en0", "192.168.1.5", "192.168.1.0/24").to_dict(),
            {"interface": "en0", "address": "192.168.1.5", "network": "192.168.1.0/24"},
        )


if __name__ == "__main__":
    unittest.main()
