# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Local IPv4 interface discovery and scan-target validation.

Safety boundary: this module decides *where* the onboarding scanner may probe.
It only reads this host's interface table (``ip -j -4 addr show`` on Linux,
``ifconfig`` elsewhere) with a short timeout and never sends a packet itself; the
fallback uses a UDP ``connect()`` to a TEST-NET address, which selects a route
without transmitting anything. Every scan target must be an IPv4 network that
lies entirely inside a private range (RFC 1918, RFC 6598 CGNAT, or RFC 3927
link-local) and contains at most ``MAX_SCAN_HOSTS`` addresses, so a typo or a
hostile request can never turn the wizard into an Internet scanner.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import shutil
import socket
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

MAX_SCAN_HOSTS = 1024  # one /22
COMMAND_TIMEOUT_SECONDS = 3.0
_MAX_TARGET_TEXT = 64

PRIVATE_SCAN_RANGES: tuple[ipaddress.IPv4Network, ...] = (
    ipaddress.IPv4Network("10.0.0.0/8"),
    ipaddress.IPv4Network("172.16.0.0/12"),
    ipaddress.IPv4Network("192.168.0.0/16"),
    ipaddress.IPv4Network("100.64.0.0/10"),  # RFC 6598 carrier-grade NAT
    ipaddress.IPv4Network("169.254.0.0/16"),  # RFC 3927 link-local (direct cables)
)

# Container bridges, veth pairs, hypervisor host-only networks, and VPN tunnels.
# Scanning a VPN could probe a remote network the operator does not own.
SKIPPED_INTERFACE_PREFIXES: tuple[str, ...] = (
    "docker",
    "br-",
    "veth",
    "virbr",
    "vmnet",
    "vboxnet",
    "utun",
    "tun",
    "tap",
    "wg",
    "tailscale",
    "zt",
    "cni",
    "flannel",
    "cali",
)

_CODE_RE = re.compile(r"[a-z][a-z0-9_]*")
_LOOPBACK_INTERFACE_RE = re.compile(r"lo\d*")
_FALLBACK_ROUTE_PROBE = ("192.0.2.1", 9)  # RFC 5737 TEST-NET-1, discard port
_TARGET_HINT = "Enter an IPv4 address or CIDR such as 192.168.1.0/24."

Runner = Callable[..., Any]


class DiscoveryError(ValueError):
    """A safe, user-facing discovery failure with a stable snake_case code.

    Attributes:
        code: Stable machine-readable code such as ``scan_target_not_private``.
        message: Human-readable explanation without untrusted remote text.
        remediation: Optional next step for the operator.

    """

    def __init__(self, code: str, message: str, remediation: str = "") -> None:
        """Create the error; ``code`` must be lowercase snake_case."""
        if not _CODE_RE.fullmatch(code):
            raise ValueError(f"discovery error codes must be snake_case: {code!r}")
        self.code = code
        self.message = message
        self.remediation = remediation
        super().__init__(f"{message} ({code})")

    def to_dict(self) -> dict[str, str]:
        """Return a JSON-ready mapping for API error bodies."""
        return {
            "code": self.code,
            "message": self.message,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class LocalNetwork:
    """One IPv4 address assigned to a local interface and its on-link network.

    Attributes:
        interface: Interface name such as ``en0`` or ``eth0``.
        address: This host's IPv4 address on that interface.
        network: CIDR string such as ``192.168.1.0/24``.

    """

    interface: str
    address: str
    network: str

    def to_dict(self) -> dict[str, str]:
        """Return a JSON-ready mapping."""
        return {
            "interface": self.interface,
            "address": self.address,
            "network": self.network,
        }


def local_ipv4_networks(
    *,
    runner: Runner = subprocess.run,
    platform: str | None = None,
    udp_probe: Callable[[], str | None] | None = None,
) -> list[LocalNetwork]:
    """Return usable local IPv4 networks; command failures never raise.

    Linux parses ``ip -j -4 addr show`` and falls back to ``ifconfig``; macOS and
    other systems parse ``ifconfig``. If neither yields a network, the
    default-route source address (UDP connect, no packet sent) is reported as a
    /24 on interface ``"default"``. Loopback, multicast, unspecified, reserved,
    and /32 addresses are dropped, as are interfaces matching
    ``SKIPPED_INTERFACE_PREFIXES``. Link-local 169.254/16 entries are kept.

    Args:
        runner: ``subprocess.run`` compatible callable, injectable for tests.
        platform: ``sys.platform`` value; defaults to the running platform.
        udp_probe: Returns the default-route source address; injectable.

    Returns:
        Deduplicated networks in interface order.

    """
    system = sys.platform if platform is None else platform
    networks: list[LocalNetwork] = []
    if system.startswith("linux"):
        ip_tool = _tool("ip", ("/usr/sbin/ip", "/sbin/ip", "/usr/bin/ip"))
        output = _run_command(runner, (ip_tool, "-j", "-4", "addr", "show"))
        if output is not None:
            networks = parse_ip_json(output)
    if not networks:
        ifconfig = _tool("ifconfig", ("/sbin/ifconfig", "/usr/sbin/ifconfig"))
        output = _run_command(runner, (ifconfig,))
        if output is not None:
            networks = parse_ifconfig(output)
    if not networks:
        probe = _default_route_address if udp_probe is None else udp_probe
        fallback = _fallback_network(probe())
        if fallback is not None:
            networks = [fallback]
    return networks


def parse_ip_json(text: str) -> list[LocalNetwork]:
    """Parse iproute2 ``ip -j -4 addr show`` output.

    Args:
        text: JSON printed by ``ip``.

    Returns:
        Usable networks; ``[]`` for malformed input.

    """
    try:
        document = json.loads(text)
    except ValueError:
        return []
    if not isinstance(document, list):
        return []
    found: list[LocalNetwork] = []
    for entry in document:
        if not isinstance(entry, dict):
            continue
        name = entry.get("ifname")
        flags = entry.get("flags")
        addr_info = entry.get("addr_info")
        if not isinstance(name, str) or not name or not isinstance(addr_info, list):
            continue
        if isinstance(flags, list) and "UP" not in flags:
            continue
        for info in addr_info:
            if not isinstance(info, dict) or info.get("family", "inet") != "inet":
                continue
            local = info.get("local")
            prefix = info.get("prefixlen")
            if not isinstance(local, str) or type(prefix) is not int:
                continue
            network = _usable_network(name, local, prefix)
            if network is not None:
                found.append(network)
    return _dedupe(found)


_IFCONFIG_HEADER_RE = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9_.:@-]*?):?\s+(?:flags=\d+<([^>]*)>|Link\b)"
)
_IFCONFIG_INET_RE = re.compile(r"\s+inet\s+(?:addr:)?(\d{1,3}(?:\.\d{1,3}){3})\b(.*)")
_IFCONFIG_MASK_RE = re.compile(
    r"(?:netmask\s+|Mask:)(0x[0-9a-fA-F]{8}|\d{1,3}(?:\.\d{1,3}){3})"
)


def parse_ifconfig(text: str) -> list[LocalNetwork]:
    """Parse BSD/macOS or Linux net-tools ``ifconfig`` output.

    Accepted address lines: ``inet 192.168.1.5 netmask 0xffffff00 ...``
    (macOS), ``inet 192.168.1.5  netmask 255.255.255.0`` (net-tools 2.x), and
    ``inet addr:192.168.1.5 ... Mask:255.255.255.0`` (net-tools 1.x).
    Interfaces whose flags lack ``UP`` are skipped.

    Args:
        text: Output of ``ifconfig`` with no arguments.

    Returns:
        Usable networks; malformed lines are ignored.

    """
    found: list[LocalNetwork] = []
    interface: str | None = None
    interface_up = True
    for line in text.splitlines():
        if line and not line[0].isspace():
            header = _IFCONFIG_HEADER_RE.match(line)
            interface = header.group(1) if header else None
            flags = header.group(2) if header else None
            interface_up = flags is None or "UP" in flags.split(",")
            continue
        if interface is None or not interface_up:
            continue
        inet = _IFCONFIG_INET_RE.fullmatch(line)
        mask = _IFCONFIG_MASK_RE.search(inet.group(2)) if inet else None
        prefix = _netmask_prefix(mask.group(1)) if mask else None
        if inet is None or prefix is None:
            continue
        network = _usable_network(interface, inet.group(1), prefix)
        if network is not None:
            found.append(network)
    return _dedupe(found)


def _netmask_prefix(raw: str) -> int | None:
    try:
        if raw.lower().startswith("0x"):
            value = int(raw, 16)
        else:
            value = int(ipaddress.IPv4Address(raw))
    except ValueError:
        return None
    prefix = bin(value).count("1")
    contiguous = (0xFFFFFFFF << (32 - prefix)) & 0xFFFFFFFF
    return prefix if value == contiguous else None


def _usable_network(interface: str, address: str, prefix: int) -> LocalNetwork | None:
    if _skipped_interface(interface) or not 0 <= prefix < 32:
        return None
    try:
        iface = ipaddress.IPv4Interface(f"{address}/{prefix}")
    except ValueError:
        return None
    ip = iface.ip
    if ip.is_loopback or ip.is_multicast or ip.is_unspecified or ip.is_reserved:
        return None
    if prefix < 31 and ip == iface.network.broadcast_address:
        return None
    return LocalNetwork(interface, str(ip), str(iface.network))


def _skipped_interface(name: str) -> bool:
    lowered = name.lower()
    return bool(_LOOPBACK_INTERFACE_RE.fullmatch(lowered)) or lowered.startswith(
        SKIPPED_INTERFACE_PREFIXES
    )


def _fallback_network(address: str | None) -> LocalNetwork | None:
    return _usable_network("default", address, 24) if address else None


def _default_route_address() -> str | None:
    """Return the kernel's source address for an off-link route.

    ``connect()`` on a UDP socket only selects a route; no datagram is sent.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(_FALLBACK_ROUTE_PROBE)
            address = probe.getsockname()[0]
    except OSError:
        return None
    return address if isinstance(address, str) else None


def _tool(name: str, fallbacks: Sequence[str]) -> str:
    """Resolve a system tool even when an app bundle starts with a minimal PATH."""
    found = shutil.which(name)
    if found:
        return found
    for candidate in fallbacks:
        if os.access(candidate, os.X_OK):
            return candidate
    return name


def _run_command(runner: Runner, argv: Sequence[str]) -> str | None:
    environment = dict(os.environ)
    environment["LC_ALL"] = "C"  # net-tools localizes "inet addr:" and "Mask:"
    try:
        completed = runner(
            list(argv),
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
            check=False,
            stdin=subprocess.DEVNULL,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if getattr(completed, "returncode", 1) != 0:
        return None
    stdout = getattr(completed, "stdout", None)
    return stdout if isinstance(stdout, str) else None


def _dedupe(networks: Sequence[LocalNetwork]) -> list[LocalNetwork]:
    return list(dict.fromkeys(networks))


def validate_scan_target(cidr: str) -> ipaddress.IPv4Network:
    """Validate an operator-entered scan target.

    Host bits are tolerated (``192.168.1.7/24`` means ``192.168.1.0/24``) and a
    bare address becomes a /32.

    Args:
        cidr: IPv4 CIDR or single IPv4 address.

    Returns:
        The validated network.

    Raises:
        DiscoveryError: ``invalid_scan_target`` for unparsable or non-IPv4
            text, ``scan_target_not_private`` unless the network lies entirely
            in 10/8, 172.16/12, 192.168/16, 100.64/10, or 169.254/16, and
            ``scan_target_too_large`` above ``MAX_SCAN_HOSTS`` addresses.

    """
    text = cidr.strip() if isinstance(cidr, str) else ""
    if not text or len(text) > _MAX_TARGET_TEXT or any(c.isspace() for c in text):
        raise DiscoveryError("invalid_scan_target", _TARGET_HINT)
    try:
        if "/" in text:
            network = ipaddress.IPv4Network(text, strict=False)
        else:
            network = ipaddress.IPv4Network(f"{ipaddress.IPv4Address(text)}/32")
    except ValueError as exc:
        raise DiscoveryError(
            "invalid_scan_target",
            _TARGET_HINT,
            "IPv6 networks and host names are not scanned.",
        ) from exc
    if (
        network.is_loopback
        or network.is_multicast
        or network.is_reserved
        or network.is_unspecified
        or not any(network.subnet_of(allowed) for allowed in PRIVATE_SCAN_RANGES)
    ):
        raise DiscoveryError(
            "scan_target_not_private",
            f"{network} is not inside a private network range.",
            "Only 10/8, 172.16/12, 192.168/16, 100.64/10, and 169.254/16 "
            "networks can be scanned.",
        )
    if network.num_addresses > MAX_SCAN_HOSTS:
        raise DiscoveryError(
            "scan_target_too_large",
            f"{network} has {network.num_addresses} addresses; "
            f"the limit is {MAX_SCAN_HOSTS} addresses.",
            "Enter a smaller network, such as the 256-address subnet of your Sparks.",
        )
    return network


def default_scan_targets(
    networks: Sequence[LocalNetwork],
) -> list[ipaddress.IPv4Network]:
    """Choose safe default scan targets from local networks.

    Args:
        networks: Local networks, usually from ``local_ipv4_networks``.

    Returns:
        A network with prefix /22 or longer as-is; a larger one clamped to the
        /24 containing this host's address. Non-private networks are skipped;
        the result is deduplicated in input order.

    """
    targets: list[ipaddress.IPv4Network] = []
    for local in networks:
        try:
            address = ipaddress.IPv4Address(local.address)
            prefix = ipaddress.IPv4Network(local.network, strict=False).prefixlen
        except ValueError:
            continue
        clamped = prefix if prefix >= 22 else 24
        candidate = ipaddress.IPv4Network(f"{address}/{clamped}", strict=False)
        try:
            target = validate_scan_target(str(candidate))
        except DiscoveryError:
            continue
        if target not in targets:
            targets.append(target)
    return targets
