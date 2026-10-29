# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Authenticated, read-only identification of a candidate DGX Spark node.

After the operator picks a scanned host, confirms its SSH host key, and selects
a local key, the controller runs one fixed probe script over SSH. The script
only reads system facts (hardware, GPU, memory, Docker access, network links,
systemd user-manager state); it never installs, starts, or changes anything.
Results classify the host and supply the reviewed values for ``nodes.json``.

Passwords never pass through the controller: authentication is key-only with
``BatchMode=yes``. If the key is not yet authorised on the node, the wizard shows
the operator an ``ssh-copy-id`` command to run themselves.
"""

from __future__ import annotations

import ipaddress
import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

from controller.transport.script_output import decode_b64, parse_fields
from controller.transport.ssh import batch_options

from .commands import Runner, run_captured

PROBE_SCRIPT = r"""set -u
emit() { printf '%s=%s\n' "$1" "$(printf '%s' "$2" | base64 -w0)"; }
emit HOSTNAME "$(hostname 2>/dev/null)"
emit OS_PRETTY "$( (. /etc/os-release 2>/dev/null && printf '%s' "${PRETTY_NAME:-}") )"
emit KERNEL "$(uname -r 2>/dev/null)"
emit ARCH "$(uname -m 2>/dev/null)"
emit DGX_RELEASE "$(head -c 2048 /etc/dgx-release 2>/dev/null)"
emit PRODUCT_NAME "$(cat /sys/class/dmi/id/product_name 2>/dev/null)"
emit SYS_VENDOR "$(cat /sys/class/dmi/id/sys_vendor 2>/dev/null)"
emit GPU "$(nvidia-smi --query-gpu=name,driver_version --format=csv,noheader \
  2>/dev/null | head -4)"
emit CUDA "$(nvidia-smi 2>/dev/null | grep -o 'CUDA Version: [0-9.]*' | head -1)"
emit MEM_TOTAL "$(awk '/^MemTotal:/ {print $2 * 1024}' /proc/meminfo 2>/dev/null)"
emit HOME_DIR "$HOME"
emit USER_NAME "$(id -un 2>/dev/null)"
emit DISK_FREE "$(df -B1 --output=avail "$HOME" 2>/dev/null | tail -1 | tr -d ' ')"
emit DOCKER_VERSION "$(docker version --format '{{.Server.Version}}' 2>/dev/null)"
emit DOCKER_RUNTIMES "$(docker info --format '{{json .Runtimes}}' 2>/dev/null)"
CDI_JSON="$(docker info --format '{{json .DiscoveredDevices}}' 2>/dev/null)"
emit DOCKER_CDI_JSON "$CDI_JSON"
case "$CDI_JSON" in
  \[*) ;;
  *) emit DOCKER_CDI_TEXT "$(docker info 2>/dev/null | grep -E '^[[:space:]]*cdi: ' \
       | head -32)" ;;
esac
emit SYSTEMD_USER "$(systemctl --user is-system-running 2>/dev/null)"
emit LINGER "$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null)"
emit IP_JSON "$(ip -j -4 addr show 2>/dev/null)"
emit RDMA "$(ls /sys/class/infiniband 2>/dev/null | tr '\n' ' ')"
emit NET_DRIVERS "$(for d in /sys/class/net/*; do n=${d##*/}; \
  drv=$(readlink -f "$d/device/driver" 2>/dev/null); \
  sp=$(cat "$d/speed" 2>/dev/null); \
  printf '%s %s %s\n' "$n" "${drv##*/}" "${sp:--1}"; done)"
emit MODELS_DIR "$(test -d "$HOME/models" && printf yes)"
emit HF_TOKEN_PRESENT "$(test -r "$HOME/.cache/huggingface/token" && printf yes)"
"""

# A CDI device name as Docker lists it, e.g. ``nvidia.com/gpu=all``.
_CDI_GPU_RE = re.compile(r"nvidia\.com/gpu=[A-Za-z0-9._:-]{1,80}")
_INTERFACE_NAME_RE = re.compile(r"[A-Za-z0-9._@-]{1,32}")

SPARK_PRODUCT_PATTERNS = (
    re.compile(r"dgx\s*spark", re.I),
    re.compile(r"ascent\s*gx10", re.I),
    re.compile(r"pro\s*max.*gb10", re.I),
    re.compile(r"gb10", re.I),
)


@dataclass(frozen=True)
class SSHTarget:
    """Explicit connection facts chosen by the operator in the wizard."""

    host: str
    user: str
    key_path: str
    port: int = 22

    def __post_init__(self) -> None:
        if not 1 <= self.port <= 65535:
            raise ValueError("SSH port must be between 1 and 65535")


@dataclass(frozen=True)
class NetworkInterface:
    name: str
    driver: str
    speed_mbps: int | None
    addresses: tuple[str, ...]  # CIDR strings

    @property
    def is_connectx(self) -> bool:
        return self.driver.startswith("mlx5")


@dataclass(frozen=True)
class NodeIdentity:
    """Facts from one authenticated probe; ``ok`` is False when SSH failed."""

    host: str
    user: str
    ok: bool
    error_code: str | None = None
    message: str = ""
    hostname: str = ""
    os_pretty: str = ""
    kernel: str = ""
    arch: str = ""
    dgx_release: str = ""
    product_name: str = ""
    sys_vendor: str = ""
    gpus: tuple[str, ...] = ()
    gpu_driver: str = ""
    cuda_version: str = ""
    memory_total_bytes: int | None = None
    home_dir: str = ""
    disk_free_bytes: int | None = None
    docker_version: str = ""
    nvidia_runtime: bool = False
    # How Docker exposes the GPU: "runtime" (nvidia runtime), "cdi" (a
    # discovered nvidia.com/gpu CDI device, Docker 28+), or None.
    gpu_container_support: str | None = None
    systemd_user_state: str = ""
    linger: bool | None = None
    interfaces: tuple[NetworkInterface, ...] = ()
    rdma_devices: tuple[str, ...] = ()
    models_dir_exists: bool = False
    hf_token_present: bool = False
    is_dgx_spark: bool = False
    spark_evidence: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    cluster_addresses: tuple[str, ...] = field(default=())

    @classmethod
    def failed(cls, target: SSHTarget, code: str, message: str) -> NodeIdentity:
        """A probe of ``target`` that did not run, with a stable error code."""
        return cls(target.host, target.user, False, code, message)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["interfaces"] = [
            {**asdict(interface), "is_connectx": interface.is_connectx}
            for interface in self.interfaces
        ]
        return value


def ssh_argv(
    target: SSHTarget, *, known_hosts_options: Sequence[str] = ()
) -> list[str]:
    """Exact argv for an onboarding probe; host/user/key are validated by the caller."""
    return [
        "ssh",
        *batch_options(8),
        "-o",
        "IdentitiesOnly=yes",
        *known_hosts_options,
        "-i",
        target.key_path,
        "-p",
        str(target.port),
        "-l",
        target.user,
        "--",
        target.host,
        "bash -s",
    ]


def classify_ssh_failure(returncode: int, stderr: str) -> tuple[str, str]:
    """Map an OpenSSH failure to a stable code and an operator-facing message."""
    text = stderr or ""
    lowered = text.lower()
    if "remote host identification has changed" in lowered:
        return (
            "host_key_changed",
            "The node's SSH host key changed since it was trusted. Verify the node "
            "before continuing.",
        )
    if (
        "host key verification failed" in lowered
        or "no ed25519 host key is known" in lowered
        or "host key is not known" in lowered
    ):
        return (
            "host_key_untrusted",
            "The node's SSH host key has not been confirmed yet.",
        )
    if "permission denied" in lowered:
        return (
            "auth_failed",
            "The node rejected this SSH key. Authorise the key on the node first.",
        )
    if (
        "could not resolve hostname" in lowered
        or "name or service not known" in lowered
    ):
        return "host_unresolvable", "The host name could not be resolved."
    if "connection refused" in lowered:
        return "ssh_refused", "SSH is not accepting connections on this host and port."
    if (
        "timed out" in lowered
        or "no route to host" in lowered
        or "network is unreachable" in lowered
    ):
        return "unreachable", "The node did not answer on the network."
    if "identity file" in lowered and "not accessible" in lowered:
        return "ssh_key_not_found", "The selected SSH key file is not accessible."
    if returncode == 255:
        return "ssh_failed", "SSH could not establish a session."
    return "probe_failed", "The read-only probe did not complete."


def probe_identity(
    target: SSHTarget,
    *,
    known_hosts_options: Sequence[str] = (),
    runner: Runner = subprocess.run,
    timeout_seconds: float = 25.0,
) -> NodeIdentity:
    """Run the fixed read-only probe and interpret its output."""
    try:
        completed = run_captured(
            runner,
            ssh_argv(target, known_hosts_options=known_hosts_options),
            timeout=timeout_seconds,
            input=PROBE_SCRIPT,
        )
    except FileNotFoundError:
        return NodeIdentity.failed(
            target, "ssh_missing", "The local OpenSSH client is not installed."
        )
    except subprocess.TimeoutExpired:
        return NodeIdentity.failed(
            target, "unreachable", "The read-only probe timed out."
        )
    if completed.returncode != 0 and "HOSTNAME=" not in (completed.stdout or ""):
        code, message = classify_ssh_failure(
            completed.returncode, completed.stderr or ""
        )
        return NodeIdentity.failed(target, code, message)
    return interpret_probe(target, completed.stdout or "")


def _clean(value: str, limit: int = 200) -> str:
    return "".join(character for character in value if character.isprintable()).strip()[
        :limit
    ]


def _int_or_none(value: str) -> int | None:
    value = value.strip()
    return int(value) if value.isdigit() else None


def _has_nvidia_runtime(runtimes_json: str) -> bool:
    try:
        runtimes = json.loads(runtimes_json or "null")
    except json.JSONDecodeError:
        return False
    return isinstance(runtimes, dict) and any(
        isinstance(name, str) and name.startswith("nvidia") for name in runtimes
    )


def _cdi_gpu_devices(devices_json: str, devices_text: str) -> tuple[str, ...]:
    """NVIDIA GPU devices Docker discovered through CDI.

    Docker 28+ reports ``DiscoveredDevices`` as JSON
    (``[{"Source": "cdi", "ID": "nvidia.com/gpu=all"}]``); the probe falls back
    to the ``cdi: nvidia.com/gpu=all`` lines of plain ``docker info``.
    """
    try:
        rows = json.loads(devices_json) if devices_json.strip() else None
    except json.JSONDecodeError:
        rows = None
    names: list[str] = []
    if isinstance(rows, list):
        names = [
            str(row.get("ID", ""))
            for row in rows
            if isinstance(row, dict) and str(row.get("Source", "")).lower() == "cdi"
        ]
    else:
        for line in devices_text.splitlines():
            source, separator, name = line.strip().partition(":")
            if separator and source == "cdi":
                names.append(name.strip())
    return tuple(name for name in names[:32] if _CDI_GPU_RE.fullmatch(name))


def _gpus(fields: Mapping[str, str]) -> tuple[tuple[str, ...], str]:
    """GPU names and the first GPU's driver version from ``nvidia-smi``."""
    gpu_lines = [
        line.strip() for line in fields.get("GPU", "").splitlines() if line.strip()
    ]
    gpus = tuple(_clean(line.split(",")[0]) for line in gpu_lines)
    driver = (
        _clean(gpu_lines[0].split(",")[1]) if gpu_lines and "," in gpu_lines[0] else ""
    )
    return gpus, driver


def _interfaces(fields: Mapping[str, str]) -> tuple[NetworkInterface, ...]:
    """Non-loopback interfaces with their IPv4 addresses, drivers and speeds."""
    drivers: dict[str, tuple[str, int | None]] = {}
    for line in fields.get("NET_DRIVERS", "").splitlines():
        parts = line.split()
        if len(parts) >= 2 and _INTERFACE_NAME_RE.fullmatch(parts[0]):
            speed = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
            drivers[parts[0]] = (_clean(parts[1], 40), speed)
    interfaces: list[NetworkInterface] = []
    try:
        ip_rows = json.loads(fields.get("IP_JSON") or "[]")
    except json.JSONDecodeError:
        ip_rows = []
    for row in ip_rows if isinstance(ip_rows, list) else []:
        if not isinstance(row, dict):
            continue
        name = row.get("ifname")
        if (
            not isinstance(name, str)
            or name == "lo"
            or not _INTERFACE_NAME_RE.fullmatch(name)
        ):
            continue
        addresses = []
        for info in row.get("addr_info") or []:
            if isinstance(info, dict) and info.get("family", "inet") == "inet":
                local, prefix = info.get("local"), info.get("prefixlen")
                try:
                    ipaddress.IPv4Address(local)
                except (ipaddress.AddressValueError, TypeError, ValueError):
                    continue
                if isinstance(prefix, int) and 0 <= prefix <= 32:
                    addresses.append(f"{local}/{prefix}")
        driver_name, speed = drivers.get(name, ("", None))
        interfaces.append(NetworkInterface(name, driver_name, speed, tuple(addresses)))
    return tuple(interfaces)


def _cluster_addresses(
    interfaces: Sequence[NetworkInterface], management: str
) -> tuple[str, ...]:
    """Sorted ConnectX IPv4 addresses other than management and link-local."""
    return tuple(
        sorted(
            {
                address.split("/")[0]
                for interface in interfaces
                if interface.is_connectx
                for address in interface.addresses
                if address.split("/")[0] != management
                and not address.startswith("169.254.")
            },
            key=lambda item: tuple(int(part) for part in item.split(".")),
        )
    )


def interpret_probe(target: SSHTarget, stdout: str) -> NodeIdentity:
    fields = {key: decode_b64(value) for key, value in parse_fields(stdout).items()}
    gpus, driver = _gpus(fields)
    cuda = _clean(fields.get("CUDA", "").replace("CUDA Version:", ""))
    interfaces = _interfaces(fields)
    product = _clean(fields.get("PRODUCT_NAME", ""))
    vendor = _clean(fields.get("SYS_VENDOR", ""))
    dgx_release = _clean(fields.get("DGX_RELEASE", ""), 400)
    arch = _clean(fields.get("ARCH", ""), 32)
    gb10_gpu = any("GB10" in gpu.upper() for gpu in gpus)
    spark_product = any(pattern.search(product) for pattern in SPARK_PRODUCT_PATTERNS)
    evidence: list[str] = []
    if gb10_gpu:
        evidence.append("GPU reports NVIDIA GB10")
    if spark_product:
        evidence.append(f"system product is {product!r}")
    if dgx_release:
        evidence.append("DGX OS release file present")
    if arch == "aarch64":
        evidence.append("aarch64 (Grace CPU)")
    is_spark = bool(evidence) and arch == "aarch64" and (gb10_gpu or spark_product)
    docker_version = _clean(fields.get("DOCKER_VERSION", ""), 40)
    nvidia_runtime = _has_nvidia_runtime(fields.get("DOCKER_RUNTIMES", ""))
    cdi_gpus = _cdi_gpu_devices(
        fields.get("DOCKER_CDI_JSON", ""), fields.get("DOCKER_CDI_TEXT", "")
    )
    gpu_container_support = "runtime" if nvidia_runtime else "cdi" if cdi_gpus else None
    linger_raw = _clean(fields.get("LINGER", ""), 8).lower()
    linger = True if linger_raw == "yes" else False if linger_raw == "no" else None
    systemd_state = _clean(fields.get("SYSTEMD_USER", ""), 32)
    warnings: list[str] = []
    if not docker_version:
        warnings.append(
            "Docker is not usable by this user (install Docker or add the user to the "
            "'docker' group)."
        )
    if docker_version and gpu_container_support is None:
        warnings.append(
            "Docker reports neither the NVIDIA runtime nor an NVIDIA CDI device; "
            "GPU containers need the NVIDIA Container Toolkit."
        )
    if linger is False:
        warnings.append(
            "systemd lingering is off: controller-started units stop when the last SSH "
            "session ends. "
            f"Run `sudo loginctl enable-linger {target.user}` on the node."
        )
    if systemd_state not in {"running", "degraded"}:
        warnings.append("The systemd user manager is not available over SSH.")
    if not gpus:
        warnings.append("nvidia-smi reported no GPU.")
    if not is_spark:
        warnings.append(
            "This host does not look like a DGX Spark (GB10); continue only if you "
            "know it is compatible."
        )
    return NodeIdentity(
        host=target.host,
        user=target.user,
        ok=True,
        hostname=_clean(fields.get("HOSTNAME", ""), 64),
        os_pretty=_clean(fields.get("OS_PRETTY", "")),
        kernel=_clean(fields.get("KERNEL", ""), 80),
        arch=arch,
        dgx_release=dgx_release,
        product_name=product,
        sys_vendor=vendor,
        gpus=gpus,
        gpu_driver=driver,
        cuda_version=cuda,
        memory_total_bytes=_int_or_none(fields.get("MEM_TOTAL", "")),
        home_dir=_clean(fields.get("HOME_DIR", ""), 200),
        disk_free_bytes=_int_or_none(fields.get("DISK_FREE", "")),
        docker_version=docker_version,
        nvidia_runtime=nvidia_runtime,
        gpu_container_support=gpu_container_support,
        systemd_user_state=systemd_state,
        linger=linger,
        interfaces=interfaces,
        rdma_devices=tuple(
            _clean(item, 32) for item in fields.get("RDMA", "").split() if item.strip()
        ),
        models_dir_exists=fields.get("MODELS_DIR", "") == "yes",
        hf_token_present=fields.get("HF_TOKEN_PRESENT", "") == "yes",
        is_dgx_spark=is_spark,
        spark_evidence=tuple(evidence),
        warnings=tuple(warnings),
        cluster_addresses=_cluster_addresses(interfaces, target.host),
    )


LINK_SCRIPT_TEMPLATE = """set -u
for peer in {peers}; do
  if ping -c 2 -W 1 "$peer" >/dev/null 2>&1; then
    printf 'LINK %s ok\\n' "$peer"
  else
    printf 'LINK %s failed\\n' "$peer"
  fi
done
"""


def check_cluster_links(
    target: SSHTarget,
    peers: Sequence[str],
    *,
    known_hosts_options: Sequence[str] = (),
    runner: Runner = subprocess.run,
    timeout_seconds: float = 30.0,
) -> dict[str, bool]:
    """From one node, ping peer cluster addresses (read-only reachability check)."""
    validated = []
    for peer in peers[:16]:
        validated.append(str(ipaddress.IPv4Address(peer)))
    if not validated:
        return {}
    script = LINK_SCRIPT_TEMPLATE.format(peers=" ".join(validated))
    try:
        completed = run_captured(
            runner,
            ssh_argv(target, known_hosts_options=known_hosts_options),
            timeout=timeout_seconds,
            input=script,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {peer: False for peer in validated}
    results = {peer: False for peer in validated}
    for line in (completed.stdout or "").splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[0] == "LINK" and parts[1] in results:
            results[parts[1]] = parts[2] == "ok"
    return results
