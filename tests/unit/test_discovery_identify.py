# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Interpretation of recorded read-only identify probe outputs.

The fixtures mirror what the probe's ``emit`` prints on a DGX OS node: one
``KEY=<base64>`` line per fact. Docker 29 on DGX OS often exposes the GPU only
through CDI, without a registered ``nvidia`` runtime.
"""

from __future__ import annotations

import base64
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from controller.discovery.identify import PROBE_SCRIPT, SSHTarget, interpret_probe

TARGET = SSHTarget(host="192.0.2.10", user="spark", key_path="/tmp/id_ed25519")

RUNC_ONLY = (
    '{"io.containerd.runc.v2":{"path":"runc","status":{"org.opencontainers.runtime-'
    'spec.features":"{}"}},"runc":{"path":"runc"}}'
)
WITH_NVIDIA = (
    '{"io.containerd.runc.v2":{"path":"runc"},"nvidia":{"path":'
    '"nvidia-container-runtime"},"runc":{"path":"runc"}}'
)
CDI_JSON = (
    '[{"Source":"cdi","ID":"nvidia.com/gpu=0"},'
    '{"Source":"cdi","ID":"nvidia.com/gpu=GPU-00000000-1111-2222-3333-444444444444"},'
    '{"Source":"cdi","ID":"nvidia.com/gpu=all"}]'
)
# ``docker info`` text on Docker versions whose template lacks DiscoveredDevices;
# the probe keeps only the ``cdi:`` lines.
CDI_TEXT = (
    "  cdi: nvidia.com/gpu=0\n"
    "  cdi: nvidia.com/gpu=GPU-00000000-1111-2222-3333-444444444444\n"
    "  cdi: nvidia.com/gpu=all"
)
GPU_WARNING = "neither the NVIDIA runtime nor an NVIDIA CDI device"


def probe_output(**overrides: str) -> str:
    """Encode facts exactly like the probe's ``emit`` shell function."""
    facts = {
        "HOSTNAME": "spark-1a2b",
        "OS_PRETTY": "Ubuntu 24.04.3 LTS",
        "ARCH": "aarch64",
        "PRODUCT_NAME": "NVIDIA DGX Spark",
        "GPU": "NVIDIA GB10, 580.95.05",
        "CUDA": "CUDA Version: 13.0",
        "MEM_TOTAL": str(128 * 1024**3),
        "HOME_DIR": "/home/spark",
        "DOCKER_VERSION": "29.1.3",
        "DOCKER_RUNTIMES": RUNC_ONLY,
        "DOCKER_CDI_JSON": "",
        "SYSTEMD_USER": "running",
        "LINGER": "yes",
        **overrides,
    }
    return "".join(
        f"{key}={base64.b64encode(value.encode()).decode()}\n"
        for key, value in facts.items()
    )


def gpu_warnings(identity) -> list[str]:
    return [warning for warning in identity.warnings if GPU_WARNING in warning]


# Docker without a DiscoveredDevices template field, plus inert node tools;
# every other probe command runs locally and only reads.
_PROBE_STUBS = {
    "docker": """case "$1 ${3:-}" in
  "version "*) printf '29.1.3' ;;
  *DiscoveredDevices*) printf null ;;
  "info ") printf 'Server:\\n  cdi: nvidia.com/gpu=all\\n' ;;
esac""",
    # GNU base64 takes -w0; BSD base64 does not.
    "base64": 'if [ "${1:-}" = -w0 ]; then shift; fi\n'
    '/usr/bin/base64 "$@" | tr -d "\\n"',
    "systemctl": "exit 1",
    "loginctl": "exit 1",
    "nvidia-smi": "exit 1",
    "ip": "exit 1",
}


class GPUContainerSupportTests(unittest.TestCase):
    def test_container_support_comes_from_the_runtime_or_a_cdi_gpu_device(
        self,
    ) -> None:
        bad_json_list = '[{"Source":"cdi","ID":"nvidia.com/gpu=all; rm"}]'
        for name, overrides, support in (
            (
                "nvidia runtime",
                {"DOCKER_RUNTIMES": WITH_NVIDIA, "DOCKER_CDI_JSON": "null"},
                "runtime",
            ),
            ("CDI JSON", {"DOCKER_CDI_JSON": CDI_JSON}, "cdi"),
            ("CDI docker info text", {"DOCKER_CDI_TEXT": CDI_TEXT}, "cdi"),
            ("no CDI JSON", {"DOCKER_CDI_JSON": ""}, None),
            ("null CDI JSON", {"DOCKER_CDI_JSON": "null"}, None),
            ("empty CDI list", {"DOCKER_CDI_JSON": "[]"}, None),
            (
                "other vendor's CDI device",
                {"DOCKER_CDI_JSON": '[{"Source":"cdi","ID":"vendor/x=0"}]'},
                None,
            ),
            (
                "invalid JSON entry wins over text",
                {"DOCKER_CDI_JSON": bad_json_list, "DOCKER_CDI_TEXT": CDI_TEXT},
                None,
            ),
        ):
            with self.subTest(name):
                identity = interpret_probe(TARGET, probe_output(**overrides))
                self.assertEqual(support, identity.gpu_container_support)
                self.assertEqual(int(support is None), len(gpu_warnings(identity)))

    def test_no_docker_reports_only_the_docker_warning(self) -> None:
        identity = interpret_probe(
            TARGET, probe_output(DOCKER_VERSION="", DOCKER_RUNTIMES="")
        )
        self.assertIsNone(identity.gpu_container_support)
        self.assertEqual([], gpu_warnings(identity))
        self.assertTrue(any("Docker is not usable" in w for w in identity.warnings))

    @unittest.skipUnless(shutil.which("bash"), "bash is required")
    def test_probe_falls_back_to_docker_info_text_without_a_cdi_list(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            bin_dir = Path(directory)
            for tool, body in _PROBE_STUBS.items():
                (bin_dir / tool).write_text("#!/bin/bash\n" + body + "\n")
                (bin_dir / tool).chmod(0o755)
            completed = subprocess.run(
                ["bash", "-s"],
                input=PROBE_SCRIPT,
                capture_output=True,
                text=True,
                env={"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": directory},
                check=False,
            )
        identity = interpret_probe(TARGET, completed.stdout)
        self.assertEqual("cdi", identity.gpu_container_support, completed.stderr)


if __name__ == "__main__":
    unittest.main()
