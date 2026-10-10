# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import base64
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from controller.operations import DeploymentSpec, NodeAssignment
from controller.orchestrator import PreflightFailed
from controller.transport.base import (
    CanaryRequest,
    CommandResult,
    LogChunk,
    NodeObservation,
    UnitState,
    canary_request_for,
    evaluate_canary_response,
)
from controller.transport.managed_ssh import (
    USER_MANAGER_CHECK,
    ClusterScriptRecipe,
    ManagedSSHNode,
    ManagedSSHTransport,
)
from controller.transport.ssh import SSHExecution, SSHExecutionError


class StubExecutor:
    def __init__(self, *outputs: str) -> None:
        self.outputs = list(outputs)
        self.scripts: list[str] = []
        self.calls: list[tuple[str, str]] = []

    def run_script(self, node_id, script, *, timeout_seconds, check=False):
        self.scripts.append(script)
        self.calls.append((node_id, script))
        output = self.outputs.pop(0)
        if isinstance(output, Exception):
            raise output
        return SSHExecution(0, output, "")


class StubRunner:
    def __init__(self) -> None:
        self.calls = []

    def start_unit(self, node_id, unit_name, recipe_id):
        self.calls.append(("start", node_id, unit_name, recipe_id))
        return CommandResult(True, "started")

    def stop_unit(self, node_id, unit_name, *, timeout_seconds, force=False):
        self.calls.append(("stop", node_id, unit_name, timeout_seconds, force))
        return CommandResult(True, "stopped")

    def unit_logs(self, node_id, unit_name, cursor=None, limit=100):
        return LogChunk(("one",), None)


class BashExecutor:
    """Run each script locally through bash with stubbed node binaries."""

    def __init__(self, environment: dict[str, str]) -> None:
        self.environment = environment
        self.calls: list[tuple[str, str, float]] = []

    def run_script(self, node_id, script, *, timeout_seconds, check=False):
        self.calls.append((node_id, script, timeout_seconds))
        result = subprocess.run(
            ["bash", "-s"],
            input=script,
            capture_output=True,
            text=True,
            env=self.environment,
            check=False,
        )
        if check and result.returncode:
            raise SSHExecutionError(result.stderr.strip() or "script failed")
        return SSHExecution(result.returncode, result.stdout, result.stderr)


# Records argv and stdin, writes the canned body to the -o file, and prints
# the -w summary like curl does.
_CANARY_CURL_STUB = """capture=$CANARY_CAPTURE
out=
while [ $# -gt 0 ]; do
  printf '%s\\n' "$1" >> "$capture/argv"
  case "$1" in
    -o) out=$2; printf '%s\\n' "$2" >> "$capture/argv"; shift ;;
  esac
  shift
done
cat > "$capture/stdin"
if [ "$CANARY_STATUS" = 000 ]; then printf '000 0.001'; exit 7; fi
printf '%s' "$CANARY_BODY" > "$out"
printf '%s 0.512' "$CANARY_STATUS"
"""


def _b64(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


def _models_b64(alias: str) -> str:
    return _b64(json.dumps({"data": [{"id": alias}]}))


_CLUSTER_ENVIRONMENT = ["MAX_MODEL_LEN=1048576", "MAX_NUM_SEQS=2"]


def _observation(*, managed=True, health=200, alias="example-model") -> str:
    models = json.dumps({"data": [{"id": alias}]}) if alias else ""
    unit = "dgx-model-production.service" if managed else ""
    pid = 42 if managed else 0
    legacy = 0 if managed else 99
    return "\n".join(
        [
            "HOSTNAME=spark-a1b2",
            f"UNIT={unit}",
            "ACTIVE=active" if managed else "ACTIVE=inactive",
            "SUB=running" if managed else "SUB=dead",
            f"PID={pid}",
            f"LEGACY_PID={legacy}",
            f"CMDLINE_B64={_b64('/opt/llama-server --alias ' + alias)}",
            f"HEALTH_CODE={health}",
            f"MODELS_B64={_b64(models)}",
            "MEM_TOTAL=130000000000",
            "MEM_AVAILABLE=14000000000",
            "SWAP_TOTAL=17000000000",
            "SWAP_FREE=16500000000",
            "UPTIME_SECONDS=1234",
        ]
    )


def _spec() -> DeploymentSpec:
    return DeploymentSpec(
        deployment_id="production",
        profile_id="example-profile",
        expected_alias="example-model",
        assignments=(NodeAssignment("spark-1"),),
        minimum_free_memory_bytes=16 * 1024**3,
        profile={
            "runner": {
                "script_id": "example",
                "script_path": "/home/operator/load-example.sh",
                "expected_sha256": "a" * 64,
                "expected_mode": "0644",
                "expected_size_bytes": 10,
            },
            "artifact": {
                "primary_path": "/models/example-00001.gguf",
                "primary_size_bytes": 20,
                "required_files": ["/models/example-00001.gguf"],
            },
            "runtime": {
                "executable_path": "/opt/llama-server",
                "build_revision": "10036",
                "build_commit": "3dafb585f",
            },
            "safety": {"maximum_swap_used_mib": 1024},
        },
    )


def _cluster_recipe() -> ClusterScriptRecipe:
    return ClusterScriptRecipe(
        profile_id="deepseek-profile",
        deployment_id="dep-deepseek-profile",
        node_ids=("spark-1", "spark-2"),
        control_node_id="spark-1",
        start_script_path="/recipes/start.sh",
        stop_script_path="/recipes/stop.sh",
        start_sha256="a" * 64,
        stop_sha256="b" * 64,
        working_directory="/recipes",
        environment_path="/recipes/.env",
        required_environment={"MAX_MODEL_LEN": "1048576", "MAX_NUM_SEQS": "2"},
        expected_recipe_commit="abc123",
        image_digest="registry/image@sha256:" + "c" * 64,
        container_name="deepseek-vllm-dspark-1",
        endpoint_port=8888,
        health_path="/health",
        models_path="/v1/models",
        expected_alias="deepseek-v4-flash-0731",
        required_command_fragments=(
            "/usr/local/bin/vllm",
            "--max-model-len",
            "1048576",
            "--max-num-seqs",
            "2",
        ),
    )


def _cluster_spec() -> DeploymentSpec:
    return DeploymentSpec(
        deployment_id="dep-deepseek-profile",
        profile_id="deepseek-profile",
        expected_alias="deepseek-v4-flash-0731",
        assignments=(
            NodeAssignment("spark-1", "head"),
            NodeAssignment("spark-2", "worker"),
        ),
        profile={
            "deployment_mode": "shared-vllm",
            "runner": {"required_environment": {}},
            "artifact": {},
            "safety": {},
        },
    )


class ManagedSSHTransportTests(unittest.TestCase):
    def transport(self, executor):
        runner = StubRunner()
        transport = ManagedSSHTransport(
            executor,
            runner,
            {"spark-1": ManagedSSHNode("spark-1", "192.0.2.10")},
        )
        return transport, runner

    def test_observe_requires_named_unit_for_managed_identity(self) -> None:
        transport, _ = self.transport(StubExecutor(_observation(managed=True)))
        observed = transport.observe("spark-1")
        self.assertTrue(observed.managed)
        self.assertEqual(observed.deployment_id, "production")
        self.assertEqual(observed.unit_state, UnitState.RUNNING)
        self.assertTrue(observed.ready_for("production", "example-model"))

    def test_stopped_unit_with_stale_pid_is_not_managed(self) -> None:
        output = (
            _observation(managed=True)
            .replace("ACTIVE=active", "ACTIVE=inactive")
            .replace("SUB=running", "SUB=dead")
        )
        executor = StubExecutor(output)
        transport, _ = self.transport(executor)

        observed = transport.observe("spark-1")

        self.assertEqual(UnitState.STOPPED, observed.unit_state)
        self.assertFalse(observed.managed)
        self.assertIsNone(observed.deployment_id)
        self.assertIn('$3 != "inactive"', executor.scripts[0])

    def test_foreign_gpu_container_occupies_an_otherwise_idle_node(self) -> None:
        output = (
            _observation(managed=False, alias="")
            .replace("LEGACY_PID=99", "LEGACY_PID=0")
            .replace("HEALTH_CODE=200", "HEALTH_CODE=000")
            + "\nFOREIGN_CONTAINERS= hand-started-vllm"
            + "\nHOST_GPU_PROCESSES="
            + f"\nFOREIGN_ENDPOINTS= 8888={_models_b64('glm-5.3-flash')}"
        )
        transport, _ = self.transport(StubExecutor(output))

        observed = transport.observe("spark-1")

        self.assertEqual(UnitState.RUNNING, observed.unit_state)
        self.assertFalse(observed.managed)
        self.assertIsNone(observed.deployment_id)
        # The foreign alias is operator evidence only; it must never defer the
        # memory gate or count toward readiness.
        self.assertIsNone(observed.active_alias)
        self.assertEqual(
            "unmanaged GPU workload observed: container hand-started-vllm · "
            "serving glm-5.3-flash on port 8888",
            observed.message,
        )
        self.assertEqual(
            {"8888": ["glm-5.3-flash"]}, observed.facts["foreign_endpoints"]
        )
        self.assertFalse(observed.stopped_for("anything", 0))

    def test_host_gpu_process_is_named_without_its_pid(self) -> None:
        output = (
            _observation(managed=False, alias="").replace(
                "LEGACY_PID=99", "LEGACY_PID=0"
            )
            + "\nFOREIGN_CONTAINERS="
            + "\nHOST_GPU_PROCESSES= python3:4242"
            + "\nFOREIGN_ENDPOINTS="
        )
        transport, _ = self.transport(StubExecutor(output))

        observed = transport.observe("spark-1")

        self.assertEqual(UnitState.RUNNING, observed.unit_state)
        self.assertEqual(("python3",), observed.facts["foreign_gpu_processes"])
        self.assertIn("host process python3", observed.message)

    @unittest.skipUnless(shutil.which("bash"), "bash is required")
    def test_user_manager_check_accepts_a_degraded_manager(self) -> None:
        for reported, accepted in (
            ("running", True),
            ("degraded", True),
            ("starting", False),
            ("offline", False),
        ):
            with self.subTest(reported), tempfile.TemporaryDirectory() as directory:
                stub = Path(directory) / "systemctl"
                stub.write_text(f"#!/bin/bash\necho {reported}\nexit 1\n")
                stub.chmod(0o755)
                result = subprocess.run(
                    ["bash", "-c", USER_MANAGER_CHECK],
                    env={"PATH": f"{directory}:/usr/bin:/bin"},
                    check=False,
                )
                self.assertEqual(accepted, result.returncode == 0)

    def test_probe_port_range_must_be_ordered(self) -> None:
        with self.assertRaises(ValueError):
            ManagedSSHNode("spark-1", "192.0.2.10", probe_port_range=(9000, 8000))

    @unittest.skipUnless(shutil.which("bash"), "bash is required")
    def test_observe_script_reports_foreign_gpu_workloads(self) -> None:
        executor = StubExecutor(_observation(managed=True))
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode(
                    "spark-1", "192.0.2.10", probe_port_range=(8000, 8999)
                )
            },
        )
        transport.observe("spark-1")
        stubs = {
            "docker": """case "$1" in
  ps) printf 'c1\\nc2\\n' ;;
  inspect)
    case "$4" in
      c1) printf '%s\\n' '/glm|[{"Capabilities":[["gpu"]]}]|[]' ;;
      c2) printf '%s\\n' '/web|null|[]' ;;
    esac ;;
esac""",
            "nvidia-smi": "printf '4194301\\n'",
            "ss": "printf '%s\\n' 'LISTEN 0 4096 0.0.0.0:8888 0.0.0.0:*' "
            "'LISTEN 0 4096 0.0.0.0:22 0.0.0.0:*'",
            "curl": """for arg in "$@"; do last=$arg; done
case "$last" in
  *:8888/v1/models) printf '{"data":[{"id":"glm-5.3-flash"}]}' ;;
  *) exit 7 ;;
esac""",
            "systemctl": "exit 0",
            "pgrep": "exit 1",
            # GNU base64 takes -w0; BSD base64 does not, so normalise both.
            "base64": 'if [ "${1:-}" = -w0 ]; then shift; fi\n'
            '/usr/bin/base64 "$@" | tr -d "\\n"',
        }
        with tempfile.TemporaryDirectory() as directory:
            bin_dir = Path(directory)
            for name, body in stubs.items():
                path = bin_dir / name
                path.write_text("#!/bin/bash\n" + body + "\n")
                path.chmod(0o755)
            result = subprocess.run(
                ["bash", "-c", executor.scripts[0]],
                capture_output=True,
                text=True,
                env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
                check=False,
            )
        fields = dict(
            line.split("=", 1) for line in result.stdout.splitlines() if "=" in line
        )
        self.assertEqual(" glm", fields["FOREIGN_CONTAINERS"], result.stderr)
        self.assertTrue(fields["FOREIGN_ENDPOINTS"].startswith(" 8888="))
        # No container cgroup owns the PID, so it counts as a host GPU process.
        self.assertEqual(" process:4194301", fields["HOST_GPU_PROCESSES"])

    def test_observe_endpoint_probe_is_bounded(self) -> None:
        executor = StubExecutor(_observation(managed=True))
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode(
                    "spark-1", "192.0.2.10", probe_port_range=(8000, 8999)
                )
            },
        )
        transport.observe("spark-1")
        listening = " ".join(
            f"'LISTEN 0 4096 0.0.0.0:{port} 0.0.0.0:*'" for port in range(8100, 8110)
        )
        with tempfile.TemporaryDirectory() as directory:
            bin_dir = Path(directory)
            calls = bin_dir / "curl.calls"
            stubs = {
                "ss": f"printf '%s\\n' {listening}",
                "curl": f'printf "%s\\n" "$*" >> {calls}\nexit 28',
                "docker": "exit 1",
                "systemctl": "exit 0",
                "pgrep": "exit 1",
            }
            for name, body in stubs.items():
                path = bin_dir / name
                path.write_text("#!/bin/bash\n" + body + "\n")
                path.chmod(0o755)
            subprocess.run(
                ["bash", "-c", executor.scripts[0]],
                capture_output=True,
                text=True,
                env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
                check=False,
            )
            probes = [call for call in calls.read_text().splitlines() if ":81" in call]
        # Six ports at one second each fit the 15 second observation budget.
        self.assertEqual(6, len(probes))
        for probe in probes:
            self.assertIn("--connect-timeout 1 --max-time 1", probe)

    def test_readiness_rejects_an_extra_served_alias(self) -> None:
        observed = NodeObservation(
            node_id="spark-1",
            reachable=True,
            free_memory_bytes=1,
            unit_state=UnitState.RUNNING,
            deployment_id="production",
            served_aliases=("example-model", "unexpected-model"),
            endpoint_healthy=True,
            managed=True,
        )
        self.assertFalse(observed.ready_for("production", "example-model"))

    def test_legacy_process_is_visible_but_not_adopted(self) -> None:
        transport, _ = self.transport(StubExecutor(_observation(managed=False)))
        observed = transport.observe("spark-1")
        self.assertFalse(observed.managed)
        self.assertIsNone(observed.deployment_id)
        self.assertEqual(observed.facts["legacy_pid"], 99)
        self.assertIn("unmanaged", observed.message)

    def test_preflight_defers_memory_when_resident_model_is_observed(self) -> None:
        check_names = [
            "script_readable",
            "script_syntax",
            "script_hash",
            "script_mode",
            "runtime_executable",
            "primary_model",
            "user_manager",
            "script_size",
            "primary_model_size",
            "model_file_1",
        ]
        preflight = "\n".join(
            [
                *(f"CHECK_{name}=ok" for name in check_names),
                f"RUNTIME_VERSION_B64={_b64('version: 10036 (3dafb585f)')}",
                "MEM_AVAILABLE=14000000000",
                "SWAP_USED=500000000",
            ]
        )
        transport, _ = self.transport(
            StubExecutor(preflight, _observation(managed=True))
        )
        checks = transport.preflight("spark-1", _spec(), NodeAssignment("spark-1"))
        self.assertTrue(all(check.passed for check in checks))
        memory = next(check for check in checks if check.name == "memory")
        self.assertTrue(memory.message.startswith("memory check deferred"))

    def test_start_and_stop_delegate_only_to_derived_named_unit(self) -> None:
        transport, runner = self.transport(StubExecutor())
        spec = _spec()
        self.assertTrue(transport.start("spark-1", spec, spec.assignments[0]).ok)
        self.assertTrue(transport.stop("spark-1", "production", timeout_seconds=5).ok)
        self.assertEqual(
            runner.calls,
            [
                ("start", "spark-1", "dgx-model-production", "example"),
                ("stop", "spark-1", "dgx-model-production", 5, False),
            ],
        )

    def test_cluster_observation_requires_marker_image_and_command_identity(
        self,
    ) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        command = json.dumps(
            [
                "bash",
                "-lc",
                "exec /usr/local/bin/vllm --max-model-len 1048576 --max-num-seqs 2",
            ]
        )
        cluster = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=true",
                f"IMAGE={_cluster_recipe().image_digest}",
                f"CMD_B64={_b64(command)}",
                f"ENV_B64={_b64(json.dumps(_CLUSTER_ENVIRONMENT))}",
                "OWNER=dep-deepseek-profile",
                "HEALTH_CODE=200",
                f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                "MEM_AVAILABLE=20000000000",
                "SWAP_USED=0",
            ]
        )
        executor = StubExecutor(idle, cluster)
        runner = StubRunner()
        transport = ManagedSSHTransport(
            executor,
            runner,
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        observed = transport.observe("spark-1")

        self.assertTrue(observed.managed)
        self.assertTrue(
            observed.ready_for("dep-deepseek-profile", "deepseek-v4-flash-0731")
        )

    def test_cluster_observation_rejects_substring_command_identity(self) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        command = json.dumps(
            [
                "bash",
                "-lc",
                "exec /usr/local/bin/vllm --max-model-len 10485760 --max-num-seqs 20",
            ]
        )
        cluster = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=true",
                f"IMAGE={_cluster_recipe().image_digest}",
                f"CMD_B64={_b64(command)}",
                f"ENV_B64={_b64(json.dumps(_CLUSTER_ENVIRONMENT))}",
                "OWNER=dep-deepseek-profile",
                "HEALTH_CODE=200",
                f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                "MEM_AVAILABLE=20000000000",
                "SWAP_USED=0",
            ]
        )
        transport = ManagedSSHTransport(
            StubExecutor(idle, cluster),
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        self.assertFalse(transport.observe("spark-1").managed)

    def test_cluster_ownership_survives_runtime_directory_loss(self) -> None:
        recipe = _cluster_recipe()
        executor = StubExecutor("", "", "CONTAINER_PRESENT=0", "", "")
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (recipe,),
        )
        self.assertTrue(transport._write_cluster_markers(recipe).ok)
        transport._observe_cluster("spark-1", recipe)
        transport._remove_cluster_markers(recipe)
        write_script, _, observe_script, remove_script, _ = executor.scripts
        read_owner = (
            observe_script.split("launcher_active=inactive")[0]
            + '\nprintf "%s" "$owner"\n'
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / "run"
            runtime.mkdir()
            env = {
                **os.environ,
                "XDG_STATE_HOME": str(root / "state"),
                "XDG_RUNTIME_DIR": str(runtime),
            }

            def run(script):
                return subprocess.run(
                    ["/bin/bash", "-c", script],
                    env=env,
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout

            run(write_script)
            marker = root / "state" / "dgx-controller" / recipe.marker_name
            self.assertTrue(marker.exists())
            self.assertEqual(0o600, marker.stat().st_mode & 0o777)
            shutil.rmtree(runtime)
            runtime.mkdir()
            self.assertEqual(recipe.deployment_id, run(read_owner))
            legacy = runtime / "dgx-controller" / recipe.marker_name
            legacy.parent.mkdir()
            legacy.write_text("legacy-owner")
            self.assertEqual(recipe.deployment_id, run(read_owner))
            marker.unlink()
            self.assertEqual("legacy-owner", run(read_owner))
            run(write_script)
            run(remove_script)
            self.assertFalse(marker.exists())
            self.assertFalse(legacy.exists())
            self.assertEqual("", run(read_owner))

    def test_failed_cluster_probe_cannot_report_idle(self) -> None:
        idle = _observation(managed=False, health=0, alias="").replace(
            "LEGACY_PID=99", "LEGACY_PID=0"
        )
        executor = StubExecutor(idle, SSHExecutionError("probe failed"))
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {"spark-1": ManagedSSHNode("spark-1", "192.0.2.10")},
            (_cluster_recipe(),),
        )
        observed = transport.observe("spark-1")
        self.assertFalse(observed.reachable)
        self.assertTrue(observed.facts["cluster_probe_failed"])

    def test_cluster_observation_rejects_wrong_flag_with_expected_decoy_token(
        self,
    ) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        command = json.dumps(
            [
                "bash",
                "-lc",
                "exec /usr/local/bin/vllm --max-model-len 4096 --dummy 1048576 "
                "--max-num-seqs 2; echo --max-model-len 1048576",
            ]
        )
        cluster = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=true",
                f"IMAGE={_cluster_recipe().image_digest}",
                f"CMD_B64={_b64(command)}",
                f"ENV_B64={_b64(json.dumps(_CLUSTER_ENVIRONMENT))}",
                "OWNER=dep-deepseek-profile",
                "HEALTH_CODE=200",
                f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                "MEM_AVAILABLE=20000000000",
                "SWAP_USED=0",
            ]
        )
        transport = ManagedSSHTransport(
            StubExecutor(idle, cluster),
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        observed = transport.observe("spark-1")

        self.assertFalse(observed.managed)
        self.assertEqual(
            ("MAX_MODEL_LEN",), observed.facts["command_flag_mismatch_keys"]
        )

    def test_cluster_observation_rejects_profile_environment_mismatch(self) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        command = json.dumps(
            [
                "bash",
                "-lc",
                "exec /usr/local/bin/vllm --max-model-len 1048576 --max-num-seqs 2",
            ]
        )
        cluster = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=true",
                f"IMAGE={_cluster_recipe().image_digest}",
                f"CMD_B64={_b64(command)}",
                f"ENV_B64={_b64(json.dumps(['MAX_MODEL_LEN=4096', 'MAX_NUM_SEQS=2']))}",
                "OWNER=dep-deepseek-profile",
                "HEALTH_CODE=200",
                f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                "MEM_AVAILABLE=20000000000",
                "SWAP_USED=0",
            ]
        )
        executor = StubExecutor(idle, cluster)
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        observed = transport.observe("spark-1")

        self.assertFalse(observed.managed)
        self.assertEqual(
            ("MAX_MODEL_LEN",), observed.facts["environment_mismatch_keys"]
        )

    def test_cluster_observation_enforces_long_prefill_threshold(self) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        recipe = replace(
            _cluster_recipe(),
            required_environment={"LONG_PREFILL_TOKEN_THRESHOLD": "1024"},
            required_command_fragments=(
                "/usr/local/bin/vllm",
                "--long-prefill-token-threshold",
            ),
        )
        for value, argument, expected in (
            ("1024", "${LONG_PREFILL_TOKEN_THRESHOLD}", True),
            ("4096", "${LONG_PREFILL_TOKEN_THRESHOLD}", False),
            ("1024", "4096", False),
            (None, "1024", True),
            (None, "4096", False),
        ):
            with self.subTest(value=value, argument=argument):
                command = json.dumps(
                    [
                        "bash",
                        "-lc",
                        "exec /usr/local/bin/vllm --long-prefill-token-threshold "
                        f'"{argument}"',
                    ]
                )
                environment = (
                    [] if value is None else [f"LONG_PREFILL_TOKEN_THRESHOLD={value}"]
                )
                cluster = "\n".join(
                    [
                        "CONTAINER_PRESENT=1",
                        "RUNNING=true",
                        f"IMAGE={recipe.image_digest}",
                        f"CMD_B64={_b64(command)}",
                        f"ENV_B64={_b64(json.dumps(environment))}",
                        "OWNER=dep-deepseek-profile",
                        "HEALTH_CODE=200",
                        f"MODELS_B64={_models_b64(recipe.expected_alias)}",
                        "MEM_AVAILABLE=20000000000",
                        "SWAP_USED=0",
                    ]
                )
                transport = ManagedSSHTransport(
                    StubExecutor(idle, cluster),
                    StubRunner(),
                    {"spark-1": ManagedSSHNode("spark-1", "192.0.2.10")},
                    (recipe,),
                )
                self.assertEqual(expected, transport.observe("spark-1").managed)

    def test_cluster_observation_enforces_fixed_kv_cache_bytes(self) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        recipe = replace(
            _cluster_recipe(),
            required_environment={"KV_CACHE_MEMORY_BYTES": "7753719563"},
            required_command_fragments=(
                "/usr/local/bin/vllm",
                "--kv-cache-memory-bytes",
            ),
        )
        for value, expected_managed in (("7753719563", True), ("7000000000", False)):
            with self.subTest(value=value):
                command = json.dumps(
                    [
                        "bash",
                        "-lc",
                        f"exec /usr/local/bin/vllm --kv-cache-memory-bytes {value}",
                    ]
                )
                cluster = "\n".join(
                    [
                        "CONTAINER_PRESENT=1",
                        "RUNNING=true",
                        f"IMAGE={recipe.image_digest}",
                        f"CMD_B64={_b64(command)}",
                        f"ENV_B64={_b64(json.dumps([]))}",
                        "OWNER=dep-deepseek-profile",
                        "HEALTH_CODE=200",
                        f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                        "MEM_AVAILABLE=20000000000",
                        "SWAP_USED=0",
                    ]
                )
                transport = ManagedSSHTransport(
                    StubExecutor(idle, cluster),
                    StubRunner(),
                    {
                        "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                        "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
                    },
                    (recipe,),
                )

                observed = transport.observe("spark-1")

                self.assertEqual(expected_managed, observed.managed)
                expected_mismatches = (
                    () if expected_managed else ("KV_CACHE_MEMORY_BYTES",)
                )
                self.assertEqual(
                    expected_mismatches,
                    observed.facts["environment_mismatch_keys"],
                )

    def test_cluster_observation_accepts_compose_substituted_command_identity(
        self,
    ) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        command = json.dumps(
            [
                "bash",
                "-lc",
                "exec /usr/local/bin/vllm --max-model-len 1048576 --max-num-seqs 2",
            ]
        )
        cluster = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=true",
                f"IMAGE={_cluster_recipe().image_digest}",
                f"CMD_B64={_b64(command)}",
                f"ENV_B64={_b64(json.dumps([]))}",
                "OWNER=dep-deepseek-profile",
                "HEALTH_CODE=200",
                f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                "MEM_AVAILABLE=20000000000",
                "SWAP_USED=0",
            ]
        )
        transport = ManagedSSHTransport(
            StubExecutor(idle, cluster),
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        observed = transport.observe("spark-1")

        self.assertTrue(observed.managed)
        self.assertEqual(
            ("MAX_MODEL_LEN", "MAX_NUM_SEQS"),
            observed.facts["command_substituted_environment_keys"],
        )
        self.assertEqual((), observed.facts["environment_mismatch_keys"])

    def test_cluster_observation_accepts_exact_environment_references(self) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        recipe = replace(
            _cluster_recipe(),
            required_command_fragments=(
                "/usr/local/bin/vllm",
                "--max-model-len",
                "--max-num-seqs",
            ),
        )
        command = json.dumps(
            [
                "bash",
                "-lc",
                'exec /usr/local/bin/vllm --max-model-len "${MAX_MODEL_LEN}" '
                '--max-num-seqs "$MAX_NUM_SEQS"',
            ]
        )
        cluster = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=true",
                f"IMAGE={recipe.image_digest}",
                f"CMD_B64={_b64(command)}",
                f"ENV_B64={_b64(json.dumps(_CLUSTER_ENVIRONMENT))}",
                "OWNER=dep-deepseek-profile",
                "HEALTH_CODE=200",
                f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                "MEM_AVAILABLE=20000000000",
                "SWAP_USED=0",
            ]
        )
        transport = ManagedSSHTransport(
            StubExecutor(idle, cluster),
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (recipe,),
        )

        observed = transport.observe("spark-1")

        self.assertTrue(observed.managed)
        self.assertEqual((), observed.facts["command_flag_mismatch_keys"])

    def test_cluster_observation_rejects_decoy_or_duplicate_command_identity(
        self,
    ) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        commands = (
            "echo --max-model-len 1048576; exec /usr/local/bin/vllm "
            "--max-model-len 4096 --max-num-seqs 2",
            "exec /usr/local/bin/vllm --max-model-len 1048576 "
            "--max-model-len 4096 --max-num-seqs 2",
            "# --max-model-len 1048576\nexec /usr/local/bin/vllm "
            "--max-model-len 4096 --max-num-seqs 2",
        )
        for command_text in commands:
            with self.subTest(command=command_text):
                command = json.dumps(["bash", "-lc", command_text])
                cluster = "\n".join(
                    [
                        "CONTAINER_PRESENT=1",
                        "RUNNING=true",
                        f"IMAGE={_cluster_recipe().image_digest}",
                        f"CMD_B64={_b64(command)}",
                        f"ENV_B64={_b64(json.dumps(['MAX_NUM_SEQS=2']))}",
                        "OWNER=dep-deepseek-profile",
                        "HEALTH_CODE=200",
                        f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                        "MEM_AVAILABLE=20000000000",
                        "SWAP_USED=0",
                    ]
                )
                transport = ManagedSSHTransport(
                    StubExecutor(idle, cluster),
                    StubRunner(),
                    {
                        "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                        "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
                    },
                    (_cluster_recipe(),),
                )

                observed = transport.observe("spark-1")

                self.assertFalse(observed.managed)
                self.assertEqual(
                    ("MAX_MODEL_LEN",),
                    observed.facts["environment_mismatch_keys"],
                )

    def test_cluster_observation_enforces_native_vision_tokenizer_mode(self) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        recipe = replace(
            _cluster_recipe(),
            required_environment={"ENABLE_LEGACY_DSV4_TOKENIZER_SHIM": "0"},
        )
        command = json.dumps(
            [
                "bash",
                "-lc",
                "exec /usr/local/bin/vllm --max-model-len 1048576 --max-num-seqs 2",
            ]
        )
        cluster = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=true",
                f"IMAGE={recipe.image_digest}",
                f"CMD_B64={_b64(command)}",
                f"ENV_B64={_b64(json.dumps(['ENABLE_LEGACY_DSV4_TOKENIZER_SHIM=1']))}",
                "OWNER=dep-deepseek-profile",
                "HEALTH_CODE=200",
                f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                "MEM_AVAILABLE=20000000000",
                "SWAP_USED=0",
            ]
        )
        executor = StubExecutor(idle, cluster)
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (recipe,),
        )

        observed = transport.observe("spark-1")

        self.assertFalse(observed.managed)
        self.assertEqual(
            ("ENABLE_LEGACY_DSV4_TOKENIZER_SHIM",),
            observed.facts["environment_mismatch_keys"],
        )

    def test_cluster_observation_enforces_rank_specific_image_id_when_pinned(
        self,
    ) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        recipe = replace(
            _cluster_recipe(),
            required_environment={
                "VLLM_HEAD_IMAGE_ID": "sha256:" + "1" * 64,
                "WORKER_VLLM_IMAGE_ID": "sha256:" + "2" * 64,
            },
        )
        command = json.dumps(
            [
                "bash",
                "-lc",
                "exec /usr/local/bin/vllm --max-model-len 1048576 --max-num-seqs 2",
            ]
        )
        cluster = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=true",
                f"IMAGE={recipe.image_digest}",
                "IMAGE_ID=sha256:" + "9" * 64,
                f"CMD_B64={_b64(command)}",
                "OWNER=dep-deepseek-profile",
                "HEALTH_CODE=200",
                f"MODELS_B64={_models_b64('deepseek-v4-flash-0731')}",
                "MEM_AVAILABLE=20000000000",
                "SWAP_USED=0",
            ]
        )
        executor = StubExecutor(idle, cluster)
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (recipe,),
        )

        observed = transport.observe("spark-1")

        self.assertFalse(observed.managed)
        self.assertFalse(observed.facts["identity_matches"])
        self.assertEqual("sha256:" + "1" * 64, observed.facts["expected_image_id"])

    def test_cluster_preflight_accepts_operator_environment_for_managed_overlay(
        self,
    ) -> None:
        check_names = (
            "recipe_commit",
            "image_digest",
            "start_script_readable",
            "start_script_syntax",
            "start_script_hash",
            "stop_script_readable",
            "stop_script_syntax",
            "stop_script_hash",
            "environment_source",
            "environment_overlay_directory",
            "user_manager",
        )
        preflight = "\n".join(
            [
                *(f"CHECK_{name}=ok" for name in check_names),
                f"RECIPE_COMMIT_B64={_b64('abc123')}",
                "SWAP_USED=0",
            ]
        )
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        executor = StubExecutor(preflight, idle, "CONTAINER_PRESENT=0")
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        checks = transport.preflight(
            "spark-1", _cluster_spec(), NodeAssignment("spark-1", "head")
        )

        self.assertTrue(all(check.passed for check in checks))
        recipe_check = next(check for check in checks if check.name == "recipe_commit")
        self.assertEqual(
            "recipe commit observed abc123; expected abc123",
            recipe_check.message,
        )
        self.assertIn("test -r /recipes/.env", executor.scripts[0])
        self.assertIn(".dgx-controller-recipe-commit", executor.scripts[0])
        self.assertIn("RECIPE_COMMIT_B64", executor.scripts[0])
        self.assertNotIn("MAX_MODEL_LEN=1048576", executor.scripts[0])

    def test_cluster_preflight_reports_observed_and_expected_recipe_commits(
        self,
    ) -> None:
        preflight = "\n".join(
            [
                "CHECK_recipe_commit=failed",
                "CHECK_image_digest=ok",
                f"RECIPE_COMMIT_B64={_b64('stale123')}",
                "SWAP_USED=0",
            ]
        )
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        executor = StubExecutor(preflight, idle, "CONTAINER_PRESENT=0")
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        checks = transport.preflight(
            "spark-2", _cluster_spec(), NodeAssignment("spark-2", "worker")
        )

        recipe_check = next(check for check in checks if check.name == "recipe_commit")
        self.assertFalse(recipe_check.passed)
        self.assertEqual(
            "recipe commit observed stale123; expected abc123",
            recipe_check.message,
        )
        error = PreflightFailed(
            (
                {
                    "node_id": "spark-2",
                    "name": recipe_check.name,
                    "message": recipe_check.message,
                    "remediation": recipe_check.remediation,
                },
            )
        )
        self.assertIn("observed stale123; expected abc123", error.message)

    def test_cluster_observation_surfaces_failed_durable_launcher(self) -> None:
        idle = _observation(managed=False).replace("LEGACY_PID=99", "LEGACY_PID=0")
        failed = "\n".join(
            [
                "CONTAINER_PRESENT=0",
                "OWNER=dep-deepseek-profile",
                "LAUNCHER_ACTIVE=failed",
                "LAUNCHER_SUB=failed",
                "LAUNCHER_RESULT=exit-code",
            ]
        )
        executor = StubExecutor(idle, failed)
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        observed = transport.observe("spark-1")

        self.assertEqual(UnitState.FAILED, observed.unit_state)
        self.assertTrue(observed.managed)
        self.assertTrue(observed.facts["launcher_failed"])

    def test_completed_launcher_reports_exact_exited_container_without_adopting_it(
        self,
    ) -> None:
        for status in ("exited", "dead"):
            with self.subTest(status=status):
                observed = self._observe_completed_cluster_container(status=status)
                self.assertEqual(UnitState.STOPPED, observed.unit_state)
                self.assertFalse(observed.managed)
                self.assertIsNone(observed.deployment_id)
                self.assertFalse(observed.is_process_running)
                self.assertFalse(observed.facts["launcher_failed"])
                self.assertEqual(
                    "dep-deepseek-profile", observed.facts.get("exited_deployment_id")
                )
                self.assertEqual(status, observed.facts["container_status"])
                self.assertEqual(137, observed.facts["container_exit_code"])

    def test_container_exit_evidence_requires_terminal_state_completed_launcher_and_identity(  # noqa: E501
        self,
    ) -> None:
        cases = (
            {"status": "created"},
            {"status": "restarting"},
            {"status": "paused"},
            {"status": ""},
            {"running": "true"},
            {"owner": "other-deployment"},
            {"owner": ""},
            {"image": "unreviewed/image:latest"},
            {"environment": ["MAX_MODEL_LEN=4096", "MAX_NUM_SEQS=2"]},
            {"launcher_sub": "running"},
            {"launcher_active": "inactive"},
            {"present": "0"},
        )
        for case in cases:
            with self.subTest(case=case):
                observed = self._observe_completed_cluster_container(**case)
                self.assertIsNone(observed.facts.get("exited_deployment_id"))

    def _observe_completed_cluster_container(
        self,
        *,
        status="exited",
        running="false",
        owner="dep-deepseek-profile",
        image=None,
        environment=None,
        launcher_sub="exited",
        launcher_active="active",
        present="1",
    ):
        idle = _observation(managed=False, health=0, alias="").replace(
            "LEGACY_PID=99", "LEGACY_PID=0"
        )
        command = json.dumps(
            [
                "bash",
                "-lc",
                "exec /usr/local/bin/vllm --max-model-len 1048576 --max-num-seqs 2",
            ]
        )
        output = "\n".join(
            [
                f"CONTAINER_PRESENT={present}",
                f"RUNNING={running}",
                f"CONTAINER_STATUS={status}",
                "CONTAINER_EXIT_CODE=137",
                f"IMAGE={image or _cluster_recipe().image_digest}",
                f"CMD_B64={_b64(command)}",
                f"ENV_B64={_b64(json.dumps(environment or _CLUSTER_ENVIRONMENT))}",
                f"OWNER={owner}",
                "HEALTH_CODE=000",
                "MODELS_B64=",
                "MEM_AVAILABLE=100",
                "SWAP_USED=0",
                f"LAUNCHER_ACTIVE={launcher_active}",
                f"LAUNCHER_SUB={launcher_sub}",
                "LAUNCHER_RESULT=success",
            ]
        )
        executor = StubExecutor(idle, output)
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )
        return transport.observe("spark-1")

    def test_cluster_recovery_rejects_mismatched_container_identity(self) -> None:
        mismatch = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=true",
                "IMAGE=unreviewed/image:latest",
                f"CMD_B64={_b64('[]')}",
                "OWNER=",
                "HEALTH_CODE=000",
                "MODELS_B64=",
                "MEM_AVAILABLE=100",
                "SWAP_USED=0",
            ]
        )
        executor = StubExecutor(mismatch)
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        result = transport.recover(_cluster_spec())

        self.assertFalse(result.ok)
        self.assertEqual("recovery_identity_mismatch", result.code)
        self.assertEqual(1, len(executor.scripts))

    def test_cluster_recovery_cleans_exact_stopped_container_after_reboot(self) -> None:
        stopped = "\n".join(
            [
                "CONTAINER_PRESENT=1",
                "RUNNING=false",
                "IMAGE=registry/image@sha256:" + "c" * 64,
                f"CMD_B64={_b64('["/usr/local/bin/vllm","--max-model-len","1048576","--max-num-seqs","2"]')}",
                f"ENV_B64={_b64('[]')}",
                "OWNER=",
                "HEALTH_CODE=000",
                "MODELS_B64=",
                "MEM_AVAILABLE=100",
                "SWAP_USED=0",
            ]
        )
        executor = StubExecutor(stopped, stopped, "", "stopped", "", "")
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        result = transport.recover(_cluster_spec())

        self.assertTrue(result.ok)
        self.assertEqual("cluster stop script completed", result.message)
        self.assertIn("script_path=/recipes/stop.sh", executor.scripts[3])

    def test_cluster_recovery_fails_closed_when_identity_probe_is_unreachable(
        self,
    ) -> None:
        executor = StubExecutor(SSHExecutionError("simulated identity probe outage"))
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )

        result = transport.recover(_cluster_spec())

        self.assertFalse(result.ok)
        self.assertEqual("node_unreachable", result.code)
        self.assertEqual(1, len(executor.scripts))

    def test_cluster_start_and_stop_use_only_pinned_head_scripts(self) -> None:
        executor = StubExecutor("", "started", "", "", "", "stopped", "", "")
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (_cluster_recipe(),),
        )
        spec = _cluster_spec()

        self.assertTrue(transport.start("spark-2", spec, spec.assignments[1]).ok)
        self.assertTrue(transport.start("spark-1", spec, spec.assignments[0]).ok)
        self.assertIn("source_path=/recipes/.env", executor.scripts[0])
        self.assertIn(
            "managed_path=/recipes/.env.dgx-controller-deepseek-profile",
            executor.scripts[0],
        )
        self.assertIn("MAX_MODEL_LEN", executor.scripts[0])
        self.assertNotIn('mv "$tmp" "$source_path"', executor.scripts[0])
        self.assertIn("dep-deepseek-profile.owner", executor.scripts[1])
        self.assertIn("dep-deepseek-profile.owner", executor.scripts[2])
        self.assertIn("script_path=/recipes/start.sh", executor.scripts[3])
        self.assertIn("sha256sum", executor.scripts[3])
        self.assertIn(
            "ENV_FILE=/recipes/.env.dgx-controller-deepseek-profile",
            executor.scripts[3],
        )
        self.assertIn("systemd-run --user", executor.scripts[3])
        self.assertTrue(
            transport.stop("spark-1", spec.deployment_id, timeout_seconds=5).ok
        )
        self.assertIn("systemctl --user stop", executor.scripts[4])
        self.assertIn("systemctl --user reset-failed", executor.scripts[4])
        self.assertIn("script_path=/recipes/stop.sh", executor.scripts[5])
        self.assertIn("sha256sum", executor.scripts[5])
        self.assertIn(
            "ENV_FILE=/recipes/.env.dgx-controller-deepseek-profile",
            executor.scripts[5],
        )

    def test_per_node_recipe_starts_and_stops_each_rank_on_its_own_node(self) -> None:
        recipe = replace(
            _cluster_recipe(),
            launch_mode="per-node",
            node_environment={
                "spark-1": {"NODE_RANK": "0", "VLLM_HOST_IP": "198.51.100.1"},
                "spark-2": {"NODE_RANK": "1", "VLLM_HOST_IP": "198.51.100.2"},
            },
        )
        executor = StubExecutor("", "", "started", "", "", "started", "", "stopped", "")
        transport = ManagedSSHTransport(
            executor,
            StubRunner(),
            {
                "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
            },
            (recipe,),
        )
        spec = _cluster_spec()

        worker = transport.start("spark-2", spec, spec.assignments[1])
        self.assertTrue(worker.ok)
        self.assertEqual(["spark-2"] * 3, [call[0] for call in executor.calls[:3]])
        self.assertIn("NODE_RANK", executor.scripts[0])
        self.assertIn("grep -Fqx -- NODE_RANK=1 ", executor.scripts[2])
        self.assertIn("systemd-run --user", executor.scripts[2])
        self.assertTrue(transport.start("spark-1", spec, spec.assignments[0]).ok)
        self.assertEqual(["spark-1"] * 3, [call[0] for call in executor.calls[3:6]])
        self.assertTrue(
            transport.stop("spark-2", spec.deployment_id, timeout_seconds=5).ok
        )
        self.assertEqual("spark-2", executor.calls[6][0])
        self.assertIn(
            'test -r "$env_file" || env_file=/recipes/.env', executor.scripts[7]
        )

    def test_per_node_recipe_requires_an_overlay_for_every_node(self) -> None:
        with self.assertRaises(ValueError):
            replace(
                _cluster_recipe(),
                launch_mode="per-node",
                node_environment={"spark-1": {"NODE_RANK": "0"}},
            )


class CompletionCanaryTests(unittest.TestCase):
    PROMPT = "Reply with OK. It's \"$(true)\"; `x` \\ 'quoted'"

    def _request(self) -> CanaryRequest:
        return CanaryRequest(120, 16, self.PROMPT)

    @unittest.skipUnless(shutil.which("bash"), "bash is required")
    def test_script_posts_one_bounded_request_and_keeps_no_text(self) -> None:
        reasoning_only = {
            "choices": [
                {
                    "message": {"content": "", "reasoning_content": "thinking"},
                    "finish_reason": "length",
                }
            ],
            "usage": {"completion_tokens": 16},
        }
        cases = (
            ("reasoning-only answer", "200", reasoning_only, True, "length", 16),
            (
                "stop",
                "200",
                {
                    "choices": [{"finish_reason": "stop"}],
                    "usage": {"completion_tokens": 2},
                },
                True,
                "stop",
                2,
            ),
            ("server error", "500", {"error": "boom"}, False, None, None),
            (
                "no usage",
                "200",
                {"choices": [{"finish_reason": "stop"}]},
                False,
                "stop",
                None,
            ),
            (
                "no finish reason",
                "200",
                {
                    "choices": [{"finish_reason": None}],
                    "usage": {"completion_tokens": 3},
                },
                False,
                None,
                3,
            ),
            (
                "zero tokens",
                "200",
                {
                    "choices": [{"finish_reason": "stop"}],
                    "usage": {"completion_tokens": 0},
                },
                False,
                "stop",
                0,
            ),
            ("connection refused", "000", {}, False, None, None),
        )
        for name, status, body, passed, reason, tokens in cases:
            with self.subTest(name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                bin_dir = root / "bin"
                capture = root / "capture"
                bin_dir.mkdir()
                capture.mkdir()
                stubs = {
                    "curl": _CANARY_CURL_STUB,
                    "base64": 'if [ "${1:-}" = -w0 ]; then shift; fi\n'
                    '/usr/bin/base64 "$@" | tr -d "\\n"',
                }
                for stub_name, stub_body in stubs.items():
                    path = bin_dir / stub_name
                    path.write_text("#!/bin/bash\n" + stub_body + "\n")
                    path.chmod(0o755)
                executor = BashExecutor(
                    {
                        "PATH": f"{bin_dir}:/usr/bin:/bin",
                        "CANARY_CAPTURE": str(capture),
                        "CANARY_STATUS": status,
                        "CANARY_BODY": json.dumps(body),
                    }
                )
                transport = ManagedSSHTransport(
                    executor,
                    StubRunner(),
                    {
                        "spark-1": ManagedSSHNode("spark-1", "192.0.2.10"),
                        "spark-2": ManagedSSHNode("spark-2", "192.0.2.11"),
                    },
                    (_cluster_recipe(),),
                )

                result = transport.completion_canary(
                    "spark-1", _cluster_spec(), self._request()
                )

                self.assertEqual(passed, result.passed, result.message)
                self.assertEqual(reason, result.finish_reason)
                self.assertEqual(tokens, result.completion_tokens)
                if status == "000":
                    self.assertIsNone(result.http_status)
                else:
                    self.assertEqual(int(status), result.http_status)
                    self.assertEqual(512, result.latency_ms)
                self.assertNotIn("thinking", repr(result))
                self.assertNotIn("OK", repr(result.evidence()))
                node_id, _script, timeout = executor.calls[0]
                self.assertEqual(("spark-1", 150), (node_id, timeout))
                argv = (capture / "argv").read_text().splitlines()
                self.assertEqual("http://127.0.0.1:8888/v1/chat/completions", argv[-1])
                self.assertEqual("120", argv[argv.index("--max-time") + 1])
                self.assertEqual("@-", argv[argv.index("--data-binary") + 1])
                self.assertEqual(
                    {
                        "model": "deepseek-v4-flash-0731",
                        "messages": [{"role": "user", "content": self.PROMPT}],
                        "max_tokens": 16,
                        "temperature": 0,
                    },
                    json.loads((capture / "stdin").read_text()),
                )

    def test_independent_unit_is_reached_like_observation(self) -> None:
        executor = StubExecutor("RESULT=200 0.010\nBODY_B64=")
        transport, _ = ManagedSSHTransportTests().transport(executor)

        result = transport.completion_canary("spark-1", _spec(), self._request())

        self.assertFalse(result.passed)
        self.assertIn(
            "http://192.0.2.10:11434/v1/chat/completions", executor.scripts[0]
        )
        self.assertIn("--max-time 120", executor.scripts[0])

    def test_unreachable_node_or_failed_script_fails_without_echoing_output(
        self,
    ) -> None:
        class FailedScript(StubExecutor):
            def run_script(self, node_id, script, *, timeout_seconds, check=False):
                super().run_script(
                    node_id, script, timeout_seconds=timeout_seconds, check=check
                )
                return SSHExecution(1, f"BODY_B64={_b64('secret answer')}", "")

        for executor in (
            StubExecutor(SSHExecutionError("connection reset")),
            FailedScript(""),
        ):
            with self.subTest(type(executor).__name__):
                transport, _ = ManagedSSHTransportTests().transport(executor)

                result = transport.completion_canary(
                    "spark-1", _spec(), self._request()
                )

                self.assertFalse(result.passed)
                self.assertEqual("completion_canary_failed", result.code)
                self.assertNotIn("secret", result.message)
                self.assertNotIn(_b64("secret answer"), result.message)

    def test_evaluator_bounds_finish_reason_and_rejects_boolean_tokens(self) -> None:
        odd = evaluate_canary_response(
            200,
            json.dumps(
                {
                    "choices": [{"finish_reason": "Stop because I said so"}],
                    "usage": {"completion_tokens": True},
                }
            ),
            5,
        )
        self.assertFalse(odd.passed)
        self.assertEqual("other", odd.finish_reason)
        self.assertIsNone(odd.completion_tokens)
        self.assertFalse(evaluate_canary_response(200, "not json", 5).passed)

    def test_disabled_or_absent_settings_skip_the_canary(self) -> None:
        self.assertIsNone(canary_request_for(_spec()))
        disabled = replace(_spec(), profile={"completion_canary": {"enabled": False}})
        self.assertIsNone(canary_request_for(disabled))
        enabled = replace(
            _spec(),
            profile={
                "completion_canary": {
                    "enabled": True,
                    "timeout_s": 120,
                    "max_tokens": 16,
                    "prompt": "Reply with OK.",
                    "completion_path": "/v1/chat/completions",
                }
            },
        )
        self.assertEqual(
            CanaryRequest(120, 16, "Reply with OK."), canary_request_for(enabled)
        )
        with self.assertRaises(ValueError):
            CanaryRequest(120, 16, "Reply with OK.", "/v1/chat?x=$(id)")


if __name__ == "__main__":
    unittest.main()
