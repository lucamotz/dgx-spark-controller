# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Recipe drafts, rendering, template scripts, and the prepare/delete workflow."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import tempfile
import time
import unittest
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

from catalog_fakes import QWEN_LINK, fixture_catalog

from controller.config import parse_nodes_document
from controller.operations import (
    DeploymentRecord,
    OperationAction,
    OperationState,
    bounded_node_progress,
    latest_node_progress,
    utc_now,
)
from controller.persistence import OperationJournal
from controller.recipes.nodeops import (
    FakeRecipeNodeOps,
    SSHRecipeNodeOps,
    parse_linger,
    parse_removal,
    parse_status,
)
from controller.recipes.render import (
    NodeFacts,
    environment_text,
    is_generated_profile_id,
    model_dir_for,
    profile_id_for,
    rank_addresses,
    render_recipe,
    template_bytes,
)
from controller.recipes.service import RecipeService
from controller.recipes.spec import (
    RecipeDraft,
    RecipeError,
    RecipeModel,
    RecipePlacement,
    RecipeRuntime,
    RecipeSettings,
    settings_from_mapping,
)
from controller.recipes.store import RecipeStore
from controller.reconcile import Reconciler
from controller.transport.fake import FakeNodeTransport
from controller.transport.managed_ssh import _command_has_exact_tokens
from controller.transport.ssh import SSHExecution

DIGEST = "sha256:" + "a" * 64
IMAGE_ID = "sha256:" + "b" * 64
NODES_DOCUMENT = {
    "schema_version": 1,
    "nodes": [
        {
            "id": node_id,
            "display_name": node_id,
            "ssh": {"address": address, "user": "spark", "key_path": "~/.ssh/k"},
            "management_address": address,
            "cluster_addresses": [cluster],
            "model_roots": ["/home/spark/models"],
            "endpoint_ports": {"start": 8000, "end": 8999},
        }
        for node_id, address, cluster in (
            ("spark-a", "192.0.2.10", "198.51.100.1"),
            ("spark-b", "192.0.2.11", "198.51.100.2"),
        )
    ],
}
NODES = {node.node_id: node for node in parse_nodes_document(NODES_DOCUMENT)}


def make_draft(**changes) -> RecipeDraft:
    draft = RecipeDraft(
        draft_id="r-0123456789ab",
        display_name="Qwen3-8B",
        served_alias="qwen3-8b",
        model=RecipeModel(
            repo_id="Qwen/Qwen3-8B",
            revision="b968826d9c46dd6066d109eabc6255188de91218",
            weight_bytes=16_000_000_000,
            weight_files=(("model-00001-of-00002.safetensors", 8_000_000_000),),
            max_position_embeddings=40_960,
        ),
        runtime=RecipeRuntime(
            release_tag="v0.31.0",
            reference=f"vllm/vllm-openai@{DIGEST}",
            arm64_digest=DIGEST,
        ),
        placement=RecipePlacement(
            node_ids=("spark-a", "spark-b"), head_node_id="spark-a"
        ),
    )
    return replace(draft, **changes)


def facts(node_id: str) -> NodeFacts:
    return NodeFacts(
        node_id=node_id,
        home="/home/spark",
        image_id=IMAGE_ID,
        vllm_version="0.31.0",
        manifest_sha256="c" * 64,
        manifest_path="model.safetensors.index.json",
        memory_total_bytes=128 * 1024**3,
    )


class RecipeSpecTests(unittest.TestCase):
    def test_settings_reject_unknown_keys_and_out_of_range_values(self) -> None:
        with self.assertRaises(RecipeError):
            settings_from_mapping({"extra_flags": "--evil"})
        with self.assertRaises(RecipeError):
            settings_from_mapping({"gpu_memory_utilization": 0.99})
        with self.assertRaises(RecipeError):
            settings_from_mapping({"tool_call_parser": "hermes; rm -rf /"})
        self.assertEqual(
            0.9,
            settings_from_mapping(
                {"gpu_memory_utilization": 0.9}
            ).gpu_memory_utilization,
        )

    def test_context_cannot_exceed_model_positions(self) -> None:
        with self.assertRaises(RecipeError) as raised:
            make_draft(settings=RecipeSettings(max_model_len=65_536))
        self.assertEqual("context_exceeds_model", raised.exception.code)

    def test_remote_code_models_require_explicit_opt_in(self) -> None:
        model = replace(make_draft().model, needs_remote_code=True)
        with self.assertRaises(RecipeError) as raised:
            make_draft(model=model)
        self.assertEqual("remote_code_required", raised.exception.code)
        make_draft(model=model, settings=RecipeSettings(trust_remote_code=True))

    def test_draft_round_trips_through_json(self) -> None:
        draft = make_draft()
        self.assertEqual(
            draft, RecipeDraft.from_dict(json.loads(json.dumps(draft.to_dict())))
        )


class RecipeRenderTests(unittest.TestCase):
    def test_generated_profile_uses_per_node_cluster_contract(self) -> None:
        rendered = render_recipe(
            make_draft(),
            NODES,
            {node: facts(node) for node in NODES},
            prepare_id="00aa11bb22",
        )
        profile = rendered.profile()
        self.assertEqual("generated", profile.origin.value)
        self.assertEqual("per-node", profile.runner.launch_mode.value)
        self.assertEqual("g-qwen3-8b-00aa11", profile.profile_id)
        self.assertEqual(f"vllm/vllm-openai@{DIGEST}", profile.runner.image_digest)
        self.assertEqual(
            {"spark-a": "0", "spark-b": "1"},
            {
                node: overlay["NODE_RANK"]
                for node, overlay in profile.runner.node_environment.items()
            },
        )
        self.assertEqual(
            "198.51.100.1", profile.runner.required_environment["MASTER_ADDR"]
        )
        self.assertLessEqual(len("dep-" + profile.profile_id), 53)
        canary = profile.readiness.completion_canary
        self.assertTrue(canary.enabled)
        self.assertEqual(
            ("Reply with OK.", 16, 120),
            (canary.prompt, canary.max_tokens, canary.timeout_s),
        )

    def test_every_prepare_registers_a_distinct_profile(self) -> None:
        draft = make_draft()
        self.assertNotEqual(
            profile_id_for(draft, "aaaaaa0000"), profile_id_for(draft, "bbbbbb0000")
        )

    def test_rank_addresses_prefer_a_shared_direct_subnet(self) -> None:
        selected = [NODES["spark-a"], NODES["spark-b"]]
        self.assertEqual(
            {"spark-a": "198.51.100.1", "spark-b": "198.51.100.2"},
            rank_addresses(selected),
        )
        self.assertEqual({"spark-a": "auto"}, rank_addresses(selected[:1]))

    def test_nodes_must_agree_on_runtime_and_weights(self) -> None:
        mismatched = {
            "spark-a": facts("spark-a"),
            "spark-b": replace(facts("spark-b"), manifest_sha256="d" * 64),
        }
        with self.assertRaises(RecipeError) as raised:
            render_recipe(make_draft(), NODES, mismatched, prepare_id="00aa11bb22")
        self.assertEqual("prepare_mismatch", raised.exception.code)

    def test_head_and_worker_image_ids_may_differ_by_image_store(self) -> None:
        # The containerd store reports the manifest digest as the image ID and
        # the classic overlay2 store the config digest, for the same pull.
        worker_id = "sha256:" + "b" * 64
        mixed = {
            "spark-a": facts("spark-a"),
            "spark-b": replace(facts("spark-b"), image_id=worker_id),
        }
        rendered = render_recipe(make_draft(), NODES, mixed, prepare_id="00aa11bb22")
        self.assertEqual(IMAGE_ID, rendered.base_environment["VLLM_HEAD_IMAGE_ID"])
        self.assertEqual(worker_id, rendered.base_environment["WORKER_VLLM_IMAGE_ID"])

    def test_environment_values_outside_the_allowlist_are_rejected(self) -> None:
        with self.assertRaises(RecipeError):
            environment_text({"MODEL_DIR": "$(touch /tmp/pwned)"})


@unittest.skipUnless(shutil.which("bash"), "bash is required")
class StartScriptContractTests(unittest.TestCase):
    """Run the pinned start.sh against a stub docker to pin its exact command."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        (self.root / "models" / "Qwen--Qwen3-8B-b968826d").mkdir(parents=True)
        (self.root / "models" / "Qwen--Qwen3-8B-b968826d" / "config.json").write_text(
            "{}"
        )
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        docker = bin_dir / "docker"
        docker.write_text(
            "#!/usr/bin/env bash\n"
            'case "$1" in\n'
            "  container) exit 1 ;;\n"
            f"  image) echo {IMAGE_ID} ;;\n"
            '  run) printf "%s\\n" "$@" > "$DOCKER_LOG" ;;\n'
            "esac\n"
        )
        docker.chmod(docker.stat().st_mode | stat.S_IEXEC)
        self.script = self.root / "start.sh"
        self.script.write_bytes(template_bytes("start.sh"))
        self.environment = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _run(self, values: dict[str, str]) -> subprocess.CompletedProcess:
        env_file = self.root / "recipe.env"
        env_file.write_text(
            "".join(f"{key}={value}\n" for key, value in values.items())
        )
        return subprocess.run(
            ["bash", str(self.script)],
            env={
                **self.environment,
                "ENV_FILE": str(env_file),
                "DOCKER_LOG": str(self.root / "docker.log"),
            },
            capture_output=True,
            text=True,
            check=False,
        )

    def _single_node_values(self) -> dict[str, str]:
        draft = make_draft(
            placement=RecipePlacement(node_ids=("spark-a",), head_node_id="spark-a")
        )
        rendered = render_recipe(
            draft, NODES, {"spark-a": facts("spark-a")}, prepare_id="00aa11bb22"
        )
        values = dict(rendered.base_environment)
        values.update(rendered.node_environment["spark-a"])
        values["MODEL_ROOT"] = str(self.root / "models")
        return values, rendered.profile()

    def test_rendered_identity_matches_the_launched_command(self) -> None:
        values, profile = self._single_node_values()
        result = self._run(values)
        self.assertEqual(0, result.returncode, result.stderr)
        argv = (self.root / "docker.log").read_text().splitlines()
        command = argv[argv.index(values["VLLM_IMAGE"]) + 1 :]
        self.assertTrue(
            _command_has_exact_tokens(
                json.dumps(command),
                profile.readiness.process.required_command_fragments,
            )
        )
        self.assertIn(
            "io.github.dgx-spark-controller.deployment=" + values["DEPLOYMENT_LABEL"],
            argv,
        )

    def test_values_are_parsed_as_data_never_executed(self) -> None:
        values, _profile = self._single_node_values()
        marker = self.root / "pwned"
        values["MODEL_DIR"] = f"$(touch {marker})"
        result = self._run(values)
        self.assertEqual(2, result.returncode)
        self.assertFalse(marker.exists())
        self.assertFalse((self.root / "docker.log").exists())

    def test_unknown_keys_are_refused(self) -> None:
        values, _profile = self._single_node_values()
        values["LD_PRELOAD"] = "/tmp/x.so"
        self.assertEqual(2, self._run(values).returncode)


def _stub_bin(root: Path, name: str, body: str) -> Path:
    bin_dir = root / "bin"
    bin_dir.mkdir(exist_ok=True)
    path = bin_dir / name
    path.write_text("#!/usr/bin/env bash\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return bin_dir


@unittest.skipUnless(shutil.which("bash"), "bash is required")
class StopScriptContractTests(unittest.TestCase):
    def _run(self, owner: str) -> tuple[subprocess.CompletedProcess, str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bin_dir = _stub_bin(
                root,
                "docker",
                'echo "$*" >> "$DOCKER_LOG"\n'
                'if [ "$1 $2" = "container inspect" ] && [ "$3" = --format ]; then '
                f"echo {owner}; fi\n",
            )
            script = root / "stop.sh"
            script.write_bytes(template_bytes("stop.sh"))
            env_file = root / "recipe.env"
            env_file.write_text("CONTAINER_NAME=dgxc-g-x\nDEPLOYMENT_LABEL=dep-g-x\n")
            log = root / "docker.log"
            result = subprocess.run(
                ["bash", str(script)],
                env={
                    **os.environ,
                    "PATH": f"{bin_dir}:{os.environ['PATH']}",
                    "ENV_FILE": str(env_file),
                    "DOCKER_LOG": str(log),
                },
                capture_output=True,
                text=True,
                check=False,
            )
            return result, log.read_text() if log.exists() else ""

    def test_removes_only_its_own_labelled_container(self) -> None:
        result, calls = self._run("dep-g-x")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("rm --force dgxc-g-x", calls)

    def test_refuses_a_container_owned_by_something_else(self) -> None:
        result, calls = self._run("someone-else")
        self.assertEqual(3, result.returncode)
        commands = [line.split()[0] for line in calls.splitlines()]
        self.assertNotIn("rm", commands)
        self.assertNotIn("stop", commands)


@unittest.skipUnless(
    shutil.which("bash") and os.uname().sysname == "Linux",
    "prepare.sh targets GNU coreutils on DGX OS",
)
class PrepareScriptContractTests(unittest.TestCase):
    def test_reports_pinned_facts_after_verifying_weights(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "models" / "Org--M-01234567"
            bin_dir = _stub_bin(
                root,
                "docker",
                'case "$1" in\n'
                "  pull) exit 0 ;;\n"
                f"  image) echo {IMAGE_ID} ;;\n"
                '  run) if [[ "$*" == *"importlib.metadata"* ]]; then '
                "printf 'VERSION=0.31.0\\nNNODES=1\\nCLI=1\\n'; else "
                f"mkdir -p {target}; printf x > {target}/model.safetensors; "
                f"echo '{{}}' > {target}/config.json; fi ;;\n"
                "esac\n",
            )
            status = root / "status"
            expected = root / "expected.txt"
            expected.write_text("1 model.safetensors\n")
            env_file = root / "prepare.env"
            env_file.write_text(
                "PREPARE_ID=0123456789\n"
                f"VLLM_IMAGE=vllm/vllm-openai@{DIGEST}\n"
                "HF_REPO=Org/M\nHF_REVISION=" + "0" * 40 + "\n"
                f"MODEL_ROOT={root / 'models'}\nMODEL_DIR=Org--M-01234567\n"
                f"STATUS_FILE={status}\nEXPECTED_FILES={expected}\n"
                "REQUIRED_FREE_BYTES=1\nCHECK_MULTINODE=0\n"
            )
            # A cancelled earlier download left a temp file; a live one is fresh.
            download = target / ".cache" / "huggingface" / "download"
            download.mkdir(parents=True)
            stale = download / "earlier.incomplete"
            stale.write_text("x")
            os.utime(stale, (time.time() - 3600, time.time() - 3600))
            fresh = download / "live.incomplete"
            fresh.write_text("x")
            script = root / "prepare.sh"
            script.write_bytes(template_bytes("prepare.sh"))
            result = subprocess.run(
                ["bash", str(script)],
                env={
                    **os.environ,
                    "PATH": f"{bin_dir}:{os.environ['PATH']}",
                    "PREPARE_ENV": str(env_file),
                },
                capture_output=True,
                text=True,
                check=False,
            )
            values, state = parse_status(status.read_text())
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("succeeded", state)
        self.assertEqual(IMAGE_ID, values["IMAGE_ID"])
        self.assertEqual("config.json", values["MANIFEST_PATH"])
        self.assertFalse(stale.exists())
        self.assertTrue(fresh.exists())


class PrepareStatusTests(unittest.TestCase):
    def test_status_parser_keeps_only_allowlisted_keys(self) -> None:
        values, state = parse_status(
            "STATE=running\nSECRET=x\nSTEP=pulling\nSTATE=succeeded\n"
        )
        self.assertEqual("succeeded", state)
        self.assertNotIn("SECRET", values)

    def test_linger_and_removal_parsers_accept_only_known_values(self) -> None:
        self.assertTrue(parse_linger("USER=spark\nLINGER=yes\n").enabled)
        status = parse_linger("USER=$(id)\nLINGER=maybe\n")
        self.assertEqual(("", ""), (status.user, status.value))
        self.assertEqual("removed", parse_removal("RESULT=removed\nREASON=\n").state)
        refused = parse_removal("RESULT=refused\nREASON=/etc/passwd\n")
        self.assertEqual(("refused", ""), (refused.state, refused.reason))
        self.assertEqual("failed", parse_removal("garbage\n").state)

    def test_progress_is_bounded_to_known_nodes_and_fields(self) -> None:
        progress = bounded_node_progress(
            {
                "spark-a": {
                    "state": "running",
                    "step": "/home/spark/models",
                    "downloaded_bytes": -1,
                    "total_bytes": True,
                    "path": "/home/spark",
                },
                "ghost": {"state": "running"},
            },
            ["spark-a", "spark-b"],
        )
        self.assertEqual(
            {
                "spark-a": {
                    "state": "running",
                    "step": "unknown",
                    "downloaded_bytes": None,
                    "total_bytes": None,
                }
            },
            progress,
        )
        self.assertEqual({}, bounded_node_progress(["spark-a"], ["spark-a"]))

    def test_generated_profile_ids_have_one_exact_shape(self) -> None:
        self.assertTrue(is_generated_profile_id("g-qwen3-8b-00aa11"))
        for value in ("g-../x-00aa11", "qwen3-8b", "g-qwen3-8b-00aa1", "g--x-00aa11"):
            self.assertFalse(is_generated_profile_id(value), value)


class LocalExecutor:
    """Run the fixed remote scripts locally with ``bash -s``, as SSH would."""

    def __init__(self, environment: dict[str, str]) -> None:
        self.environment = environment
        self.scripts: list[str] = []

    def run_script(
        self, node_id: str, script: str, *, timeout_seconds: float, check=False
    ) -> SSHExecution:
        del node_id, timeout_seconds, check
        self.scripts.append(script)
        completed = subprocess.run(
            ["bash", "-s"],
            input=script,
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        return SSHExecution(completed.returncode, completed.stdout, completed.stderr)


@unittest.skipUnless(shutil.which("bash"), "bash is required")
class StopPrepareScriptContractTests(unittest.TestCase):
    """Stopping a prepare returns only once its download container is gone."""

    def run_stop(self, inspections_while_present: int) -> tuple[bool, list[str]]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "docker.log"
            # ``container inspect`` succeeds while the container still exists.
            _stub_bin(
                root,
                "docker",
                'echo "$*" >> "$DOCKER_LOG"\n'
                'if [ "$1 $2" = "container inspect" ]; then\n'
                '  n=$(grep -c "^container inspect" "$DOCKER_LOG")\n'
                '  [ "$n" -le "$PRESENT" ]\n'
                "fi\n",
            )
            _stub_bin(root, "systemctl", "exit 0\n")
            bin_dir = _stub_bin(root, "sleep", "exit 0\n")
            executor = LocalExecutor(
                {
                    "PATH": f"{bin_dir}:/usr/bin:/bin",
                    "DOCKER_LOG": str(log),
                    "PRESENT": str(inspections_while_present),
                }
            )
            result = SSHRecipeNodeOps(executor).stop_prepare("spark-a", "00aa11bb22")
            return result.ok, log.read_text().splitlines()

    def test_waits_for_an_asynchronous_container_removal(self) -> None:
        ok, calls = self.run_stop(inspections_while_present=3)
        self.assertTrue(ok)
        self.assertEqual(4, sum(c.startswith("container inspect") for c in calls))
        self.assertIn("rm --force dgxc-prepare-00aa11bb22", calls)

    def test_reports_a_container_that_never_goes_away(self) -> None:
        ok, calls = self.run_stop(inspections_while_present=1000)
        self.assertFalse(ok)
        self.assertEqual(30, sum(c.startswith("container inspect") for c in calls))


@unittest.skipUnless(shutil.which("bash"), "bash is required")
class RemovalScriptContractTests(unittest.TestCase):
    """Run the bounded removal scripts against stub docker and systemctl."""

    PROFILE = "g-qwen3-8b-00aa11"
    PREPARE = "00aa11bb22"
    IMAGE = f"vllm/vllm-openai@{DIGEST}"

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.home = self.root / "home"
        self.recipes = self.home / ".local/share/dgx-spark-controller/recipes"
        self.models = self.root / "models"
        self.log = self.root / "docker.log"
        _stub_bin(
            self.root,
            "docker",
            'echo "$*" >> "$DOCKER_LOG"\n'
            'case "$1 $2" in\n'
            '  "version "*) exit 0 ;;\n'
            '  "ps --all") if [ "$3" = --format ]; then printf "%b" "${CONTAINERS:-}";'
            ' else printf "%b" "${USERS:-}"; fi ;;\n'
            '  "ps --no-trunc") printf "%b" "${RUNNING:-}" ;;\n'
            '  "image inspect") [ -n "${IMAGE_PRESENT:-}" ] || exit 1;'
            f" echo {IMAGE_ID} ;;\n"
            '  "volume inspect") [ -n "${VOLUME_PRESENT:-}" ] || exit 1 ;;\n'
            "esac\n",
        )
        bin_dir = _stub_bin(
            self.root,
            "systemctl",
            'case "$*" in\n'
            '  *is-active*) exit "${UNIT_ACTIVE:-3}" ;;\n'
            '  *list-units*) printf "%b" "${ACTIVE_PREPARES:-}" ;;\n'
            "esac\n",
        )
        self.environment = {
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "HOME": str(self.home),
            "DOCKER_LOG": str(self.log),
        }

    def tearDown(self) -> None:
        self.directory.cleanup()

    def ops(self, **environment: str) -> tuple[SSHRecipeNodeOps, LocalExecutor]:
        executor = LocalExecutor({**self.environment, **environment})
        return SSHRecipeNodeOps(executor), executor

    def recipe_directory(self, name: str = PROFILE) -> Path:
        target = self.recipes / name
        target.mkdir(parents=True)
        (target / "start.sh").write_text("#!/bin/bash\n")
        return target

    def test_removes_exactly_the_generated_recipe_directory(self) -> None:
        target = self.recipe_directory()
        sibling = self.recipe_directory("g-other-000000")
        ops, _executor = self.ops()
        result = ops.remove_recipe_directory(
            "spark-a", str(self.home), self.PROFILE, prepare_id=self.PREPARE
        )
        self.assertEqual("removed", result.state)
        self.assertFalse(target.exists())
        self.assertTrue((sibling / "start.sh").exists())
        again = ops.remove_recipe_directory("spark-a", str(self.home), self.PROFILE)
        self.assertEqual("absent", again.state)

    def test_refuses_symlinks_at_the_target_and_below_home(self) -> None:
        victim = self.root / "victim"
        victim.mkdir()
        (victim / "keep").write_text("x")
        self.recipes.mkdir(parents=True)
        (self.recipes / self.PROFILE).symlink_to(victim)
        ops, _executor = self.ops()
        result = ops.remove_recipe_directory("spark-a", str(self.home), self.PROFILE)
        self.assertEqual(("refused", "symlink"), (result.state, result.reason))
        self.assertTrue((victim / "keep").exists())

        (self.recipes / self.PROFILE).unlink()
        shutil.rmtree(self.recipes)
        outside = self.root / "outside"
        (outside / self.PROFILE).mkdir(parents=True)
        (outside / self.PROFILE / "keep").write_text("x")
        self.recipes.symlink_to(outside)
        result = ops.remove_recipe_directory("spark-a", str(self.home), self.PROFILE)
        self.assertEqual("refused", result.state)
        self.assertTrue((outside / self.PROFILE / "keep").exists())

    def test_refuses_while_its_container_or_prepare_unit_exists(self) -> None:
        target = self.recipe_directory()
        ops, _executor = self.ops(CONTAINERS=f"other\\ndgxc-{self.PROFILE}\\n")
        result = ops.remove_recipe_directory("spark-a", str(self.home), self.PROFILE)
        self.assertEqual(("refused", "container-exists"), (result.state, result.reason))
        ops, _executor = self.ops(UNIT_ACTIVE="0")
        result = ops.remove_recipe_directory(
            "spark-a", str(self.home), self.PROFILE, prepare_id=self.PREPARE
        )
        self.assertEqual(("refused", "prepare-active"), (result.state, result.reason))
        ops, _executor = self.ops()
        result = ops.remove_recipe_directory(
            "spark-a", str(self.root / "elsewhere"), self.PROFILE
        )
        self.assertEqual(("refused", "home-mismatch"), (result.state, result.reason))
        self.assertTrue(target.exists())

    def test_model_directory_is_kept_while_anything_may_use_it(self) -> None:
        name = "Qwen--Qwen3-8B-b968826d"
        (self.models / name).mkdir(parents=True)
        (self.models / "Other--Model-01234567").mkdir()
        for environment, reason in (
            ({"ACTIVE_PREPARES": "dgx-prepare-x.service loaded active\\n"}, "prepare"),
            ({"RUNNING": "dgxc-prepare-0123456789 python3\\n"}, "prepare"),
            ({"RUNNING": f'dgxc-g-x "vllm serve /models/{name}"\\n'}, "in-use"),
        ):
            ops, _executor = self.ops(**environment)
            result = ops.remove_model_directory("spark-a", str(self.models), name)
            self.assertEqual("refused", result.state)
            self.assertTrue(result.reason.startswith(reason), result.reason)
        ops, _executor = self.ops()
        result = ops.remove_model_directory("spark-a", str(self.models), name)
        self.assertEqual("removed", result.state)
        self.assertFalse((self.models / name).exists())
        self.assertTrue((self.models / "Other--Model-01234567").exists())

    def test_image_is_removed_only_by_its_pinned_reference_when_unused(self) -> None:
        ops, _executor = self.ops(IMAGE_PRESENT="1", USERS="abc123\\n")
        self.assertEqual("refused", ops.remove_image("spark-a", self.IMAGE).state)
        self.assertNotIn("image rm", self.log.read_text())
        ops, _executor = self.ops(IMAGE_PRESENT="1")
        self.assertEqual("removed", ops.remove_image("spark-a", self.IMAGE).state)
        self.assertIn(f"image rm -- {self.IMAGE}", self.log.read_text())
        ops, _executor = self.ops(VOLUME_PRESENT="1", USERS="abc123\\n")
        self.assertEqual("refused", ops.remove_cache_volume("spark-a").state)
        ops, _executor = self.ops(VOLUME_PRESENT="1")
        self.assertEqual("removed", ops.remove_cache_volume("spark-a").state)

    def test_invalid_targets_never_reach_a_node(self) -> None:
        ops, executor = self.ops()
        attempts: list[Callable[[], Any]] = [
            lambda: ops.remove_recipe_directory("n", "/home/../etc", self.PROFILE),
            lambda: ops.remove_recipe_directory("n", "/", self.PROFILE),
            lambda: ops.remove_recipe_directory("n", "/home/spark", "../x"),
            lambda: ops.remove_recipe_directory("n", "/home/spark", "profiles"),
            lambda: ops.remove_model_directory("n", "/", "Org--M-01234567"),
            lambda: ops.remove_model_directory("n", "/models/", "Org--M-01234567"),
            lambda: ops.remove_model_directory("n", "/models", "x"),
            lambda: ops.remove_model_directory("n", "/models", "../--M-01234567"),
            lambda: ops.remove_image("n", "vllm/vllm-openai:latest"),
            lambda: ops.remove_image("n", "ubuntu@" + DIGEST),
        ]
        for attempt in attempts:
            with self.assertRaises(ValueError):
                attempt()
        self.assertEqual([], executor.scripts)


class RecordingExecutor:
    """Record each remote script with its limits instead of running it."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, float, bool]] = []

    def run_script(
        self, node_id: str, script: str, *, timeout_seconds: float, check=False
    ) -> SSHExecution:
        del node_id
        self.calls.append((script, timeout_seconds, check))
        return SSHExecution(0, "/home/spark\n", "")


class RemoteScriptTextTests(unittest.TestCase):
    """The fixed remote scripts are reviewed text; any change must be deliberate."""

    DIRECTORY = (
        "/home/spark/.local/share/dgx-spark-controller/recipes/g-qwen3-8b-00aa11"
    )
    PINNED: dict[str, tuple[str, float, bool]] = {
        "remote_home": (
            "b48af7d640756ab226d99f59ff4e9e27de7d3859be4f4d7c2dca40dc00c6f94e",
            15,
            True,
        ),
        "check_linger": (
            "c1a25c68af8e52e775921549f40f063d5c4c0cf151fddf3616134a1a975edac2",
            15,
            True,
        ),
        "install_files": (
            "41547f9c5fef7a83aeb2f3fc12c415561b74ce3d75b554a7a0950dcfe3f5a73e",
            60,
            False,
        ),
        "start_prepare": (
            "baa4a1c5532877c468d04dba068eee96aac8f2d0d3c8049185e4f8921db716a2",
            30,
            False,
        ),
        "prepare_status": (
            "86729bba095ad1c8027e9e31d7c329f64720bddf6537e0d5262a42ada3d9777a",
            30,
            True,
        ),
        "stop_prepare": (
            "9870c7db1c4e2a0043f74be10589d1e90a9500124431613e4a8eb30cf85924a3",
            60,
            False,
        ),
        "remove_recipe_directory": (
            "6bd2a7bfc3f2131150846c9bcf830bcc96564c4a2c3c24935632e3a5e4fef323",
            120,
            False,
        ),
        "remove_recipe_directory_of_prepare": (
            "53e6ac22c36c28eae163c94d99436783cf5978e84c2d726673ebe2fe15b5dc58",
            120,
            False,
        ),
        "remove_model_directory": (
            "1396ed249acea479368032f16d31d6309e165bcd60973e72d9e3af7d0d0bb369",
            600,
            False,
        ),
        "remove_image": (
            "9988135973f5029144a4c753060e92743cbe92e0c2482f4c33d416ed2225eb8b",
            300,
            False,
        ),
        "remove_cache_volume": (
            "6525cddcbf23c5801010fa4ff6057ca9400e82cae4f00efaa4163686bfd54c6a",
            60,
            False,
        ),
    }

    def test_rendered_scripts_match_the_reviewed_text(self) -> None:
        directory = self.DIRECTORY
        files = {
            "prepare.sh": (b"#!/bin/bash\necho ok\n", 0o755),
            "prepare-00aa11bb22.env": (b"A=1\n", 0o600),
        }
        calls: dict[str, Callable[[SSHRecipeNodeOps], Any]] = {
            "remote_home": lambda ops: ops.remote_home("spark-a"),
            "check_linger": lambda ops: ops.check_linger("spark-a"),
            "install_files": lambda ops: ops.install_files("spark-a", directory, files),
            "start_prepare": lambda ops: ops.start_prepare(
                "spark-a", directory, "00aa11bb22"
            ),
            "prepare_status": lambda ops: ops.prepare_status(
                "spark-a",
                directory,
                "00aa11bb22",
                "/home/spark/models/Qwen--Qwen3-8B-b968826d",
            ),
            "stop_prepare": lambda ops: ops.stop_prepare("spark-a", "00aa11bb22"),
            "remove_recipe_directory": lambda ops: ops.remove_recipe_directory(
                "spark-a", "/home/spark", "g-qwen3-8b-00aa11"
            ),
            "remove_recipe_directory_of_prepare": (
                lambda ops: ops.remove_recipe_directory(
                    "spark-a",
                    "/home/spark",
                    "g-qwen3-8b-00aa11",
                    prepare_id="00aa11bb22",
                )
            ),
            "remove_model_directory": lambda ops: ops.remove_model_directory(
                "spark-a", "/home/spark/models", "Qwen--Qwen3-8B-b968826d"
            ),
            "remove_image": lambda ops: ops.remove_image(
                "spark-a", f"vllm/vllm-openai@{DIGEST}"
            ),
            "remove_cache_volume": lambda ops: ops.remove_cache_volume("spark-a"),
        }
        for name, call in calls.items():
            executor = RecordingExecutor()
            call(SSHRecipeNodeOps(executor))
            ((script, timeout_seconds, check),) = executor.calls
            digest = hashlib.sha256(script.encode("utf-8")).hexdigest()
            self.assertEqual(
                self.PINNED[name],
                (digest, timeout_seconds, check),
                f"{name}: the remote script changed; review it, then update PINNED",
            )


class RecipeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.journal = OperationJournal()
        self.ops = FakeRecipeNodeOps(list(NODES))
        self.generated: list[str] = []
        self.removed: list[str] = []
        self.services: list[RecipeService] = []
        self.service = self._service()
        self.profiles_dir = self.root / "profiles"

    def tearDown(self) -> None:
        for service in self.services:
            service.close(wait=True)
        self.journal.close()
        self.directory.cleanup()

    def _service(self, **overrides: Any) -> RecipeService:
        options: dict[str, Any] = {
            "store": RecipeStore(self.root / "recipes"),
            "journal": self.journal,
            "catalog": fixture_catalog(),
            "nodes_provider": lambda: NODES,
            "node_ops_provider": lambda: self.ops,
            "generated_profiles_dir": self.root / "profiles",
            "on_profile_generated": self.generated.append,
            "on_profiles_removed": self.removed.extend,
            "poll_interval_seconds": 0,
            "progress_interval_seconds": 0,
        }
        options.update(overrides)
        service = RecipeService(**options)
        self.services.append(service)
        return service

    def _slow_service(self) -> RecipeService:
        self.ops.polls_per_step = 40
        return self._service(poll_interval_seconds=0.005, progress_interval_seconds=60)

    def _create(self, **extra) -> dict:
        body = {
            "model_link": QWEN_LINK,
            "release_tag": "v0.31.0",
            "node_ids": ["spark-a", "spark-b"],
            **extra,
        }
        return self.service.create_draft(body)

    def _wait(self, operation_id: str) -> OperationState:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            record = self.journal.get_operation(operation_id)
            if record is not None and record.state.terminal:
                return record.state
            time.sleep(0.01)
        self.fail("operation did not finish")

    def _wait_until(self, predicate: Callable[[], Any]) -> Any:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.01)
        self.fail("condition not reached")

    def _codes(self, operation_id: str) -> list[str]:
        return [event.code for event in self.journal.events(operation_id)]

    def _prepared(self, **extra) -> tuple[str, str]:
        draft = self._create(**extra)
        operation = self.service.submit_prepare(
            draft["draft_id"], idempotency_key=f"prepare-{draft['draft_id']}"
        )
        self.assertEqual(OperationState.SUCCEEDED, self._wait(operation.id))
        return draft["draft_id"], self.service.get_draft(draft["draft_id"])[
            "profile_id"
        ]

    def _working_directory(self, profile_id: str) -> str:
        document = json.loads((self.profiles_dir / f"{profile_id}.json").read_text())
        return document["runner"]["working_directory"]

    def _interrupted_prepare(self) -> tuple[str, str, RecipeService]:
        """Start a slow prepare, then stop the controller while it downloads."""
        first = self._slow_service()
        draft = self._create()
        operation = first.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-interrupted"
        )
        self._wait_until(lambda: "prepare_progress" in self._codes(operation.id))
        first.close(wait=True)
        self.assertEqual(
            OperationState.RUNNING, self.journal.get_operation(operation.id).state
        )
        # Leaving the controller must not stop the download on the nodes.
        self.assertEqual([], self.ops.stopped)
        return operation.id, draft["draft_id"], first

    def test_link_to_prepared_profile(self) -> None:
        draft = self._create()
        self.assertEqual(
            "b968826d9c46dd6066d109eabc6255188de91218", draft["model"]["revision"]
        )
        operation = self.service.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-1"
        )
        self.assertEqual(OperationState.SUCCEEDED, self._wait(operation.id))
        prepared = self.service.get_draft(draft["draft_id"])
        self.assertEqual("prepared", prepared["status"])
        self.assertEqual([prepared["profile_id"]], self.generated)
        self.assertEqual([prepared["profile_id"]], prepared["generated_profile_ids"])
        document = json.loads(
            (self.profiles_dir / f"{prepared['profile_id']}.json").read_text()
        )
        self.assertEqual("generated", document["origin"])
        installed = self.ops.files[("spark-b", document["runner"]["working_directory"])]
        self.assertEqual(template_bytes("start.sh"), installed["start.sh"])

    def test_progress_is_journaled_per_node_without_paths(self) -> None:
        draft = self._create()
        operation = self.service.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-progress"
        )
        self.assertEqual(OperationState.SUCCEEDED, self._wait(operation.id))
        progress = latest_node_progress(
            self.journal.events(operation.id), ("spark-a", "spark-b")
        )
        self.assertEqual(["spark-a", "spark-b"], list(progress))
        for item in progress.values():
            self.assertEqual("succeeded", item["state"])
            self.assertEqual(16_381_516_776, item["total_bytes"])
            self.assertIsInstance(item["downloaded_bytes"], int)
        self.assertNotIn("/", json.dumps(progress))
        steps = {
            item["step"]
            for event in self.journal.events(operation.id)
            for item in event.details.get("progress_by_node", {}).values()
        }
        self.assertTrue({"pulling", "downloading", "verifying"} <= steps)

    def test_preparing_recipe_exposes_progress_by_node(self) -> None:
        service = self._slow_service()
        draft = self._create()
        operation = service.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-live-progress"
        )
        resource = self._wait_until(
            lambda: (item := service.get_draft(draft["draft_id"])).get(
                "progress_by_node"
            )
            and item
        )
        self.assertEqual({"spark-a", "spark-b"}, set(resource["progress_by_node"]))
        self.assertTrue(service.cancel_operation(operation.id))
        self.assertEqual(OperationState.CANCELLED, self._wait(operation.id))
        self.assertNotIn("progress_by_node", service.get_draft(draft["draft_id"]))

    def test_failed_node_marks_recipe_failed_and_removes_its_directory(self) -> None:
        self.ops.fail_nodes = {
            "spark-b": "not enough free disk space in the model root"
        }
        draft = self._create()
        operation = self.service.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-2"
        )
        self.assertEqual(OperationState.FAILED, self._wait(operation.id))
        self.assertEqual("failed", self.service.get_draft(draft["draft_id"])["status"])
        self.assertEqual({"spark-a", "spark-b"}, {node for node, _ in self.ops.stopped})
        self.assertEqual([], self.generated)
        # Exactly this prepare's directory is gone on every node; the partial
        # weights and the pulled image stay so the next prepare resumes.
        self.assertEqual({}, self.ops.files)
        self.assertEqual(
            ["directory", "directory"], [kind for kind, *_ in self.ops.removals]
        )
        self.assertEqual(2, len(self.ops.model_dirs))
        self.assertEqual(2, len(self.ops.images))
        self.assertIn("prepare_directory_removed", self._codes(operation.id))

    def test_cancelled_prepare_removes_its_directory_and_keeps_downloads(self) -> None:
        service = self._slow_service()
        draft = self._create()
        operation = service.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-cancel"
        )
        self._wait_until(lambda: "prepare_progress" in self._codes(operation.id))
        self.assertTrue(service.cancel_operation(operation.id))
        self.assertEqual(OperationState.CANCELLED, self._wait(operation.id))
        self.assertEqual({}, self.ops.files)
        self.assertEqual(2, len(self.ops.model_dirs))
        self.assertEqual("draft", service.get_draft(draft["draft_id"])["status"])
        self.assertIn("prepare_directory_removed", self._codes(operation.id))

    def test_delete_removes_a_directory_a_cancelled_prepare_left(self) -> None:
        service = self._slow_service()
        draft = self._create()
        self.ops.refuse_removal["spark-b"] = "container-exists"
        operation = service.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-left"
        )
        self._wait_until(lambda: "prepare_progress" in self._codes(operation.id))
        self.assertTrue(service.cancel_operation(operation.id))
        self.assertEqual(OperationState.CANCELLED, self._wait(operation.id))
        self.assertIn("prepare_cleanup_incomplete", self._codes(operation.id))
        self.assertEqual(["spark-b"], [node for node, _ in self.ops.files])
        self.assertTrue(service.get_draft(draft["draft_id"])["node_files_left"])
        # The local delete would forget the files, so it is refused.
        with self.assertRaises(RecipeError) as raised:
            service.delete_draft(draft["draft_id"])
        self.assertEqual("recipe_prepared", raised.exception.code)

        # While the node still refuses, the delete fails and keeps the draft.
        failed = service.submit_delete(draft["draft_id"], {}, idempotency_key="d-1")
        self.assertEqual(OperationState.FAILED, self._wait(failed.id))
        self.assertEqual(
            "recipe_delete_incomplete",
            self.journal.get_operation(failed.id).error_code,
        )
        self.assertEqual("draft", service.get_draft(draft["draft_id"])["status"])

        del self.ops.refuse_removal["spark-b"]
        deleted = service.submit_delete(draft["draft_id"], {}, idempotency_key="d-2")
        self.assertEqual(OperationState.SUCCEEDED, self._wait(deleted.id))
        self.assertEqual({}, self.ops.files)
        self.assertIn("prepare_directory_removed", self._codes(deleted.id))

    def test_cleanup_never_removes_a_registered_profile_directory(self) -> None:
        def explode(_profile_id: str) -> None:
            raise RuntimeError("reload failed")

        service = self._service(on_profile_generated=explode)
        draft = self._create()
        operation = service.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-registered"
        )
        self.assertEqual(OperationState.SUCCEEDED, self._wait(operation.id))
        profile_id = service.get_draft(draft["draft_id"])["profile_id"]
        self._wait_until(lambda: not service.live_operation_ids())
        self.assertIn(("spark-a", self._working_directory(profile_id)), self.ops.files)
        self.assertEqual([], self.ops.removals)

    def test_prepare_never_reuses_a_registered_profile_id(self) -> None:
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        (self.profiles_dir / "g-qwen3-8b-aaaaaa.json").write_text("{}\n")
        draft = self._create()
        with patch(
            "controller.recipes.service.secrets.token_hex",
            side_effect=["aaaaaa0000", "bbbbbb0000"],
        ):
            operation = self.service.submit_prepare(
                draft["draft_id"], idempotency_key="prepare-unique"
            )
            self.assertEqual(OperationState.SUCCEEDED, self._wait(operation.id))
        self.assertEqual(
            "g-qwen3-8b-bbbbbb", self.service.get_draft(draft["draft_id"])["profile_id"]
        )

    def test_prepare_requires_lingering_before_any_node_change(self) -> None:
        self.ops.linger["spark-b"] = "no"
        draft = self._create()
        operation = self.service.submit_prepare(
            draft["draft_id"], idempotency_key="prepare-linger"
        )
        self.assertEqual(OperationState.FAILED, self._wait(operation.id))
        record = self.journal.get_operation(operation.id)
        self.assertEqual("linger_required", record.error_code)
        self.assertIn("sudo loginctl enable-linger spark", record.remediation)
        self.assertIn("spark-b", record.remediation)
        self.assertNotIn("spark-a", record.remediation)
        self.assertEqual({}, self.ops.files)
        self.assertEqual([], self.ops.removals)
        self.assertEqual("failed", self.service.get_draft(draft["draft_id"])["status"])

    def test_reused_idempotency_key_is_a_conflict_not_a_crash(self) -> None:
        draft = self._create()
        self.journal.create_or_get_operation(
            action=OperationAction.RECONCILE,
            idempotency_key="shared-key",
            request={"kind": "reconcile"},
            node_ids=("spark-a",),
        )
        with self.assertRaises(RecipeError) as raised:
            self.service.submit_prepare(draft["draft_id"], idempotency_key="shared-key")
        self.assertEqual("idempotency_conflict", raised.exception.code)

    def test_restart_cancels_a_prepare_left_queued(self) -> None:
        draft = self._create()
        operation, _ = self.journal.create_or_get_operation(
            action=OperationAction.PREPARE,
            idempotency_key="stale-prepare",
            request={"kind": "prepare", "draft_id": draft["draft_id"]},
            node_ids=("spark-a",),
        )
        stored = self.service.store.get(draft["draft_id"])
        self.service.store.save(
            stored.with_status("preparing", last_operation_id=operation.id)
        )
        self.service.recover_interrupted()
        record = self.journal.get_operation(operation.id)
        self.assertEqual(OperationState.CANCELLED, record.state)
        self.assertIn("queued_operation_interrupted", self._codes(operation.id))
        self.assertEqual("draft", self.service.get_draft(draft["draft_id"])["status"])
        self.assertEqual({}, self.ops.files)

    def test_restart_reattaches_to_a_running_prepare(self) -> None:
        operation_id, draft_id, _first = self._interrupted_prepare()
        second = self._service()
        second.recover_interrupted()
        self.assertEqual(OperationState.SUCCEEDED, self._wait(operation_id))
        self.assertIn("prepare_reattached", self._codes(operation_id))
        prepared = second.get_draft(draft_id)
        self.assertEqual("prepared", prepared["status"])
        self.assertEqual([prepared["profile_id"]], self.generated)
        self.assertTrue((self.profiles_dir / f"{prepared['profile_id']}.json").exists())
        self.assertEqual([], self.ops.removals)

    def test_restart_without_node_state_needs_attention_and_keeps_files(self) -> None:
        operation_id, draft_id, _first = self._interrupted_prepare()
        self.ops.unreachable.add("spark-b")
        second = self._service()
        second.recover_interrupted()
        self.assertEqual(OperationState.NEEDS_ATTENTION, self._wait(operation_id))
        record = self.journal.get_operation(operation_id)
        self.assertEqual("prepare_interrupted", record.error_code)
        self.assertEqual("failed", second.get_draft(draft_id)["status"])
        self.assertEqual([], self.ops.stopped)
        self.assertEqual([], self.ops.removals)
        self.assertEqual(2, len(self.ops.files))

    def test_restart_after_the_units_vanished_fails_and_cleans_up(self) -> None:
        operation_id, draft_id, _first = self._interrupted_prepare()
        prepare_id = next(
            event.details["prepare_id"]
            for event in self.journal.events(operation_id)
            if event.code == "prepare_started"
        )
        for node_id in NODES:
            self.ops.stop_prepare(node_id, prepare_id)
        self.ops.stopped.clear()
        second = self._service()
        second.recover_interrupted()
        self.assertEqual(OperationState.FAILED, self._wait(operation_id))
        self.assertEqual(
            "prepare_interrupted", self.journal.get_operation(operation_id).error_code
        )
        self.assertEqual({}, self.ops.files)
        self.assertEqual("failed", second.get_draft(draft_id)["status"])

    def test_reconciliation_leaves_recipe_operations_to_the_recipe_service(
        self,
    ) -> None:
        operation, _ = self.journal.create_or_get_operation(
            action=OperationAction.PREPARE,
            idempotency_key="running-prepare",
            request={"kind": "prepare", "draft_id": "r-0123456789ab"},
            node_ids=("spark-a",),
        )
        self.journal.claim_operation(operation.id)
        report = Reconciler(self.journal, FakeNodeTransport(["spark-a"])).reconcile()
        self.assertEqual((), report.needs_attention_operations)
        self.assertEqual(
            OperationState.RUNNING, self.journal.get_operation(operation.id).state
        )

    def test_delete_removes_profiles_and_directories_but_keeps_downloads(
        self,
    ) -> None:
        draft_id, profile_id = self._prepared()
        working = self._working_directory(profile_id)
        operation = self.service.submit_delete(draft_id, {}, idempotency_key="delete-1")
        self.assertEqual(OperationState.SUCCEEDED, self._wait(operation.id))
        self.assertFalse((self.profiles_dir / f"{profile_id}.json").exists())
        with self.assertRaises(RecipeError) as raised:
            self.service.get_draft(draft_id)
        self.assertEqual("recipe_not_found", raised.exception.code)
        self.assertNotIn(("spark-a", working), self.ops.files)
        self.assertNotIn(("spark-b", working), self.ops.files)
        self.assertEqual(2, len(self.ops.model_dirs))
        self.assertEqual(2, len(self.ops.images))
        self.assertEqual({"spark-a", "spark-b"}, self.ops.volumes)
        self.assertEqual([profile_id], self.removed)
        result = self.journal.get_operation(operation.id).result
        self.assertEqual([profile_id], result["deleted_profiles"])
        self.assertEqual([], result["artifacts"])
        self.assertEqual({}, self.journal.active_locks())

    def test_delete_with_artifacts_removes_what_nothing_else_uses(self) -> None:
        draft_id, _profile_id = self._prepared()
        operation = self.service.submit_delete(
            draft_id, {"remove_artifacts": True}, idempotency_key="delete-2"
        )
        self.assertEqual(OperationState.SUCCEEDED, self._wait(operation.id))
        self.assertEqual(set(), self.ops.model_dirs)
        self.assertEqual(set(), self.ops.images)
        self.assertEqual(set(), self.ops.volumes)
        report = self.journal.get_operation(operation.id).result["artifacts"]
        self.assertEqual(
            {
                (node, kind): "removed"
                for node in ("spark-a", "spark-b")
                for kind in ("weights", "image", "cache-volume")
            },
            {(item["node_id"], item["kind"]): item["state"] for item in report},
        )

    def test_delete_keeps_artifacts_another_recipe_uses(self) -> None:
        first_id, _first_profile = self._prepared(served_alias="qwen-a")
        _second_id, second_profile = self._prepared(served_alias="qwen-b")
        operation = self.service.submit_delete(
            first_id, {"remove_artifacts": True}, idempotency_key="delete-3"
        )
        self.assertEqual(OperationState.SUCCEEDED, self._wait(operation.id))
        self.assertEqual(2, len(self.ops.model_dirs))
        self.assertEqual(2, len(self.ops.images))
        self.assertEqual({"spark-a", "spark-b"}, self.ops.volumes)
        report = self.journal.get_operation(operation.id).result["artifacts"]
        self.assertEqual({"in-use"}, {item["state"] for item in report})
        self.assertNotIn("cache-volume", {item["kind"] for item in report})
        self.assertIn(
            ("spark-a", self._working_directory(second_profile)), self.ops.files
        )

    def test_delete_is_refused_while_a_profile_is_in_use(self) -> None:
        draft_id, profile_id = self._prepared()
        deployment = DeploymentRecord(
            id=f"dep-{profile_id}",
            profile_id=profile_id,
            expected_alias="qwen3-8b",
            node_ids=("spark-a", "spark-b"),
            desired_state="ready",
            observed_state="ready",
            spec={},
            endpoint=None,
            owning_operation_id=None,
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        self.journal.save_deployment(deployment)
        with self.assertRaises(RecipeError) as raised:
            self.service.submit_delete(draft_id, {}, idempotency_key="delete-4")
        self.assertEqual("profile_in_use", raised.exception.code)
        self.journal.save_deployment(replace(deployment, observed_state="stopped"))
        self.journal.create_or_get_operation(
            action=OperationAction.LOAD,
            idempotency_key="queued-load",
            request={
                "kind": "load",
                "deployment": {
                    "deployment_id": f"dep-{profile_id}",
                    "profile_id": profile_id,
                },
            },
            node_ids=("spark-a",),
        )
        with self.assertRaises(RecipeError) as raised:
            self.service.submit_delete(draft_id, {}, idempotency_key="delete-5")
        self.assertEqual("profile_in_use", raised.exception.code)
        self.assertEqual("prepared", self.service.get_draft(draft_id)["status"])
        self.assertTrue((self.profiles_dir / f"{profile_id}.json").exists())

    def test_delete_fails_cleanly_while_a_lifecycle_lease_is_held(self) -> None:
        draft_id, profile_id = self._prepared()
        other, _ = self.journal.create_or_get_operation(
            action=OperationAction.LOAD,
            idempotency_key="other-load",
            request={"kind": "load", "deployment": {"profile_id": "reviewed"}},
            node_ids=("spark-a",),
        )
        self.journal.claim_operation(other.id)
        self.journal.acquire_locks(other.id, ["spark-a"])
        operation = self.service.submit_delete(draft_id, {}, idempotency_key="delete-6")
        self.assertEqual(OperationState.FAILED, self._wait(operation.id))
        self.assertEqual(
            "profile_in_use", self.journal.get_operation(operation.id).error_code
        )
        self.assertTrue((self.profiles_dir / f"{profile_id}.json").exists())
        self.assertIn(("spark-a", self._working_directory(profile_id)), self.ops.files)
        self.assertEqual("prepared", self.service.get_draft(draft_id)["status"])

    def test_refused_node_removal_keeps_the_profile_for_a_retry(self) -> None:
        draft_id, profile_id = self._prepared()
        self.ops.refuse_removal["spark-b"] = "container-exists"
        operation = self.service.submit_delete(draft_id, {}, idempotency_key="delete-7")
        self.assertEqual(OperationState.FAILED, self._wait(operation.id))
        record = self.journal.get_operation(operation.id)
        self.assertEqual("recipe_delete_incomplete", record.error_code)
        self.assertIn("profile_kept", self._codes(operation.id))
        self.assertTrue((self.profiles_dir / f"{profile_id}.json").exists())
        draft = self.service.get_draft(draft_id)
        self.assertEqual(
            ("prepared", profile_id), (draft["status"], draft["profile_id"])
        )
        del self.ops.refuse_removal["spark-b"]
        retry = self.service.submit_delete(draft_id, {}, idempotency_key="delete-8")
        self.assertEqual(OperationState.SUCCEEDED, self._wait(retry.id))
        self.assertFalse((self.profiles_dir / f"{profile_id}.json").exists())

    def test_local_delete_is_only_for_recipes_without_profiles(self) -> None:
        draft = self._create()
        self.service.delete_draft(draft["draft_id"])
        draft_id, _profile_id = self._prepared()
        with self.assertRaises(RecipeError) as raised:
            self.service.delete_draft(draft_id)
        self.assertEqual("recipe_prepared", raised.exception.code)
        self.assertEqual([], self.ops.removals)

    def test_queued_delete_can_be_cancelled_behind_a_prepare(self) -> None:
        draft_id, profile_id = self._prepared()
        service = self._slow_service()
        other = self._create(served_alias="qwen-other")
        prepare = service.submit_prepare(
            other["draft_id"], idempotency_key="prepare-blocking"
        )
        delete = service.submit_delete(draft_id, {}, idempotency_key="delete-9")
        self.assertEqual("deleting", service.get_draft(draft_id)["status"])
        with self.assertRaises(RecipeError) as raised:
            service.submit_prepare(draft_id, idempotency_key="prepare-while-deleting")
        self.assertEqual("recipe_busy", raised.exception.code)
        self.assertTrue(service.cancel_operation(delete.id))
        self.assertEqual(OperationState.CANCELLED, self._wait(delete.id))
        self.assertEqual("prepared", service.get_draft(draft_id)["status"])
        self.assertTrue(service.cancel_operation(prepare.id))
        self.assertEqual(OperationState.CANCELLED, self._wait(prepare.id))
        self.assertTrue((self.profiles_dir / f"{profile_id}.json").exists())

    def test_delete_options_are_validated(self) -> None:
        draft_id, _profile_id = self._prepared()
        for body in ({"remove_artifacts": "yes"}, {"path": "/"}):
            with self.assertRaises(RecipeError) as raised:
                self.service.submit_delete(draft_id, body, idempotency_key="delete-x")
            self.assertEqual("invalid_request", raised.exception.code)

    def test_restart_settles_interrupted_deletes(self) -> None:
        draft_id, _profile_id = self._prepared()
        operation, _ = self.journal.create_or_get_operation(
            action=OperationAction.DELETE_RECIPE,
            idempotency_key="stale-delete",
            request={
                "kind": "delete-recipe",
                "draft_id": draft_id,
                "previous_status": "prepared",
            },
            node_ids=("spark-a",),
        )
        self.journal.claim_operation(operation.id)
        self.journal.acquire_locks(operation.id, ["spark-a"], cluster=False)
        stored = self.service.store.get(draft_id)
        self.service.store.save(
            stored.with_status("deleting", last_operation_id=operation.id)
        )
        self.service.recover_interrupted()
        record = self.journal.get_operation(operation.id)
        self.assertEqual(OperationState.NEEDS_ATTENTION, record.state)
        self.assertEqual("delete_interrupted", record.error_code)
        self.assertEqual({}, self.journal.active_locks())
        self.assertEqual("prepared", self.service.get_draft(draft_id)["status"])

    def test_unknown_request_fields_and_unconfigured_nodes_are_rejected(self) -> None:
        with self.assertRaises(RecipeError):
            self._create(command="vllm serve x")
        with self.assertRaises(RecipeError):
            self.service.create_draft(
                {
                    "model_link": QWEN_LINK,
                    "release_tag": "v0.31.0",
                    "node_ids": ["ghost"],
                }
            )

    def test_model_directory_name_matches_the_removal_shape(self) -> None:
        self.assertEqual("Qwen--Qwen3-8B-b968826d", model_dir_for(make_draft()))


if __name__ == "__main__":
    unittest.main()
