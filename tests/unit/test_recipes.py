# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Recipe drafts, rendering, the pinned start script, and the prepare workflow."""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path

from catalog_fakes import QWEN_LINK, fixture_catalog

from controller.config import parse_nodes_document
from controller.operations import OperationAction, OperationState
from controller.persistence import OperationJournal
from controller.recipes.nodeops import FakeRecipeNodeOps, parse_status
from controller.recipes.render import (
    NodeFacts,
    environment_text,
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
from controller.transport.managed_ssh import _command_has_exact_tokens

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


class PrepareStatusTests(unittest.TestCase):
    def test_status_parser_keeps_only_allowlisted_keys(self) -> None:
        values, state = parse_status(
            "STATE=running\nSECRET=x\nSTEP=pulling\nSTATE=succeeded\n"
        )
        self.assertEqual("succeeded", state)
        self.assertNotIn("SECRET", values)


class RecipeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.journal = OperationJournal()
        self.ops = FakeRecipeNodeOps(list(NODES))
        self.generated: list[str] = []
        self.service = RecipeService(
            store=RecipeStore(root / "recipes"),
            journal=self.journal,
            catalog=fixture_catalog(),
            nodes_provider=lambda: NODES,
            node_ops_provider=lambda: self.ops,
            generated_profiles_dir=root / "profiles",
            on_profile_generated=self.generated.append,
            poll_interval_seconds=0,
            progress_interval_seconds=0,
        )
        self.profiles_dir = root / "profiles"

    def tearDown(self) -> None:
        self.service.close(wait=True)
        self.journal.close()
        self.directory.cleanup()

    def _create(self, **extra) -> dict:
        body = {
            "model_link": QWEN_LINK,
            "release_tag": "v0.31.0",
            "node_ids": ["spark-a", "spark-b"],
            **extra,
        }
        return self.service.create_draft(body)

    def _wait(self, operation_id: str) -> OperationState:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            record = self.journal.get_operation(operation_id)
            if record is not None and record.state.terminal:
                return record.state
            time.sleep(0.01)
        self.fail("prepare did not finish")

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
        document = json.loads(
            (self.profiles_dir / f"{prepared['profile_id']}.json").read_text()
        )
        self.assertEqual("generated", document["origin"])
        installed = self.ops.files[("spark-b", document["runner"]["working_directory"])]
        self.assertEqual(template_bytes("start.sh"), installed["start.sh"])

    def test_failed_node_marks_recipe_failed_and_stops_every_unit(self) -> None:
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

    def test_restart_resolves_a_prepare_left_queued(self) -> None:
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
        self.assertTrue(self.journal.get_operation(operation.id).state.terminal)
        self.assertEqual("failed", self.service.get_draft(draft["draft_id"])["status"])

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


if __name__ == "__main__":
    unittest.main()
