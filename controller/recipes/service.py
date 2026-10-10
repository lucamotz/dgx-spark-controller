# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Recipe builder service: drafts, catalog lookups, and the prepare operation.

The service turns a pasted Hugging Face link plus a chosen vLLM release into a
validated draft, then runs a journaled *prepare* operation that pulls the
digest-pinned image and downloads the pinned weights on every selected node,
installs the pinned recipe scripts, and registers a generated profile. Loading
that profile afterwards uses the ordinary orchestrated lifecycle (preflight,
stop conflicts, verify memory recovery, start, verify readiness).

Preparation runs on its own single worker so a long download never blocks an
unload or load of the currently served model; it never stops, starts, or
adopts a model process.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import secrets
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from controller.domain import NodeConfig
from controller.operations import (
    OperationAction,
    OperationPhase,
    OperationRecord,
    OperationState,
)
from controller.persistence import OperationJournal
from controller.recipes.catalog_client import CatalogClient, CatalogError
from controller.recipes.nodeops import PrepareStatus, RecipeNodeOps
from controller.recipes.render import (
    NodeFacts,
    check_port,
    common_model_root,
    environment_text,
    expected_files_text,
    model_dir_for,
    placement_nodes,
    prepare_environment,
    profile_id_for,
    render_recipe,
    template_bytes,
    working_directory_for,
)
from controller.recipes.spec import (
    RecipeDraft,
    RecipeError,
    RecipeModel,
    RecipePlacement,
    RecipeRuntime,
    RecipeSettings,
    settings_from_mapping,
    validate_node_selection,
)
from controller.recipes.store import RecipeStore

IMAGE_ID_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
DRAFT_KEYS = frozenset(
    {
        "model_link",
        "release_tag",
        "cuda_variant",
        "node_ids",
        "head_node_id",
        "served_alias",
        "display_name",
        "settings",
    }
)
GIB = 1024**3


class RecipeService:
    """Owns recipe drafts and runs prepare operations on a dedicated worker."""

    def __init__(
        self,
        *,
        store: RecipeStore,
        journal: OperationJournal,
        catalog: CatalogClient,
        nodes_provider: Callable[[], Mapping[str, NodeConfig]],
        node_ops_provider: Callable[[], RecipeNodeOps | None],
        generated_profiles_dir: Path,
        on_profile_generated: Callable[[str], None] = lambda _profile_id: None,
        poll_interval_seconds: float = 5.0,
        prepare_timeout_seconds: float = 6 * 3600.0,
        progress_interval_seconds: float = 30.0,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.journal = journal
        self.catalog = catalog
        self._nodes = nodes_provider
        self._node_ops = node_ops_provider
        self.generated_profiles_dir = Path(generated_profiles_dir)
        self._on_profile_generated = on_profile_generated
        self.poll_interval_seconds = poll_interval_seconds
        self.prepare_timeout_seconds = prepare_timeout_seconds
        self.progress_interval_seconds = progress_interval_seconds
        self._sleep = sleep
        self._monotonic = monotonic
        self._mutex = threading.RLock()
        self._cancel: dict[str, threading.Event] = {}
        self._closed = False
        self._worker = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="dgx-controller-prepare"
        )

    # ------------------------------------------------------------------ catalog

    def inspect_model(self, link: Any) -> dict[str, Any]:
        """Resolve a Hugging Face link to pinned metadata and memory-fit estimates."""
        from controller.catalog import huggingface

        if not isinstance(link, str) or not link.strip() or len(link) > 600:
            raise RecipeError(
                "invalid_model_link", "Paste a Hugging Face model link or 'org/name'."
            )
        info = self.catalog.model_info(link)
        node_count = max(1, len(self._nodes()))
        fits = [
            huggingface.estimate_fit(info, count).to_dict()
            for count in range(1, min(node_count, 4) + 1)
        ]
        max_positions = info.max_position_embeddings
        return {
            "model": info.to_dict(),
            "fit": fits,
            "suggested": {
                "served_alias": huggingface.default_served_alias(info),
                "display_name": info.repo_id.split("/", 1)[1],
                "max_model_len": min(max_positions, 32_768)
                if max_positions
                else 32_768,
                "trust_remote_code": bool(getattr(info, "requires_remote_code", False)),
            },
        }

    def list_releases(self, *, include_prereleases: bool = False) -> dict[str, Any]:
        return {
            "items": [
                release.to_dict()
                for release in self.catalog.releases(
                    include_prereleases=include_prereleases
                )
            ]
        }

    # ------------------------------------------------------------------- drafts

    def list_drafts(self) -> list[dict[str, Any]]:
        return [self._draft_resource(draft) for draft in self.store.list()]

    def get_draft(self, draft_id: str) -> dict[str, Any]:
        return self._draft_resource(self.store.get(draft_id))

    def create_draft(self, body: Mapping[str, Any]) -> dict[str, Any]:
        self._require_keys(body, required={"model_link", "release_tag", "node_ids"})
        model, info = self._model_from_link(body["model_link"])
        runtime = self._runtime(
            body["release_tag"], body.get("cuda_variant", "default")
        )
        placement = self._placement(body["node_ids"], body.get("head_node_id"))
        from controller.catalog import huggingface

        nodes = self._nodes()
        default_port = self._default_port(
            [nodes[node_id] for node_id in placement.node_ids]
        )
        suggested_len = min(model.max_position_embeddings or 32_768, 32_768)
        base = RecipeSettings(
            max_model_len=suggested_len,
            max_num_batched_tokens=max(8_192, min(suggested_len, 16_384)),
            trust_remote_code=model.needs_remote_code,
            port=default_port,
        )
        settings = settings_from_mapping(body.get("settings") or {}, base=base)
        draft = RecipeDraft(
            draft_id=self.store.new_draft_id(),
            display_name=self._display_name(
                body.get("display_name"), info.repo_id.split("/", 1)[1]
            ),
            served_alias=self._alias(
                body.get("served_alias"), huggingface.default_served_alias(info)
            ),
            model=model,
            runtime=runtime,
            placement=placement,
            settings=settings,
        )
        self._check_draft_against_nodes(draft)
        return self._draft_resource(self.store.save(draft))

    def update_draft(self, draft_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        self._require_keys(body, required=set())
        with self._mutex:
            draft = self.store.get(draft_id)
            if draft.status == "preparing":
                raise RecipeError(
                    "recipe_busy",
                    "This recipe is being prepared and cannot change now.",
                    "Wait for the prepare operation to finish or cancel it.",
                )
            model = draft.model
            if "model_link" in body:
                model, _info = self._model_from_link(body["model_link"])
            runtime = draft.runtime
            if "release_tag" in body or "cuda_variant" in body:
                runtime = self._runtime(
                    body.get("release_tag", draft.runtime.release_tag),
                    body.get("cuda_variant", draft.runtime.cuda_variant),
                )
            placement = draft.placement
            if "node_ids" in body or "head_node_id" in body:
                placement = self._placement(
                    body.get("node_ids", list(draft.placement.node_ids)),
                    body.get("head_node_id", draft.placement.head_node_id),
                )
            settings = settings_from_mapping(
                body.get("settings") or {}, base=draft.settings
            )
            updated = RecipeDraft(
                draft_id=draft.draft_id,
                display_name=self._display_name(
                    body.get("display_name"), draft.display_name
                ),
                served_alias=self._alias(body.get("served_alias"), draft.served_alias),
                model=model,
                runtime=runtime,
                placement=placement,
                settings=settings,
                status="draft",
                profile_id=draft.profile_id,
                last_operation_id=draft.last_operation_id,
                created_at=draft.created_at,
            )
            self._check_draft_against_nodes(updated)
            return self._draft_resource(self.store.save(updated))

    def delete_draft(self, draft_id: str) -> None:
        with self._mutex:
            draft = self.store.get(draft_id)
            if draft.status == "preparing":
                raise RecipeError(
                    "recipe_busy",
                    "A recipe cannot be deleted while it is being prepared.",
                )
            self.store.delete(draft_id)

    # ------------------------------------------------------------------ prepare

    def submit_prepare(self, draft_id: str, *, idempotency_key: str) -> OperationRecord:
        with self._mutex:
            if self._closed:
                raise RecipeError(
                    "controller_shutting_down", "The controller is shutting down."
                )
            if self._node_ops() is None:
                raise RecipeError(
                    "setup_required", "Configure nodes before preparing a recipe."
                )
            draft = self.store.get(draft_id)
            self._check_draft_against_nodes(draft)
            try:
                operation, created = self.journal.create_or_get_operation(
                    action=OperationAction.PREPARE,
                    idempotency_key=idempotency_key,
                    request={
                        "kind": "prepare",
                        "draft_id": draft.draft_id,
                        "served_alias": draft.served_alias,
                    },
                    node_ids=draft.placement.ordered_node_ids,
                )
            except ValueError as error:
                raise RecipeError(
                    "idempotency_conflict",
                    "This idempotency key was already used for a different operation.",
                    "Retry with a new idempotency key.",
                ) from error
            if not created:
                return operation
            if draft.status == "preparing":
                self.journal.finish(
                    operation.id,
                    state=OperationState.CANCELLED,
                    code="prepare_already_running",
                    message="This recipe is already being prepared.",
                    result={"cancelled": True, "draft_id": draft.draft_id},
                )
                raise RecipeError(
                    "recipe_busy", "This recipe is already being prepared."
                )
            self.store.save(
                draft.with_status(
                    "preparing",
                    message="Queued for preparation.",
                    last_operation_id=operation.id,
                )
            )
            self._cancel[operation.id] = threading.Event()
            self._worker.submit(self._run_prepare, operation.id, draft.draft_id)
            return operation

    def cancel_prepare(self, operation_id: str) -> bool:
        """Request cancellation of a queued or running prepare.

        Returns False if the operation is unknown.
        """
        with self._mutex:
            event = self._cancel.get(operation_id)
        if event is None:
            return False
        event.set()
        return True

    def live_operation_ids(self) -> frozenset[str]:
        with self._mutex:
            return frozenset(self._cancel)

    def recover_interrupted(self) -> None:
        """Resolve prepare work that a controller restart left unfinished.

        Called once at startup, before this process can have started a prepare,
        so every queued or running prepare operation in the journal is stale.
        The node-side prepare unit may still be running; preparing again resumes
        the download.
        """
        message = (
            "Preparation was interrupted by a controller restart. Prepare again "
            "to resume. Downloads continue where they stopped."
        )
        live = self.live_operation_ids()
        stale = self.journal.list_operations(
            states=[OperationState.QUEUED, OperationState.RUNNING]
        )
        for record in stale:
            if record.action is not OperationAction.PREPARE or record.id in live:
                continue
            if record.state is OperationState.QUEUED:
                record, _claimed = self.journal.claim_operation(record.id)
            if not record.state.terminal:
                self.journal.finish(
                    record.id,
                    state=OperationState.NEEDS_ATTENTION,
                    code="prepare_interrupted",
                    message=message,
                    error_code="prepare_interrupted",
                    remediation="Prepare the recipe again.",
                )
        for draft in self.store.list():
            if draft.status != "preparing":
                continue
            last = (
                self.journal.get_operation(draft.last_operation_id)
                if draft.last_operation_id
                else None
            )
            if last is None or last.state.terminal:
                self.store.save(draft.with_status("failed", message=message))

    def close(self, *, wait: bool = False) -> None:
        with self._mutex:
            self._closed = True
            events = list(self._cancel.values())
        for event in events:
            event.set()
        self._worker.shutdown(wait=wait, cancel_futures=False)

    def _run_prepare(self, operation_id: str, draft_id: str) -> None:
        cancel = self._cancel.get(operation_id) or threading.Event()
        started_nodes: list[str] = []
        prepare_id = secrets.token_hex(5)
        ops: RecipeNodeOps | None = None
        try:
            record, claimed = self.journal.claim_operation(operation_id)
            if not claimed:
                return
            draft = self.store.get(draft_id)
            nodes = self._nodes()
            selected = placement_nodes(draft, nodes)
            check_port(draft, selected)
            model_root = common_model_root(selected)
            ops = self._node_ops()
            if ops is None:
                raise RecipeError(
                    "setup_required", "Node configuration is unavailable."
                )
            homes = {node.node_id: ops.remote_home(node.node_id) for node in selected}
            if len(set(homes.values())) != 1:
                raise RecipeError(
                    "home_directory_mismatch",
                    "Selected nodes report different home directories.",
                    "Use the same Linux user name on every node in a multi-node "
                    "placement.",
                )
            home = next(iter(homes.values()))
            profile_id = profile_id_for(draft, prepare_id)
            directory = working_directory_for(home, profile_id)
            model_path = f"{model_root.rstrip('/')}/{model_dir_for(draft)}"
            status_file = f"{directory}/prepare-{prepare_id}.status"
            expected_files = f"{directory}/expected-files-{prepare_id}.txt"
            environment = prepare_environment(
                draft,
                prepare_id=prepare_id,
                model_root=model_root,
                status_file=status_file,
                expected_files=expected_files,
            )
            files = {
                "prepare.sh": (template_bytes("prepare.sh"), 0o755),
                f"prepare-{prepare_id}.env": (
                    environment_text(environment).encode("utf-8"),
                    0o600,
                ),
                f"expected-files-{prepare_id}.txt": (
                    expected_files_text(draft).encode("utf-8"),
                    0o600,
                ),
            }
            self.journal.advance(
                operation_id,
                phase=OperationPhase.PREPARING,
                code="prepare_started",
                message=(
                    f"Preparing {draft.model.repo_id}@{draft.model.revision[:8]} "
                    f"with vLLM {draft.runtime.release_tag} "
                    f"on {len(selected)} node(s)."
                ),
                details={"profile_id": profile_id, "prepare_id": prepare_id},
            )
            for node in selected:
                self._require(ops.install_files(node.node_id, directory, files))
            for node in selected:
                started_nodes.append(node.node_id)
                self._require(ops.start_prepare(node.node_id, directory, prepare_id))
            statuses = self._wait_for_prepare(
                operation_id,
                ops,
                selected,
                directory,
                prepare_id,
                model_path,
                draft,
                cancel,
            )
            if statuses is None:
                self._stop_units(ops, started_nodes, prepare_id)
                self.journal.finish(
                    operation_id,
                    state=OperationState.CANCELLED,
                    code="prepare_cancelled",
                    message=(
                        "Preparation was cancelled. Downloaded files are kept for a "
                        "later prepare."
                    ),
                    result={"cancelled": True, "draft_id": draft.draft_id},
                )
                self.store.save(
                    self.store.get(draft_id).with_status(
                        "draft", message="Preparation was cancelled."
                    )
                )
                return
            facts = {
                node_id: self._node_facts(node_id, status, homes[node_id])
                for node_id, status in statuses.items()
            }
            rendered = render_recipe(draft, nodes, facts, prepare_id=prepare_id)
            if rendered.working_directory != directory:
                raise RecipeError(
                    "prepare_mismatch",
                    "The prepared recipe directory changed unexpectedly.",
                )
            for node in selected:
                self._require(
                    ops.install_files(node.node_id, directory, rendered.files())
                )
            self.journal.advance(
                operation_id,
                phase=OperationPhase.COMMITTING,
                code="profile_generated",
                message=f"Registering generated profile {profile_id}.",
                details={"profile_id": profile_id},
            )
            self._write_profile(profile_id, rendered.profile_document)
            self.store.save(
                self.store.get(draft_id).with_status(
                    "prepared",
                    message=(
                        f"Prepared as profile {profile_id}. "
                        "Load it from Models when ready."
                    ),
                    profile_id=profile_id,
                )
            )
            self.journal.finish(
                operation_id,
                state=OperationState.SUCCEEDED,
                code="prepare_succeeded",
                message=f"Recipe prepared. Profile {profile_id} is ready to load.",
                result={"profile_id": profile_id, "draft_id": draft.draft_id},
            )
            self._on_profile_generated(profile_id)
        except (
            Exception
        ) as error:  # every failure becomes a durable, safe terminal record
            if ops is not None and started_nodes:
                self._stop_units(ops, started_nodes, prepare_id)
            code = getattr(error, "code", "prepare_failed")
            message = getattr(error, "message", None) or "Recipe preparation failed."
            remediation = (
                getattr(error, "remediation", "")
                or "Inspect the operation events, fix the cause, and prepare again."
            )
            try:
                current = self.journal.get_operation(operation_id)
                if current is not None and not current.state.terminal:
                    if current.state is OperationState.QUEUED:
                        self.journal.claim_operation(operation_id)
                    self.journal.finish(
                        operation_id,
                        state=OperationState.FAILED,
                        code="prepare_failed",
                        message=str(message),
                        error_code=str(code),
                        remediation=str(remediation),
                    )
            finally:
                with contextlib.suppress(RecipeError):
                    self.store.save(
                        self.store.get(draft_id).with_status(
                            "failed", message=str(message)
                        )
                    )
        finally:
            with self._mutex:
                self._cancel.pop(operation_id, None)

    def _wait_for_prepare(
        self,
        operation_id: str,
        ops: RecipeNodeOps,
        selected: list[NodeConfig],
        directory: str,
        prepare_id: str,
        model_path: str,
        draft: RecipeDraft,
        cancel: threading.Event,
    ) -> dict[str, PrepareStatus] | None:
        deadline = self._monotonic() + self.prepare_timeout_seconds
        last_report: dict[str, str] = {}
        last_progress_at = self._monotonic()
        missing_polls = {node.node_id: 0 for node in selected}
        while True:
            if cancel.is_set():
                return None
            statuses = {
                node.node_id: ops.prepare_status(
                    node.node_id, directory, prepare_id, model_path
                )
                for node in selected
            }
            for node_id, status in statuses.items():
                if status.state == "failed":
                    raise RecipeError(
                        "prepare_failed",
                        f"{node_id}: {status.message or 'prepare failed'}",
                        "Check free disk space, Docker access, and (for gated models) "
                        "`hf auth login` on that node.",
                    )
                missing_polls[node_id] = (
                    missing_polls[node_id] + 1
                    if status.state in {"absent", "unknown"}
                    else 0
                )
                if missing_polls[node_id] >= 6:
                    raise RecipeError(
                        "prepare_unobservable",
                        f"{node_id}: the prepare unit stopped reporting progress.",
                        "Check SSH connectivity and the node's user systemd manager, "
                        "then prepare again.",
                    )
            if all(status.state == "succeeded" for status in statuses.values()):
                return statuses
            now = self._monotonic()
            summary = {
                node_id: f"{status.step}:{status.state}"
                for node_id, status in statuses.items()
            }
            if (
                summary != last_report
                or now - last_progress_at >= self.progress_interval_seconds
            ):
                parts = []
                for node_id, status in statuses.items():
                    detail = status.step or status.state
                    if (
                        status.step == "downloading"
                        and status.downloaded_bytes is not None
                    ):
                        detail += (
                            f" {status.downloaded_bytes / GIB:.1f}"
                            f"/{draft.model.weight_bytes / GIB:.1f} GiB"
                        )
                    parts.append(f"{node_id}: {detail}")
                self.journal.advance(
                    operation_id,
                    phase=OperationPhase.PREPARING,
                    code="prepare_progress",
                    message=" · ".join(parts),
                )
                last_report = summary
                last_progress_at = now
            if now >= deadline:
                raise RecipeError(
                    "prepare_timeout",
                    "Preparation did not finish in time.",
                    "Prepare again; completed downloads are reused.",
                )
            self._sleep(self.poll_interval_seconds)

    @staticmethod
    def _node_facts(node_id: str, status: PrepareStatus, home: str) -> NodeFacts:
        facts = status.facts
        image_id = facts.get("IMAGE_ID", "")
        manifest = facts.get("MANIFEST_SHA256", "")
        manifest_path = facts.get("MANIFEST_PATH", "")
        version = facts.get("VLLM_VERSION", "")
        memory = facts.get("MEM_TOTAL", "")
        if (
            not IMAGE_ID_RE.fullmatch(image_id)
            or not SHA256_RE.fullmatch(manifest)
            or manifest_path not in {"config.json", "model.safetensors.index.json"}
            or not re.fullmatch(r"[A-Za-z0-9._+-]{1,64}", version)
            or not memory.isdigit()
        ):
            raise RecipeError(
                "prepare_incomplete",
                f"{node_id}: prepare reported incomplete or malformed results.",
            )
        return NodeFacts(
            node_id=node_id,
            home=facts.get("HOME_DIR") or home,
            image_id=image_id,
            vllm_version=version,
            manifest_sha256=manifest,
            manifest_path=manifest_path,
            memory_total_bytes=int(memory),
        )

    def _stop_units(
        self, ops: RecipeNodeOps, node_ids: list[str], prepare_id: str
    ) -> None:
        for node_id in node_ids:
            with contextlib.suppress(Exception):
                ops.stop_prepare(node_id, prepare_id)

    def _write_profile(self, profile_id: str, document: Mapping[str, Any]) -> Path:
        directory = self.generated_profiles_dir
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        target = directory / f"{profile_id}.json"
        handle, temporary = tempfile.mkstemp(
            prefix=".profile-", suffix=".json", dir=directory
        )
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(document, stream, indent=2)
                stream.write("\n")
            os.chmod(temporary, 0o600)
            os.replace(temporary, target)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary)
            raise
        return target

    @staticmethod
    def _require(result: Any) -> None:
        if not result.ok:
            raise RecipeError(
                result.code or "prepare_failed",
                result.message or "A remote prepare step failed.",
            )

    # ------------------------------------------------------------------ helpers

    def _model_from_link(self, link: Any) -> tuple[RecipeModel, Any]:
        if not isinstance(link, str) or not link.strip() or len(link) > 600:
            raise RecipeError(
                "invalid_model_link", "Paste a Hugging Face model link or 'org/name'."
            )
        info = self.catalog.model_info(link)
        if info.weight_format != "safetensors":
            raise RecipeError(
                "unsupported_weight_format",
                f"This revision publishes {info.weight_format} weights; the vLLM "
                "recipe needs safetensors.",
                "Choose a repository (or quantized variant) that ships safetensors "
                "weights.",
            )
        model = RecipeModel(
            repo_id=info.repo_id,
            revision=info.revision,
            weight_bytes=info.weight_bytes,
            weight_files=tuple(
                (item.name, item.size_bytes) for item in info.weight_files
            ),
            max_position_embeddings=info.max_position_embeddings,
            architectures=tuple(info.architectures),
            quantization=info.quantization,
            gated=bool(info.gated),
            needs_remote_code=bool(getattr(info, "requires_remote_code", False)),
        )
        return model, info

    def _runtime(self, release_tag: Any, cuda_variant: Any) -> RecipeRuntime:
        if not isinstance(release_tag, str) or not isinstance(cuda_variant, str):
            raise RecipeError("invalid_release", "Choose a vLLM release from the list.")
        image = self.catalog.runtime_image(release_tag, cuda_variant)
        return RecipeRuntime(
            release_tag=image.release_tag,
            reference=image.reference,
            arm64_digest=image.arm64_digest,
            cuda_variant=image.cuda_variant,
        )

    def _placement(self, node_ids: Any, head_node_id: Any) -> RecipePlacement:
        if not isinstance(node_ids, list) or not all(
            isinstance(item, str) for item in node_ids
        ):
            raise RecipeError(
                "invalid_placement", "node_ids must be a list of configured node ids."
            )
        validate_node_selection(node_ids, list(self._nodes()))
        head = (
            head_node_id
            if isinstance(head_node_id, str) and head_node_id
            else (node_ids[0] if node_ids else "")
        )
        return RecipePlacement(node_ids=tuple(node_ids), head_node_id=head)

    def _check_draft_against_nodes(self, draft: RecipeDraft) -> None:
        nodes = self._nodes()
        selected = placement_nodes(draft, nodes)
        check_port(draft, selected)
        common_model_root(selected)

    @staticmethod
    def _default_port(selected: list[NodeConfig]) -> int:
        for candidate in (8000, 8888, 8080):
            if all(
                node.endpoint_ports.start <= candidate <= node.endpoint_ports.end
                for node in selected
            ):
                return candidate
        start = max(node.endpoint_ports.start for node in selected)
        return max(start, 1024)

    @staticmethod
    def _alias(value: Any, default: str) -> str:
        if value is None or value == "":
            return default
        if not isinstance(value, str):
            raise RecipeError("invalid_alias", "The served model name must be text.")
        return value.strip()

    @staticmethod
    def _display_name(value: Any, default: str) -> str:
        if value is None or value == "":
            return default
        if not isinstance(value, str):
            raise RecipeError("invalid_recipe", "The recipe name must be text.")
        return value.strip()

    @staticmethod
    def _require_keys(body: Mapping[str, Any], *, required: set[str]) -> None:
        if not isinstance(body, Mapping):
            raise RecipeError(
                "invalid_request", "The request body must be a JSON object."
            )
        unknown = sorted(set(body) - DRAFT_KEYS)
        if unknown:
            raise RecipeError(
                "invalid_request", f"Unsupported recipe field {unknown[0]!r}."
            )
        missing = sorted(required - set(body))
        if missing:
            raise RecipeError(
                "invalid_request", f"Recipe field {missing[0]!r} is required."
            )

    def _draft_resource(self, draft: RecipeDraft) -> dict[str, Any]:
        item = draft.to_dict()
        item["node_count"] = draft.node_count
        item["model_dir"] = model_dir_for(draft)
        item["qualified"] = False
        return item


__all__ = ["RecipeService", "RecipeError", "CatalogError"]
