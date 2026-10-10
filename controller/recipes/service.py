# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Recipe builder service: drafts, catalog lookups, prepare and delete.

The service turns a pasted Hugging Face link plus a chosen vLLM release into a
validated draft, then runs a journaled *prepare* operation that pulls the
digest-pinned image and downloads the pinned weights on every selected node,
installs the pinned recipe scripts, and registers a generated profile. Loading
that profile afterwards uses the ordinary orchestrated lifecycle (preflight,
stop conflicts, verify memory recovery, start, verify readiness).

Preparation and deletion share one dedicated worker, so a long download never
blocks an unload or load of the currently served model; neither ever stops,
starts, or adopts a model process. A failed or cancelled prepare removes its
own node directory, a restarted controller reattaches to prepare units that
are still running, and a journaled *delete-recipe* operation removes a
recipe's generated profiles and node files, optionally with the weights and
image that nothing else on a node still references.
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
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from controller.config import ConfigError, load_profile
from controller.domain import ModelProfile, NodeConfig
from controller.operations import (
    RECIPE_ACTIONS,
    LockConflict,
    OperationAction,
    OperationPhase,
    OperationRecord,
    OperationState,
    bounded_node_progress,
    latest_node_progress,
)
from controller.persistence import OperationJournal
from controller.recipes.catalog_client import CatalogClient, CatalogError
from controller.recipes.nodeops import (
    PREPARE_ID_RE,
    PrepareStatus,
    RecipeNodeOps,
    RemovalResult,
    require_removal_target,
)
from controller.recipes.render import (
    NodeFacts,
    environment_text,
    expected_files_text,
    is_generated_profile_id,
    model_dir_for,
    model_dir_name,
    model_path_for,
    prepare_environment,
    profile_id_for,
    render_recipe,
    resolve_placement,
    single_home,
    template_bytes,
    working_directory_for,
)
from controller.recipes.spec import (
    BUSY_STATES,
    DRAFT_STATES,
    IMAGE_REFERENCE_RE,
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
LIFECYCLE_ACTIONS = frozenset(
    {OperationAction.LOAD, OperationAction.UNLOAD, OperationAction.RECOVER}
)
DELETE_KEYS = frozenset({"remove_artifacts"})
# Node leases held while a delete removes files a lifecycle operation could use.
DELETE_LEASE_SECONDS = 900.0
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


class _Detached(Exception):
    """The controller is shutting down; leave the node-side prepare running."""


@dataclass
class _PrepareRun:
    """Everything one prepare run needs to poll, finish, or clean up."""

    draft: RecipeDraft
    nodes: Mapping[str, NodeConfig]
    selected: list[NodeConfig]
    homes: dict[str, str]
    prepare_id: str
    profile_id: str
    directory: str
    model_root: str
    model_path: str
    touched: list[str] = field(default_factory=list)
    started: list[str] = field(default_factory=list)

    @property
    def node_ids(self) -> list[str]:
        return [node.node_id for node in self.selected]


@dataclass(frozen=True)
class _GeneratedProfile:
    """A generated profile file in the data directory and its parsed contents."""

    path: Path
    profile: ModelProfile

    @property
    def profile_id(self) -> str:
        return self.profile.profile_id

    @property
    def node_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.profile.node_selector.allowed_node_ids))

    @property
    def draft_id(self) -> str | None:
        return self.profile.provenance.get("draft_id")

    @property
    def prepare_id(self) -> str | None:
        value = self.profile.provenance.get("prepare_id", "")
        return value if PREPARE_ID_RE.fullmatch(value) else None


@dataclass(frozen=True)
class _ArtifactReference:
    """Weights and an image that something other than the deleted recipe needs."""

    node_ids: frozenset[str] | None  # None: any node
    paths: frozenset[str]
    images: frozenset[str]

    def needs_model(self, node_id: str, model_path: str) -> bool:
        return (self.node_ids is None or node_id in self.node_ids) and any(
            path == model_path or path.startswith(f"{model_path}/")
            for path in self.paths
        )

    def needs_image(self, node_id: str, image: str) -> bool:
        return (self.node_ids is None or node_id in self.node_ids) and (
            image in self.images
        )


class RecipeService:
    """Owns recipe drafts and runs prepare and delete operations on one worker."""

    def __init__(
        self,
        *,
        store: RecipeStore,
        journal: OperationJournal,
        catalog: CatalogClient,
        nodes_provider: Callable[[], Mapping[str, NodeConfig]],
        node_ops_provider: Callable[[], RecipeNodeOps | None],
        generated_profiles_dir: Path,
        profiles_provider: Callable[[], Mapping[str, ModelProfile]] = dict,
        on_profile_generated: Callable[[str], None] = lambda _profile_id: None,
        on_profiles_removed: Callable[[list[str]], None] = lambda _ids: None,
        poll_interval_seconds: float = 5.0,
        prepare_timeout_seconds: float = 6 * 3600.0,
        progress_interval_seconds: float = 15.0,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.journal = journal
        self.catalog = catalog
        self._nodes = nodes_provider
        self._node_ops = node_ops_provider
        self._profiles = profiles_provider
        self.generated_profiles_dir = Path(generated_profiles_dir)
        self._on_profile_generated = on_profile_generated
        self._on_profiles_removed = on_profiles_removed
        self.poll_interval_seconds = poll_interval_seconds
        self.prepare_timeout_seconds = prepare_timeout_seconds
        self.progress_interval_seconds = progress_interval_seconds
        self._monotonic = monotonic
        self._mutex = threading.RLock()
        self._cancel: dict[str, threading.Event] = {}
        self._live: set[str] = set()
        self._closed = False
        self._stopping = threading.Event()
        # Polling sleeps wake up early on shutdown unless a test injects a clock.
        self._sleep = sleep or (lambda seconds: self._stopping.wait(seconds) and None)
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
        generated = self._generated_profiles()
        leftovers = self._drafts_with_node_files_left()
        return [
            self._draft_resource(draft, generated, leftovers)
            for draft in self.store.list()
        ]

    def get_draft(self, draft_id: str) -> dict[str, Any]:
        return self._draft_resource(
            self.store.get(draft_id), self._generated_profiles()
        )

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
        return self._draft_resource(self.store.save(draft), self._generated_profiles())

    def update_draft(self, draft_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        self._require_keys(body, required=set())
        with self._mutex:
            draft = self.store.get(draft_id)
            if draft.status in BUSY_STATES:
                raise RecipeError(
                    "recipe_busy",
                    f"This recipe is {draft.status} and cannot change now.",
                    "Wait for its operation to finish, or cancel a prepare.",
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
            return self._draft_resource(
                self.store.save(updated), self._generated_profiles()
            )

    def delete_draft(self, draft_id: str) -> None:
        """Delete a draft that never produced a profile; nothing runs remotely."""
        with self._mutex:
            draft = self.store.get(draft_id)
            if draft.status in BUSY_STATES:
                raise RecipeError(
                    "recipe_busy",
                    f"A recipe cannot be deleted while it is {draft.status}.",
                    "Wait for its operation to finish, or cancel a prepare.",
                )
            if self._profiles_for_draft(draft):
                raise RecipeError(
                    "recipe_prepared",
                    "This recipe has generated profiles and recipe files on its nodes.",
                    "Use Delete on the recipe card instead: it removes the "
                    "generated profiles and their node files as an operation.",
                )
            if draft_id in self._drafts_with_node_files_left():
                raise RecipeError(
                    "recipe_prepared",
                    "An earlier prepare of this recipe left files on its nodes.",
                    "Use Delete on the recipe card instead: it removes them as "
                    "an operation.",
                )
            self.store.delete(draft_id)

    # ------------------------------------------------------------------ prepare

    def submit_prepare(self, draft_id: str, *, idempotency_key: str) -> OperationRecord:
        with self._mutex:
            self._require_open()
            if self._node_ops() is None:
                raise RecipeError(
                    "setup_required", "Configure nodes before preparing a recipe."
                )
            draft = self.store.get(draft_id)
            self._check_draft_against_nodes(draft)
            operation, created = self._create_operation(
                action=OperationAction.PREPARE,
                idempotency_key=idempotency_key,
                request={
                    "kind": "prepare",
                    "draft_id": draft.draft_id,
                    "served_alias": draft.served_alias,
                },
                node_ids=draft.placement.ordered_node_ids,
            )
            if not created:
                return operation
            if draft.status in BUSY_STATES:
                self.journal.finish(
                    operation.id,
                    state=OperationState.CANCELLED,
                    code="prepare_already_running",
                    message=f"This recipe is already {draft.status}.",
                    result={"cancelled": True, "draft_id": draft.draft_id},
                )
                raise RecipeError(
                    "recipe_busy", f"This recipe is already {draft.status}."
                )
            self.store.save(
                draft.with_status(
                    "preparing",
                    message="Queued for preparation.",
                    last_operation_id=operation.id,
                )
            )
            self._cancel[operation.id] = threading.Event()
            self._live.add(operation.id)
            self._worker.submit(self._run_prepare, operation.id, draft.draft_id)
            return operation

    def submit_delete(
        self, draft_id: str, body: Mapping[str, Any], *, idempotency_key: str
    ) -> OperationRecord:
        """Queue a journaled delete of a recipe and everything it generated.

        Args:
            draft_id: The recipe draft to delete.
            body: ``{}`` or ``{"remove_artifacts": bool}``.
            idempotency_key: The validated ``Idempotency-Key`` header.

        Returns:
            The queued (or, for a repeated key, existing) operation.

        Raises:
            RecipeError: ``profile_in_use`` while a deployment of one of the
                recipe's profiles is not stopped or a lifecycle operation for
                it is queued or running; ``recipe_busy`` while the recipe is
                being prepared or deleted.

        """
        remove_artifacts = self._delete_options(body)
        with self._mutex:
            self._require_open()
            if self._node_ops() is None:
                raise RecipeError(
                    "setup_required", "Configure nodes before deleting a recipe."
                )
            draft = self.store.get(draft_id)
            if draft.status in BUSY_STATES:
                raise RecipeError(
                    "recipe_busy",
                    f"This recipe is {draft.status} and cannot be deleted now.",
                    "Wait for its operation to finish, or cancel a prepare.",
                )
            profiles = self._profiles_for_draft(draft)
            self._require_not_in_use(profiles)
            node_ids = set(draft.placement.node_ids)
            for generated in profiles.values():
                node_ids.update(generated.node_ids)
            operation, created = self._create_operation(
                action=OperationAction.DELETE_RECIPE,
                idempotency_key=idempotency_key,
                request={
                    "kind": OperationAction.DELETE_RECIPE.value,
                    "draft_id": draft.draft_id,
                    "profile_ids": sorted(profiles),
                    "remove_artifacts": remove_artifacts,
                    "previous_status": draft.status,
                },
                node_ids=tuple(sorted(node_ids)),
            )
            if not created:
                return operation
            self.store.save(
                draft.with_status(
                    "deleting",
                    message="Queued for deletion.",
                    last_operation_id=operation.id,
                )
            )
            self._live.add(operation.id)
            self._worker.submit(self._run_delete, operation.id)
            return operation

    def cancel_operation(self, operation_id: str) -> bool:
        """Cancel a recipe operation if this service owns it.

        A queued or running prepare is signalled and stops its node units at
        its next poll. A queued delete is cancelled at once; a running delete
        is short and finishes on its own.

        Returns:
            False if this service does not own a cancellable operation with
            that id, so the lifecycle application decides instead.

        """
        with self._mutex:
            event = self._cancel.get(operation_id)
            if event is not None:
                event.set()
                return True
            if operation_id not in self._live:
                return False
        record = self.journal.get_operation(operation_id)
        if record is None or record.action is not OperationAction.DELETE_RECIPE:
            return False
        # Claiming is atomic, so the worker can never start a delete that was
        # cancelled here, and a delete the worker already claimed stays running.
        record, claimed = self.journal.claim_operation(operation_id)
        if not claimed:
            return False
        self.journal.finish(
            operation_id,
            state=OperationState.CANCELLED,
            code="operation_cancelled",
            message="Deletion cancelled before anything was removed.",
            result={"cancelled": True, "draft_id": record.request.get("draft_id")},
        )
        self._settle_deleting_draft(record, "Deletion was cancelled.")
        return True

    def live_operation_ids(self) -> frozenset[str]:
        with self._mutex:
            return frozenset(self._live)

    def recover_interrupted(self) -> None:
        """Resolve recipe operations that a controller restart left unfinished.

        Called once at startup, before this process can have started any
        recipe operation. A queued prepare or delete never reached a node and
        is cancelled. A running prepare is reattached on the recipe worker: it
        finishes normally when every node's unit is still running or already
        succeeded, and ends ``needs-attention`` only when a node's state
        cannot be read. A running delete ends ``needs-attention``; deleting
        again is safe because every removal step is idempotent.
        """
        live = self.live_operation_ids()
        stale = self.journal.list_operations(
            states=[OperationState.QUEUED, OperationState.RUNNING]
        )
        for record in sorted(stale, key=lambda item: item.created_at):
            if record.action not in RECIPE_ACTIONS or record.id in live:
                continue
            draft_id = str(record.request.get("draft_id", ""))
            if record.state is OperationState.QUEUED:
                self.journal.finish(
                    record.id,
                    state=OperationState.CANCELLED,
                    code="queued_operation_interrupted",
                    message="Controller restart cancelled work that had not started.",
                    result={"cancelled": True, "draft_id": draft_id},
                )
                if record.action is OperationAction.PREPARE:
                    self._settle_preparing_draft(
                        draft_id,
                        record.id,
                        "draft",
                        "The controller restarted before this preparation "
                        "started. Prepare again.",
                    )
                else:
                    self._settle_deleting_draft(
                        record,
                        "The controller restarted before this deletion started.",
                    )
            elif record.action is OperationAction.PREPARE:
                with self._mutex:
                    self._cancel[record.id] = threading.Event()
                    self._live.add(record.id)
                self._worker.submit(self._resume_prepare, record.id, draft_id)
            else:
                self.journal.release_locks(record.id)
                self.journal.finish(
                    record.id,
                    state=OperationState.NEEDS_ATTENTION,
                    code="delete_interrupted",
                    message="Deletion was interrupted by a controller restart.",
                    error_code="delete_interrupted",
                    remediation="Delete the recipe again; finished steps are skipped.",
                )
                self._settle_deleting_draft(
                    record, "Deletion was interrupted by a controller restart."
                )
        for draft in self.store.list():
            if draft.status not in BUSY_STATES:
                continue
            last = (
                self.journal.get_operation(draft.last_operation_id)
                if draft.last_operation_id
                else None
            )
            if last is not None and not last.state.terminal:
                continue
            if draft.status == "preparing":
                self.store.save(
                    draft.with_status(
                        "failed",
                        message="Preparation was interrupted. Prepare again; "
                        "downloads continue where they stopped.",
                    )
                )
            elif last is not None:
                self._settle_deleting_draft(last, "Deletion did not finish.")
            else:
                self.store.save(
                    draft.with_status("draft", message="Deletion did not finish.")
                )

    def close(self, *, wait: bool = False) -> None:
        """Stop local work without cancelling node-side prepare units.

        A prepare that is still downloading keeps running on its nodes, and its
        operation stays ``running`` in the journal, so the next controller
        start reattaches to it. Only an explicit cancel stops a prepare.
        """
        with self._mutex:
            self._closed = True
            self._stopping.set()
        self._worker.shutdown(wait=wait, cancel_futures=False)

    def _run_prepare(self, operation_id: str, draft_id: str) -> None:
        cancel = self._cancel_event(operation_id)
        run: _PrepareRun | None = None
        ops: RecipeNodeOps | None = None
        try:
            if self._stopping.is_set():
                return
            _record, claimed = self.journal.claim_operation(operation_id)
            if not claimed:
                return
            if cancel.is_set():
                self._finish_cancelled(operation_id, draft_id)
                return
            draft = self.store.get(draft_id)
            ops = self._require_node_ops()
            run = self._plan_prepare(draft, ops)
            self._require_lingering(ops, run)
            environment = prepare_environment(
                draft,
                prepare_id=run.prepare_id,
                model_root=run.model_root,
                status_file=f"{run.directory}/prepare-{run.prepare_id}.status",
                expected_files=f"{run.directory}/expected-files-{run.prepare_id}.txt",
            )
            files = {
                "prepare.sh": (template_bytes("prepare.sh"), 0o755),
                f"prepare-{run.prepare_id}.env": (
                    environment_text(environment).encode("utf-8"),
                    0o600,
                ),
                f"expected-files-{run.prepare_id}.txt": (
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
                    f"on {len(run.selected)} node(s)."
                ),
                details={"profile_id": run.profile_id, "prepare_id": run.prepare_id},
            )
            for node in run.selected:
                run.touched.append(node.node_id)
                self._require(ops.install_files(node.node_id, run.directory, files))
            for node in run.selected:
                run.started.append(node.node_id)
                self._require(
                    ops.start_prepare(node.node_id, run.directory, run.prepare_id)
                )
            self._complete_prepare(operation_id, draft_id, ops, run, cancel)
        except _Detached:
            return
        except Exception as error:  # every failure becomes a durable terminal record
            self._fail_prepare(operation_id, draft_id, ops, run, error)
        finally:
            self._forget(operation_id)

    def _resume_prepare(self, operation_id: str, draft_id: str) -> None:
        """Reattach to a prepare that a controller restart interrupted."""
        cancel = self._cancel_event(operation_id)
        run: _PrepareRun | None = None
        ops: RecipeNodeOps | None = None
        try:
            if self._stopping.is_set():
                return
            record = self.journal.get_operation(operation_id)
            if record is None or record.state.terminal:
                return
            started = self._prepare_started(operation_id)
            if started is None:
                # The restart happened before anything was installed on a node.
                self.journal.finish(
                    operation_id,
                    state=OperationState.CANCELLED,
                    code="prepare_interrupted",
                    message="The controller restarted before preparation reached "
                    "the nodes. Prepare again.",
                    result={"cancelled": True, "draft_id": draft_id},
                )
                self._settle_preparing_draft(
                    draft_id,
                    operation_id,
                    "draft",
                    "The controller restarted before preparation reached the "
                    "nodes. Prepare again.",
                )
                return
            profile_id, prepare_id = started
            if self._profile_path(profile_id).exists():
                # The profile was registered before the restart; only the
                # journal and draft updates are missing.
                self._finish_registered(operation_id, draft_id, profile_id)
                return
            try:
                draft = self.store.get(draft_id)
                ops = self._require_node_ops()
                run = self._plan_prepare(draft, ops, prepare_id=prepare_id)
                if run.profile_id != profile_id:
                    raise RecipeError(
                        "prepare_mismatch", "The recipe changed during the restart."
                    )
                statuses = {
                    node.node_id: ops.prepare_status(
                        node.node_id, run.directory, prepare_id, run.model_path
                    )
                    for node in run.selected
                }
            except Exception as error:
                self._prepare_needs_attention(
                    operation_id,
                    draft_id,
                    "The prepare units could not be checked after a controller "
                    f"restart: {getattr(error, 'message', None) or error}",
                )
                return
            unknown = [
                node_id
                for node_id, status in statuses.items()
                if status.state not in {"running", "succeeded", "failed", "absent"}
            ]
            if unknown:
                self._prepare_needs_attention(
                    operation_id,
                    draft_id,
                    "The prepare units could not be checked after a controller "
                    f"restart on {', '.join(unknown)}.",
                )
                return
            # From here on every node's state is known, so a failure is a
            # normal one: stop what runs, remove the directory, and say why.
            run.touched = run.node_ids
            run.started = run.node_ids
            self.journal.advance(
                operation_id,
                code="prepare_reattached",
                message="Reattached to the prepare units after a controller "
                f"restart on {len(run.selected)} node(s).",
            )
            absent = [
                node_id
                for node_id, status in statuses.items()
                if status.state == "absent"
            ]
            if absent:
                raise RecipeError(
                    "prepare_interrupted",
                    f"{absent[0]}: the prepare unit is not running and reported no "
                    "result.",
                    "Prepare again; complete downloads are reused.",
                )
            if record.phase is OperationPhase.COMMITTING:
                if not all(status.state == "succeeded" for status in statuses.values()):
                    raise RecipeError(
                        "prepare_interrupted",
                        "Preparation was interrupted while registering the profile.",
                        "Prepare again; complete downloads are reused.",
                    )
                self._commit_prepare(operation_id, draft_id, ops, run, statuses)
                return
            self._complete_prepare(operation_id, draft_id, ops, run, cancel)
        except _Detached:
            return
        except Exception as error:
            self._fail_prepare(operation_id, draft_id, ops, run, error)
        finally:
            self._forget(operation_id)

    def _plan_prepare(
        self, draft: RecipeDraft, ops: RecipeNodeOps, *, prepare_id: str | None = None
    ) -> _PrepareRun:
        """Read-only planning: placement, home directories and target paths."""
        nodes = self._nodes()
        selected, model_root = resolve_placement(draft, nodes)
        homes = {node.node_id: ops.remote_home(node.node_id) for node in selected}
        home = single_home(homes.values())
        prepare_id = prepare_id or self._new_prepare_id(draft)
        profile_id = profile_id_for(draft, prepare_id)
        return _PrepareRun(
            draft=draft,
            nodes=nodes,
            selected=selected,
            homes=homes,
            prepare_id=prepare_id,
            profile_id=profile_id,
            directory=working_directory_for(home, profile_id),
            model_root=model_root,
            model_path=model_path_for(model_root, draft),
        )

    def _new_prepare_id(self, draft: RecipeDraft) -> str:
        """A prepare id whose profile id names no existing profile or directory.

        Failure cleanup removes the node directory of its own profile id, so
        that id must never belong to a profile that is already registered.
        """
        taken = {path.stem for path in self.generated_profiles_dir.glob("*.json")}
        taken.update(item.profile_id for item in self.store.list() if item.profile_id)
        for _attempt in range(16):
            prepare_id = secrets.token_hex(5)
            if profile_id_for(draft, prepare_id) not in taken:
                return prepare_id
        raise RecipeError("prepare_failed", "No unused profile id could be chosen.")

    def _require_lingering(self, ops: RecipeNodeOps, run: _PrepareRun) -> None:
        """Fail before any node mutation unless every node user lingers.

        Prepare runs for minutes in a ``systemd --user`` unit, which survives
        the controller's SSH session only when lingering is enabled.
        """
        missing: list[tuple[str, str]] = []
        for node in run.selected:
            status = ops.check_linger(node.node_id)
            if not status.enabled:
                missing.append((node.node_id, status.user or node.ssh.user))
        if not missing:
            return
        users = {user for _node_id, user in missing}
        if len(users) == 1:
            commands = f"`sudo loginctl enable-linger {users.pop()}` on " + _join_and(
                [node_id for node_id, _user in missing]
            )
        else:
            commands = _join_and(
                [
                    f"`sudo loginctl enable-linger {user}` on {node_id}"
                    for node_id, user in missing
                ]
            )
        raise RecipeError(
            "linger_required",
            "Lingering is not enabled for the node user on "
            f"{_join_and([node_id for node_id, _user in missing])}, so the "
            "prepare unit would stop when the controller's SSH session ends.",
            f"Run {commands}, then prepare again.",
        )

    def _complete_prepare(
        self,
        operation_id: str,
        draft_id: str,
        ops: RecipeNodeOps,
        run: _PrepareRun,
        cancel: threading.Event,
    ) -> None:
        statuses = self._wait_for_prepare(operation_id, ops, run, cancel)
        if statuses is None:
            self._stop_units(ops, run.started, run.prepare_id)
            self._cleanup_prepare(operation_id, ops, run)
            self._finish_cancelled(operation_id, draft_id)
            return
        self._commit_prepare(operation_id, draft_id, ops, run, statuses)

    def _commit_prepare(
        self,
        operation_id: str,
        draft_id: str,
        ops: RecipeNodeOps,
        run: _PrepareRun,
        statuses: Mapping[str, PrepareStatus],
    ) -> None:
        facts = {
            node_id: self._node_facts(node_id, status, run.homes[node_id])
            for node_id, status in statuses.items()
        }
        rendered = render_recipe(run.draft, run.nodes, facts, prepare_id=run.prepare_id)
        if rendered.working_directory != run.directory:
            raise RecipeError(
                "prepare_mismatch",
                "The prepared recipe directory changed unexpectedly.",
            )
        for node in run.selected:
            self._require(
                ops.install_files(node.node_id, run.directory, rendered.files())
            )
        self.journal.advance(
            operation_id,
            phase=OperationPhase.COMMITTING,
            code="profile_generated",
            message=f"Registering generated profile {run.profile_id}.",
            details={"profile_id": run.profile_id},
        )
        self._write_profile(run.profile_id, rendered.profile_document)
        self._finish_registered(operation_id, draft_id, run.profile_id)

    def _finish_registered(
        self, operation_id: str, draft_id: str, profile_id: str
    ) -> None:
        with contextlib.suppress(RecipeError):
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
            result={"profile_id": profile_id, "draft_id": draft_id},
        )
        self._on_profile_generated(profile_id)

    def _finish_cancelled(self, operation_id: str, draft_id: str) -> None:
        self.journal.finish(
            operation_id,
            state=OperationState.CANCELLED,
            code="prepare_cancelled",
            message=(
                "Preparation was cancelled. Downloaded weights and the runtime "
                "image are kept for a later prepare."
            ),
            result={"cancelled": True, "draft_id": draft_id},
        )
        self._settle_preparing_draft(
            draft_id, operation_id, "draft", "Preparation was cancelled."
        )

    def _fail_prepare(
        self,
        operation_id: str,
        draft_id: str,
        ops: RecipeNodeOps | None,
        run: _PrepareRun | None,
        error: Exception,
    ) -> None:
        if self._stopping.is_set():
            # Shutting down: leave nodes and journal alone; the next start
            # reattaches to or settles this operation.
            return
        if ops is not None and run is not None:
            if run.started:
                self._stop_units(ops, run.started, run.prepare_id)
            if run.touched:
                self._cleanup_prepare(operation_id, ops, run)
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
            self._settle_preparing_draft(draft_id, operation_id, "failed", str(message))

    def _prepare_needs_attention(
        self, operation_id: str, draft_id: str, message: str
    ) -> None:
        current = self.journal.get_operation(operation_id)
        if current is not None and not current.state.terminal:
            self.journal.finish(
                operation_id,
                state=OperationState.NEEDS_ATTENTION,
                code="prepare_interrupted",
                message=message,
                error_code="prepare_interrupted",
                remediation=(
                    "Check SSH access to the node, then prepare again. A prepare "
                    "unit may still be running there; downloads resume."
                ),
            )
        self._settle_preparing_draft(draft_id, operation_id, "failed", message)

    def _cleanup_prepare(
        self, operation_id: str, ops: RecipeNodeOps, run: _PrepareRun
    ) -> None:
        """Remove this prepare's own node directory after its units stopped.

        Weights and the image stay: downloads resume on the next prepare and
        are shared by every recipe of the same model revision and release.
        """
        if self._profile_path(run.profile_id).exists():
            return  # registered: the directory now belongs to a profile
        removed: list[str] = []
        kept: list[str] = []
        for node_id in run.touched:
            try:
                result = ops.remove_recipe_directory(
                    node_id,
                    run.homes[node_id],
                    run.profile_id,
                    prepare_id=run.prepare_id,
                )
            except Exception:
                result = RemovalResult("failed", "unreachable")
            if result.ok:
                removed.append(node_id)
            else:
                kept.append(f"{node_id} ({result.reason or result.state})")
        with contextlib.suppress(Exception):
            if kept:
                self.journal.advance(
                    operation_id,
                    code="prepare_cleanup_incomplete",
                    message=(
                        "This prepare's recipe directory was kept on "
                        f"{', '.join(kept)}. Deleting the recipe removes it "
                        "once its prepare unit has stopped."
                    ),
                )
            elif removed:
                self.journal.advance(
                    operation_id,
                    code="prepare_directory_removed",
                    message=(
                        "Removed this prepare's recipe directory on "
                        f"{len(removed)} node(s). Downloaded weights and the "
                        "runtime image are kept for the next prepare."
                    ),
                )

    def _wait_for_prepare(
        self,
        operation_id: str,
        ops: RecipeNodeOps,
        run: _PrepareRun,
        cancel: threading.Event,
    ) -> dict[str, PrepareStatus] | None:
        deadline = self._monotonic() + self.prepare_timeout_seconds
        last_report: dict[str, str] = {}
        last_progress_at = self._monotonic()
        missing_polls = {node_id: 0 for node_id in run.node_ids}
        while True:
            if self._stopping.is_set():
                raise _Detached
            if cancel.is_set():
                return None
            statuses = {
                node.node_id: ops.prepare_status(
                    node.node_id, run.directory, run.prepare_id, run.model_path
                )
                for node in run.selected
            }
            if self._stopping.is_set():
                raise _Detached
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
            finished = all(status.state == "succeeded" for status in statuses.values())
            now = self._monotonic()
            summary = {
                node_id: f"{status.step}:{status.state}"
                for node_id, status in statuses.items()
            }
            if (
                finished
                or summary != last_report
                or now - last_progress_at >= self.progress_interval_seconds
            ):
                self._journal_progress(operation_id, run, statuses)
                last_report = summary
                last_progress_at = now
            if finished:
                return statuses
            if now >= deadline:
                raise RecipeError(
                    "prepare_timeout",
                    "Preparation did not finish in time.",
                    "Prepare again; completed downloads are reused.",
                )
            self._sleep(self.poll_interval_seconds)

    def _journal_progress(
        self,
        operation_id: str,
        run: _PrepareRun,
        statuses: Mapping[str, PrepareStatus],
    ) -> None:
        """Journal one progress event with bounded, path-free per-node details."""
        total = run.draft.model.weight_bytes
        progress = bounded_node_progress(
            {
                node_id: {
                    "state": status.state,
                    "step": status.step or status.state,
                    "downloaded_bytes": status.downloaded_bytes,
                    "total_bytes": total,
                }
                for node_id, status in statuses.items()
            },
            run.node_ids,
        )
        parts = []
        for node_id, item in progress.items():
            detail = str(item["step"])
            downloaded = item["downloaded_bytes"]
            if item["step"] == "downloading" and isinstance(downloaded, int):
                detail += f" {downloaded / GIB:.1f}/{total / GIB:.1f} GiB"
            parts.append(f"{node_id}: {detail}")
        self.journal.advance(
            operation_id,
            phase=OperationPhase.PREPARING,
            code="prepare_progress",
            message=" · ".join(parts),
            details={"progress_by_node": progress},
        )

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

    def _prepare_started(self, operation_id: str) -> tuple[str, str] | None:
        """Profile and prepare ids journaled before the first node mutation."""
        for event in self.journal.events(operation_id):
            if event.code != "prepare_started":
                continue
            profile_id = event.details.get("profile_id")
            prepare_id = event.details.get("prepare_id")
            if (
                is_generated_profile_id(profile_id)
                and isinstance(prepare_id, str)
                and PREPARE_ID_RE.fullmatch(prepare_id)
            ):
                return str(profile_id), prepare_id
        return None

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

    # ------------------------------------------------------------------- delete

    def _run_delete(self, operation_id: str) -> None:
        record: OperationRecord | None = None
        locked = False
        try:
            if self._stopping.is_set():
                return
            record, claimed = self.journal.claim_operation(operation_id)
            if not claimed:
                if record.state.terminal:
                    self._settle_deleting_draft(record, "Deletion was cancelled.")
                return
            draft = self.store.get(str(record.request.get("draft_id", "")))
            ops = self._require_node_ops()
            nodes = self._nodes()
            targets = self._profiles_for_draft(draft)
            leftovers = self._unregistered_prepares(draft, targets)
            remove_artifacts = record.request.get("remove_artifacts") is True
            node_ids = sorted(
                {node_id for item in targets.values() for node_id in item.node_ids}
                | {
                    node_id
                    for *_, prepare_nodes in leftovers
                    for node_id in prepare_nodes
                }
                | set(draft.placement.node_ids)
            )
            configured = [node_id for node_id in node_ids if node_id in nodes]
            self.journal.advance(
                operation_id,
                phase=OperationPhase.DELETING,
                code="delete_started",
                message=(
                    f"Deleting {draft.display_name} and {len(targets)} generated "
                    f"profile(s) on {len(configured)} node(s)."
                ),
                details={"profile_ids": sorted(targets)},
            )
            self._lock(operation_id, configured)
            locked = True
            # Checked again under the node leases: a lifecycle operation that
            # was queued after the request was accepted cannot slip through.
            self._require_not_in_use(targets)
            deleted, kept = self._delete_profiles(
                operation_id, ops, nodes, configured, targets
            )
            kept_directories = self._delete_unregistered_directories(
                operation_id, ops, nodes, configured, leftovers
            )
            artifacts: list[dict[str, str]] = []
            if remove_artifacts:
                artifacts = self._delete_artifacts(
                    operation_id, ops, nodes, configured, draft, targets, deleted
                )
            if kept or kept_directories:
                reasons = []
                if kept:
                    reasons.append(
                        f"Profile(s) {', '.join(sorted(kept))} were kept because "
                        "their node files could not be removed."
                    )
                if kept_directories:
                    reasons.append(
                        "Recipe directories of an earlier prepare were kept on "
                        f"{', '.join(kept_directories)}."
                    )
                self.journal.finish(
                    operation_id,
                    state=OperationState.FAILED,
                    code="delete_failed",
                    message=" ".join(reasons),
                    result={
                        "draft_id": draft.draft_id,
                        "deleted_profiles": deleted,
                        "artifacts": artifacts,
                    },
                    error_code="recipe_delete_incomplete",
                    remediation=(
                        "Fix the reason in the operation events (for example an "
                        "unreachable node or a container that still exists), then "
                        "delete the recipe again."
                    ),
                )
                self._settle_deleting_draft(record, "Deletion did not finish.")
                return
            with contextlib.suppress(RecipeError):
                self.store.delete(draft.draft_id)
            self.journal.finish(
                operation_id,
                state=OperationState.SUCCEEDED,
                code="delete_succeeded",
                message=(
                    f"Deleted the recipe and {len(deleted)} generated profile(s)."
                ),
                result={
                    "draft_id": draft.draft_id,
                    "deleted_profiles": deleted,
                    "artifacts": artifacts,
                },
            )
        except Exception as error:
            if self._stopping.is_set():
                return
            current = self.journal.get_operation(operation_id)
            if current is not None and not current.state.terminal:
                if current.state is OperationState.QUEUED:
                    self.journal.claim_operation(operation_id)
                self.journal.finish(
                    operation_id,
                    state=OperationState.FAILED,
                    code="delete_failed",
                    message=str(
                        getattr(error, "message", None) or "Recipe deletion failed."
                    ),
                    error_code=str(getattr(error, "code", "delete_failed")),
                    remediation=str(
                        getattr(error, "remediation", "")
                        or "Inspect the operation events, then delete again."
                    ),
                )
            if record is not None:
                self._settle_deleting_draft(record, "Deletion did not finish.")
        finally:
            if locked:
                with contextlib.suppress(Exception):
                    self.journal.release_locks(operation_id)
            self._forget(operation_id)

    def _delete_profiles(
        self,
        operation_id: str,
        ops: RecipeNodeOps,
        nodes: Mapping[str, NodeConfig],
        configured: list[str],
        targets: Mapping[str, _GeneratedProfile],
    ) -> tuple[list[str], list[str]]:
        """Remove each profile's node directories, then its profile file.

        A profile file is removed only after every configured node's directory
        is gone, so a failed delete leaves a registered profile that can be
        deleted again rather than files nothing points to.
        """
        deleted: list[str] = []
        kept: list[str] = []
        homes: dict[str, str] = {}
        for profile_id, generated in sorted(targets.items()):
            problems: list[str] = []
            for node_id in generated.node_ids:
                if node_id not in nodes:
                    self._event(
                        operation_id,
                        "delete_node_skipped",
                        f"{node_id} is no longer configured; files of {profile_id} "
                        "there are left untouched.",
                    )
                    continue
                self._lock(operation_id, configured)
                if node_id not in homes:
                    try:
                        homes[node_id] = ops.remote_home(node_id)
                    except Exception:
                        problems.append(f"{node_id} (unreachable)")
                        continue
                expected = working_directory_for(homes[node_id], profile_id)
                if generated.profile.runner.working_directory != expected:
                    problems.append(f"{node_id} (working-directory-mismatch)")
                    continue
                result = ops.remove_recipe_directory(
                    node_id,
                    homes[node_id],
                    profile_id,
                    prepare_id=generated.prepare_id,
                )
                if not result.ok:
                    problems.append(f"{node_id} ({result.reason or result.state})")
            if problems:
                kept.append(profile_id)
                self._event(
                    operation_id,
                    "profile_kept",
                    f"Kept {profile_id}: its recipe directory could not be removed "
                    f"on {', '.join(problems)}.",
                )
                continue
            with contextlib.suppress(FileNotFoundError):
                generated.path.unlink()
            deleted.append(profile_id)
            self._event(
                operation_id,
                "profile_deleted",
                f"Deleted generated profile {profile_id} and its recipe directories.",
            )
        if deleted:
            # A reload problem is reported by the host; the files are gone.
            with contextlib.suppress(Exception):
                self._on_profiles_removed(deleted)
        return deleted, kept

    def _delete_artifacts(
        self,
        operation_id: str,
        ops: RecipeNodeOps,
        nodes: Mapping[str, NodeConfig],
        configured: list[str],
        draft: RecipeDraft,
        targets: Mapping[str, _GeneratedProfile],
        deleted: list[str],
    ) -> list[dict[str, str]]:
        """Remove weights, images and the cache volume nothing else needs.

        Candidates come from the deleted profiles and the draft itself (a
        failed prepare may have downloaded weights). Weights are removed only
        below a model root configured for that node, and nothing is removed
        while another profile or a prepared or preparing draft on that node
        still references it.
        """
        models: dict[str, set[tuple[str, str]]] = {}
        images: dict[str, set[str]] = {}
        outside: set[str] = set()

        def add(node_id: str, model_root: str, model_dir: str, image: str) -> None:
            node = nodes.get(node_id)
            if node is None:
                return
            root = model_root.rstrip("/")
            try:
                require_removal_target(model_root=root, model_dir=model_dir)
            except ValueError:
                outside.add(node_id)
                return
            if root not in {item.rstrip("/") for item in node.model_roots}:
                outside.add(node_id)
                return
            models.setdefault(node_id, set()).add((root, model_dir))
            if IMAGE_REFERENCE_RE.fullmatch(image):
                images.setdefault(node_id, set()).add(image)

        for profile_id in deleted:
            profile = targets[profile_id].profile
            try:
                model_dir = model_dir_name(
                    profile.provenance.get("hf_repo", ""),
                    profile.provenance.get("hf_revision", ""),
                )
            except ValueError:
                model_dir = ""
            suffix = f"/{model_dir}/config.json"
            model_root = profile.artifact.primary_path.removesuffix(suffix)
            image = profile.runner.image_digest or ""
            for node_id in targets[profile_id].node_ids:
                if not model_dir or model_root == profile.artifact.primary_path:
                    outside.add(node_id)
                else:
                    add(node_id, model_root, model_dir, image)
        for node_id in draft.placement.node_ids:
            node = nodes.get(node_id)
            if node is not None and node.model_roots:
                add(
                    node_id,
                    node.model_roots[0],
                    model_dir_for(draft),
                    draft.runtime.reference,
                )

        references = self._artifact_references(draft.draft_id, set(deleted))
        remaining = self._generated_profiles()
        report: list[dict[str, str]] = []

        def note(node_id: str, kind: str, result: RemovalResult) -> None:
            report.append({"node_id": node_id, "kind": kind, "state": result.state})
            label = {"weights": "weights", "image": "runtime image"}.get(
                kind, "cache volume"
            )
            detail = {
                "removed": "removed",
                "absent": "already absent",
                "in-use": "kept: another recipe or profile on this node uses it",
            }.get(result.state, f"kept ({result.reason or result.state})")
            self._event(
                operation_id,
                "artifact_" + result.state.replace("-", "_"),
                f"{node_id}: {label} {detail}.",
            )

        in_use = RemovalResult("in-use")
        for node_id in configured:
            self._lock(operation_id, configured)
            if node_id in outside:
                note(node_id, "weights", RemovalResult("refused", "outside-model-root"))
            for model_root, model_dir in sorted(models.get(node_id, ())):
                model_path = f"{model_root}/{model_dir}"
                if any(ref.needs_model(node_id, model_path) for ref in references):
                    note(node_id, "weights", in_use)
                else:
                    note(
                        node_id,
                        "weights",
                        self._safe_removal(
                            ops.remove_model_directory, node_id, model_root, model_dir
                        ),
                    )
            for image in sorted(images.get(node_id, ())):
                if any(ref.needs_image(node_id, image) for ref in references):
                    note(node_id, "image", in_use)
                else:
                    note(
                        node_id,
                        "image",
                        self._safe_removal(ops.remove_image, node_id, image),
                    )
            lost_profile = any(
                node_id in targets[profile_id].node_ids for profile_id in deleted
            )
            still_used = any(node_id in item.node_ids for item in remaining.values())
            if lost_profile and not still_used:
                note(
                    node_id,
                    "cache-volume",
                    self._safe_removal(ops.remove_cache_volume, node_id),
                )
        return report

    def _artifact_references(
        self, draft_id: str, deleted: set[str]
    ) -> list[_ArtifactReference]:
        """Everything that still needs weights or images after this delete."""
        references: list[_ArtifactReference] = []
        profiles: list[ModelProfile] = [
            item.profile for item in self._generated_profiles().values()
        ]
        profiles.extend(self._profiles().values())
        for profile in profiles:
            if profile.profile_id in deleted:
                continue
            allowed = profile.node_selector.allowed_node_ids
            references.append(
                _ArtifactReference(
                    node_ids=frozenset(allowed) if allowed else None,
                    paths=frozenset(
                        (
                            profile.artifact.primary_path,
                            *profile.artifact.required_files,
                        )
                    ),
                    images=frozenset(
                        {profile.runner.image_digest}
                        if profile.runner.image_digest
                        else ()
                    ),
                )
            )
        nodes = self._nodes()
        for other in self.store.list():
            if other.draft_id == draft_id or other.status not in {
                "prepared",
                "preparing",
            }:
                continue
            paths = {
                model_path_for(nodes[node_id].model_roots[0], other)
                for node_id in other.placement.node_ids
                if node_id in nodes and nodes[node_id].model_roots
            }
            references.append(
                _ArtifactReference(
                    node_ids=frozenset(other.placement.node_ids),
                    paths=frozenset(paths),
                    images=frozenset({other.runtime.reference}),
                )
            )
        return references

    @staticmethod
    def _safe_removal(
        remove: Callable[..., RemovalResult], *args: str
    ) -> RemovalResult:
        try:
            return remove(*args)
        except ValueError:
            return RemovalResult("refused", "invalid-target")
        except Exception:
            return RemovalResult("failed", "unreachable")

    def _require_not_in_use(self, profiles: Mapping[str, Any]) -> None:
        reason = self._in_use(set(profiles))
        if reason:
            raise RecipeError(
                "profile_in_use",
                reason,
                "Unload the deployment and let its operations finish, then delete "
                "the recipe.",
            )

    def _in_use(self, profile_ids: set[str]) -> str | None:
        """Why one of ``profile_ids`` cannot be deleted now, if it cannot."""
        if not profile_ids:
            return None
        for deployment in self.journal.list_deployments(include_stopped=False):
            if deployment.profile_id in profile_ids:
                return (
                    f"Deployment {deployment.id} of profile {deployment.profile_id} "
                    f"is {deployment.observed_state}, not stopped."
                )
        for operation in self.journal.list_operations(
            states=[OperationState.QUEUED, OperationState.RUNNING]
        ):
            if operation.action not in LIFECYCLE_ACTIONS:
                continue
            profile_id = self._lifecycle_profile_id(operation)
            if profile_id in profile_ids:
                return (
                    f"A {operation.action.value} operation for profile {profile_id} "
                    f"is {operation.state.value}."
                )
        return None

    def _lifecycle_profile_id(self, operation: OperationRecord) -> str | None:
        deployment = operation.request.get("deployment")
        if isinstance(deployment, Mapping):
            value = deployment.get("profile_id")
            return value if isinstance(value, str) else None
        deployment_id = operation.request.get("deployment_id")
        if isinstance(deployment_id, str):
            record = self.journal.get_deployment(deployment_id)
            return record.profile_id if record is not None else None
        return None

    def _lock(self, operation_id: str, node_ids: Iterable[str]) -> None:
        """Take or renew this delete's node leases (never the cluster lease)."""
        wanted = list(node_ids)
        if not wanted:
            return
        try:
            self.journal.acquire_locks(
                operation_id,
                wanted,
                lease_seconds=DELETE_LEASE_SECONDS,
                cluster=False,
            )
        except LockConflict as error:
            raise RecipeError(
                "profile_in_use",
                f"{error.resource} is locked by lifecycle operation "
                f"{error.owner_operation_id}.",
                "Wait for that operation to finish, then delete the recipe again.",
            ) from error

    def _settle_deleting_draft(self, record: OperationRecord, message: str) -> None:
        """Return a draft from ``deleting`` to the state its files now support."""
        draft_id = str(record.request.get("draft_id", ""))
        previous = str(record.request.get("previous_status", "draft"))
        with contextlib.suppress(RecipeError):
            draft = self.store.get(draft_id)
            if draft.status != "deleting" or draft.last_operation_id != record.id:
                return
            remaining = self._profiles_for_draft(draft)
            profile_id = draft.profile_id if draft.profile_id in remaining else None
            status = (
                previous
                if previous in DRAFT_STATES and previous not in BUSY_STATES
                else "draft"
            )
            if status == "prepared" and profile_id is None:
                status = "draft"
            self.store.save(
                draft.with_status(status, message=message, profile_id=profile_id)
            )

    # ----------------------------------------------------------------- internal

    def _settle_preparing_draft(
        self, draft_id: str, operation_id: str, status: str, message: str
    ) -> None:
        with contextlib.suppress(RecipeError):
            draft = self.store.get(draft_id)
            if draft.status == "preparing" and draft.last_operation_id == operation_id:
                self.store.save(draft.with_status(status, message=message))

    def _generated_profiles(self) -> dict[str, _GeneratedProfile]:
        """Valid generated profiles in the data directory, by profile id."""
        profiles: dict[str, _GeneratedProfile] = {}
        directory = self.generated_profiles_dir
        if not directory.is_dir():
            return profiles
        for path in sorted(directory.glob("*.json")):
            if not is_generated_profile_id(path.stem):
                continue
            try:
                profile = load_profile(path)
            except (ConfigError, ValueError, OSError):
                continue
            if profile.profile_id == path.stem and profile.origin.value == "generated":
                profiles[profile.profile_id] = _GeneratedProfile(path, profile)
        return profiles

    def _drafts_with_node_files_left(self) -> set[str]:
        """Drafts an earlier prepare may have left a recipe directory for.

        The journal records it: that prepare's own cleanup was incomplete, or
        it ended ``needs-attention`` and left node files alone.
        """
        drafts: set[str] = set()
        for operation in self.journal.list_operations(
            states=[
                OperationState.FAILED,
                OperationState.CANCELLED,
                OperationState.NEEDS_ATTENTION,
            ]
        ):
            draft_id = operation.request.get("draft_id")
            if (
                operation.action is not OperationAction.PREPARE
                or not isinstance(draft_id, str)
                or draft_id in drafts
            ):
                continue
            started = self._prepare_started(operation.id)
            if started is None or self._profile_path(started[0]).exists():
                continue
            if operation.state is OperationState.NEEDS_ATTENTION or any(
                event.code == "prepare_cleanup_incomplete"
                for event in self.journal.events(operation.id)
            ):
                drafts.add(draft_id)
        return drafts

    def _unregistered_prepares(
        self, draft: RecipeDraft, targets: Mapping[str, _GeneratedProfile]
    ) -> list[tuple[str, str, tuple[str, ...]]]:
        """Finished prepares of a draft whose profile was never registered.

        A cancelled or failed prepare removes its own recipe directory, but a
        node can be unreachable or still busy at that moment. Delete retries
        those directories so that removing a recipe leaves nothing behind.

        Returns:
            ``(profile_id, prepare_id, node_ids)`` per such prepare.

        """
        found: list[tuple[str, str, tuple[str, ...]]] = []
        for operation in self.journal.list_operations():
            if (
                operation.action is not OperationAction.PREPARE
                or not operation.state.terminal
                or operation.request.get("draft_id") != draft.draft_id
            ):
                continue
            started = self._prepare_started(operation.id)
            if started is None:
                continue
            profile_id, prepare_id = started
            if profile_id in targets or self._profile_path(profile_id).exists():
                continue
            found.append((profile_id, prepare_id, tuple(operation.node_ids)))
        return found

    def _delete_unregistered_directories(
        self,
        operation_id: str,
        ops: RecipeNodeOps,
        nodes: Mapping[str, NodeConfig],
        configured: list[str],
        leftovers: list[tuple[str, str, tuple[str, ...]]],
    ) -> list[str]:
        """Remove directories earlier prepares left; return the nodes that refused."""
        problems: list[str] = []
        homes: dict[str, str] = {}
        for profile_id, prepare_id, node_ids in leftovers:
            removed: list[str] = []
            for node_id in node_ids:
                if node_id not in nodes:
                    self._event(
                        operation_id,
                        "delete_node_skipped",
                        f"{node_id} is no longer configured; files of {profile_id} "
                        "there are left untouched.",
                    )
                    continue
                self._lock(operation_id, configured)
                try:
                    if node_id not in homes:
                        homes[node_id] = ops.remote_home(node_id)
                    result = ops.remove_recipe_directory(
                        node_id, homes[node_id], profile_id, prepare_id=prepare_id
                    )
                except Exception:
                    result = RemovalResult("failed", "unreachable")
                if result.state == "removed":
                    removed.append(node_id)
                elif not result.ok:
                    problems.append(f"{node_id} ({result.reason or result.state})")
            if removed:
                self._event(
                    operation_id,
                    "prepare_directory_removed",
                    f"Removed the recipe directory an earlier prepare left on "
                    f"{', '.join(removed)}.",
                )
        return problems

    def _profiles_for_draft(self, draft: RecipeDraft) -> dict[str, _GeneratedProfile]:
        return {
            profile_id: item
            for profile_id, item in self._generated_profiles().items()
            if item.draft_id == draft.draft_id or profile_id == draft.profile_id
        }

    def _profile_path(self, profile_id: str) -> Path:
        return self.generated_profiles_dir / f"{profile_id}.json"

    def _create_operation(self, **kwargs: Any) -> tuple[OperationRecord, bool]:
        try:
            return self.journal.create_or_get_operation(**kwargs)
        except ValueError as error:
            raise RecipeError(
                "idempotency_conflict",
                "This idempotency key was already used for a different operation.",
                "Retry with a new idempotency key.",
            ) from error

    def _require_open(self) -> None:
        if self._closed:
            raise RecipeError(
                "controller_shutting_down", "The controller is shutting down."
            )

    def _require_node_ops(self) -> RecipeNodeOps:
        ops = self._node_ops()
        if ops is None:
            raise RecipeError("setup_required", "Node configuration is unavailable.")
        return ops

    def _cancel_event(self, operation_id: str) -> threading.Event:
        with self._mutex:
            return self._cancel.get(operation_id) or threading.Event()

    def _forget(self, operation_id: str) -> None:
        with self._mutex:
            self._cancel.pop(operation_id, None)
            self._live.discard(operation_id)

    def _event(self, operation_id: str, code: str, message: str) -> None:
        with contextlib.suppress(Exception):
            self.journal.advance(operation_id, code=code, message=message)

    @staticmethod
    def _delete_options(body: Mapping[str, Any]) -> bool:
        if not isinstance(body, Mapping) or set(body) - DELETE_KEYS:
            raise RecipeError(
                "invalid_request", "Send only the optional 'remove_artifacts' flag."
            )
        value = body.get("remove_artifacts", False)
        if not isinstance(value, bool):
            raise RecipeError(
                "invalid_request", "remove_artifacts must be true or false."
            )
        return value

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
        resolve_placement(draft, self._nodes())

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

    def _draft_resource(
        self,
        draft: RecipeDraft,
        generated: Mapping[str, _GeneratedProfile],
        leftovers: set[str] | None = None,
    ) -> dict[str, Any]:
        item = draft.to_dict()
        if leftovers is None:
            leftovers = self._drafts_with_node_files_left()
        item["node_files_left"] = draft.draft_id in leftovers
        item["node_count"] = draft.node_count
        item["model_dir"] = model_dir_for(draft)
        item["qualified"] = False
        item["generated_profile_ids"] = sorted(
            profile_id
            for profile_id, profile in generated.items()
            if profile.draft_id == draft.draft_id or profile_id == draft.profile_id
        )
        if draft.status == "preparing" and draft.last_operation_id:
            record = self.journal.get_operation(draft.last_operation_id)
            if record is not None:
                progress = latest_node_progress(
                    self.journal.events(record.id), record.node_ids
                )
                if progress:
                    item["progress_by_node"] = progress
        return item


def _join_and(items: list[str]) -> str:
    """Join names for a sentence: ``a``, ``a and b``, ``a, b and c``."""
    if len(items) <= 1:
        return "".join(items)
    return f"{', '.join(items[:-1])} and {items[-1]}"


__all__ = ["RecipeService", "RecipeError", "CatalogError"]
