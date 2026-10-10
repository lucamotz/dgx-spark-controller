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

This module keeps the drafts, request validation and operation bookkeeping;
:mod:`controller.recipes.prepare` and :mod:`controller.recipes.delete` run the
two operations on the worker.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from controller.catalog import huggingface
from controller.domain import ModelProfile, NodeConfig
from controller.operations import (
    RECIPE_ACTIONS,
    OperationAction,
    OperationRecord,
    OperationState,
    latest_node_progress,
)
from controller.persistence import OperationJournal
from controller.recipes.catalog_client import CatalogClient, CatalogError
from controller.recipes.delete import DeleteRunner, delete_options
from controller.recipes.generated import GeneratedProfile, GeneratedProfileStore
from controller.recipes.journaling import (
    drafts_with_node_files_left,
    finish_cancelled,
)
from controller.recipes.nodeops import RecipeNodeOps
from controller.recipes.prepare import PrepareRunner
from controller.recipes.render import model_dir_for, resolve_placement
from controller.recipes.spec import (
    BUSY_STATES,
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
MAX_MODEL_LINK_LENGTH = 600
DEFAULT_MAX_MODEL_LEN = 32_768


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
        self.generated_profiles_dir = Path(generated_profiles_dir)
        self._nodes = nodes_provider
        self._node_ops = node_ops_provider
        self._generated = GeneratedProfileStore(self.generated_profiles_dir)
        self._mutex = threading.RLock()
        self._cancel: dict[str, threading.Event] = {}
        self._live: set[str] = set()
        self._closed = False
        self._stopping = threading.Event()
        self._prepare = PrepareRunner(
            journal=journal,
            store=store,
            generated=self._generated,
            nodes=nodes_provider,
            node_ops=self._require_node_ops,
            stopping=self._stopping,
            on_profile_generated=on_profile_generated,
            poll_interval_seconds=poll_interval_seconds,
            prepare_timeout_seconds=prepare_timeout_seconds,
            progress_interval_seconds=progress_interval_seconds,
            sleep=sleep,
            monotonic=monotonic,
        )
        self._delete = DeleteRunner(
            journal=journal,
            store=store,
            generated=self._generated,
            nodes=nodes_provider,
            node_ops=self._require_node_ops,
            stopping=self._stopping,
            reviewed_profiles=profiles_provider,
            on_profiles_removed=on_profiles_removed,
        )
        self._worker = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="dgx-controller-prepare"
        )

    # ------------------------------------------------------------------ catalog

    def inspect_model(self, link: Any) -> dict[str, Any]:
        """Resolve a Hugging Face link to pinned metadata and memory-fit estimates."""
        info = self._model_info(link)
        node_count = max(1, len(self._nodes()))
        fits = [
            huggingface.estimate_fit(info, count).to_dict()
            for count in range(1, min(node_count, 4) + 1)
        ]
        return {
            "model": info.to_dict(),
            "fit": fits,
            "suggested": {
                "served_alias": huggingface.default_served_alias(info),
                "display_name": _model_name(info),
                "max_model_len": _suggested_max_model_len(info.max_position_embeddings),
                "trust_remote_code": _needs_remote_code(info),
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
        generated = self._generated.load()
        leftovers = drafts_with_node_files_left(self.journal, self._generated)
        return [
            self._draft_resource(draft, generated, leftovers)
            for draft in self.store.list()
        ]

    def get_draft(self, draft_id: str) -> dict[str, Any]:
        return self._draft_resource(self.store.get(draft_id), self._generated.load())

    def create_draft(self, body: Mapping[str, Any]) -> dict[str, Any]:
        _require_keys(body, required={"model_link", "release_tag", "node_ids"})
        model, info = self._model_from_link(body["model_link"])
        runtime = self._runtime(
            body["release_tag"], body.get("cuda_variant", "default")
        )
        placement = self._placement(body["node_ids"], body.get("head_node_id"))
        nodes = self._nodes()
        default_port = _default_port([nodes[node_id] for node_id in placement.node_ids])
        suggested_len = _suggested_max_model_len(model.max_position_embeddings)
        base = RecipeSettings(
            max_model_len=suggested_len,
            max_num_batched_tokens=max(8_192, min(suggested_len, 16_384)),
            trust_remote_code=model.needs_remote_code,
            port=default_port,
        )
        settings = settings_from_mapping(body.get("settings") or {}, base=base)
        draft = RecipeDraft(
            draft_id=self.store.new_draft_id(),
            display_name=_display_name(body.get("display_name"), _model_name(info)),
            served_alias=_alias(
                body.get("served_alias"), huggingface.default_served_alias(info)
            ),
            model=model,
            runtime=runtime,
            placement=placement,
            settings=settings,
        )
        self._check_draft_against_nodes(draft)
        return self._draft_resource(self.store.save(draft), self._generated.load())

    def update_draft(self, draft_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        _require_keys(body, required=set())
        with self._mutex:
            draft = self.store.get(draft_id)
            _require_idle(
                draft, f"This recipe is {draft.status} and cannot change now."
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
                display_name=_display_name(
                    body.get("display_name"), draft.display_name
                ),
                served_alias=_alias(body.get("served_alias"), draft.served_alias),
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
                self.store.save(updated), self._generated.load()
            )

    def delete_draft(self, draft_id: str) -> None:
        """Delete a draft that never produced a profile; nothing runs remotely."""
        with self._mutex:
            draft = self.store.get(draft_id)
            _require_idle(
                draft, f"A recipe cannot be deleted while it is {draft.status}."
            )
            if self._generated.for_draft(draft):
                raise RecipeError(
                    "recipe_prepared",
                    "This recipe has generated profiles and recipe files on its nodes.",
                    "Use Delete on the recipe card instead: it removes the "
                    "generated profiles and their node files as an operation.",
                )
            if draft_id in drafts_with_node_files_left(self.journal, self._generated):
                raise RecipeError(
                    "recipe_prepared",
                    "An earlier prepare of this recipe left files on its nodes.",
                    "Use Delete on the recipe card instead: it removes them as "
                    "an operation.",
                )
            self.store.delete(draft_id)

    # --------------------------------------------------------------- operations

    def submit_prepare(self, draft_id: str, *, idempotency_key: str) -> OperationRecord:
        with self._mutex:
            self._require_open()
            self._require_nodes_configured("preparing")
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
                finish_cancelled(
                    self.journal,
                    operation.id,
                    draft.draft_id,
                    code="prepare_already_running",
                    message=f"This recipe is already {draft.status}.",
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
            cancel = self._cancel[operation.id] = threading.Event()
            self._start(operation.id, self._prepare.run, draft.draft_id, cancel)
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
        remove_artifacts = delete_options(body)
        with self._mutex:
            self._require_open()
            self._require_nodes_configured("deleting")
            draft = self.store.get(draft_id)
            _require_idle(
                draft, f"This recipe is {draft.status} and cannot be deleted now."
            )
            profiles = self._generated.for_draft(draft)
            self._delete.require_not_in_use(profiles)
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
            self._start(operation.id, self._delete.run)
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
        return self._delete.cancel_queued(operation_id)

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
                finish_cancelled(
                    self.journal,
                    record.id,
                    draft_id,
                    code="queued_operation_interrupted",
                    message="Controller restart cancelled work that had not started.",
                )
                if record.action is OperationAction.PREPARE:
                    self._prepare.settle_draft(
                        draft_id,
                        record.id,
                        "draft",
                        "The controller restarted before this preparation "
                        "started. Prepare again.",
                    )
                else:
                    self._delete.settle_draft(
                        record,
                        "The controller restarted before this deletion started.",
                    )
            elif record.action is OperationAction.PREPARE:
                with self._mutex:
                    cancel = self._cancel[record.id] = threading.Event()
                    self._start(record.id, self._prepare.resume, draft_id, cancel)
            else:
                self._delete.interrupted(record)
        self._settle_busy_drafts()

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

    # ----------------------------------------------------------------- internal

    def _start(self, operation_id: str, run: Callable[..., None], *args: Any) -> None:
        """Queue ``run(operation_id, *args)`` on the worker (caller holds the mutex)."""
        self._live.add(operation_id)
        self._worker.submit(self._run_tracked, operation_id, run, *args)

    def _run_tracked(
        self, operation_id: str, run: Callable[..., None], *args: Any
    ) -> None:
        try:
            run(operation_id, *args)
        finally:
            with self._mutex:
                self._cancel.pop(operation_id, None)
                self._live.discard(operation_id)

    def _settle_busy_drafts(self) -> None:
        """Release drafts whose preparing or deleting operation already ended."""
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
                self._delete.settle_draft(last, "Deletion did not finish.")
            else:
                self.store.save(
                    draft.with_status("draft", message="Deletion did not finish.")
                )

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

    def _require_nodes_configured(self, activity: str) -> None:
        if self._node_ops() is None:
            raise RecipeError(
                "setup_required", f"Configure nodes before {activity} a recipe."
            )

    def _require_node_ops(self) -> RecipeNodeOps:
        ops = self._node_ops()
        if ops is None:
            raise RecipeError("setup_required", "Node configuration is unavailable.")
        return ops

    def _model_info(self, link: Any) -> huggingface.HFModelInfo:
        if (
            not isinstance(link, str)
            or not link.strip()
            or len(link) > MAX_MODEL_LINK_LENGTH
        ):
            raise RecipeError(
                "invalid_model_link", "Paste a Hugging Face model link or 'org/name'."
            )
        return self.catalog.model_info(link)

    def _model_from_link(
        self, link: Any
    ) -> tuple[RecipeModel, huggingface.HFModelInfo]:
        info = self._model_info(link)
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
            needs_remote_code=_needs_remote_code(info),
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

    def _draft_resource(
        self,
        draft: RecipeDraft,
        generated: Mapping[str, GeneratedProfile],
        leftovers: set[str] | None = None,
    ) -> dict[str, Any]:
        item = draft.to_dict()
        if leftovers is None:
            leftovers = drafts_with_node_files_left(self.journal, self._generated)
        item["node_files_left"] = draft.draft_id in leftovers
        item["node_count"] = draft.node_count
        item["model_dir"] = model_dir_for(draft)
        item["qualified"] = False
        item["generated_profile_ids"] = sorted(
            profile_id
            for profile_id, profile in generated.items()
            if profile.belongs_to(draft)
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


def _require_keys(body: Mapping[str, Any], *, required: set[str]) -> None:
    if not isinstance(body, Mapping):
        raise RecipeError("invalid_request", "The request body must be a JSON object.")
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


def _require_idle(draft: RecipeDraft, message: str) -> None:
    """Refuse a change while a prepare or delete owns the draft."""
    if draft.status in BUSY_STATES:
        raise RecipeError(
            "recipe_busy",
            message,
            "Wait for its operation to finish, or cancel a prepare.",
        )


def _alias(value: Any, default: str) -> str:
    return _text_or_default(
        value, default, "invalid_alias", "The served model name must be text."
    )


def _display_name(value: Any, default: str) -> str:
    return _text_or_default(
        value, default, "invalid_recipe", "The recipe name must be text."
    )


def _text_or_default(value: Any, default: str, code: str, message: str) -> str:
    if value is None or value == "":
        return default
    if not isinstance(value, str):
        raise RecipeError(code, message)
    return value.strip()


def _default_port(selected: list[NodeConfig]) -> int:
    """The first of 8000, 8888 and 8080 every node allows, else the highest start."""
    for candidate in (8000, 8888, 8080):
        if all(
            node.endpoint_ports.start <= candidate <= node.endpoint_ports.end
            for node in selected
        ):
            return candidate
    start = max(node.endpoint_ports.start for node in selected)
    return max(start, 1024)


def _suggested_max_model_len(max_positions: int | None) -> int:
    return min(max_positions or DEFAULT_MAX_MODEL_LEN, DEFAULT_MAX_MODEL_LEN)


def _model_name(info: huggingface.HFModelInfo) -> str:
    return info.repo_id.split("/", 1)[1]


def _needs_remote_code(info: huggingface.HFModelInfo) -> bool:
    return bool(getattr(info, "requires_remote_code", False))


__all__ = ["RecipeService", "RecipeError", "CatalogError"]
