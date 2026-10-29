# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Journaled recipe deletion: generated profiles, node files and artifacts.

A delete removes every generated profile of a recipe together with its recipe
directory on each node, then the directories that earlier unregistered
prepares left. With ``remove_artifacts`` it also removes the weights, the
runtime image and the cache volume that nothing else on a node still needs.

A delete holds node leases (never the cluster lease) while it removes files a
lifecycle operation could use, and refuses while a deployment of one of the
recipe's profiles is not stopped. Every removal step is idempotent, so a
delete that failed or was interrupted can simply run again.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from controller.domain import ModelProfile, NodeConfig
from controller.operations import (
    LockConflict,
    OperationAction,
    OperationPhase,
    OperationRecord,
    OperationState,
)
from controller.persistence import OperationJournal
from controller.recipes.generated import GeneratedProfile, GeneratedProfileStore
from controller.recipes.journaling import (
    UnregisteredPrepare,
    error_details,
    finish_cancelled,
    finish_unless_terminal,
    note,
    unregistered_prepares,
)
from controller.recipes.nodeops import (
    RecipeNodeOps,
    RemovalResult,
    require_removal_target,
)
from controller.recipes.render import (
    model_dir_for,
    model_dir_name,
    model_path_for,
    working_directory_for,
)
from controller.recipes.spec import (
    BUSY_STATES,
    DRAFT_STATES,
    IMAGE_REFERENCE_RE,
    RecipeDraft,
    RecipeError,
)
from controller.recipes.store import RecipeStore

LIFECYCLE_ACTIONS = frozenset(
    {OperationAction.LOAD, OperationAction.UNLOAD, OperationAction.RECOVER}
)
DELETE_KEYS = frozenset({"remove_artifacts"})
# Node leases held while a delete removes files a lifecycle operation could use.
DELETE_LEASE_SECONDS = 900.0
_ARTIFACT_LABELS = {"weights": "weights", "image": "runtime image"}
_ARTIFACT_OUTCOMES = {
    "removed": "removed",
    "absent": "already absent",
    "in-use": "kept: another recipe or profile on this node uses it",
}


def delete_options(body: Mapping[str, Any]) -> bool:
    """Validate a delete request body and return its ``remove_artifacts`` flag."""
    if not isinstance(body, Mapping) or set(body) - DELETE_KEYS:
        raise RecipeError(
            "invalid_request", "Send only the optional 'remove_artifacts' flag."
        )
    value = body.get("remove_artifacts", False)
    if not isinstance(value, bool):
        raise RecipeError("invalid_request", "remove_artifacts must be true or false.")
    return value


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


@dataclass
class _ArtifactCandidates:
    """Weights and images a delete may remove, per configured node."""

    models: dict[str, set[tuple[str, str]]] = field(default_factory=dict)
    images: dict[str, set[str]] = field(default_factory=dict)
    outside: set[str] = field(default_factory=set)

    def add(
        self,
        node_id: str,
        node: NodeConfig | None,
        model_root: str,
        model_dir: str,
        image: str,
    ) -> None:
        """Accept weights only below a model root configured for that node."""
        if node is None:
            return
        root = model_root.rstrip("/")
        try:
            require_removal_target(model_root=root, model_dir=model_dir)
        except ValueError:
            self.outside.add(node_id)
            return
        if root not in {item.rstrip("/") for item in node.model_roots}:
            self.outside.add(node_id)
            return
        self.models.setdefault(node_id, set()).add((root, model_dir))
        if IMAGE_REFERENCE_RE.fullmatch(image):
            self.images.setdefault(node_id, set()).add(image)


class DeleteRunner:
    """Runs delete-recipe operations on the recipe worker thread."""

    def __init__(
        self,
        *,
        journal: OperationJournal,
        store: RecipeStore,
        generated: GeneratedProfileStore,
        nodes: Callable[[], Mapping[str, NodeConfig]],
        node_ops: Callable[[], RecipeNodeOps],
        stopping: threading.Event,
        reviewed_profiles: Callable[[], Mapping[str, ModelProfile]],
        on_profiles_removed: Callable[[list[str]], None],
    ) -> None:
        self.journal = journal
        self.store = store
        self.generated = generated
        self._nodes = nodes
        self._node_ops = node_ops
        self._stopping = stopping
        self._reviewed_profiles = reviewed_profiles
        self._on_profiles_removed = on_profiles_removed

    def run(self, operation_id: str) -> None:
        """Run a queued delete to a terminal record, unless shutting down."""
        record: OperationRecord | None = None
        locked = False
        try:
            if self._stopping.is_set():
                return
            record, claimed = self.journal.claim_operation(operation_id)
            if not claimed:
                if record.state.terminal:
                    self.settle_draft(record, "Deletion was cancelled.")
                return
            draft = self.store.get(str(record.request.get("draft_id", "")))
            ops = self._node_ops()
            nodes = self._nodes()
            targets = self.generated.for_draft(draft)
            leftovers = unregistered_prepares(
                self.journal, self.generated, draft.draft_id, targets
            )
            remove_artifacts = record.request.get("remove_artifacts") is True
            node_ids = sorted(
                {node_id for item in targets.values() for node_id in item.node_ids}
                | {node_id for item in leftovers for node_id in item.node_ids}
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
            self.require_not_in_use(targets)
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
            result = {
                "draft_id": draft.draft_id,
                "deleted_profiles": deleted,
                "artifacts": artifacts,
            }
            if kept or kept_directories:
                self._finish_incomplete(
                    operation_id, record, kept, kept_directories, result
                )
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
                result=result,
            )
        except Exception as error:
            if self._stopping.is_set():
                return
            error_code, message, remediation = error_details(
                error,
                code="delete_failed",
                message="Recipe deletion failed.",
                remediation="Inspect the operation events, then delete again.",
            )
            if record is not None:
                self.settle_draft(record, "Deletion did not finish.")
            finish_unless_terminal(
                self.journal,
                operation_id,
                state=OperationState.FAILED,
                code="delete_failed",
                message=message,
                error_code=error_code,
                remediation=remediation,
            )
        finally:
            if locked:
                with contextlib.suppress(Exception):
                    self.journal.release_locks(operation_id)

    def cancel_queued(self, operation_id: str) -> bool:
        """Cancel a delete that has not started; False if there is none."""
        record = self.journal.get_operation(operation_id)
        if record is None or record.action is not OperationAction.DELETE_RECIPE:
            return False
        # Claiming is atomic, so the worker can never start a delete that was
        # cancelled here, and a delete the worker already claimed stays running.
        record, claimed = self.journal.claim_operation(operation_id)
        if not claimed:
            return False
        self.settle_draft(record, "Deletion was cancelled.")
        finish_cancelled(
            self.journal,
            operation_id,
            record.request.get("draft_id"),
            code="operation_cancelled",
            message="Deletion cancelled before anything was removed.",
        )
        return True

    def interrupted(self, record: OperationRecord) -> None:
        """End a delete that a controller restart cut off while it ran."""
        self.journal.release_locks(record.id)
        self.settle_draft(record, "Deletion was interrupted by a controller restart.")
        self.journal.finish(
            record.id,
            state=OperationState.NEEDS_ATTENTION,
            code="delete_interrupted",
            message="Deletion was interrupted by a controller restart.",
            error_code="delete_interrupted",
            remediation="Delete the recipe again. Finished steps are skipped.",
        )

    def settle_draft(self, record: OperationRecord, message: str) -> None:
        """Return a draft from ``deleting`` to the state its files now support."""
        draft_id = str(record.request.get("draft_id", ""))
        previous = str(record.request.get("previous_status", "draft"))
        with contextlib.suppress(RecipeError):
            draft = self.store.get(draft_id)
            if draft.status != "deleting" or draft.last_operation_id != record.id:
                return
            remaining = self.generated.for_draft(draft)
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

    def require_not_in_use(self, profiles: Mapping[str, Any]) -> None:
        """Refuse while a deployment or lifecycle operation uses a profile."""
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

    def _finish_incomplete(
        self,
        operation_id: str,
        record: OperationRecord,
        kept: list[str],
        kept_directories: list[str],
        result: dict[str, Any],
    ) -> None:
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
        self.settle_draft(record, "Deletion did not finish.")
        self.journal.finish(
            operation_id,
            state=OperationState.FAILED,
            code="delete_failed",
            message=" ".join(reasons),
            result=result,
            error_code="recipe_delete_incomplete",
            remediation=(
                "Fix the reason in the operation events (for example an "
                "unreachable node or a container that still exists), then "
                "delete the recipe again."
            ),
        )

    def _skip_unconfigured(
        self,
        operation_id: str,
        nodes: Mapping[str, NodeConfig],
        node_id: str,
        profile_id: str,
    ) -> bool:
        """Whether ``node_id`` left the configuration; its files stay untouched."""
        if node_id in nodes:
            return False
        note(
            self.journal,
            operation_id,
            "delete_node_skipped",
            f"{node_id} is no longer configured. Files of {profile_id} "
            "there are left untouched.",
        )
        return True

    def _delete_profiles(
        self,
        operation_id: str,
        ops: RecipeNodeOps,
        nodes: Mapping[str, NodeConfig],
        configured: list[str],
        targets: Mapping[str, GeneratedProfile],
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
                if self._skip_unconfigured(operation_id, nodes, node_id, profile_id):
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
                    problems.append(f"{node_id} ({result.detail})")
            if problems:
                kept.append(profile_id)
                note(
                    self.journal,
                    operation_id,
                    "profile_kept",
                    f"Kept {profile_id}: its recipe directory could not be removed "
                    f"on {', '.join(problems)}.",
                )
                continue
            with contextlib.suppress(FileNotFoundError):
                generated.path.unlink()
            deleted.append(profile_id)
            note(
                self.journal,
                operation_id,
                "profile_deleted",
                f"Deleted generated profile {profile_id} and its recipe directories.",
            )
        if deleted:
            # A reload problem is reported by the host; the files are gone.
            with contextlib.suppress(Exception):
                self._on_profiles_removed(deleted)
        return deleted, kept

    def _delete_unregistered_directories(
        self,
        operation_id: str,
        ops: RecipeNodeOps,
        nodes: Mapping[str, NodeConfig],
        configured: list[str],
        leftovers: list[UnregisteredPrepare],
    ) -> list[str]:
        """Remove directories earlier prepares left; return the nodes that refused."""
        problems: list[str] = []
        homes: dict[str, str] = {}
        for profile_id, prepare_id, node_ids in leftovers:
            removed: list[str] = []
            for node_id in node_ids:
                if self._skip_unconfigured(operation_id, nodes, node_id, profile_id):
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
                    problems.append(f"{node_id} ({result.detail})")
            if removed:
                note(
                    self.journal,
                    operation_id,
                    "prepare_directory_removed",
                    f"Removed the recipe directory an earlier prepare left on "
                    f"{', '.join(removed)}.",
                )
        return problems

    def _delete_artifacts(
        self,
        operation_id: str,
        ops: RecipeNodeOps,
        nodes: Mapping[str, NodeConfig],
        configured: list[str],
        draft: RecipeDraft,
        targets: Mapping[str, GeneratedProfile],
        deleted: list[str],
    ) -> list[dict[str, str]]:
        """Remove weights, images and the cache volume nothing else needs.

        Candidates come from the deleted profiles and the draft itself (a
        failed prepare may have downloaded weights). Weights are removed only
        below a model root configured for that node, and nothing is removed
        while another profile or a prepared or preparing draft on that node
        still references it.
        """
        candidates = _artifact_candidates(nodes, draft, targets, deleted)
        references = self._artifact_references(draft.draft_id, set(deleted))
        remaining = self.generated.load()
        report: list[dict[str, str]] = []

        def record(node_id: str, kind: str, result: RemovalResult) -> None:
            report.append({"node_id": node_id, "kind": kind, "state": result.state})
            label = _ARTIFACT_LABELS.get(kind, "cache volume")
            detail = _ARTIFACT_OUTCOMES.get(result.state, f"kept ({result.detail})")
            note(
                self.journal,
                operation_id,
                "artifact_" + result.state.replace("-", "_"),
                f"{node_id}: {label} {detail}.",
            )

        in_use = RemovalResult("in-use")
        for node_id in configured:
            self._lock(operation_id, configured)
            if node_id in candidates.outside:
                record(
                    node_id, "weights", RemovalResult("refused", "outside-model-root")
                )
            for model_root, model_dir in sorted(candidates.models.get(node_id, ())):
                model_path = f"{model_root}/{model_dir}"
                if any(ref.needs_model(node_id, model_path) for ref in references):
                    record(node_id, "weights", in_use)
                else:
                    record(
                        node_id,
                        "weights",
                        ops.remove_model_directory(node_id, model_root, model_dir),
                    )
            for image in sorted(candidates.images.get(node_id, ())):
                if any(ref.needs_image(node_id, image) for ref in references):
                    record(node_id, "image", in_use)
                else:
                    record(node_id, "image", ops.remove_image(node_id, image))
            lost_profile = any(
                node_id in targets[profile_id].node_ids for profile_id in deleted
            )
            still_used = any(node_id in item.node_ids for item in remaining.values())
            if lost_profile and not still_used:
                record(node_id, "cache-volume", ops.remove_cache_volume(node_id))
        return report

    def _artifact_references(
        self, draft_id: str, deleted: set[str]
    ) -> list[_ArtifactReference]:
        """Everything that still needs weights or images after this delete."""
        references: list[_ArtifactReference] = []
        profiles: list[ModelProfile] = [
            item.profile for item in self.generated.load().values()
        ]
        profiles.extend(self._reviewed_profiles().values())
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


def _artifact_candidates(
    nodes: Mapping[str, NodeConfig],
    draft: RecipeDraft,
    targets: Mapping[str, GeneratedProfile],
    deleted: list[str],
) -> _ArtifactCandidates:
    """Weights and images of the deleted profiles and of the draft itself."""
    candidates = _ArtifactCandidates()
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
                candidates.outside.add(node_id)
            else:
                candidates.add(
                    node_id, nodes.get(node_id), model_root, model_dir, image
                )
    for node_id in draft.placement.node_ids:
        node = nodes.get(node_id)
        if node is not None and node.model_roots:
            candidates.add(
                node_id,
                node,
                node.model_roots[0],
                model_dir_for(draft),
                draft.runtime.reference,
            )
    return candidates
