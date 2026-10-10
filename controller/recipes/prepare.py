# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Journaled prepare runs: install, start, poll, register, and clean up.

A prepare installs the pinned ``prepare.sh`` and its environment into a new
per-profile directory on every selected node, starts it in a named
``systemd --user`` unit, and polls the status file each unit writes. Once
every node succeeded, it renders the recipe from the facts the nodes reported,
installs the start and stop scripts, and registers the generated profile.

A failed or cancelled prepare stops its units and removes only its own node
directory; downloaded weights and the pulled image stay for the next prepare.
A controller shutdown leaves the units running and the operation ``running``,
and :meth:`PrepareRunner.resume` reattaches to them after the restart.
"""

from __future__ import annotations

import contextlib
import re
import secrets
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from controller.domain import NodeConfig
from controller.operations import (
    OperationPhase,
    OperationState,
    bounded_node_progress,
)
from controller.persistence import OperationJournal
from controller.recipes.generated import GeneratedProfileStore
from controller.recipes.journaling import (
    error_details,
    finish_cancelled,
    finish_unless_terminal,
    note,
    prepare_started,
)
from controller.recipes.nodeops import PrepareStatus, RecipeNodeOps, RemovalResult
from controller.recipes.render import (
    NodeFacts,
    environment_text,
    expected_files_text,
    model_path_for,
    prepare_environment,
    profile_id_for,
    render_recipe,
    resolve_placement,
    single_home,
    template_bytes,
    working_directory_for,
)
from controller.recipes.spec import RecipeDraft, RecipeError
from controller.recipes.store import RecipeStore

IMAGE_ID_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
VERSION_RE = re.compile(r"[A-Za-z0-9._+-]{1,64}\Z")
MANIFEST_PATHS = frozenset({"config.json", "model.safetensors.index.json"})
# Consecutive polls without a reported state after which a unit counts as lost.
MAX_MISSING_POLLS = 6
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

    def files(self) -> dict[str, tuple[bytes, int]]:
        """``prepare.sh`` plus this run's environment and expected weight files."""
        environment = prepare_environment(
            self.draft,
            prepare_id=self.prepare_id,
            model_root=self.model_root,
            status_file=f"{self.directory}/prepare-{self.prepare_id}.status",
            expected_files=f"{self.directory}/expected-files-{self.prepare_id}.txt",
        )
        return {
            "prepare.sh": (template_bytes("prepare.sh"), 0o755),
            f"prepare-{self.prepare_id}.env": (
                environment_text(environment).encode("utf-8"),
                0o600,
            ),
            f"expected-files-{self.prepare_id}.txt": (
                expected_files_text(self.draft).encode("utf-8"),
                0o600,
            ),
        }


class PrepareRunner:
    """Runs and reattaches prepare operations on the recipe worker thread."""

    def __init__(
        self,
        *,
        journal: OperationJournal,
        store: RecipeStore,
        generated: GeneratedProfileStore,
        nodes: Callable[[], Mapping[str, NodeConfig]],
        node_ops: Callable[[], RecipeNodeOps],
        stopping: threading.Event,
        on_profile_generated: Callable[[str], None],
        poll_interval_seconds: float,
        prepare_timeout_seconds: float,
        progress_interval_seconds: float,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.journal = journal
        self.store = store
        self.generated = generated
        self._nodes = nodes
        self._node_ops = node_ops
        self._stopping = stopping
        self._on_profile_generated = on_profile_generated
        self.poll_interval_seconds = poll_interval_seconds
        self.prepare_timeout_seconds = prepare_timeout_seconds
        self.progress_interval_seconds = progress_interval_seconds
        self._monotonic = monotonic
        # Polling sleeps wake up early on shutdown unless a test injects a clock.
        self._sleep = sleep or (lambda seconds: self._stopping.wait(seconds) and None)

    def run(self, operation_id: str, draft_id: str, cancel: threading.Event) -> None:
        """Run a queued prepare to a terminal record, unless shutting down."""
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
            ops = self._node_ops()
            run = self._plan(draft, ops)
            self._require_lingering(ops, run)
            files = run.files()
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
                _require_ok(ops.install_files(node.node_id, run.directory, files))
            for node in run.selected:
                run.started.append(node.node_id)
                _require_ok(
                    ops.start_prepare(node.node_id, run.directory, run.prepare_id)
                )
            self._complete(operation_id, draft_id, ops, run, cancel)
        except _Detached:
            return
        except Exception as error:  # every failure becomes a durable terminal record
            self._fail(operation_id, draft_id, ops, run, error)

    def resume(self, operation_id: str, draft_id: str, cancel: threading.Event) -> None:
        """Reattach to a prepare that a controller restart interrupted."""
        run: _PrepareRun | None = None
        ops: RecipeNodeOps | None = None
        try:
            if self._stopping.is_set():
                return
            record = self.journal.get_operation(operation_id)
            if record is None or record.state.terminal:
                return
            started = prepare_started(self.journal, operation_id)
            if started is None:
                # The restart happened before anything was installed on a node.
                message = (
                    "The controller restarted before preparation reached the "
                    "nodes. Prepare again."
                )
                self.settle_draft(draft_id, operation_id, "draft", message)
                finish_cancelled(
                    self.journal,
                    operation_id,
                    draft_id,
                    code="prepare_interrupted",
                    message=message,
                )
                return
            profile_id, prepare_id = started
            if self.generated.exists(profile_id):
                # The profile was registered before the restart; only the
                # journal and draft updates are missing.
                self._finish_registered(operation_id, draft_id, profile_id)
                return
            try:
                ops, run, statuses = self._observe(draft_id, profile_id, prepare_id)
            except Exception as error:
                self._needs_attention(
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
                self._needs_attention(
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
                self._commit(operation_id, draft_id, ops, run, statuses)
                return
            self._complete(operation_id, draft_id, ops, run, cancel)
        except _Detached:
            return
        except Exception as error:
            self._fail(operation_id, draft_id, ops, run, error)

    def settle_draft(
        self, draft_id: str, operation_id: str, status: str, message: str
    ) -> None:
        """Move a draft out of ``preparing`` if this operation still owns it."""
        with contextlib.suppress(RecipeError):
            draft = self.store.get(draft_id)
            if draft.status == "preparing" and draft.last_operation_id == operation_id:
                self.store.save(draft.with_status(status, message=message))

    def _plan(
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

    def _observe(
        self, draft_id: str, profile_id: str, prepare_id: str
    ) -> tuple[RecipeNodeOps, _PrepareRun, dict[str, PrepareStatus]]:
        """Plan an interrupted prepare again and read every node's unit state."""
        draft = self.store.get(draft_id)
        ops = self._node_ops()
        run = self._plan(draft, ops, prepare_id=prepare_id)
        if run.profile_id != profile_id:
            raise RecipeError(
                "prepare_mismatch", "The recipe changed during the restart."
            )
        return ops, run, self._poll(ops, run)

    def _new_prepare_id(self, draft: RecipeDraft) -> str:
        """A prepare id whose profile id names no existing profile or directory.

        Failure cleanup removes the node directory of its own profile id, so
        that id must never belong to a profile that is already registered.
        """
        taken = self.generated.file_ids()
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

    def _complete(
        self,
        operation_id: str,
        draft_id: str,
        ops: RecipeNodeOps,
        run: _PrepareRun,
        cancel: threading.Event,
    ) -> None:
        statuses = self._wait(operation_id, ops, run, cancel)
        if statuses is None:
            self._stop_units(ops, run.started, run.prepare_id)
            self._cleanup(operation_id, ops, run)
            self._finish_cancelled(operation_id, draft_id)
            return
        self._commit(operation_id, draft_id, ops, run, statuses)

    def _commit(
        self,
        operation_id: str,
        draft_id: str,
        ops: RecipeNodeOps,
        run: _PrepareRun,
        statuses: Mapping[str, PrepareStatus],
    ) -> None:
        facts = {
            node_id: _node_facts(node_id, status, run.homes[node_id])
            for node_id, status in statuses.items()
        }
        rendered = render_recipe(run.draft, run.nodes, facts, prepare_id=run.prepare_id)
        if rendered.working_directory != run.directory:
            raise RecipeError(
                "prepare_mismatch",
                "The prepared recipe directory changed unexpectedly.",
            )
        for node in run.selected:
            _require_ok(
                ops.install_files(node.node_id, run.directory, rendered.files())
            )
        self.journal.advance(
            operation_id,
            phase=OperationPhase.COMMITTING,
            code="profile_generated",
            message=f"Registering generated profile {run.profile_id}.",
            details={"profile_id": run.profile_id},
        )
        self.generated.write(run.profile_id, rendered.profile_document)
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
        self.settle_draft(draft_id, operation_id, "draft", "Preparation was cancelled.")
        finish_cancelled(
            self.journal,
            operation_id,
            draft_id,
            code="prepare_cancelled",
            message=(
                "Preparation was cancelled. Downloaded weights and the runtime "
                "image are kept for a later prepare."
            ),
        )

    def _fail(
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
                self._cleanup(operation_id, ops, run)
        error_code, message, remediation = error_details(
            error,
            code="prepare_failed",
            message="Recipe preparation failed.",
            remediation="Inspect the operation events, fix the cause, and prepare "
            "again.",
        )
        self.settle_draft(draft_id, operation_id, "failed", message)
        finish_unless_terminal(
            self.journal,
            operation_id,
            state=OperationState.FAILED,
            code="prepare_failed",
            message=message,
            error_code=error_code,
            remediation=remediation,
        )

    def _needs_attention(self, operation_id: str, draft_id: str, message: str) -> None:
        self.settle_draft(draft_id, operation_id, "failed", message)
        finish_unless_terminal(
            self.journal,
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

    def _cleanup(self, operation_id: str, ops: RecipeNodeOps, run: _PrepareRun) -> None:
        """Remove this prepare's own node directory after its units stopped.

        Weights and the image stay: downloads resume on the next prepare and
        are shared by every recipe of the same model revision and release.
        """
        if self.generated.exists(run.profile_id):
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
                kept.append(f"{node_id} ({result.detail})")
        if kept:
            note(
                self.journal,
                operation_id,
                "prepare_cleanup_incomplete",
                f"This prepare's recipe directory was kept on {', '.join(kept)}. "
                "Deleting the recipe removes it once its prepare unit has stopped.",
            )
        elif removed:
            note(
                self.journal,
                operation_id,
                "prepare_directory_removed",
                f"Removed this prepare's recipe directory on {len(removed)} "
                "node(s). Downloaded weights and the runtime image are kept for "
                "the next prepare.",
            )

    def _wait(
        self,
        operation_id: str,
        ops: RecipeNodeOps,
        run: _PrepareRun,
        cancel: threading.Event,
    ) -> dict[str, PrepareStatus] | None:
        """Poll until every node succeeded; ``None`` once cancelled.

        Raises:
            _Detached: The controller is shutting down.
            RecipeError: A node failed or went silent, or the prepare timed out.

        """
        deadline = self._monotonic() + self.prepare_timeout_seconds
        last_report: dict[str, str] = {}
        last_progress_at = self._monotonic()
        missing_polls = dict.fromkeys(run.node_ids, 0)
        while True:
            if self._stopping.is_set():
                raise _Detached
            if cancel.is_set():
                return None
            statuses = self._poll(ops, run)
            if self._stopping.is_set():
                raise _Detached
            _require_progressing(statuses, missing_polls)
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

    @staticmethod
    def _poll(ops: RecipeNodeOps, run: _PrepareRun) -> dict[str, PrepareStatus]:
        return {
            node.node_id: ops.prepare_status(
                node.node_id, run.directory, run.prepare_id, run.model_path
            )
            for node in run.selected
        }

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
    def _stop_units(ops: RecipeNodeOps, node_ids: list[str], prepare_id: str) -> None:
        for node_id in node_ids:
            with contextlib.suppress(Exception):
                ops.stop_prepare(node_id, prepare_id)


def _require_progressing(
    statuses: Mapping[str, PrepareStatus], missing_polls: dict[str, int]
) -> None:
    """Raise for a failed node or one whose unit stopped reporting."""
    for node_id, status in statuses.items():
        if status.state == "failed":
            raise RecipeError(
                "prepare_failed",
                f"{node_id}: {status.message or 'prepare failed'}",
                "Check free disk space, Docker access, and (for gated models) "
                "`hf auth login` on that node.",
            )
        missing_polls[node_id] = (
            missing_polls[node_id] + 1 if status.state in {"absent", "unknown"} else 0
        )
        if missing_polls[node_id] >= MAX_MISSING_POLLS:
            raise RecipeError(
                "prepare_unobservable",
                f"{node_id}: the prepare unit stopped reporting progress.",
                "Check SSH connectivity and the node's user systemd manager, "
                "then prepare again.",
            )


def _node_facts(node_id: str, status: PrepareStatus, home: str) -> NodeFacts:
    """Validate the facts a succeeded prepare reported for one node."""
    facts = status.facts
    image_id = facts.get("IMAGE_ID", "")
    manifest = facts.get("MANIFEST_SHA256", "")
    manifest_path = facts.get("MANIFEST_PATH", "")
    version = facts.get("VLLM_VERSION", "")
    memory = facts.get("MEM_TOTAL", "")
    if (
        not IMAGE_ID_RE.fullmatch(image_id)
        or not SHA256_RE.fullmatch(manifest)
        or manifest_path not in MANIFEST_PATHS
        or not VERSION_RE.fullmatch(version)
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


def _require_ok(result: Any) -> None:
    """Turn a failed remote prepare step into a :class:`RecipeError`."""
    if not result.ok:
        raise RecipeError(
            result.code or "prepare_failed",
            result.message or "A remote prepare step failed.",
        )


def _join_and(items: list[str]) -> str:
    """Join names for a sentence: ``a``, ``a and b``, ``a, b and c``."""
    if len(items) <= 1:
        return "".join(items)
    return f"{', '.join(items[:-1])} and {items[-1]}"
