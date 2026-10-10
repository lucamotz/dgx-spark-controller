# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Journal records of recipe operations: outcomes, events and prepare history.

Prepare and delete runs end their operations through these helpers, so every
terminal record has the same shape. The history readers find what an earlier
prepare may have left on its nodes from the ``prepare_started`` event that
every prepare journals before its first node mutation.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from typing import NamedTuple

from controller.operations import OperationAction, OperationState
from controller.persistence import OperationJournal
from controller.recipes.generated import GeneratedProfile, GeneratedProfileStore
from controller.recipes.nodeops import PREPARE_ID_RE
from controller.recipes.render import is_generated_profile_id


class UnregisteredPrepare(NamedTuple):
    """A finished prepare of a draft whose profile was never registered."""

    profile_id: str
    prepare_id: str
    node_ids: tuple[str, ...]


def note(journal: OperationJournal, operation_id: str, code: str, message: str) -> None:
    """Journal an informational event; a journal failure never stops the run."""
    with contextlib.suppress(Exception):
        journal.advance(operation_id, code=code, message=message)


def finish_cancelled(
    journal: OperationJournal,
    operation_id: str,
    draft_id: str | None,
    *,
    code: str,
    message: str,
) -> None:
    journal.finish(
        operation_id,
        state=OperationState.CANCELLED,
        code=code,
        message=message,
        result={"cancelled": True, "draft_id": draft_id},
    )


def finish_unless_terminal(
    journal: OperationJournal,
    operation_id: str,
    *,
    state: OperationState,
    code: str,
    message: str,
    error_code: str,
    remediation: str,
) -> None:
    """End an operation that has not ended yet, claiming it first if queued."""
    current = journal.get_operation(operation_id)
    if current is None or current.state.terminal:
        return
    if current.state is OperationState.QUEUED:
        journal.claim_operation(operation_id)
    journal.finish(
        operation_id,
        state=state,
        code=code,
        message=message,
        error_code=error_code,
        remediation=remediation,
    )


def error_details(
    error: Exception, *, code: str, message: str, remediation: str
) -> tuple[str, str, str]:
    """Return the error code, message and remediation a failure reports.

    An error with ``code``, ``message`` or ``remediation`` attributes (a
    recipe or catalog error) reports its own; anything it lacks falls back to
    the given generic text.
    """
    return (
        str(getattr(error, "code", code)),
        str(getattr(error, "message", None) or message),
        str(getattr(error, "remediation", "") or remediation),
    )


def prepare_started(
    journal: OperationJournal, operation_id: str
) -> tuple[str, str] | None:
    """Profile and prepare ids journaled before the first node mutation."""
    for event in journal.events(operation_id):
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


def drafts_with_node_files_left(
    journal: OperationJournal, generated: GeneratedProfileStore
) -> set[str]:
    """Drafts an earlier prepare may have left a recipe directory for.

    The journal records it: that prepare's own cleanup was incomplete, or
    it ended ``needs-attention`` and left node files alone.
    """
    drafts: set[str] = set()
    for operation in journal.list_operations(
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
        started = prepare_started(journal, operation.id)
        if started is None or generated.exists(started[0]):
            continue
        if operation.state is OperationState.NEEDS_ATTENTION or any(
            event.code == "prepare_cleanup_incomplete"
            for event in journal.events(operation.id)
        ):
            drafts.add(draft_id)
    return drafts


def unregistered_prepares(
    journal: OperationJournal,
    generated: GeneratedProfileStore,
    draft_id: str,
    targets: Mapping[str, GeneratedProfile],
) -> list[UnregisteredPrepare]:
    """Finished prepares of a draft whose profile was never registered.

    A cancelled or failed prepare removes its own recipe directory, but a
    node can be unreachable or still busy at that moment. Delete retries
    those directories so that removing a recipe leaves nothing behind.
    """
    found: list[UnregisteredPrepare] = []
    for operation in journal.list_operations():
        if (
            operation.action is not OperationAction.PREPARE
            or not operation.state.terminal
            or operation.request.get("draft_id") != draft_id
        ):
            continue
        started = prepare_started(journal, operation.id)
        if started is None:
            continue
        profile_id, prepare_id = started
        if profile_id in targets or generated.exists(profile_id):
            continue
        found.append(
            UnregisteredPrepare(profile_id, prepare_id, tuple(operation.node_ids))
        )
    return found
