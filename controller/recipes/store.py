# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Local persistence for recipe drafts (one JSON file per draft)."""

from __future__ import annotations

import json
import secrets
import threading
from pathlib import Path

from controller.operations import utc_now
from controller.paths import write_private_file
from controller.recipes.spec import DRAFT_ID_RE, RecipeDraft, RecipeError


class RecipeStore:
    """Atomic, private JSON files under the controller's data directory."""

    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)
        self._mutex = threading.RLock()

    @staticmethod
    def new_draft_id() -> str:
        return "r-" + secrets.token_hex(6)

    def _path(self, draft_id: str) -> Path:
        if not DRAFT_ID_RE.fullmatch(draft_id):
            raise RecipeError(
                "invalid_recipe_id",
                "The recipe id is invalid.",
                "Choose a recipe from the list.",
            )
        return self.directory / f"{draft_id}.json"

    def get(self, draft_id: str) -> RecipeDraft:
        path = self._path(draft_id)
        with self._mutex:
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                raise _not_found() from None
            except (OSError, json.JSONDecodeError) as error:
                raise RecipeError(
                    "invalid_recipe",
                    f"The stored recipe cannot be read: {type(error).__name__}.",
                ) from error
        return RecipeDraft.from_dict(raw)

    def list(self) -> list[RecipeDraft]:
        drafts: list[RecipeDraft] = []
        with self._mutex:
            if not self.directory.is_dir():
                return []
            for path in sorted(self.directory.glob("r-*.json")):
                try:
                    drafts.append(self.get(path.stem))
                except RecipeError:
                    continue
        return sorted(drafts, key=lambda draft: draft.updated_at, reverse=True)

    def save(self, draft: RecipeDraft) -> RecipeDraft:
        now = utc_now()
        stored = draft.with_status(
            draft.status,
            message=draft.message,
            created_at=draft.created_at or now,
            updated_at=now,
        )
        path = self._path(stored.draft_id)
        payload = json.dumps(stored.to_dict(), indent=2, sort_keys=True) + "\n"
        with self._mutex:
            write_private_file(path, payload, prefix=".recipe-")
        return stored

    def delete(self, draft_id: str) -> None:
        path = self._path(draft_id)
        with self._mutex:
            try:
                path.unlink()
            except FileNotFoundError:
                raise _not_found() from None


def _not_found() -> RecipeError:
    return RecipeError(
        "recipe_not_found", "The recipe does not exist.", "Refresh the recipe list."
    )
