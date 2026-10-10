# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Generated profile files that successful prepares register.

Each prepare registers one ``<profile-id>.json`` in the controller's data
directory. Only a file whose name has the exact generated-profile shape, that
parses as a profile with that id, and whose origin is ``generated`` counts;
anything else in the directory is ignored.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from controller.config import ConfigError, load_profile
from controller.domain import ModelProfile
from controller.recipes.nodeops import PREPARE_ID_RE
from controller.recipes.render import is_generated_profile_id
from controller.recipes.spec import RecipeDraft
from controller.recipes.store import write_private_file


@dataclass(frozen=True)
class GeneratedProfile:
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

    def belongs_to(self, draft: RecipeDraft) -> bool:
        """Whether a prepare of ``draft`` registered this profile."""
        return self.draft_id == draft.draft_id or self.profile_id == draft.profile_id


class GeneratedProfileStore:
    """The generated profile files in one data directory."""

    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)

    def path(self, profile_id: str) -> Path:
        return self.directory / f"{profile_id}.json"

    def exists(self, profile_id: str) -> bool:
        """Whether a profile file with this id is registered."""
        return self.path(profile_id).exists()

    def file_ids(self) -> set[str]:
        """The id of every profile file, whether or not it is valid."""
        return {path.stem for path in self.directory.glob("*.json")}

    def load(self) -> dict[str, GeneratedProfile]:
        """Valid generated profiles, by profile id."""
        profiles: dict[str, GeneratedProfile] = {}
        if not self.directory.is_dir():
            return profiles
        for path in sorted(self.directory.glob("*.json")):
            if not is_generated_profile_id(path.stem):
                continue
            try:
                profile = load_profile(path)
            except (ConfigError, ValueError, OSError):
                continue
            if profile.profile_id == path.stem and profile.origin.value == "generated":
                profiles[profile.profile_id] = GeneratedProfile(path, profile)
        return profiles

    def for_draft(self, draft: RecipeDraft) -> dict[str, GeneratedProfile]:
        """Valid generated profiles that prepares of ``draft`` registered."""
        return {
            profile_id: item
            for profile_id, item in self.load().items()
            if item.belongs_to(draft)
        }

    def write(self, profile_id: str, document: Mapping[str, Any]) -> Path:
        """Register a profile document; the file is private to this user."""
        target = self.path(profile_id)
        write_private_file(
            target, json.dumps(document, indent=2) + "\n", prefix=".profile-"
        )
        return target
