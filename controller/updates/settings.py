# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Update schedule and download source, chosen in the browser.

The settings live in ``<data>/update-settings.json`` (mode ``0600``, written
atomically). Unknown keys fail closed. A schedule runs on the selected
weekdays at one local wall-clock time of the controller host; a slot is
started at most once (its idempotency key names it) and only while the
controller is running within :data:`GRACE` after it.

The download source is either the Internet, reached by the controller's
update relay, or a directory on the controller host. The directory is the
one browser-supplied path in the controller: it is only ever read, by the
relay, below its resolved location, and never reaches a remote command.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as wall_time
from pathlib import Path
from typing import Any

from controller.operations import utc_now
from controller.paths import write_private_file
from controller.updates.relay import RelaySource

SCHEMA_VERSION = 1
WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
SOURCES = ("online", "directory")
GRACE = timedelta(hours=1)
_TIME_RE = re.compile(r"([01][0-9]|2[0-3]):([0-5][0-9])\Z")
_KEYS = frozenset(
    {
        "schema_version",
        "enabled",
        "weekdays",
        "time",
        "source",
        "directory",
        "updated_at",
    }
)
_REQUEST_KEYS = frozenset({"enabled", "weekdays", "time", "source", "directory"})


class SettingsError(ValueError):
    """Invalid update settings, with a stable code for the API."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class UpdateSettings:
    """When updates run and where their downloads come from."""

    enabled: bool = False
    weekdays: tuple[str, ...] = ("sun",)
    time: str = "03:00"
    source: str = "online"
    directory: str | None = None
    updated_at: str = ""

    def __post_init__(self) -> None:
        if not self.weekdays or any(day not in WEEKDAYS for day in self.weekdays):
            raise SettingsError("invalid_schedule", "Choose at least one weekday.")
        if len(set(self.weekdays)) != len(self.weekdays):
            raise SettingsError("invalid_schedule", "Each weekday may appear once.")
        if not _TIME_RE.fullmatch(self.time):
            raise SettingsError("invalid_schedule", "The time must be HH:MM.")
        if self.source not in SOURCES:
            raise SettingsError("invalid_update_source", "Unknown update source.")
        if (self.source == "directory") != bool(self.directory):
            raise SettingsError(
                "invalid_update_source", "A directory source needs a directory."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "enabled": self.enabled,
            "weekdays": list(self.weekdays),
            "time": self.time,
            "source": self.source,
            "directory": self.directory,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> UpdateSettings:
        if set(value) - _KEYS or value.get("schema_version") != SCHEMA_VERSION:
            raise SettingsError("invalid_update_settings", "Unsupported settings file.")
        enabled, weekdays = value.get("enabled"), value.get("weekdays")
        directory, updated_at = value.get("directory"), value.get("updated_at", "")
        if (
            not isinstance(enabled, bool)
            or not isinstance(weekdays, list)
            or not all(isinstance(day, str) for day in weekdays)
            or not isinstance(value.get("time"), str)
            or not isinstance(value.get("source"), str)
            or not (directory is None or isinstance(directory, str))
            or not isinstance(updated_at, str)
        ):
            raise SettingsError("invalid_update_settings", "Malformed settings file.")
        return cls(
            enabled=enabled,
            weekdays=tuple(weekdays),
            time=value["time"],
            source=value["source"],
            directory=directory,
            updated_at=updated_at,
        )

    def relay_source(self) -> RelaySource:
        if self.source == "directory" and self.directory:
            return RelaySource("directory", Path(self.directory))
        return RelaySource("online")

    def _clock(self) -> wall_time:
        hours, minutes = self.time.split(":")
        return wall_time(int(hours), int(minutes))

    def _slot(self, day: date) -> datetime | None:
        if WEEKDAYS[day.weekday()] not in self.weekdays:
            return None
        # A naive local wall-clock time, made aware in the host's zone.
        return datetime.combine(day, self._clock()).astimezone()

    def next_run(self, now: datetime) -> datetime | None:
        """The first slot after ``now`` (aware), or ``None`` when disabled."""
        if not self.enabled:
            return None
        for offset in range(8):
            slot = self._slot(now.date() + timedelta(days=offset))
            if slot is not None and slot > now:
                return slot
        return None

    def due_slot(self, now: datetime) -> datetime | None:
        """The latest slot at or before ``now`` that may still start.

        A slot is due within :data:`GRACE` after its time, and only if the
        schedule was saved before it, so saving never starts a past slot.
        """
        if not self.enabled:
            return None
        saved = _parse_time(self.updated_at)
        for offset in range(8):
            slot = self._slot(now.date() - timedelta(days=offset))
            if slot is None or slot > now:
                continue
            if now - slot > GRACE or (saved is not None and slot < saved):
                return None
            return slot
        return None


def slot_key(slot: datetime) -> str:
    """The idempotency key of a scheduled slot; one operation per slot."""
    return "scheduled-update-" + slot.astimezone(UTC).strftime("%Y%m%dT%H%MZ")


def parse_request(body: Mapping[str, Any], current: UpdateSettings) -> UpdateSettings:
    """Validate a browser settings request into new settings."""
    if set(body) - _REQUEST_KEYS:
        raise SettingsError(
            "invalid_request", "The request contains unsupported fields."
        )
    merged = {**current.to_dict(), **body}
    source = merged.get("source")
    directory = merged.get("directory") if source == "directory" else None
    if source == "directory":
        directory = require_directory(directory)
    weekdays = merged.get("weekdays")
    if not isinstance(weekdays, list) or not all(day in WEEKDAYS for day in weekdays):
        raise SettingsError("invalid_schedule", "Choose weekdays from mon to sun.")
    enabled = merged.get("enabled")
    if not isinstance(enabled, bool):
        raise SettingsError("invalid_schedule", "enabled must be true or false.")
    for name in ("time", "source"):
        if not isinstance(merged.get(name), str):
            raise SettingsError("invalid_schedule", f"{name} must be text.")
    return UpdateSettings(
        enabled=enabled,
        weekdays=tuple(day for day in WEEKDAYS if day in weekdays),
        time=merged["time"],
        source=merged["source"],
        directory=directory,
        updated_at=utc_now(),
    )


def require_directory(value: object) -> str:
    """An existing, readable local directory, as its resolved absolute path."""
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or len(value) > 1024
        or any(char in value for char in "\0\n\r")
    ):
        raise SettingsError(
            "invalid_update_source", "Enter the absolute path of a local directory."
        )
    path = Path(value).resolve()
    if not path.is_dir() or not os.access(path, os.R_OK | os.X_OK):
        raise SettingsError(
            "invalid_update_source",
            "The update directory does not exist or cannot be read.",
        )
    return str(path)


class SettingsStore:
    """Atomic JSON persistence of :class:`UpdateSettings`."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> UpdateSettings:
        """The saved settings; defaults when absent. Raises on a bad file."""
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return UpdateSettings()
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as error:
            raise SettingsError(
                "invalid_update_settings", "The settings file is not JSON."
            ) from error
        if not isinstance(value, Mapping):
            raise SettingsError("invalid_update_settings", "Malformed settings file.")
        return UpdateSettings.from_dict(value)

    def save(self, settings: UpdateSettings) -> None:
        write_private_file(
            self.path,
            json.dumps(settings.to_dict(), indent=2, sort_keys=True) + "\n",
            prefix=".update-settings-",
        )


def _parse_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None
