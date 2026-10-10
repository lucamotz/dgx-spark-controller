# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Lenient parsers for the ``KEY=VALUE`` output of fixed remote scripts.

Remote output is evidence, not trusted input: malformed lines are skipped and
malformed values decode to an empty or zero value instead of raising.
"""

from __future__ import annotations

import base64
from collections.abc import Iterable


def parse_key_values(items: Iterable[str]) -> dict[str, str]:
    """Split each item on its first ``=``; items without a key are skipped.

    A repeated key keeps its last value.
    """
    fields: dict[str, str] = {}
    for item in items:
        key, separator, value = item.partition("=")
        if separator and key:
            fields[key] = value
    return fields


def parse_fields(text: str) -> dict[str, str]:
    """Parse newline-delimited ``KEY=VALUE`` script output."""
    return parse_key_values(text.splitlines())


def decode_b64(value: str) -> str:
    """Decode one base64 field as UTF-8 text, or return ``""`` if malformed."""
    if not value:
        return ""
    try:
        return base64.b64decode(value, validate=True).decode("utf-8", errors="replace")
    except (ValueError, UnicodeError):
        return ""


def parse_int(value: str | None) -> int:
    """Read a numeric field, truncating any fraction; missing or non-numeric is 0."""
    try:
        return int(float(value or "0"))
    except ValueError:
        return 0
