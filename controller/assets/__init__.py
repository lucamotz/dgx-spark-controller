# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Packaged non-code assets for the desktop launcher and OS integrations.

The PNG icons are committed build outputs of :mod:`controller.assets.generate_icon`
so that installing the controller never requires an image toolchain.
"""

from __future__ import annotations

from pathlib import Path

ASSETS_DIR = Path(__file__).resolve().parent
ICON_NAME = "dgx-spark-controller"
#: 1024 px master icon (macOS bundle source, Dock icon).
ICON_PNG = ASSETS_DIR / f"{ICON_NAME}.png"
#: 256 px icon for the Linux hicolor theme and GTK/Qt window icons.
ICON_PNG_256 = ASSETS_DIR / f"{ICON_NAME}-256.png"

__all__ = ["ASSETS_DIR", "ICON_NAME", "ICON_PNG", "ICON_PNG_256"]
