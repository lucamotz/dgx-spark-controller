# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Render the application icon with the standard library only.

The mark mirrors the web UI's ``▦`` wordmark glyph: a rounded square with an
orthogonal grid in the palette of ``controller/webui/styles.css`` (``--moss``
to ``--moss-dark`` tile, ``--moss-soft`` grid, ``--amber`` centre cell).
Shapes are signed-distance fields, so every size is anti-aliased analytically
and the output is reproducible.

Regenerate the committed PNG files after a design change::

    python -m controller.assets.generate_icon
"""

from __future__ import annotations

import math
import struct
import zlib

from controller.assets import ICON_PNG, ICON_PNG_256

OUTPUTS = {1024: ICON_PNG, 256: ICON_PNG_256}

Color = tuple[int, int, int]

MOSS: Color = (0x3D, 0x64, 0x56)
MOSS_DARK: Color = (0x29, 0x48, 0x3F)
MOSS_SOFT: Color = (0xE2, 0xEE, 0xE7)
AMBER: Color = (0xAE, 0x76, 0x2C)
BLACK: Color = (0, 0, 0)

# Geometry in 1024-unit canvas space, following the macOS icon grid.
TILE_HALF = 412.0
TILE_RADIUS = 185.0
SHADOW_OFFSET = 10.0
SHADOW_SIGMA = 12.0
SHADOW_ALPHA = 0.24
GLYPH_HALF = 246.0
GLYPH_RADIUS = 58.0
STROKE = 40.0
CELL_GAP = 16.0


def rounded_box(px: float, py: float, half_w: float, half_h: float, r: float) -> float:
    """Return the signed distance to a rounded box centred at the origin.

    Args:
        px: Sample x coordinate.
        py: Sample y coordinate.
        half_w: Half of the box width.
        half_h: Half of the box height.
        r: Corner radius.

    Returns:
        Negative inside the box, positive outside.

    """
    qx = abs(px) - half_w + r
    qy = abs(py) - half_h + r
    outside = math.hypot(max(qx, 0.0), max(qy, 0.0))
    inside = min(max(qx, qy), 0.0)
    return outside + inside - r


def _coverage(distance_px: float) -> float:
    return min(max(0.5 - distance_px, 0.0), 1.0)


def _over(dst: list[float], color: Color, alpha: float) -> None:
    if alpha <= 0.0:
        return
    dst_alpha = dst[3]
    out_alpha = alpha + dst_alpha * (1.0 - alpha)
    for channel in range(3):
        source = color[channel] / 255.0
        blended = source * alpha + dst[channel] * dst_alpha * (1.0 - alpha)
        dst[channel] = blended / out_alpha
    dst[3] = out_alpha


def _lerp(a: Color, b: Color, t: float) -> Color:
    return (
        round(a[0] + (b[0] - a[0]) * t),
        round(a[1] + (b[1] - a[1]) * t),
        round(a[2] + (b[2] - a[2]) * t),
    )


def _glyph_distance(ux: float, uy: float) -> float:
    third = GLYPH_HALF * 2.0 / 3.0
    half_stroke = STROKE / 2.0
    distance = abs(rounded_box(ux, uy, GLYPH_HALF, GLYPH_HALF, GLYPH_RADIUS))
    distance -= half_stroke
    for offset in (-third / 2.0, third / 2.0):
        vertical = rounded_box(ux - offset, uy, half_stroke, GLYPH_HALF, 0.0)
        horizontal = rounded_box(ux, uy - offset, GLYPH_HALF, half_stroke, 0.0)
        distance = min(distance, vertical, horizontal)
    return distance


def _pixel(ux: float, uy: float, scale: float, tile_color: Color) -> list[float]:
    pixel = [0.0, 0.0, 0.0, 0.0]
    shadow_distance = rounded_box(
        ux, uy - SHADOW_OFFSET, TILE_HALF, TILE_HALF, TILE_RADIUS
    )
    falloff = max(shadow_distance, 0.0) / SHADOW_SIGMA
    _over(pixel, BLACK, SHADOW_ALPHA * math.exp(-0.5 * falloff**2))
    tile = _coverage(rounded_box(ux, uy, TILE_HALF, TILE_HALF, TILE_RADIUS) * scale)
    _over(pixel, tile_color, tile)
    if max(abs(ux), abs(uy)) <= GLYPH_HALF + STROKE + 1.0 / scale:
        cell_half = GLYPH_HALF / 3.0 - STROKE / 2.0 - CELL_GAP
        accent = rounded_box(ux, uy, cell_half, cell_half, 10.0)
        _over(pixel, AMBER, _coverage(accent * scale) * tile)
        _over(pixel, MOSS_SOFT, _coverage(_glyph_distance(ux, uy) * scale) * tile)
    return pixel


def render_rgba(size: int) -> list[bytes]:
    """Rasterise the icon.

    Args:
        size: Edge length in pixels.

    Returns:
        ``size`` rows of 8-bit straight-alpha RGBA pixels.

    """
    scale = size / 1024.0
    centre = size / 2.0
    reach = TILE_HALF + SHADOW_OFFSET + 4.0 * SHADOW_SIGMA
    rows: list[bytes] = []
    for y in range(size):
        uy = (y + 0.5 - centre) / scale
        depth = min(max((uy + TILE_HALF) / (2.0 * TILE_HALF), 0.0), 1.0)
        tile_color = _lerp(MOSS, MOSS_DARK, depth)
        row = bytearray()
        for x in range(size):
            ux = (x + 0.5 - centre) / scale
            if max(abs(ux), abs(uy)) > reach:
                row.extend(b"\x00\x00\x00\x00")
                continue
            pixel = _pixel(ux, uy, scale, tile_color)
            row.extend(round(channel * 255.0) for channel in pixel)
        rows.append(bytes(row))
    return rows


def encode_png(rows: list[bytes]) -> bytes:
    """Encode RGBA rows as a deterministic PNG.

    Args:
        rows: Equal-length rows of 8-bit RGBA pixels.

    Returns:
        The PNG file contents.

    """
    height = len(rows)
    width = len(rows[0]) // 4 if rows else 0

    def chunk(tag: bytes, data: bytes) -> bytes:
        checksum = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", checksum)

    header = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
    raw = b"".join(b"\x00" + row for row in rows)
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", header)
        + chunk(b"IDAT", zlib.compress(raw, 9))
        + chunk(b"IEND", b"")
    )


def render_png(size: int) -> bytes:
    """Return the icon as PNG bytes at ``size`` x ``size`` pixels."""
    return encode_png(render_rgba(size))


def main() -> int:
    """Write the committed icon PNG files."""
    for size, target in OUTPUTS.items():
        target.write_bytes(render_png(size))
        print(f"wrote {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
