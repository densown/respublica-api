#!/usr/bin/env python3
"""Fix GeoJSON polygon winding for d3-geo / RFC 7946 (exterior CCW, holes CW)."""

from __future__ import annotations

import json
import os
import sys


def reverse_ring(ring: list) -> list:
    return list(reversed(ring))


def ring_is_counterclockwise(ring: list) -> bool:
    """
    True if ring is counter-clockwise in lon/lat plane (exterior rings per RFC 7946).
    Uses shoelace / surveyor's formula with explicit closure.
    """
    pts = list(ring)
    if len(pts) >= 2 and pts[0] == pts[-1]:
        pts = pts[:-1]
    n = len(pts)
    if n < 3:
        return True
    s = 0.0
    for i in range(n):
        j = (i + 1) % n
        s += pts[i][0] * pts[j][1] - pts[j][0] * pts[i][1]
    return s > 0


def fix_polygon(coords: list) -> list:
    """Exterior ring counter-clockwise; holes clockwise (RFC 7946)."""
    fixed: list = []
    for i, ring in enumerate(coords):
        ccw = ring_is_counterclockwise(ring)
        if i == 0:
            fixed.append(reverse_ring(ring) if not ccw else ring)
        else:
            fixed.append(ring if not ccw else reverse_ring(ring))
    return fixed


def main() -> None:
    src = os.environ.get("KREISE_GEOJSON", "/mnt/data/geodata/kreise.geojson")
    dst = src
    if len(sys.argv) >= 2:
        src = sys.argv[1]
    if len(sys.argv) >= 3:
        dst = sys.argv[2]
    else:
        dst = src

    with open(src, encoding="utf-8") as f:
        data = json.load(f)

    for feat in data["features"]:
        geom = feat["geometry"]
        if geom["type"] == "Polygon":
            geom["coordinates"] = fix_polygon(geom["coordinates"])
        elif geom["type"] == "MultiPolygon":
            geom["coordinates"] = [fix_polygon(poly) for poly in geom["coordinates"]]

    with open(dst, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))

    print("Fixed winding order for", len(data["features"]), "features")
    print("Wrote:", dst)


if __name__ == "__main__":
    main()
