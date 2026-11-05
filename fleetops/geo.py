"""Geometry helpers.

Routes are short (a few km), so we project lat/lon onto a flat local plane
(equirectangular) in metres. Error is well under 1 m at this scale, and it makes
"how far along the route is this bus" simple vector math.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

EARTH_R = 6_371_000.0  # metres


@dataclass(frozen=True)
class LocalFrame:
    """Flat x/y frame (metres) centred on an origin lat/lon."""

    lat0: float
    lon0: float

    def to_xy(self, lat: float, lon: float) -> tuple[float, float]:
        x = math.radians(lon - self.lon0) * EARTH_R * math.cos(math.radians(self.lat0))
        y = math.radians(lat - self.lat0) * EARTH_R
        return x, y

    def to_latlon(self, x: float, y: float) -> tuple[float, float]:
        lat = self.lat0 + math.degrees(y / EARTH_R)
        lon = self.lon0 + math.degrees(x / (EARTH_R * math.cos(math.radians(self.lat0))))
        return lat, lon


class Polyline:
    """A route path. Knows cumulative distance at each vertex and can
    snap a point to 'distance along the route'."""

    def __init__(self, pts: list[tuple[float, float]]):
        if len(pts) < 2:
            raise ValueError("polyline needs at least 2 points")
        self.pts = pts
        self.cum = [0.0]
        for (x1, y1), (x2, y2) in zip(pts, pts[1:]):
            self.cum.append(self.cum[-1] + math.hypot(x2 - x1, y2 - y1))

    @property
    def length(self) -> float:
        return self.cum[-1]

    def point_at(self, d: float) -> tuple[float, float]:
        """Point at distance d along the line (clamped to the ends)."""
        d = max(0.0, min(d, self.length))
        for i in range(len(self.pts) - 1):
            if d <= self.cum[i + 1] or i == len(self.pts) - 2:
                seg = self.cum[i + 1] - self.cum[i]
                t = 0.0 if seg == 0 else (d - self.cum[i]) / seg
                (x1, y1), (x2, y2) = self.pts[i], self.pts[i + 1]
                return x1 + t * (x2 - x1), y1 + t * (y2 - y1)
        return self.pts[-1]  # unreachable

    def heading_at(self, d: float) -> float:
        """Compass heading in degrees (0 = north) of the segment at distance d."""
        d = max(0.0, min(d, self.length))
        for i in range(len(self.pts) - 1):
            if d <= self.cum[i + 1] or i == len(self.pts) - 2:
                (x1, y1), (x2, y2) = self.pts[i], self.pts[i + 1]
                return (math.degrees(math.atan2(x2 - x1, y2 - y1)) + 360) % 360
        return 0.0

    def project(self, x: float, y: float, d_min: float = 0.0, d_max: float | None = None) -> tuple[float, float]:
        """Snap (x, y) onto the line, only considering the window [d_min, d_max].

        Returns (distance_along, offset_metres). Restricting the window stops a
        noisy ping from snapping to an earlier part of a route that doubles back.
        """
        if d_max is None:
            d_max = self.length
        best_d, best_off = d_min, float("inf")
        for i in range(len(self.pts) - 1):
            a, b = self.cum[i], self.cum[i + 1]
            if b < d_min or a > d_max:
                continue
            (x1, y1), (x2, y2) = self.pts[i], self.pts[i + 1]
            dx, dy = x2 - x1, y2 - y1
            seg2 = dx * dx + dy * dy
            t = 0.0 if seg2 == 0 else ((x - x1) * dx + (y - y1) * dy) / seg2
            t = max(0.0, min(1.0, t))
            d = a + t * (b - a)
            d = max(d_min, min(d, d_max))
            px, py = self.point_at(d)
            off = math.hypot(x - px, y - py)
            if off < best_off:
                best_d, best_off = d, off
        return best_d, best_off
