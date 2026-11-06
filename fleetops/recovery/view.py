"""A read-only snapshot of everything a recovery decision may look at."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from ..geo import LocalFrame

ROAD_FACTOR = 1.4          # straight-line -> road distance
DISPATCH_PREP_S = 180.0    # driver walk-round + pull-out
DEPOT_SPEED_MPS = 8.0      # average urban speed for a deadheading bus


@dataclass
class FleetView:
    incident: dict                    # incident_id, type, bus_id, opened_ts, closed_ts, detail
    bus: dict                         # bus_id, route_id, trip_id, lat, lon, last_ts, finished, next_stop_idx
    route: dict                       # route_id, name, riders, stops: [{stop_id, name, lat, lon, dist_m}]
    depots: dict[str, dict]           # depot_id -> {name, lat, lon}
    spares: list[dict]                # bus_id, depot_id, capacity, status
    drivers: list[dict]               # driver_id, name, depot_id, status
    now_ts: float                     # event time ("now" in the bus clock)
    excluded_spares: set[str] = field(default_factory=set)   # rejected earlier by a dispatcher
    reserved_spares: set[str] = field(default_factory=set)   # proposed in another incident's pending plan
    reserved_drivers: set[str] = field(default_factory=set)

    def spare_free(self, s: dict) -> bool:
        return (s["status"] == "available" and s["bus_id"] not in self.excluded_spares
                and s["bus_id"] not in self.reserved_spares)

    def driver_free(self, d: dict) -> bool:
        return d["status"] == "available" and d["driver_id"] not in self.reserved_drivers

    # ---------------------------------------------------------------- lookups
    def spare(self, bus_id: str) -> dict | None:
        return next((s for s in self.spares if s["bus_id"] == bus_id), None)

    def driver(self, driver_id: str) -> dict | None:
        return next((d for d in self.drivers if d["driver_id"] == driver_id), None)

    @property
    def gps_age_s(self) -> float | None:
        if self.bus.get("last_ts") is None:
            return None
        return max(0.0, self.now_ts - self.bus["last_ts"])

    def remaining_stops(self) -> list[dict]:
        return self.route["stops"][self.bus.get("next_stop_idx") or 0:]

    # ------------------------------------------------------------------ maths
    def distance_m(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        f = LocalFrame(lat1, lon1)
        x, y = f.to_xy(lat2, lon2)
        return math.hypot(x, y)

    def eta_s(self, spare_bus_id: str) -> float | None:
        """System-computed time for a spare to reach the broken-down bus."""
        s = self.spare(spare_bus_id)
        if s is None or self.bus.get("lat") is None:
            return None
        dep = self.depots[s["depot_id"]]
        d = self.distance_m(dep["lat"], dep["lon"], self.bus["lat"], self.bus["lon"]) * ROAD_FACTOR
        return DISPATCH_PREP_S + d / DEPOT_SPEED_MPS
