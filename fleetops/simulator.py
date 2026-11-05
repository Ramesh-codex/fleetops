"""Deterministic fleet simulator.

Same seed -> same routes, same pings, same faults. That makes every test and
benchmark reproducible.

The simulator also records GROUND TRUTH (when each bus really reached each stop),
which is what lets us measure ETA error instead of guessing at it.

Faults it can inject, mimicking real in-vehicle GPS devices over cellular:
  * traffic slowdowns on some road segments
  * breakdowns (bus sits still for minutes, still pinging)
  * GPS noise (position jitter)
  * duplicate pings (device retry after a lost ACK)
  * out-of-order delivery (network reordering)
  * GPS dropouts (bus goes silent for a while)
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

from .geo import LocalFrame, Polyline
from .models import GpsPing, Route, Stop

ORIGIN = (37.3382, -121.8863)  # synthetic area; no real routes or students


@dataclass
class SimConfig:
    seed: int = 42
    n_buses: int = 10
    stops_per_route: int = 10
    ping_interval_s: float = 5.0
    cruise_mps: float = 9.0              # ~32 km/h urban average
    dwell_s: tuple[float, float] = (20.0, 45.0)
    traffic_prob: float = 0.15           # chance a segment is congested
    traffic_factor: tuple[float, float] = (0.3, 0.5)
    breakdown_prob: float = 0.0          # chance a trip has a breakdown
    breakdown_s: tuple[float, float] = (300.0, 900.0)
    gps_noise_m: float = 5.0
    duplicate_prob: float = 0.0
    reorder_prob: float = 0.0
    dropout_prob: float = 0.0            # chance a trip has a GPS dropout
    dropout_s: tuple[float, float] = (90.0, 240.0)
    start_ts: float = 1_700_000_000.0


@dataclass
class SimResult:
    frame: LocalFrame
    routes: dict[str, Route]
    bus_route: dict[str, str]                      # bus_id -> route_id
    trip_of: dict[str, str]                        # bus_id -> trip_id
    pings: list[GpsPing]                           # in DELIVERY order
    truth_arrival: dict[tuple[str, str], float]    # (bus_id, stop_id) -> ts
    dropouts: dict[str, tuple[float, float]] = field(default_factory=dict)
    breakdowns: dict[str, tuple[float, float]] = field(default_factory=dict)
    end_ts: float = 0.0


def _make_route(rng: random.Random, frame: LocalFrame, idx: int, n_stops: int) -> tuple[Route, Polyline]:
    # Spread route start points around the origin, then walk forward with
    # mild turns so routes look like plausible streets.
    ang0 = rng.uniform(0, 2 * math.pi)
    x, y = math.cos(ang0) * rng.uniform(500, 4000), math.sin(ang0) * rng.uniform(500, 4000)
    heading = rng.uniform(0, 2 * math.pi)
    pts = [(x, y)]
    for _ in range(n_stops - 1):
        heading += rng.uniform(-0.6, 0.6)
        step = rng.uniform(300, 700)
        x, y = x + math.sin(heading) * step, y + math.cos(heading) * step
        pts.append((x, y))
    line = Polyline(pts)
    stops = []
    for i, (px, py) in enumerate(pts):
        lat, lon = frame.to_latlon(px, py)
        name = "School" if i == n_stops - 1 else f"Stop {i + 1}"
        stops.append(Stop(stop_id=f"R{idx:02d}-S{i:02d}", name=name, lat=lat, lon=lon, dist_m=line.cum[i]))
    path = [frame.to_latlon(px, py) for px, py in pts]
    return Route(route_id=f"R{idx:02d}", name=f"Route {idx}", path=path, stops=stops), line


def simulate(cfg: SimConfig) -> SimResult:
    rng = random.Random(cfg.seed)
    frame = LocalFrame(*ORIGIN)
    routes: dict[str, Route] = {}
    lines: dict[str, Polyline] = {}
    bus_route, trip_of = {}, {}
    truth: dict[tuple[str, str], float] = {}
    dropouts, breakdowns = {}, {}
    per_bus_pings: dict[str, list[GpsPing]] = {}
    end_ts = cfg.start_ts

    for b in range(cfg.n_buses):
        route, line = _make_route(rng, frame, b + 1, cfg.stops_per_route)
        routes[route.route_id] = route
        lines[route.route_id] = line
        bus_id, trip_id = f"BUS-{101 + b}", f"T-{b + 1:03d}"
        bus_route[bus_id], trip_of[bus_id] = route.route_id, trip_id

        # Per-segment speeds (some congested).
        seg_speed = []
        for _ in range(len(route.stops) - 1):
            v = cfg.cruise_mps * rng.uniform(0.8, 1.15)
            if rng.random() < cfg.traffic_prob:
                v *= rng.uniform(*cfg.traffic_factor)
            seg_speed.append(v)

        t = cfg.start_ts + rng.uniform(0, 300)   # staggered departures
        trip_start = t
        has_breakdown = rng.random() < cfg.breakdown_prob
        breakdown_at_d = rng.uniform(0.3, 0.7) * line.length if has_breakdown else None
        breakdown_len = rng.uniform(*cfg.breakdown_s) if has_breakdown else 0.0
        has_dropout = rng.random() < cfg.dropout_prob
        dropout_len = rng.uniform(*cfg.dropout_s) if has_dropout else 0.0
        dropout_start = None

        # Integrate motion at 1 s resolution.
        d, seg, dwell_left, broke_left = 0.0, 0, 0.0, 0.0
        truth[(bus_id, route.stops[0].stop_id)] = t
        dwell_left = rng.uniform(*cfg.dwell_s)  # boarding at first stop
        next_ping = t
        seq = 0
        pings: list[GpsPing] = []
        while True:
            if t >= next_ping:
                if has_dropout and dropout_start is None and t - trip_start > 0.35 * (line.length / cfg.cruise_mps):
                    dropout_start = t
                    dropouts[bus_id] = (t, t + dropout_len)
                in_dropout = dropout_start is not None and dropout_start <= t < dropout_start + dropout_len
                if not in_dropout:
                    x, y = line.point_at(d)
                    x += rng.gauss(0, cfg.gps_noise_m)
                    y += rng.gauss(0, cfg.gps_noise_m)
                    lat, lon = frame.to_latlon(x, y)
                    moving = dwell_left <= 0 and broke_left <= 0
                    pings.append(GpsPing(
                        event_id=f"{bus_id}-{seq}", bus_id=bus_id, trip_id=trip_id, seq=seq,
                        ts=round(t, 3), lat=lat, lon=lon,
                        speed_mps=round(seg_speed[min(seg, len(seg_speed) - 1)] if moving else 0.0, 2),
                        heading=round(line.heading_at(d), 1) % 360,
                    ))
                seq += 1
                next_ping += cfg.ping_interval_s

            if seg >= len(route.stops) - 1 and dwell_left <= 0:
                break
            t += 1.0
            if dwell_left > 0:
                dwell_left -= 1.0
                continue
            if broke_left > 0:
                broke_left -= 1.0
                continue
            if breakdown_at_d is not None and d >= breakdown_at_d:
                breakdowns[bus_id] = (t, t + breakdown_len)
                broke_left, breakdown_at_d = breakdown_len, None
                continue
            d += seg_speed[seg]
            next_stop_d = route.stops[seg + 1].dist_m
            if d >= next_stop_d:
                d = next_stop_d
                seg += 1
                truth[(bus_id, route.stops[seg].stop_id)] = t
                if seg < len(route.stops) - 1:
                    dwell_left = rng.uniform(*cfg.dwell_s)

        # Devices send a final "arrived" ping; without it the last stop is never
        # confirmed and a finished bus looks like it lost GPS.
        if not pings or pings[-1].ts < t:
            x, y = line.point_at(line.length)
            lat, lon = frame.to_latlon(x, y)
            pings.append(GpsPing(
                event_id=f"{bus_id}-{seq}", bus_id=bus_id, trip_id=trip_id, seq=seq, ts=round(t, 3),
                lat=lat, lon=lon, speed_mps=0.0, heading=round(line.heading_at(line.length), 1) % 360))

        per_bus_pings[bus_id] = pings
        end_ts = max(end_ts, t)

    # Delivery-order faults, applied per bus.
    delivered: list[tuple[float, int, GpsPing]] = []
    order = 0
    for bus_id, pings in per_bus_pings.items():
        out: list[GpsPing] = []
        i = 0
        while i < len(pings):
            p = pings[i]
            if i + 1 < len(pings) and rng.random() < cfg.reorder_prob:
                out.extend([pings[i + 1], p])   # swap: newer arrives first
                i += 2
                continue
            out.append(p)
            if rng.random() < cfg.duplicate_prob:
                out.append(p.model_copy())      # identical retry
            i += 1
        # delivery clock: keep each bus's delivery sequence but merge buses by device ts
        for k, p in enumerate(out):
            delivered.append((max(p.ts, out[k - 1].ts if k else p.ts), order, p))
            order += 1
    delivered.sort(key=lambda r: (r[0], r[1]))

    return SimResult(
        frame=frame, routes=routes, bus_route=bus_route, trip_of=trip_of,
        pings=[p for _, _, p in delivered], truth_arrival=truth,
        dropouts=dropouts, breakdowns=breakdowns, end_ts=end_ts,
    )
