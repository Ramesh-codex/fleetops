"""Tracking core: turns a stream of raw GPS pings into bus state, stop ETAs,
parent "bus is approaching" notifications, and GPS-loss incidents.

Design rules (each one is covered by a test):
  1. Idempotent: the same ping delivered twice changes nothing the second time.
  2. Ordered per bus: a ping older than one already applied is dropped, so a
     late packet can never move a bus backwards.
  3. One notification per (trip, stop), ever.
  4. Pure logic, no I/O: the same class runs behind Kafka, an HTTP endpoint,
     or a test, and replaying the same input gives the same output.
"""

from __future__ import annotations

import itertools
from collections import deque
from dataclasses import dataclass, field

from .geo import LocalFrame, Polyline
from .models import SCHEMA_VERSION, GpsPing, Incident, IncidentType, Notification, Route

PASSED_TOL_M = 20.0        # within this distance of a stop counts as "at/passed"
MAX_SPEED_MPS = 30.0       # physical cap used to bound how far a bus can jump
SEEN_IDS_PER_BUS = 512     # dedup window


@dataclass
class BusState:
    bus_id: str
    route_id: str
    trip_id: str | None = None
    last_seq: int = -1
    last_ts: float | None = None
    lat: float | None = None
    lon: float | None = None
    d: float = 0.0                       # metres along route
    speed_est: float | None = None       # m/s estimate used for ETAs
    next_stop_idx: int = 0
    finished: bool = False
    notified: set[str] = field(default_factory=set)
    seen_ids: deque = field(default_factory=lambda: deque(maxlen=SEEN_IDS_PER_BUS))
    seen_set: set[str] = field(default_factory=set)


class Tracker:
    """
    eta_mode:
      "naive"  — speed = EWMA of all observed movement, including time standing
                 at stops; ETA = remaining distance / speed.
      "moving" — speed = EWMA only over pings where the bus actually moved, plus
                 a fixed dwell allowance for every stop still ahead.
    """

    def __init__(
        self,
        routes: dict[str, Route],
        frame: LocalFrame,
        bus_route: dict[str, str],
        *,
        notify_lead_s: float = 300.0,
        gps_lost_after_s: float = 60.0,
        eta_mode: str = "moving",
        ewma_alpha: float = 0.3,
        default_speed_mps: float = 9.0,
        min_speed_mps: float = 1.5,
        dwell_allowance_s: float = 30.0,
        require_warmup: bool = True,
    ):
        if eta_mode not in ("naive", "moving"):
            raise ValueError(eta_mode)
        self.routes = routes
        self.frame = frame
        self.lines = {
            rid: Polyline([frame.to_xy(lat, lon) for lat, lon in r.path]) for rid, r in routes.items()
        }
        self.buses = {b: BusState(bus_id=b, route_id=r) for b, r in bus_route.items()}
        self.notify_lead_s = notify_lead_s
        self.gps_lost_after_s = gps_lost_after_s
        self.eta_mode = eta_mode
        self.alpha = ewma_alpha
        self.default_speed = default_speed_mps
        self.min_speed = min_speed_mps
        self.dwell_allowance = dwell_allowance_s
        # Cold start: on a trip's first ping there is no measured speed, so the
        # ETA uses a default speed and alerts for far stops fired minutes early
        # (found by the benchmark). With warmup on, no alert is sent until the
        # bus has a measured speed.
        self.require_warmup = require_warmup
        self.notifications: list[Notification] = []
        self.incidents: dict[str, Incident] = {}
        self._open_incident: dict[str, str] = {}      # bus_id -> incident_id
        self._incident_ids = itertools.count(1)
        self.counters = {
            "accepted": 0, "duplicate": 0, "out_of_order": 0,
            "unknown_bus": 0, "bad_schema": 0, "notifications": 0,
        }

    # ------------------------------------------------------------------ ingest
    def ingest(self, p: GpsPing) -> str:
        """Apply one ping. Returns the outcome label (also counted)."""
        outcome = self._ingest(p)
        self.counters[outcome] += 1
        return outcome

    def _ingest(self, p: GpsPing) -> str:
        if p.schema_version != SCHEMA_VERSION:
            return "bad_schema"
        st = self.buses.get(p.bus_id)
        if st is None:
            return "unknown_bus"
        if p.event_id in st.seen_set:
            return "duplicate"
        if p.seq <= st.last_seq:
            # Older than something already applied. Applying it would move the
            # bus backwards and corrupt the speed estimate, so drop it.
            self._remember(st, p.event_id)
            return "out_of_order"

        self._remember(st, p.event_id)
        if st.trip_id != p.trip_id:          # new trip: reset trip-scoped state
            st.trip_id, st.d, st.next_stop_idx, st.finished = p.trip_id, 0.0, 0, False
            st.notified.clear()
            st.speed_est = None

        line = self.lines[st.route_id]
        x, y = self.frame.to_xy(p.lat, p.lon)
        dt = (p.ts - st.last_ts) if st.last_ts is not None else None
        window = (dt or 60.0) * MAX_SPEED_MPS + 50.0
        d_proj, _off = line.project(x, y, d_min=max(0.0, st.d - 30.0), d_max=st.d + window)
        new_d = max(st.d, d_proj)            # buses don't reverse on a route

        if dt and dt > 0:
            obs = (new_d - st.d) / dt
            moved = (new_d - st.d) > 5.0
            if self.eta_mode == "naive" or moved:
                st.speed_est = obs if st.speed_est is None else (
                    self.alpha * obs + (1 - self.alpha) * st.speed_est)

        st.d, st.last_seq, st.last_ts, st.lat, st.lon = new_d, p.seq, p.ts, p.lat, p.lon

        stops = self.routes[st.route_id].stops
        while st.next_stop_idx < len(stops) and st.d >= stops[st.next_stop_idx].dist_m - PASSED_TOL_M:
            st.next_stop_idx += 1
        if st.next_stop_idx >= len(stops):
            st.finished = True

        self._close_gps_lost(st, p.ts)
        self._maybe_notify(st, p.ts)
        return "accepted"

    @staticmethod
    def _remember(st: BusState, event_id: str) -> None:
        if len(st.seen_ids) == st.seen_ids.maxlen:
            st.seen_set.discard(st.seen_ids[0])
        st.seen_ids.append(event_id)
        st.seen_set.add(event_id)

    # -------------------------------------------------------------------- ETAs
    def _speed(self, st: BusState) -> float:
        v = st.speed_est if st.speed_est is not None else self.default_speed
        return max(v, self.min_speed)

    def etas(self, bus_id: str) -> dict[str, float]:
        """Predicted arrival timestamp for every stop still ahead."""
        st = self.buses[bus_id]
        if st.last_ts is None or st.finished:
            return {}
        v = self._speed(st)
        out = {}
        stops = self.routes[st.route_id].stops
        for k, s in enumerate(stops[st.next_stop_idx:]):
            eta = st.last_ts + (s.dist_m - st.d) / v
            if self.eta_mode == "moving":
                eta += k * self.dwell_allowance    # stops between here and s
            out[s.stop_id] = eta
        return out

    def _maybe_notify(self, st: BusState, now: float) -> None:
        if self.require_warmup and st.speed_est is None:
            return
        for stop_id, eta in self.etas(st.bus_id).items():
            if stop_id in st.notified:
                continue
            if eta - now <= self.notify_lead_s:
                st.notified.add(stop_id)
                self.notifications.append(Notification(
                    bus_id=st.bus_id, trip_id=st.trip_id or "", stop_id=stop_id,
                    sent_ts=now, predicted_arrival_ts=eta))
                self.counters["notifications"] += 1

    # --------------------------------------------------------------- incidents
    def check_stale(self, now: float) -> list[Incident]:
        """Open a GPS_LOST incident for any active bus silent too long."""
        opened = []
        for st in self.buses.values():
            if st.last_ts is None or st.finished or st.bus_id in self._open_incident:
                continue
            if now - st.last_ts > self.gps_lost_after_s:
                iid = f"INC-{next(self._incident_ids):05d}"
                inc = Incident(incident_id=iid, type=IncidentType.GPS_LOST, bus_id=st.bus_id,
                               opened_ts=now, detail=f"no GPS for {now - st.last_ts:.0f}s")
                self.incidents[iid] = inc
                self._open_incident[st.bus_id] = iid
                opened.append(inc)
        return opened

    def _close_gps_lost(self, st: BusState, now: float) -> None:
        iid = self._open_incident.pop(st.bus_id, None)
        if iid:
            self.incidents[iid].closed_ts = now

    # ------------------------------------------------------------------- views
    def snapshot(self) -> list[dict]:
        out = []
        for st in self.buses.values():
            stops = self.routes[st.route_id].stops
            nxt = stops[st.next_stop_idx] if st.next_stop_idx < len(stops) else None
            etas = self.etas(st.bus_id)
            out.append({
                "bus_id": st.bus_id, "route_id": st.route_id, "trip_id": st.trip_id,
                "lat": st.lat, "lon": st.lon, "last_ts": st.last_ts,
                "progress_m": round(st.d, 1),
                "speed_est_mps": None if st.speed_est is None else round(st.speed_est, 2),
                "next_stop": nxt.stop_id if nxt else None,
                "next_stop_eta_ts": etas.get(nxt.stop_id) if nxt else None,
                "finished": st.finished,
                "gps_lost": st.bus_id in self._open_incident,
            })
        return out
