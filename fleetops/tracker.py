"""Tracking core: turns a stream of raw GPS pings into bus state, stop ETAs,
parent "bus is approaching" notifications, and GPS-loss incidents.

Design rules (each one is covered by a test):
  1. Idempotent: the same ping delivered twice changes nothing the second time.
  2. Ordered per bus: a ping older than one already applied is dropped, so a
     late packet can never move a bus backwards.
  3. One "approaching" alert per (trip, stop). If the bus then breaks down,
     parents who were alerted get one "delayed" update, and one fresh
     alert once the bus is moving and close again.
  4. Pure logic, no I/O: the same class runs behind Kafka, an HTTP endpoint,
     or a test, and replaying the same input gives the same output.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from .geo import LocalFrame, Polyline
from .models import SCHEMA_VERSION, GpsPing, Incident, IncidentType, Notification, NotificationKind, Route

PASSED_TOL_M = 20.0        # within this distance of a stop counts as "at/passed"
MAX_SPEED_MPS = 30.0       # physical cap used to bound how far a bus can jump
SEEN_IDS_PER_BUS = 512     # dedup window
# Breakdown = less than MOVED_M of progress over a SLIDING window of
# breakdown_after_s. Two earlier versions failed on the benchmark:
#  * a 15 m threshold: progress only ratchets forward (max of noisy positions),
#    so a parked bus creeps 10-15 m from GPS noise; incidents closed mid-
#    breakdown and reopened;
#  * a fixed anchor ("no progress since the last 30 m step"): the anchor could
#    be set while the bus was still moving, so it stopped already ~20 m ahead of
#    it, creep added the rest, the window reset and detection came 91 s late.
# Ending a breakdown needs more progress than starting one (hysteresis).
MOVED_M = 30.0
RESUME_M = 50.0


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
    delayed: set[str] = field(default_factory=set)       # stops sent a DELAY_UPDATE this trip
    motion: deque = field(default_factory=deque)         # (ts, d) over the last breakdown window
    bd_anchor_d: float = 0.0                             # d when the open breakdown was detected
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
        breakdown_after_s: float = 150.0,
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
        # A bus that keeps pinging but makes no progress for this long, away from
        # the end of its trip, is a suspected breakdown. Stop dwell in the data
        # is at most 45 s, and slow traffic still moves, so 150 s leaves margin.
        self.breakdown_after_s = breakdown_after_s
        self._open_breakdown: dict[str, str] = {}     # bus_id -> incident_id
        self.notifications: list[Notification] = []
        self.incidents: dict[str, Incident] = {}
        self._open_incident: dict[str, str] = {}      # bus_id -> incident_id
        # Change tracking, so a persistence layer can save exactly what changed.
        self._dirty: set[str] = set()
        self._new_notifications: list[Notification] = []
        self._dirty_incidents: set[str] = set()
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
        new_trip = st.trip_id is not None and p.trip_id != st.trip_id
        if new_trip:
            if st.last_ts is not None and p.ts < st.last_ts:
                return "out_of_order"            # straggler from an older trip
            # New trip: the device's seq counter restarts, so reset trip state.
            st.trip_id, st.d, st.next_stop_idx, st.finished = p.trip_id, 0.0, 0, False
            st.last_seq, st.speed_est, st.last_ts = -1, None, None   # no speed across trips
            st.notified.clear()
            st.delayed.clear()
            st.motion.clear()
        if p.seq <= st.last_seq:
            # Older than something already applied. Applying it would move the
            # bus backwards and corrupt the speed estimate, so drop it.
            self._remember(st, p.event_id)
            return "out_of_order"

        self._remember(st, p.event_id)
        st.trip_id = p.trip_id
        self._dirty.add(st.bus_id)

        line = self.lines[st.route_id]
        x, y = self.frame.to_xy(p.lat, p.lon)
        dt = (p.ts - st.last_ts) if st.last_ts is not None else None
        if dt is not None and not new_trip:
            self._record_gap(st, p.ts)
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
        self._check_breakdown(st, p.ts)
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
        """Predicted arrival timestamp for every stop still ahead.

        Empty while a breakdown is open: showing "1 min" for a bus that isn't
        moving would be confidently wrong (it was, on the first map build)."""
        st = self.buses[bus_id]
        if st.last_ts is None or st.finished or bus_id in self._open_breakdown:
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
        if st.bus_id in self._open_breakdown:
            return                      # a stopped bus has no meaningful ETA
        for stop_id, eta in self.etas(st.bus_id).items():
            if stop_id in st.notified:
                continue
            if eta - now <= self.notify_lead_s:
                st.notified.add(stop_id)
                kind = (NotificationKind.APPROACHING_AFTER_DELAY if stop_id in st.delayed
                        else NotificationKind.BUS_APPROACHING)
                self._emit(st, stop_id, kind, now, eta)

    def _emit(self, st: BusState, stop_id: str, kind: NotificationKind, now: float, eta: float | None) -> None:
        n = Notification(kind=kind.value, bus_id=st.bus_id, trip_id=st.trip_id or "", stop_id=stop_id,
                         sent_ts=now, predicted_arrival_ts=eta)
        self.notifications.append(n)
        self._new_notifications.append(n)
        self.counters["notifications"] += 1

    # -------------------------------------------------------------- breakdowns
    def _check_breakdown(self, st: BusState, now: float) -> None:
        """Open/close BREAKDOWN_SUSPECTED from progress over a sliding window."""
        if st.bus_id in self._open_breakdown:
            if st.d - st.bd_anchor_d > RESUME_M:      # moving again
                iid = self._open_breakdown.pop(st.bus_id)
                self.incidents[iid].closed_ts = now
                self._dirty_incidents.add(iid)
                st.motion.clear()
            return
        st.motion.append((now, st.d))
        # Keep exactly one sample at or before the window start, so the oldest
        # sample tells us where the bus was breakdown_after_s ago.
        while len(st.motion) > 1 and st.motion[1][0] <= now - self.breakdown_after_s:
            st.motion.popleft()
        oldest_ts, oldest_d = st.motion[0]
        if st.finished or now - oldest_ts < self.breakdown_after_s:
            return
        if st.d - oldest_d >= MOVED_M:
            return
        st.bd_anchor_d = st.d
        iid = f"INC-{st.bus_id}-{st.trip_id}-BD-{int(oldest_ts)}"
        self.incidents[iid] = Incident(
            incident_id=iid, type=IncidentType.BREAKDOWN_SUSPECTED, bus_id=st.bus_id, opened_ts=now,
            detail=f"under {MOVED_M:.0f} m progress in {now - oldest_ts:.0f}s, at {st.d:.0f} m along route")
        self._open_breakdown[st.bus_id] = iid
        self._dirty_incidents.add(iid)
        # Parents already told "about 5 minutes" for stops still ahead now have
        # wrong information: tell them, and re-arm a fresh alert for later.
        stops = self.routes[st.route_id].stops
        for s in stops[st.next_stop_idx:]:
            if s.stop_id in st.notified and s.stop_id not in st.delayed:
                st.delayed.add(s.stop_id)
                st.notified.discard(s.stop_id)
                self._emit(st, s.stop_id, NotificationKind.DELAY_UPDATE, now, None)

    # --------------------------------------------------------------- incidents
    def check_stale(self, now: float | Callable[[str], float]) -> list[Incident]:
        """Open a GPS_LOST incident for any active bus silent too long.

        `now` is event time. It can be one number, or a function giving the
        event time to judge each bus against (a Kafka consumer reads several
        partitions that progress at different speeds, so 'now' differs per bus).
        """
        opened = []
        for st in self.buses.values():
            if st.last_ts is None or st.finished or st.bus_id in self._open_incident:
                continue
            bus_now = now(st.bus_id) if callable(now) else now
            if bus_now - st.last_ts > self.gps_lost_after_s:
                # Deterministic id: a replay after a crash produces the same id,
                # so the database can de-duplicate it.
                iid = f"INC-{st.bus_id}-{st.trip_id}-{int(st.last_ts)}"
                inc = Incident(incident_id=iid, type=IncidentType.GPS_LOST, bus_id=st.bus_id,
                               opened_ts=bus_now, detail=f"no GPS for {bus_now - st.last_ts:.0f}s")
                self.incidents[iid] = inc
                self._open_incident[st.bus_id] = iid
                self._dirty_incidents.add(iid)
                opened.append(inc)
        return opened

    def _record_gap(self, st: BusState, ping_ts: float) -> None:
        """A ping arrived after a silence longer than the GPS-loss threshold.

        If no incident is open yet (the periodic check never ran inside the
        gap, e.g. a consumer catching up on a backlog in large batches), record
        it now, already closed. Detection then does not depend on how often
        check_stale() happens to run. Same id as check_stale() would use."""
        if st.bus_id in self._open_incident or st.finished:
            return
        if ping_ts - st.last_ts <= self.gps_lost_after_s:
            return
        iid = f"INC-{st.bus_id}-{st.trip_id}-{int(st.last_ts)}"
        if iid in self.incidents:
            return
        opened = st.last_ts + self.gps_lost_after_s
        self.incidents[iid] = Incident(incident_id=iid, type=IncidentType.GPS_LOST, bus_id=st.bus_id,
                                       opened_ts=opened, closed_ts=ping_ts,
                                       detail=f"no GPS for {ping_ts - st.last_ts:.0f}s (found on resume)")
        self._dirty_incidents.add(iid)

    def _close_gps_lost(self, st: BusState, now: float) -> None:
        iid = self._open_incident.pop(st.bus_id, None)
        if iid:
            self.incidents[iid].closed_ts = now
            self._dirty_incidents.add(iid)

    # ------------------------------------------------------------------- views
    def snapshot(self) -> list[dict]:
        out = []
        for st in self.buses.values():
            out.append({
                "bus_id": st.bus_id, "route_id": st.route_id, "trip_id": st.trip_id,
                "lat": st.lat, "lon": st.lon, "last_ts": st.last_ts,
                "progress_m": round(st.d, 1),
                "speed_est_mps": None if st.speed_est is None else round(st.speed_est, 2),
                "finished": st.finished,
                **self._next_stop_view(st),
            })
        return out

    # ------------------------------------------------------- persistence hooks
    def drain_changes(self) -> tuple[list[dict], list[Notification], list[Incident]]:
        """Return everything changed since the last drain, and reset."""
        buses = [self.export_bus(b) for b in sorted(self._dirty)]
        notes, self._new_notifications = self._new_notifications, []
        incs = [self.incidents[i] for i in sorted(self._dirty_incidents)]
        self._dirty, self._dirty_incidents = set(), set()
        return buses, notes, incs

    def export_bus(self, bus_id: str) -> dict:
        st = self.buses[bus_id]
        return {
            "bus_id": st.bus_id, "route_id": st.route_id, "trip_id": st.trip_id,
            "last_seq": st.last_seq, "last_ts": st.last_ts, "lat": st.lat, "lon": st.lon,
            "d": st.d, "speed_est": st.speed_est, "next_stop_idx": st.next_stop_idx,
            "finished": st.finished, "notified": sorted(st.notified),
            "delayed": sorted(st.delayed), "motion": [list(m) for m in st.motion],
            "bd_anchor_d": st.bd_anchor_d,
            # derived, for read models; ignored by restore()
            **self._next_stop_view(st),
        }

    def _next_stop_view(self, st: BusState) -> dict:
        stops = self.routes[st.route_id].stops
        nxt = stops[st.next_stop_idx] if st.next_stop_idx < len(stops) else None
        return {
            "next_stop": nxt.stop_id if nxt else None,
            "next_stop_eta_ts": self.etas(st.bus_id).get(nxt.stop_id) if nxt else None,
            "gps_lost": st.bus_id in self._open_incident,
            "breakdown": st.bus_id in self._open_breakdown,
        }

    def restore(self, buses: list[dict], open_incidents: list[Incident]) -> None:
        """Rebuild state after a restart. The dedup window is not persisted;
        last_seq alone is enough to reject anything already applied."""
        for row in buses:
            st = self.buses.get(row["bus_id"])
            if st is None:
                continue
            st.trip_id, st.last_seq, st.last_ts = row["trip_id"], row["last_seq"], row["last_ts"]
            st.lat, st.lon, st.d, st.speed_est = row["lat"], row["lon"], row["d"], row["speed_est"]
            st.next_stop_idx, st.finished = row["next_stop_idx"], row["finished"]
            st.notified = set(row["notified"])
            st.delayed = set(row.get("delayed") or [])
            st.motion = deque(tuple(m) for m in (row.get("motion") or []))
            st.bd_anchor_d = row.get("bd_anchor_d") or 0.0
        for inc in open_incidents:
            self.incidents[inc.incident_id] = inc
            if inc.type == IncidentType.BREAKDOWN_SUSPECTED:
                self._open_breakdown[inc.bus_id] = inc.incident_id
            else:
                self._open_incident[inc.bus_id] = inc.incident_id
