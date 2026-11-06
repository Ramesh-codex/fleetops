"""HTTP service: bus state, alerts, incidents, metrics and a live map.

Two modes (FLEETOPS_MODE):
  demo (default) - replays the simulator through an in-process tracker.
                   No Kafka or database needed.
  db             - read-only view of what the Kafka consumer wrote to Postgres.

Run:  uvicorn fleetops.api:app --port 8000
Env:  FLEETOPS_MODE, FLEETOPS_DB, FLEETOPS_SEED (42), FLEETOPS_BUSES (12),
      FLEETOPS_SPEEDUP (10), FLEETOPS_FAULTS (1)
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from .geo import LocalFrame
from .models import GpsPing
from .simulator import SimConfig, simulate
from .tracker import Tracker

PINGS = Counter("fleetops_pings_total", "Pings processed by outcome", ["outcome"])
INGEST = Histogram("fleetops_ingest_seconds", "Time to apply one ping",
                   buckets=(1e-6, 5e-6, 1e-5, 2.5e-5, 5e-5, 1e-4, 5e-4, 1e-3, 1e-2))
ALERTS = Counter("fleetops_notifications_total", "Parent 'bus approaching' alerts sent")
GPS_LOST_OPEN = Gauge("fleetops_gps_lost_open", "Buses currently in GPS_LOST")
ACTIVE = Gauge("fleetops_buses_active", "Buses with an unfinished trip")
SIM_LAG = Gauge("fleetops_sim_lag_seconds", "How far ingestion lags the simulated clock")

STATIC = Path(__file__).parent / "static"


class Runtime:
    """Holds the current simulation run and its tracker."""

    def __init__(self) -> None:
        self.seed = int(os.getenv("FLEETOPS_SEED", "42"))
        self.n_buses = int(os.getenv("FLEETOPS_BUSES", "12"))
        self.speedup = float(os.getenv("FLEETOPS_SPEEDUP", "10"))
        self.faults = os.getenv("FLEETOPS_FAULTS", "1") == "1"
        self.run_no = 0
        self.new_run()

    def new_run(self) -> None:
        cfg = SimConfig(seed=self.seed + self.run_no, n_buses=self.n_buses)
        if self.faults:
            cfg.duplicate_prob, cfg.reorder_prob = 0.03, 0.03
            cfg.dropout_prob, cfg.breakdown_prob = 0.25, 0.15
        self.sim = simulate(cfg)
        self.tracker = Tracker(self.sim.routes, self.sim.frame, self.sim.bus_route)
        self.cursor = 0
        self.sim_t0 = self.sim.pings[0].ts
        self.wall_t0 = time.monotonic()
        self.run_no += 1

    @property
    def sim_now(self) -> float:
        return self.sim_t0 + (time.monotonic() - self.wall_t0) * self.speedup

    def ingest(self, p: GpsPing) -> str:
        before = len(self.tracker.notifications)
        t0 = time.perf_counter()
        outcome = self.tracker.ingest(p)
        INGEST.observe(time.perf_counter() - t0)
        PINGS.labels(outcome).inc()
        ALERTS.inc(len(self.tracker.notifications) - before)
        return outcome

    def step(self) -> None:
        now = self.sim_now
        pings = self.sim.pings
        while self.cursor < len(pings) and pings[self.cursor].ts <= now:
            self.ingest(pings[self.cursor])
            self.cursor += 1
        self.tracker.check_stale(now)
        snap = self.tracker.snapshot()
        GPS_LOST_OPEN.set(sum(b["gps_lost"] for b in snap))
        ACTIVE.set(sum(not b["finished"] for b in snap))
        if self.cursor < len(pings):
            SIM_LAG.set(max(0.0, now - pings[self.cursor].ts))
        # Loop the demo: start a fresh morning 30 sim-seconds after the last ping.
        if self.cursor >= len(pings) and now > pings[-1].ts + 30:
            self.new_run()


MODE = os.getenv("FLEETOPS_MODE", "demo")
rt: Runtime | None = None
pg = None          # psycopg connection in db mode


async def _replay_loop() -> None:
    while True:
        rt.step()
        await asyncio.sleep(0.2)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global rt, pg
    task = None
    if MODE == "db":
        from . import db
        from .runtime import ensure_seeded, wait_for
        pg = wait_for(lambda: db.connect(), "postgres")
        ensure_seeded(pg)
    else:
        rt = Runtime()
        task = asyncio.create_task(_replay_loop())
    yield
    if task:
        task.cancel()
    if pg:
        pg.close()


app = FastAPI(title="FleetOps tracking", version="0.2.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


def _db():
    from . import db
    return db


@app.get("/health")
def health():
    if MODE == "db":
        row = pg.execute("SELECT max(last_ts) AS t FROM bus_state").fetchone()
        return {"status": "ok", "mode": MODE, "run": 1, "sim_now": row["t"]}
    return {"status": "ok", "mode": MODE, "run": rt.run_no, "sim_now": rt.sim_now}


@app.get("/routes")
def routes():
    if MODE == "db":
        return [r.model_dump() for r in _db().load_routes(pg)[0].values()]
    return [r.model_dump() for r in rt.sim.routes.values()]


@app.get("/buses")
def buses():
    if MODE == "db":
        rows = _db().q_buses(pg)
        now = max((r["last_ts"] or 0 for r in rows), default=None)
        return {"sim_now": now, "buses": rows}
    return {"sim_now": rt.sim_now, "buses": rt.tracker.snapshot()}


@app.get("/buses/{bus_id}")
def bus(bus_id: str):
    if MODE == "db":
        row = next((b for b in _db().q_buses(pg) if b["bus_id"] == bus_id), None)
        if row is None:
            raise HTTPException(404, "unknown bus")
        return row
    if bus_id not in rt.tracker.buses:
        raise HTTPException(404, "unknown bus")
    snap = next(b for b in rt.tracker.snapshot() if b["bus_id"] == bus_id)
    return {**snap, "stop_etas": rt.tracker.etas(bus_id)}


@app.get("/notifications")
def notifications(limit: int = Query(50, ge=1, le=1000)):
    if MODE == "db":
        return _db().q_notifications(pg, limit)
    return [n.model_dump() for n in rt.tracker.notifications[-limit:][::-1]]


@app.get("/incidents")
def incidents():
    if MODE == "db":
        return _db().q_incidents(pg)
    return [i.model_dump() for i in sorted(rt.tracker.incidents.values(), key=lambda i: -i.opened_ts)]


@app.get("/stops/near")
def stops_near(lat: float = Query(ge=-90, le=90), lon: float = Query(ge=-180, le=180),
               radius_m: float = Query(500, gt=0, le=20000)):
    """Stops within radius_m of a point, nearest first (PostGIS in db mode)."""
    if MODE == "db":
        return _db().q_stops_near(pg, lat, lon, radius_m)
    frame = LocalFrame(lat, lon)
    out = []
    for r in rt.sim.routes.values():
        for st in r.stops:
            x, y = frame.to_xy(st.lat, st.lon)
            dist = math.hypot(x, y)
            if dist <= radius_m:
                out.append({"stop_id": st.stop_id, "route_id": r.route_id, "name": st.name,
                            "lat": st.lat, "lon": st.lon, "distance_m": dist})
    return sorted(out, key=lambda s: s["distance_m"])[:50]


@app.get("/stats")
def stats():
    if MODE == "db":
        return {"run": 1, "counters": _db().q_counts(pg)}
    return {"run": rt.run_no, "counters": rt.tracker.counters,
            "pings_replayed": rt.cursor, "pings_total": len(rt.sim.pings)}


@app.post("/pings")
def post_ping(p: GpsPing):
    """Demo-mode ingestion entry point. In db mode pings arrive through Kafka."""
    if MODE == "db":
        raise HTTPException(405, "db mode: publish pings to Kafka instead")
    return {"outcome": rt.ingest(p)}


# ----------------------------------------------------------- recovery (db mode)
def _tokens() -> dict[str, tuple[str, str]]:
    """token -> (actor, role). FLEETOPS_TOKENS="role:token,..." overrides the
    dev tokens, which exist only when FLEETOPS_DEV_AUTH is not 0."""
    raw = os.getenv("FLEETOPS_TOKENS")
    if not raw and os.getenv("FLEETOPS_DEV_AUTH", "1") != "0":
        raw = "admin:dev-admin,dispatcher:dev-dispatcher,viewer:dev-viewer"
    out = {}
    for part in (raw or "").split(","):
        if ":" in part:
            role, tok = part.split(":", 1)
            out[tok.strip()] = (f"{role.strip()}-user", role.strip())
    return out


def _who(authorization: str | None) -> tuple[str, str]:
    tok = (authorization or "").removeprefix("Bearer ").strip()
    who = _tokens().get(tok)
    if who is None:
        raise HTTPException(401, "missing or invalid token")
    return who


def _db_only():
    if MODE != "db":
        raise HTTPException(501, "recovery runs in db mode (docker compose)")


@app.get("/auth/dev-tokens")
def dev_tokens():
    """Demo convenience for the map's 'act as' selector. Disabled with FLEETOPS_DEV_AUTH=0."""
    if os.getenv("FLEETOPS_DEV_AUTH", "1") == "0" or os.getenv("FLEETOPS_TOKENS"):
        raise HTTPException(404)
    return {role: tok for tok, (_actor, role) in _tokens().items()}


@app.get("/plans")
def plans(limit: int = Query(20, ge=1, le=200), authorization: str | None = Header(None)):
    _db_only()
    _who(authorization)
    from .recovery import service
    return service.list_plans(pg, limit)


@app.get("/plans/{plan_id}")
def plan(plan_id: str, authorization: str | None = Header(None)):
    _db_only()
    _who(authorization)
    from .recovery import service
    p = service.get_plan(pg, plan_id)
    if p is None:
        raise HTTPException(404, "unknown plan")
    return p


class Decision(BaseModel):
    note: str = Field("", max_length=500)


def _decide(fn, *args):
    from .recovery import service
    try:
        return fn(pg, *args)
    except service.Forbidden as e:
        raise HTTPException(403, str(e))
    except service.Conflict as e:
        raise HTTPException(409, {"error": str(e), **e.detail})
    except KeyError:
        raise HTTPException(404, "not found")


@app.post("/plans/{plan_id}/approve")
def approve_plan(plan_id: str, authorization: str | None = Header(None)):
    _db_only()
    actor, role = _who(authorization)
    from .recovery import service
    return _decide(service.approve, plan_id, actor, role)


@app.post("/plans/{plan_id}/reject")
def reject_plan(plan_id: str, body: Decision = Decision(), authorization: str | None = Header(None)):
    _db_only()
    actor, role = _who(authorization)
    from .recovery import service
    return _decide(service.reject, plan_id, actor, role, body.note)


@app.post("/incidents/{incident_id}/replan")
def replan_incident(incident_id: str, authorization: str | None = Header(None)):
    _db_only()
    actor, role = _who(authorization)
    from .recovery import service
    from .recovery.worker import make_planner
    return _decide(service.replan, incident_id, make_planner(), actor, role)


@app.get("/audit")
def audit(limit: int = Query(100, ge=1, le=1000), authorization: str | None = Header(None)):
    _db_only()
    _actor, role = _who(authorization)
    if role not in ("admin", "dispatcher"):
        raise HTTPException(403, "audit log is for dispatchers and admins")
    from .recovery import service
    return service.audit_log(pg, limit)


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")
