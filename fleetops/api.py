"""HTTP service: replays the simulator through the tracker in (accelerated)
real time and serves state, alerts, incidents, metrics, and a live map.

Run:  uvicorn fleetops.api:app --port 8000
Env:  FLEETOPS_SEED (42), FLEETOPS_BUSES (12), FLEETOPS_SPEEDUP (10),
      FLEETOPS_FAULTS (1 = inject duplicates/reorders/dropouts/breakdowns)
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

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


rt: Runtime | None = None


async def _replay_loop() -> None:
    while True:
        rt.step()
        await asyncio.sleep(0.2)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global rt
    rt = Runtime()
    task = asyncio.create_task(_replay_loop())
    yield
    task.cancel()


app = FastAPI(title="FleetOps tracking", version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/health")
def health():
    return {"status": "ok", "run": rt.run_no, "sim_now": rt.sim_now}


@app.get("/routes")
def routes():
    return [r.model_dump() for r in rt.sim.routes.values()]


@app.get("/buses")
def buses():
    return {"sim_now": rt.sim_now, "buses": rt.tracker.snapshot()}


@app.get("/buses/{bus_id}")
def bus(bus_id: str):
    if bus_id not in rt.tracker.buses:
        raise HTTPException(404, "unknown bus")
    snap = next(b for b in rt.tracker.snapshot() if b["bus_id"] == bus_id)
    return {**snap, "stop_etas": rt.tracker.etas(bus_id)}


@app.get("/notifications")
def notifications(limit: int = 50):
    return [n.model_dump() for n in rt.tracker.notifications[-limit:][::-1]]


@app.get("/incidents")
def incidents():
    return [i.model_dump() for i in sorted(rt.tracker.incidents.values(), key=lambda i: -i.opened_ts)]


@app.get("/stats")
def stats():
    return {"run": rt.run_no, "counters": rt.tracker.counters,
            "pings_replayed": rt.cursor, "pings_total": len(rt.sim.pings)}


@app.post("/pings")
def post_ping(p: GpsPing):
    """External ingestion entry point (a Kafka consumer will call the same code path)."""
    return {"outcome": rt.ingest(p)}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")
