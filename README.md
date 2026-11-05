# FleetOps — Real-Time School Bus Tracking & Parent ETA Pipeline

A portfolio project: turn a stream of raw, messy bus GPS pings into live bus
positions, per-stop ETAs, "your bus is ~5 minutes away" alerts for parents, and
GPS-loss incidents for dispatchers.

> Independent project. Synthetic data only — no real routes, students, or company systems.

**Status: v0.1** — tracking core, simulator, HTTP API, live map, metrics, tests, benchmark.
Kafka and PostgreSQL/PostGIS come in v0.2 (see Roadmap). Nothing below is claimed unless it runs.

![Live map](docs/screenshot.png)

## What it does

```
simulator ──pings──▶ tracker ──▶ bus state + stop ETAs ──▶ /buses, live map
 (seeded,            (dedup, ordering,                 ──▶ parent alerts  (/notifications)
  fault injection)    route snapping, ETA)             ──▶ GPS_LOST incidents (/incidents)
                                                       ──▶ Prometheus metrics (/metrics)
```

- **Simulator** (`fleetops/simulator.py`) — seeded, so every run is reproducible.
  Generates routes, stops, and 5-second GPS pings with GPS noise, traffic, dwell at stops,
  breakdowns, duplicate pings, out-of-order delivery, and GPS dropouts. It also records
  **ground truth** (when each bus really reached each stop), which is how ETA accuracy is measured.
- **Tracker** (`fleetops/tracker.py`) — pure logic, no I/O, so the same code runs behind
  HTTP, a Kafka consumer, or a test.
  - Idempotent: a duplicate ping changes nothing (bounded per-bus dedup window).
  - Ordered: a ping older than one already applied is dropped, so a bus never moves backwards.
  - Snaps each ping onto the route within a window, so a noisy ping can't jump to the
    wrong part of a route that doubles back.
  - Exactly one alert per (trip, stop).
  - Opens a `GPS_LOST` incident after 60 s of silence; closes it when pings resume.
    Finished trips and stopped-but-pinging buses are never flagged.
- **API + map** (`fleetops/api.py`) — replays the simulator in accelerated real time.

## Engineering findings

**1. Cold-start ETAs fired alerts minutes early.** On a trip's first ping there is no
measured speed, so the ETA used a default speed. For a short route that made stops
2+ km away look under 5 minutes out, and the alert went out ~7 minutes early.
Found by tracing one bus in the benchmark. Fix: send no alerts until the bus has a
measured speed (`require_warmup`). Covered by `test_warmup_blocks_alerts_on_first_ping`.

**2. A naive moving-average ETA was wrong on average AND still alerted early.**
Averaging speed over every ping (including time standing at stops) makes the estimate
swing: it drops during each stop, then overshoots once the bus moves. On average it
predicts arrivals ~3 minutes too late — yet alerts fire *early*, because an alert is a
threshold crossing: it fires on the first prediction under 5 minutes, i.e. the most
optimistic one. Fix ("moving" mode): average speed only while the bus is moving, and
add a dwell allowance per stop ahead. ETA error 4–6 minutes out dropped from 272 s to
57 s in clean traffic, and early alerts in heavy traffic from 30.4% to 2.3%.

**3. A finished bus looked like a GPS failure.** The first simulator version sent no
ping at arrival, so the final stop was never confirmed and every completed trip opened
a false `GPS_LOST` incident (6 of 10 incidents in the first smoke run were false).
Fix: an explicit arrival ping, plus a test that finished buses are never flagged.

**Known limit:** breakdowns still cause ~26% early alerts — the alert went out before the
bus broke down, and no ETA model can foresee that. The fix belongs to the next layer:
detect the breakdown and send parents a "delayed" update (v0.3, incident agent).

## Benchmark (measured, reproducible)

`python bench/run_bench.py` — 5 seeds × 30 buses per scenario. Full report: [`bench/RESULTS.md`](bench/RESULTS.md).
Measured on Windows 11, Python 3.14. Alert-quality and fault results are bit-identical to a Linux / Python 3.13 run with the same seeds, so the pipeline is fully deterministic. Throughput varies by machine.

| | |
|---|---|
| Throughput, one in-memory tracker | 95,573 pings/sec, p50 10.7 µs / p99 19.3 µs per ping (300 buses, faults on) |
| Faults: 10% duplicates, 10% reordering, dropouts on 30% of trips | 0 duplicate alerts; 53/53 dropouts detected, 53/53 closed on recovery; 150/150 buses finished |

| scenario | ETA method | median alert lead | early alerts (>10 min) | ETA error 4–6 min out |
|---|---|---|---|---|
| clean | naive | 433 s | 0.4% | 272 s |
| clean | moving + warmup | 305 s | 0.0% | 57 s |
| heavy traffic | naive | 533 s | 30.4% | 208 s |
| heavy traffic | moving + warmup | 351 s | 2.3% | 104 s |
| breakdowns | naive | 531 s | 40.2% | 269 s |
| breakdowns | moving + warmup | 347 s | 25.8% | 112 s |

Target lead is 300 s. Only stops more than 6 minutes from the trip start are scored.

## Run it

```bash
python -m venv .venv
# Windows: .venv\Scripts\Activate.ps1    Linux/macOS: source .venv/bin/activate
pip install -e ".[dev]"
pytest                                  # 33 tests
python bench/run_bench.py               # benchmark report
uvicorn fleetops.api:app --port 8000    # then open http://localhost:8000
```

Or with Docker: `docker compose up --build`, then open http://localhost:8000.

Environment variables: `FLEETOPS_SEED` (42), `FLEETOPS_BUSES` (12),
`FLEETOPS_SPEEDUP` (10 = 10× real time), `FLEETOPS_FAULTS` (1).

## API

| Method | Path | |
|---|---|---|
| GET | `/buses` | All buses: position, progress, speed estimate, next stop + ETA, status |
| GET | `/buses/{id}` | One bus plus ETAs for every stop ahead |
| GET | `/routes` | Routes and stops |
| GET | `/notifications` | Recent parent alerts |
| GET | `/incidents` | GPS_LOST incidents (open and closed) |
| GET | `/stats` | Ingestion counters (accepted / duplicate / out-of-order / rejected) |
| POST | `/pings` | Ingest one ping (validated; idempotent) |
| GET | `/metrics` | Prometheus metrics |
| GET | `/docs` | OpenAPI docs |

## Design decisions

- **Drop late pings instead of re-ordering them.** Buffering to re-sort adds latency to
  every ping; at 5-second intervals one dropped ping costs nothing. The benchmark shows no
  alert or incident errors from it.
- **Flat local coordinates (metres).** Routes are a few km, so an equirectangular
  projection is accurate to well under a metre and keeps route snapping simple.
- **Pure tracker, I/O at the edges.** Makes replay deterministic and lets v0.2 put Kafka
  in front without touching the logic.

## Roadmap

- **v0.2** — Kafka ingestion (consumer group, schema-version check, dead-letter topic),
  PostgreSQL/PostGIS for routes, alerts and incidents, Grafana dashboard; measure end-to-end
  latency and consumer lag under replay and broker restarts.
- **v0.3** — breakdown and delay detection, plus an AI recovery agent with real tools,
  deterministic plan validation, and dispatcher approval.
- **v0.4** — electric bus charging scheduler (depot charger limits, tariff windows,
  state-of-charge constraints; greedy vs OR-Tools).
