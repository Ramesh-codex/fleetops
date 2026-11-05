# FleetOps — Real-Time School Bus Tracking & Parent ETA Pipeline

A portfolio project: turn a stream of raw, messy bus GPS pings into live bus
positions, per-stop ETAs, "your bus is ~5 minutes away" alerts for parents, and
GPS-loss incidents for dispatchers.

> Independent project. Synthetic data only — no real routes, students, or company systems.

**Status: v0.2** — Kafka ingestion, PostgreSQL/PostGIS persistence, crash-safe consumer,
dead-letter handling, Prometheus + Grafana, on top of the v0.1 tracking core.
Nothing below is claimed unless it runs.

![Live map](docs/screenshot.png)

## Architecture

```
producer ──▶ Kafka: gps.pings.v1 ──▶ consumer ──────────────────▶ PostgreSQL + PostGIS ──▶ API + live map
(simulator,   key = bus_id, so      decode/validate              bus_state, alerts,        (FLEETOPS_MODE=db)
 faults,      each bus's pings      ├─ bad ─▶ gps.pings.dlq      incidents, dead_letters
 poison msgs) stay in order         └─ good ─▶ tracker            ONE transaction / batch,
                                                                  THEN Kafka offset commit
        consumer + API ──/metrics──▶ Prometheus ──▶ Grafana ("FleetOps pipeline" dashboard)
```

`FLEETOPS_MODE=demo` (default) still runs the whole thing in one process with no Kafka or
database, which is what `uvicorn fleetops.api:app` gives you.

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
- **Consumer** (`fleetops/consumer.py`) — at-least-once from Kafka, effects applied once.
  Per batch: validate → bad messages to the dead-letter topic and table → good pings to the
  tracker → changed bus state + new alerts + incidents in **one Postgres transaction** →
  only then commit Kafka offsets. On restart the tracker is rebuilt from `bus_state`.
- **Broker** (`fleetops/broker.py`) — real Kafka (kafka-python, idempotent producer,
  manual commits) and an in-memory broker with the same semantics (partitions, keyed
  ordering, redelivery of uncommitted messages) so crash behaviour is unit-testable.
- **Database** (`fleetops/db.py`, `fleetops/migrations/`) — PostGIS geometry for routes,
  stops and bus positions; `UNIQUE(trip_id, stop_id)` on alerts as a second guard.
- **API + map** (`fleetops/api.py`) — demo mode replays the simulator in-process;
  db mode serves what the consumer wrote, plus `GET /stops/near` (PostGIS `ST_DWithin`).

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

### v0.2 (pipeline)

**4. Kafka commits for rows that were never committed.** A plain read on the psycopg
connection silently opened an outer transaction, so every `with conn.transaction():`
batch became a SAVEPOINT inside it. The consumer believed each batch was saved and
committed the Kafka offsets, while no other connection could see the rows — and a crash
would have lost them for good. The in-process tests passed because they read through the
same connection. Caught when the API (a second connection) showed an empty database.
Fix: autocommit connections, so each `transaction()` block is a real COMMIT. Regression
test reads through a separate connection, and fails without the fix.

**5. Parallel partitions made healthy buses look silent.** Using the newest timestamp seen
on *any* partition as "now" opened false GPS-loss incidents for buses on partitions that
were simply behind (3 false incidents for 6 buses in the first run). Fix: each bus is
judged against its own partition's event-time watermark, falling back to the global one
only when its whole partition has gone quiet.

**6. Batch size decided whether a GPS dropout was seen.** While catching up on a backlog,
one 200-message batch covered ~165 s of bus time, so a 90–240 s dropout could start and end
between two checks and never be recorded. Fix: the tracker also records the gap when the
next ping arrives, with the same incident id the periodic check would have used. Pipeline
incidents now match a single ordered stream exactly.

**7. Restoring tracker state is what prevents duplicate alerts after a crash.** The consumer
was killed between the DB commit and the Kafka offset commit every 4th batch. Restarting
with tracker state restored from Postgres: 0 re-alert attempts. Restarting with an empty
tracker: every redelivered ping re-triggered alerts, and only the database constraint kept
parents from receiving them (numbers in the pipeline benchmark below).

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

### Pipeline benchmark (Kafka → consumer → Postgres)

`python bench/pipeline_bench.py` against `docker compose up -d kafka postgres`. CI runs it
on every push against real Kafka and posts the report in the run summary.
Results: [`bench/PIPELINE_RESULTS.md`](bench/PIPELINE_RESULTS.md) (pending first real-Kafka run).

## Run it

```bash
python -m venv .venv
# Windows: .venv\Scripts\Activate.ps1    Linux/macOS: source .venv/bin/activate
pip install -e ".[dev]"
pytest                                  # pipeline tests skip without a database
python bench/run_bench.py               # tracking benchmark
uvicorn fleetops.api:app --port 8000    # demo mode: open http://localhost:8000
```

Full pipeline with Docker Desktop:

```bash
docker compose up --build -d
# map        http://localhost:8000
# grafana    http://localhost:3000   (dashboard "FleetOps pipeline", no login)
# prometheus http://localhost:9090
```

Pipeline tests and benchmark against the compose Kafka and Postgres:

```bash
docker compose up -d kafka postgres
docker compose exec postgres psql -U postgres -c "CREATE DATABASE fleetops_test"
# Windows PowerShell: $env:FLEETOPS_TEST_DB="..."; $env:FLEETOPS_TEST_KAFKA="localhost:9094"
FLEETOPS_TEST_DB=postgresql://postgres:postgres@localhost:5432/fleetops_test \
FLEETOPS_TEST_KAFKA=localhost:9094 pytest
python bench/pipeline_bench.py --out bench/PIPELINE_RESULTS.md
```

Environment variables: `FLEETOPS_MODE` (demo | db), `FLEETOPS_KAFKA`, `FLEETOPS_DB`,
`FLEETOPS_SEED` (42), `FLEETOPS_BUSES` (12), `FLEETOPS_SPEEDUP` (10 = 10× real time),
`FLEETOPS_FAULTS` (1), `FLEETOPS_POISON` (0.001 = share of malformed messages).

## API

| Method | Path | |
|---|---|---|
| GET | `/buses` | All buses: position, progress, speed estimate, next stop + ETA, status |
| GET | `/buses/{id}` | One bus plus ETAs for every stop ahead |
| GET | `/routes` | Routes and stops |
| GET | `/notifications` | Recent parent alerts |
| GET | `/incidents` | GPS_LOST incidents (open and closed) |
| GET | `/stats` | Demo: ingestion counters. DB: stored alerts / incidents / dead letters |
| GET | `/stops/near?lat=&lon=&radius_m=` | Stops near a point, nearest first (PostGIS in db mode) |
| POST | `/pings` | Demo mode only: ingest one ping (validated; idempotent) |
| GET | `/metrics` | Prometheus metrics |
| GET | `/docs` | OpenAPI docs |

## Design decisions

- **Drop late pings instead of re-ordering them.** Buffering to re-sort adds latency to
  every ping; at 5-second intervals one dropped ping costs nothing. The benchmark shows no
  alert or incident errors from it.
- **Flat local coordinates (metres).** Routes are a few km, so an equirectangular
  projection is accurate to well under a metre and keeps route snapping simple.
- **Pure tracker, I/O at the edges.** Makes replay deterministic; v0.2 put Kafka and
  Postgres around it with only small additions (change tracking, state export/restore).
- **Key Kafka messages by bus id.** Per-bus order is all the tracker needs, and it lets
  the topic scale out by partitions.
- **Commit offsets after the DB transaction, not before.** Before would lose data on a
  crash; after can redeliver, which the restored `last_seq` and the alert constraint absorb.
- **Kafka is the only source of new data; Postgres holds derived state.** The API never
  writes, so it can scale independently and can't corrupt tracker state.

## Roadmap

- **v0.2.x** — measure end-to-end latency and lag recovery under a broker restart; run two
  consumers in one group and verify partition rebalancing.
- **v0.3** — breakdown and delay detection, plus an AI recovery agent with real tools,
  deterministic plan validation, and dispatcher approval.
- **v0.4** — electric bus charging scheduler (depot charger limits, tariff windows,
  state-of-charge constraints; greedy vs OR-Tools).
