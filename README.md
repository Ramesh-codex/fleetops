# FleetOps — Real-Time School Bus Tracking & Parent ETA Pipeline

A portfolio project: turn a stream of raw, messy bus GPS pings into live bus
positions, per-stop ETAs, "your bus is ~5 minutes away" alerts for parents, and
GPS-loss incidents for dispatchers.

> Independent project. Synthetic data only — no real routes, students, or company systems.

**Status: v0.3** — breakdown detection, parent delay updates, and an AI recovery agent that
proposes a replacement bus for a dispatcher to approve; on top of v0.2 (Kafka,
PostgreSQL/PostGIS, crash-safe consumer, dead letters, Prometheus + Grafana).
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

open BREAKDOWN incident ──▶ recovery worker ──▶ planner: Claude + read-only tools
                                               (rule planner if no key / timeout / bad output)
                                          ──▶ deterministic validator ──▶ plan: pending approval
dispatcher (map UI or API, role-checked) ──▶ approve: re-validate under row locks ──▶ assign spare,
                                             notify parents, audit log
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
  - Opens a `BREAKDOWN_SUSPECTED` incident when a bus that is still pinging makes less
    than 30 m of progress in a sliding 150 s window. Parents who were already told "about
    5 minutes" get a **delayed** update, then a fresh alert once the bus is moving and
    close again. No ETA is shown while a breakdown is open.
- **Consumer** (`fleetops/consumer.py`) — at-least-once from Kafka, effects applied once.
  Per batch: validate → bad messages to the dead-letter topic and table → good pings to the
  tracker → changed bus state + new alerts + incidents in **one Postgres transaction** →
  only then commit Kafka offsets. On restart the tracker is rebuilt from `bus_state`.
- **Broker** (`fleetops/broker.py`) — real Kafka (kafka-python, idempotent producer,
  manual commits) and an in-memory broker with the same semantics (partitions, keyed
  ordering, redelivery of uncommitted messages) so crash behaviour is unit-testable.
- **Database** (`fleetops/db.py`, `fleetops/migrations/`) — PostGIS geometry for routes,
  stops and bus positions; `UNIQUE(trip_id, stop_id)` on alerts as a second guard.
- **Recovery** (`fleetops/recovery/`) — see "AI recovery agent" below.
- **API + map** (`fleetops/api.py`) — demo mode replays the simulator in-process;
  db mode serves what the consumer wrote, plus `GET /stops/near` (PostGIS `ST_DWithin`).

## AI recovery agent

When a breakdown opens, the recovery worker builds a plan:

1. **Investigate with read-only tools** over a snapshot of the fleet: `get_incident_details`,
   `get_vehicle_status`, `get_route_details`, `find_available_buses`, `find_available_drivers`,
   `calculate_replacement_route`, `estimate_recovery_eta`, `validate_recovery_plan`, then
   `submit_plan_for_approval`. No tool can change anything; unknown ids return an error, not a guess.
2. **The model only chooses ids.** ETA, seats and riders in the stored plan are computed by the
   system. In the eval, the model claimed "2 minutes"; the plan stores the system's own number.
3. **Deterministic validator**: incident still open, spare exists and is free, enough seats for
   the route's riders, driver available at the same depot, ETA under 30 min. A failing proposal
   goes back to the model once as an error `tool_result`; after that the rule planner takes over.
4. **Rule planner fallback** (nearest feasible spare, smallest bus that fits) when there is no
   API key, the call times out, or the model can't produce a valid plan. The plan records which
   planner made it and why the LLM was bypassed.
5. **Dispatcher decides.** Nothing changes and no parent is told anything until approval.
   Approval re-runs the validator inside a transaction that locks the spare and driver rows,
   then assigns them, notifies parents on the remaining stops, and writes the audit log.
   Viewers can read plans but not decide; every denied attempt is audited.

Claude (`claude-haiku-5-5` by default) is used when `ANTHROPIC_API_KEY` is set; otherwise the rule
planner runs, so the demo and CI cost nothing. In CI and tests the model is replaced by a scripted
client with the same response shape as the Anthropic SDK. The Claude path has also been run live
against the full Docker stack: it planned real breakdowns, flagged tight capacity and an
unconfirmed breakdown as risks on its own, and every plan passed the validator.

| eval case (spec) | result | test |
|---|---|---|
| No replacement bus available | `no_feasible_plan`, reason stated | `test_case_no_replacement_bus_available` |
| Insufficient vehicle capacity | `no_feasible_plan`, "no available spare seats 90 riders" | `test_case_insufficient_capacity` |
| Driver unavailable | `no_feasible_plan`, "no available driver at …" | `test_case_driver_unavailable` |
| Stale GPS information | plan flagged with a stale-GPS risk | `test_case_stale_gps_is_flagged_as_risk` |
| Contradictory tool results (model says nothing fits, tools disagree) | model challenged; rule plan used | `test_case_llm_wrongly_says_nothing_is_feasible` |
| LLM timeout | rule planner, `llm_error` recorded | `test_case_llm_timeout_falls_back_to_rules` |
| Model invents a bus id | validator rejects twice; rule planner used | `test_case_llm_invents_a_bus_falls_back_to_rules` |
| Model corrects itself after validator feedback | second proposal accepted | `test_case_llm_corrects_itself_after_validator_feedback` |
| Duplicate incident events | exactly one plan | `test_case_duplicate_incident_event_creates_one_plan` |
| Approval rejected | rejected, audited; re-plan excludes that bus | `test_case_approval_rejected_then_replan_excludes_that_bus` |
| Unauthorized action attempt | 403, nothing changed, `approve_denied` audited | `test_case_unauthorized_action_attempt`, `test_api_roles_and_decisions` |
| Two plans, one spare | second approval blocked under lock, recorded | `test_two_plans_cannot_assign_the_same_spare` |
| Breakdown ends before a decision | approval blocked; plan superseded | `test_approval_blocked_after_breakdown_ends` |
| Model leaks markup into its text fields (seen live) | clean rationale, risks recovered | `test_case_llm_leaks_markup_into_rationale` |

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
tracker: 514 re-alert attempts over 18 crashes on real Kafka, and only the database
constraint kept parents from receiving them. Two layers, each tested on its own.

**8. One image, three services, one wrong health check.** The Dockerfile's health check
called the API's `/health`, but the consumer and producer run from the same image and
don't serve it, so Compose marked the consumer unhealthy and the stack never came up in CI.
Fix: each service declares what "healthy" means for it (consumer: its metrics endpoint).

**9. Every consumer panel in Grafana had a phantom zero series.** Seen on the live
dashboard: consumer lag, alerts and GPS_LOST each showed two lines, one stuck at 0. The API
imported two startup helpers from `consumer.py`; importing that module registered the
consumer's Prometheus metrics in the API process too, and Prometheus scrapes both. Fix:
helpers moved to `runtime.py`, dashboard queries scoped to `job="consumer"`, and a
regression test that starts the API in db mode and checks its `/metrics`.

**10. Restarting the producer rewound the world.** Also seen live: after `docker compose up
--build`, accepted pings dropped to 0 and out-of-order rose to ~22/s. The restarted producer
replayed "day 0" with the same ids and timestamps, and the consumer correctly rejected all of
it as already applied (0 duplicate alerts). The consumer was right; the simulator was wrong.
Plain wall-clock anchoring doesn't fix it either: at 10x, simulated time runs ahead of the
wall clock, so a restart would land behind pings already sent and be dropped as stale. Fix:
a virtual clock (`origin + (wall - origin) * speedup`) that only moves forward, plus a
per-start id tag. Regression test restarts the producer mid-day and checks the new trips
are accepted.

### v0.3 (breakdowns)

**11. Breakdowns left parents with confidently wrong alerts.** In the breakdown scenario,
25.8% of parents' last message was a "5 minutes away" alert sent over 10 minutes early: the
alert went out, then the bus broke down. No ETA model can predict that, so the fix is
detection plus correction: a breakdown incident, a "delayed" update to every parent already
alerted for a stop ahead, and a fresh alert once the bus moves. Early: 25.8% → 0.3%;
median lead of the last alert: 347 s → 299 s (target 300); 0 false breakdowns in clean or
heavy-traffic runs.

**12. Two breakdown detectors that looked right and weren't.** Both were caught by
comparing incidents against the simulator's ground truth.
- *15 m "moved" threshold:* route progress only ratchets forward (the max of noisy
  positions), so a parked bus creeps 10–15 m from GPS noise alone. Incidents closed
  mid-breakdown and reopened (2 buses got 2 incidents each).
- *Fixed anchor ("no progress since the last 30 m step"):* the anchor could be set while the
  bus was still moving, so it stopped ~20 m past it, creep added the rest, the window reset,
  and detection came 91 s late.
- Fix: a sliding window ("< 30 m in the last 150 s") plus hysteresis (50 m of progress to
  end a breakdown). Across 6 seeds and 60 breakdowns: exactly one incident each, detected
  146–152 s after the stop in the typical case, 0 false positives.

### v0.3 (recovery agent)

**13. Every concurrent breakdown was offered the same spare and driver.** Seen in the first demo
data: three plans, all "send SP-201 with DRV-11". Pending plans reserved nothing, so the
approval lock would have refused every approval after the first. Fix: spares and drivers named in
another incident's pending plan are soft-reserved for planning. Regression test plans three
simultaneous breakdowns and approves all of them.

**14. The fix for 13 created a deadlock.** Applying soft reservations at *approval* time meant two
pending plans naming the same spare each blocked the other, so neither could ever be approved.
Caught by the test that forces that clash. Reservations now guide planning only; at approval the
row lock and real assignment status decide, and the first approval wins.

**15. Every plan said "stale GPS".** Seen on the map: buses reporting normally were flagged
139–266 s stale. The recovery code measured GPS age against the newest timestamp across all
buses, and during a backlog some Kafka partitions run minutes ahead: the same mistake as
finding 5, made again in a new module. Fix: use the consumer's own partition-aware judgement
(`bus_state.gps_lost`) instead of recomputing it.

**16. A blocked approval would have erased its own audit record.** Raising the "plan no longer
valid" error inside the transaction rolled back the audit row and the status change with it.
Caught in review before the tests ran; the race test now asserts the `approve_blocked` row exists.

**17. A real model put its risk list inside the rationale.** Only visible in the live run with
Claude: one plan's "Why" ended in raw markup (`</rationale> <parameter name="risks">[...]`) and its
risk list was empty. The ids and numbers were fine, since the validator checks those, but the
dispatcher saw junk text and lost the model's warnings. Fix: the submit tool is now `strict`, and
the free-text fields are cleaned before storing: the leaked part is split off, the risk list is
recovered when it parses, stray tags are removed. The scripted-client tests could never have
produced this; it took a real model.

**Known limit:** after a breakdown, stops just past where the bus stopped get their fresh
alert late (10% of scored stops in the breakdown scenario; 92 of the 93 late alerts are
these). The earliest anyone can know the bus is coming again is when it moves, and a stop
1 minute away then gets a 1-minute warning. Those parents have already received the
"delayed" update, so they are not waiting outside on wrong information.

## Benchmark (measured, reproducible)

`python bench/run_bench.py` — 5 seeds × 30 buses per scenario. Full report: [`bench/RESULTS.md`](bench/RESULTS.md).
Measured on Windows 11, Python 3.14. Alert-quality and fault results are bit-identical to a Linux / Python 3.13 run with the same seeds, so the pipeline is fully deterministic. Throughput varies by machine.

| | |
|---|---|
| Throughput, one in-memory tracker | 82,846 pings/sec, p50 12.1 µs / p99 24.8 µs per ping (300 buses, faults on) |
| Faults: 10% duplicates, 10% reordering, dropouts on 30% of trips | 0 duplicate alerts; 53/53 dropouts detected, 53/53 closed on recovery; 150/150 buses finished |

| scenario | ETA method | median alert lead | early alerts (>10 min) | ETA error 4–6 min out |
|---|---|---|---|---|
| clean | naive | 433 s | 0.4% | 272 s |
| clean | moving + warmup | 305 s | 0.0% | 57 s |
| heavy traffic | naive | 533 s | 30.4% | 208 s |
| heavy traffic | moving + warmup | 351 s | 2.3% | 104 s |
| breakdowns | naive | 531 s | 40.2% | 269 s |
| breakdowns | moving + warmup | 347 s | 25.8% | 112 s |
| breakdowns | + breakdown detection & delay updates | 299 s | 0.3% | 103 s |

Scored on the **last message each parent got before the bus arrived** (a "delayed" update
cancels an earlier alert). Target lead is 300 s. Only stops more than 6 minutes from the
trip start are scored. Breakdown detection raised 0 false incidents in every scenario.

### Pipeline benchmark (Kafka → consumer → Postgres)

![Grafana dashboard](docs/grafana.png)

*Live run on Docker Desktop. 16:20-16:26: a restarted producer replays the morning and the consumer rejects all of it as already applied (orange; finding 10). From 16:26: the fixed producer's new trips are accepted and alerts resume. Alerts blocked by the DB stay at 0 throughout.*

`python bench/pipeline_bench.py` against `docker compose up -d kafka postgres`. CI runs it
on every push against real Kafka 3.9 and posts the report in the run summary.
Numbers below are from a GitHub Actions runner: [`bench/PIPELINE_RESULTS.md`](bench/PIPELINE_RESULTS.md).

| | |
|---|---|
| Backlog drain, one consumer (validate + track + Postgres transaction + offset commit) | 9,731 msg/s; batch of 500: p50 27 ms, p99 48 ms (38,047 pings, 200 buses) |
| Crash between DB commit and offset commit, every 4th batch (18 crashes) | 0 duplicate alerts stored, 0 missing vs an uninterrupted run |
| … restarting with state restored from Postgres | 0 re-alert attempts |
| … restarting with an empty tracker | 514 re-alert attempts, all blocked only by the DB constraint |

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

Recovery agent with Claude instead of the rule planner (optional, uses your API credits):

```powershell
$env:ANTHROPIC_API_KEY = "sk-ant-..."     # Linux/macOS: export ANTHROPIC_API_KEY=...
docker compose up -d recovery
```

On the map, "Act as" switches between viewer, dispatcher and admin using demo tokens
(`dev-viewer`, `dev-dispatcher`, `dev-admin`). Set `FLEETOPS_DEV_AUTH=0` and
`FLEETOPS_TOKENS="dispatcher:<secret>,..."` to turn the demo tokens off.

Environment variables: `FLEETOPS_MODE` (demo | db), `FLEETOPS_PLANNER` (auto | rules | llm),
`FLEETOPS_LLM_MODEL`, `FLEETOPS_KAFKA`, `FLEETOPS_DB`,
`FLEETOPS_SEED` (42), `FLEETOPS_BUSES` (12), `FLEETOPS_SPEEDUP` (10 = 10× real time),
`FLEETOPS_FAULTS` (1), `FLEETOPS_POISON` (0.001 = share of malformed messages).

## API

| Method | Path | |
|---|---|---|
| GET | `/buses` | All buses: position, progress, speed estimate, next stop + ETA, status |
| GET | `/buses/{id}` | One bus plus ETAs for every stop ahead |
| GET | `/routes` | Routes and stops |
| GET | `/notifications` | Recent parent messages: approaching, delayed, approaching after delay |
| GET | `/incidents` | GPS_LOST and BREAKDOWN_SUSPECTED incidents (open and closed) |
| GET | `/stats` | Demo: ingestion counters. DB: stored alerts / incidents / dead letters |
| GET | `/stops/near?lat=&lon=&radius_m=` | Stops near a point, nearest first (PostGIS in db mode) |
| POST | `/pings` | Demo mode only: ingest one ping (validated; idempotent) |
| GET | `/plans`, `/plans/{id}` | Recovery plans (any role) |
| POST | `/plans/{id}/approve`, `/plans/{id}/reject` | Dispatcher or admin |
| POST | `/incidents/{id}/replan` | New plan after a rejection (dispatcher or admin) |
| GET | `/audit` | Audit log (dispatcher or admin) |
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
- **v0.3.x** — measure LLM vs rule planner on a fixed set of breakdowns (agreement, latency,
  cost); route-level ETA for the replacement instead of straight-line × road factor.
- **v0.4** — electric bus charging scheduler (depot charger limits, tariff windows,
  state-of-charge constraints; greedy vs OR-Tools).
