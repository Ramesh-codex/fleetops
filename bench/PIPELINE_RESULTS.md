# FleetOps pipeline benchmark

Machine: GitHub Actions runner (Linux-7.0.0-1019-azure-x86_64), Python 3.12.16. Broker: real Kafka 3.9.1
(KRaft, single node, 4 partitions) at localhost:9094, PostGIS 16-3.4, both from `docker compose`.
Synthetic data only. Source: CI run 42133302329, commit fc735cd.

## 1. Backlog throughput (one consumer, process + Postgres write + offset commit)

38,047 pings from 200 buses already in the topic; batches of up to 500.

| produce rate | consume rate (end to end) | batch p50 | batch p99 | alerts written |
|---|---|---|---|---|
| 8,433 msg/s | 9,731 msg/s | 27 ms | 48 ms | 1,798 |

## 2. Crash recovery (consumer killed after the DB commit, before the offset commit, every 4th batch)

| restart strategy | crashes | duplicate alerts stored | alert inserts blocked by DB | missing vs no-crash run | extra vs no-crash run |
|---|---|---|---|---|---|
| restore tracker state from Postgres | 18 | 0 | 0 | 0 / 270 | 0 |
| start with an empty tracker | 18 | 0 | 514 | 0 / 270 | 0 |

'Blocked by DB' = the tracker tried to alert again and only the UNIQUE(trip_id, stop_id) constraint stopped it.
'Missing'/'extra' compare against one uninterrupted run.
