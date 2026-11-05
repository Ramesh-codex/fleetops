"""Message broker interface with two implementations:

* KafkaBroker  — the real thing (kafka-python), used in Docker Compose and CI.
* MemoryBroker — an in-process stand-in with the Kafka semantics this project
                 relies on, so the pipeline's crash behaviour is unit-testable:
                   - topics split into partitions, keyed messages hash to one
                     partition (so one bus's pings stay in order);
                   - consumers in a group commit offsets explicitly;
                   - anything not committed is redelivered to the next
                     consumer in the group (that is what a crash looks like).
"""

from __future__ import annotations

import time
import zlib
from dataclasses import dataclass
from typing import Protocol

PINGS_TOPIC = "gps.pings.v1"
DLQ_TOPIC = "gps.pings.dlq"


@dataclass(frozen=True)
class Message:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    timestamp_ms: int          # producer send time (wall clock); used for end-to-end latency


class Producer(Protocol):
    def send(self, topic: str, key: bytes | None, value: bytes) -> None: ...
    def flush(self) -> None: ...


class Consumer(Protocol):
    def poll(self, max_records: int, timeout_ms: int) -> list[Message]: ...
    def commit(self, offsets: dict[tuple[str, int], int]) -> None: ...
    def lag(self) -> int | None: ...
    def close(self) -> None: ...


def partition_for(key: bytes | None, n: int) -> int:
    return 0 if key is None else zlib.crc32(key) % n


# ------------------------------------------------------------------ in-memory
class MemoryBroker:
    def __init__(self, partitions: int = 4):
        self.n = partitions
        self.log: dict[str, list[list[Message]]] = {}
        self.committed: dict[tuple[str, str, int], int] = {}    # (group, topic, part) -> next offset

    def _topic(self, t: str) -> list[list[Message]]:
        return self.log.setdefault(t, [[] for _ in range(self.n)])

    def producer(self) -> "MemoryProducer":
        return MemoryProducer(self)

    def consumer(self, group: str, topic: str) -> "MemoryConsumer":
        return MemoryConsumer(self, group, topic)

    def messages(self, topic: str) -> list[Message]:
        return [m for part in self._topic(topic) for m in part]


class MemoryProducer:
    def __init__(self, b: MemoryBroker):
        self.b = b

    def send(self, topic: str, key: bytes | None, value: bytes) -> None:
        parts = self.b._topic(topic)
        p = partition_for(key, len(parts))
        parts[p].append(Message(topic, p, len(parts[p]), key, value, int(time.time() * 1000)))

    def flush(self) -> None:
        pass


class MemoryConsumer:
    """Single consumer owning all partitions of one topic for its group."""

    def __init__(self, b: MemoryBroker, group: str, topic: str):
        self.b, self.group, self.topic = b, group, topic
        parts = b._topic(topic)
        self.pos = [b.committed.get((group, topic, i), 0) for i in range(len(parts))]
        self._rr = 0

    def poll(self, max_records: int = 500, timeout_ms: int = 0) -> list[Message]:
        parts = self.b._topic(self.topic)
        out: list[Message] = []
        # Round-robin across partitions, like a real consumer fetching from several.
        while len(out) < max_records:
            progressed = False
            for k in range(len(parts)):
                i = (self._rr + k) % len(parts)
                if self.pos[i] < len(parts[i]) and len(out) < max_records:
                    out.append(parts[i][self.pos[i]])
                    self.pos[i] += 1
                    progressed = True
            self._rr += 1
            if not progressed:
                break
        return out

    def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        for (topic, part), nxt in offsets.items():
            self.b.committed[(self.group, topic, part)] = nxt

    def lag(self) -> int:
        parts = self.b._topic(self.topic)
        return sum(len(parts[i]) - self.b.committed.get((self.group, self.topic, i), 0)
                   for i in range(len(parts)))

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------- Kafka
class KafkaBroker:
    def __init__(self, bootstrap: str):
        self.bootstrap = bootstrap

    def producer(self) -> "KafkaProducerAdapter":
        return KafkaProducerAdapter(self.bootstrap)

    def consumer(self, group: str, topic: str) -> "KafkaConsumerAdapter":
        return KafkaConsumerAdapter(self.bootstrap, group, topic)


class KafkaProducerAdapter:
    def __init__(self, bootstrap: str):
        from kafka import KafkaProducer
        # acks=all + idempotence: a broker hiccup can't silently drop or double-write a ping.
        self.p = KafkaProducer(bootstrap_servers=bootstrap, acks="all", enable_idempotence=True,
                               linger_ms=5, retries=10)

    def send(self, topic: str, key: bytes | None, value: bytes) -> None:
        self.p.send(topic, key=key, value=value)

    def flush(self) -> None:
        self.p.flush()


class KafkaConsumerAdapter:
    def __init__(self, bootstrap: str, group: str, topic: str):
        from kafka import KafkaConsumer
        self.topic = topic
        self.c = KafkaConsumer(topic, bootstrap_servers=bootstrap, group_id=group,
                               enable_auto_commit=False,          # we commit after the DB write
                               auto_offset_reset="earliest", max_poll_records=500)

    def poll(self, max_records: int = 500, timeout_ms: int = 500) -> list[Message]:
        batches = self.c.poll(timeout_ms=timeout_ms, max_records=max_records)
        out = []
        for tp, recs in batches.items():
            for r in recs:
                out.append(Message(r.topic, r.partition, r.offset, r.key, r.value, r.timestamp))
        return out

    def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata
        self.c.commit({TopicPartition(t, p): OffsetAndMetadata(o, "", -1) for (t, p), o in offsets.items()})

    def lag(self) -> int | None:
        try:
            parts = self.c.assignment()
            if not parts:
                return None
            end = self.c.end_offsets(list(parts))
            return sum(end[tp] - (self.c.committed(tp) or 0) for tp in parts)
        except Exception:
            return None

    def close(self) -> None:
        self.c.close()
