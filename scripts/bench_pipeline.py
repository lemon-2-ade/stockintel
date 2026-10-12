"""Kafka throughput, end-to-end latency and failure-injection benchmarks.

    # 1) producer throughput + broker-ack latency (scratch topic, deleted after)
    uv run python scripts/bench_pipeline.py produce --n 200000 --out OUT.json

    # 2) end-to-end latency through the running stream processor
    #    (needs a broker and `python -m stream_processor.main` running)
    uv run python scripts/bench_pipeline.py e2e --rate 500 --duration 30 --out OUT.json

    # 3) the same, with a fault injected and then removed (shell commands you provide)
    uv run python scripts/bench_pipeline.py e2e --rate 200 --duration 40 \
        --fault-cmd "pkill -f stream_processor.main" --fault-at 10 \
        --recover-cmd "./restart_processor.sh" --recover-at 20 --drain-s 60

Uses the production producer configuration (idempotent, acks=all, lz4, linger 5 ms)
and the production serde. Every number is measured on the machine and broker given by
``KAFKA_BOOTSTRAP_SERVERS``; the JSON output records the host so results are not
mistaken for something they are not.

``e2e`` measures Kafka-to-Kafka latency: from just before ``produce()`` of a raw bar
to the arrival of its enriched event at a consumer (broker hop, stream processing,
broker hop). It does not include the sinks, Redis or the API/WebSocket. Clocks are the
local wall clock, so run the benchmark on the same host as the processor.
"""

from __future__ import annotations

import argparse
import functools
import itertools
import json
import math
import os
import platform
import random
import statistics
import subprocess
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from shared.config import KafkaSettings
from shared.kafka.client_config import admin_config, consumer_config, producer_config
from shared.kafka.serde import EventDeserializationError, JsonEventSerde
from shared.kafka.topics import Topic
from shared.schemas import BarInterval, EnrichedBarEvent, MarketBarEvent


def host_info() -> dict[str, Any]:
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "bootstrap_servers": KafkaSettings().bootstrap_servers,
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def pct(values: list[float], q: float) -> float:
    if not values:
        return math.nan
    s = sorted(values)
    return s[min(len(s) - 1, math.ceil(q * len(s)) - 1)]


def summarize_ms(values_s: list[float]) -> dict[str, float | int]:
    ms = [v * 1000 for v in values_s]
    return {
        "n": len(ms),
        "mean_ms": round(statistics.fmean(ms), 3) if ms else math.nan,
        "p50_ms": round(pct(ms, 0.50), 3),
        "p90_ms": round(pct(ms, 0.90), 3),
        "p95_ms": round(pct(ms, 0.95), 3),
        "p99_ms": round(pct(ms, 0.99), 3),
        "max_ms": round(max(ms), 3) if ms else math.nan,
    }


def make_bars(symbols: list[str], n: int, seed: int) -> list[MarketBarEvent]:
    """Valid random-walk 1-minute bars; symbol i%len, strictly increasing time per symbol."""
    rng = random.Random(seed)  # noqa: S311 - benchmark data, not security
    base = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(days=30)
    price = {s: 100.0 + rng.random() * 50 for s in symbols}
    out: list[MarketBarEvent] = []
    for i in range(n):
        sym = symbols[i % len(symbols)]
        k = i // len(symbols)
        o = price[sym]
        c = o * (1 + rng.gauss(0, 0.001))
        hi, lo = max(o, c) * 1.0005, min(o, c) * 0.9995
        price[sym] = c
        out.append(
            MarketBarEvent(
                source="bench",
                symbol=sym,
                timestamp=base + timedelta(minutes=k),
                interval=BarInterval.M1,
                open=round(o, 4),
                high=round(hi, 4),
                low=round(lo, 4),
                close=round(c, 4),
                volume=int(rng.random() * 100_000),
            )
        )
    return out


# ------------------------------------------------------------------------------ produce
def cmd_produce(args: argparse.Namespace) -> dict[str, Any]:
    kafka = KafkaSettings()
    topic = f"bench.raw.{uuid.uuid4().hex[:6]}"
    admin = AdminClient(admin_config(kafka, client_id="bench-admin"))
    for fut in admin.create_topics(
        [NewTopic(topic, num_partitions=args.partitions, replication_factor=1)]
    ).values():
        fut.result(timeout=30)

    symbols = [f"BN{i:02d}" for i in range(args.symbols)]
    serde = JsonEventSerde()
    events = make_bars(symbols, args.n, seed=7)
    t0 = time.perf_counter()
    payloads: list[tuple[bytes, bytes, list[tuple[str, str | bytes | None]]]] = [
        (e.partition_key().encode(), serde.serialize(e), list(serde.headers(e))) for e in events
    ]
    serialize_s = time.perf_counter() - t0

    latencies: list[float] = []
    failures = 0

    def on_delivery(*args: Any, sent: float) -> None:
        nonlocal failures
        if args[0] is not None:
            failures += 1
        else:
            latencies.append(time.perf_counter() - sent)

    producer = Producer(producer_config(kafka, client_id="bench-producer"))
    buffer_full = 0
    start = time.perf_counter()
    for key, value, headers in payloads:
        sent = time.perf_counter()
        while True:
            try:
                producer.produce(
                    topic,
                    key=key,
                    value=value,
                    headers=headers,
                    on_delivery=functools.partial(on_delivery, sent=sent),
                )
                break
            except BufferError:
                buffer_full += 1
                producer.poll(0.05)
        producer.poll(0)
    producer.flush(120)
    elapsed = time.perf_counter() - start
    try:
        for fut in admin.delete_topics([topic]).values():
            fut.result(timeout=30)
    except KafkaException:
        pass
    return {
        "benchmark": "kafka_produce",
        "host": host_info(),
        "config": {
            "events": args.n,
            "partitions": args.partitions,
            "symbols": args.symbols,
            "producer": "production config (idempotent, acks=all, lz4, linger 5 ms)",
            "avg_payload_bytes": round(sum(len(p[1]) for p in payloads) / len(payloads), 1),
        },
        "results": {
            "elapsed_s": round(elapsed, 3),
            "events_per_s": round(args.n / elapsed, 1),
            "delivered": len(latencies),
            "failed": failures,
            "local_queue_full_events": buffer_full,
            "serialize_events_per_s": round(args.n / serialize_s, 1),
            "ack_latency": summarize_ms(latencies),
        },
        "notes": [
            "ack latency = produce() call to broker acknowledgement, including local queueing; "
            "with events produced back-to-back it reflects saturation, not an idle broker",
            "serialization is done before the timed section and reported separately",
        ],
    }


# ---------------------------------------------------------------------------------- e2e
class Collector(threading.Thread):
    """Reads market.enriched from its current end with a manually assigned consumer.

    No consumer group: a group member that loses its coordinator (broker restart) would
    rejoin and reset to "latest", silently skipping the events produced during the
    outage and making the pipeline look lossy.
    """

    def __init__(self, kafka: KafkaSettings, sent: dict[str, float]) -> None:
        super().__init__(daemon=True)
        run = uuid.uuid4().hex[:6]
        conf = consumer_config(kafka, group_id=f"bench-{run}", client_id=f"bench-{run}")
        conf["enable.auto.commit"] = False
        self.consumer = Consumer(conf)
        self.sent = sent
        self.serde = JsonEventSerde()
        self.stop = threading.Event()
        self.arrivals: list[tuple[float, str, EnrichedBarEvent]] = []
        self.ready = threading.Event()

    def run(self) -> None:
        topic = Topic.MARKET_ENRICHED.value
        partitions = self.consumer.list_topics(topic, timeout=30).topics[topic].partitions
        assignment = []
        for p in partitions:
            _low, high = self.consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
            assignment.append(TopicPartition(topic, p, high))
        self.consumer.assign(assignment)
        self.ready.set()
        while not self.stop.is_set():
            msg = self.consumer.poll(0.1)
            now = time.time()
            if msg is None or msg.error():
                continue
            try:
                ev = self.serde.deserialize(msg.value(), EnrichedBarEvent)
            except EventDeserializationError:
                continue
            if str(ev.source_event_id) in self.sent:
                self.arrivals.append((now, str(ev.source_event_id), ev))
        self.consumer.close()


def cmd_e2e(args: argparse.Namespace) -> dict[str, Any]:
    kafka = KafkaSettings()
    run = uuid.uuid4().hex[:5].upper()  # fresh symbols: reruns must not look like duplicates
    symbols = [f"E{run}{i:02d}" for i in range(args.symbols)]
    total = int(args.rate * args.duration)
    events = make_bars(symbols, total, seed=11)
    serde = JsonEventSerde()
    sent: dict[str, float] = {}
    collector = Collector(kafka, sent)
    collector.start()
    if not collector.ready.wait(30):
        raise SystemExit("consumer did not get an assignment on market.enriched")
    time.sleep(1.0)

    producer = Producer(producer_config(kafka, client_id="bench-e2e"))
    marks: dict[str, float] = {}

    def run_cmd(name: str, cmd: str, at: float, t_start: float) -> None:
        time.sleep(max(0.0, at - (time.time() - t_start)))
        marks[f"{name}_at_s"] = round(time.time() - t_start, 3)
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)  # noqa: S602
        marks[f"{name}_exit_code"] = res.returncode
        marks[f"{name}_done_s"] = round(time.time() - t_start, 3)

    t_start = time.time()
    helpers = []
    for name, cmd, at in (
        ("fault", args.fault_cmd, args.fault_at),
        ("recover", args.recover_cmd, args.recover_at),
    ):
        if cmd:
            th = threading.Thread(target=run_cmd, args=(name, cmd, at, t_start), daemon=True)
            th.start()
            helpers.append(th)

    slip: list[float] = []
    for i, ev in enumerate(events):
        due = t_start + i / args.rate
        delay = due - time.time()
        if delay > 0:
            time.sleep(delay)
        else:
            slip.append(-delay)
        ev = ev.model_copy(update={"produced_at": datetime.now(UTC)})  # noqa: PLW2901
        sent[str(ev.event_id)] = time.time()
        producer.produce(
            Topic.MARKET_RAW.value,
            key=ev.partition_key().encode(),
            value=serde.serialize(ev),
            headers=list(serde.headers(ev)),
        )
        producer.poll(0)
    producer.flush(60)
    send_s = time.time() - t_start

    deadline = time.time() + args.drain_s
    while time.time() < deadline and len({a[1] for a in collector.arrivals}) < total:
        time.sleep(0.2)
    collector.stop.set()
    collector.join(10)
    for th in helpers:
        th.join(1)

    first: dict[str, float] = {}
    dup = 0
    for at, sid, _ev in collector.arrivals:
        if sid in first:
            dup += 1
        else:
            first[sid] = at
    lat = [first[sid] - sent[sid] for sid in first]
    internal = [
        ev.processing_latency_ms / 1000
        for _, sid, ev in collector.arrivals
        if sid in first and ev.processing_latency_ms is not None
    ]
    times = sorted(first.values())
    gaps = [b - a for a, b in itertools.pairwise(times)]
    per_symbol: dict[str, datetime] = {}
    out_of_order = 0
    for _, sid, arrived in sorted(collector.arrivals, key=lambda a: a[0]):
        if first.get(sid) is None:
            continue
        prev = per_symbol.get(arrived.symbol)
        if prev is not None and arrived.timestamp < prev:
            out_of_order += 1
        per_symbol[arrived.symbol] = arrived.timestamp
    result: dict[str, Any] = {
        "sent": total,
        "received_distinct": len(first),
        "missing": total - len(first),
        "duplicates": dup,
        "out_of_order_per_symbol": out_of_order,
        "send_duration_s": round(send_s, 3),
        "achieved_send_rate": round(total / send_s, 1),
        "send_schedule_slip_ms_p99": round(pct([s * 1000 for s in slip], 0.99), 3) if slip else 0.0,
        "end_to_end_latency": summarize_ms(lat),
        "processor_reported_latency": summarize_ms(internal),
        "max_arrival_gap_s": round(max(gaps), 3) if gaps else None,
        "arrivals_per_second": [
            sum(1 for t in times if int(t - t_start) == sec)
            for sec in range(int(times[-1] - t_start) + 1)
        ]
        if times
        else [],
    }
    if "recover_at_s" in marks and times:
        recover_wall = t_start + marks["recover_at_s"]
        after = [t for t in times if t >= recover_wall]
        result["first_enriched_after_recovery_s"] = (
            round(after[0] - recover_wall, 3) if after else None
        )
    return {
        "benchmark": "end_to_end_latency",
        "host": host_info(),
        "config": {
            "rate_per_s": args.rate,
            "duration_s": args.duration,
            "symbols": args.symbols,
            "fault_cmd": args.fault_cmd,
            "recover_cmd": args.recover_cmd,
        },
        "fault_marks": marks,
        "results": result,
        "notes": [
            "latency = just before produce() of the raw bar to arrival of its enriched event",
            "kafka-to-kafka only: sinks, Redis and the API are not in the path",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("produce")
    p.add_argument("--n", type=int, default=100_000)
    p.add_argument("--partitions", type=int, default=3)
    p.add_argument("--symbols", type=int, default=12)
    e = sub.add_parser("e2e")
    e.add_argument("--rate", type=float, default=200.0, help="events per second")
    e.add_argument("--duration", type=float, default=30.0, help="seconds of load")
    e.add_argument("--symbols", type=int, default=12)
    e.add_argument("--drain-s", type=float, default=20.0)
    e.add_argument("--fault-cmd")
    e.add_argument("--fault-at", type=float, default=10.0)
    e.add_argument("--recover-cmd")
    e.add_argument("--recover-at", type=float, default=20.0)
    for sp in (p, e):
        sp.add_argument("--out", help="write the JSON result here")
    args = parser.parse_args()
    result = cmd_produce(args) if args.cmd == "produce" else cmd_e2e(args)
    text = json.dumps(result, indent=2)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
