# Performance

Everything here was measured by a script in this repository against a real
Kafka 4.0.0 broker (KRaft, single node), PostgreSQL 16 and Redis 7, all on one
2-vCPU machine that also ran the load generator and the service under test.
That is a constraint on how to read the numbers: they show how the system
behaves and where it bends, not what a production deployment would achieve.
Raw results are committed in [`docs/benchmarks/`](benchmarks/); the tables
below are generated from them by `scripts/summarize_benchmarks.py` (a test
fails if they drift).

Each failure result also records output per second (`arrivals_per_second`), so
the gaps can be read directly. An earlier 45 s run was discarded and repeated
because its per-second timeline did not agree with its summary and the cause
was not found; the committed result is the repeat.

Not run here: Docker Compose itself (the services ran as local processes
against the same broker and stores), multi-broker clusters, TLS/SASL, network
latency between hosts, or more than one replica of anything.

## How to reproduce

```bash
# broker + stores up, topics provisioned (make dev, or any Kafka at KAFKA_BOOTSTRAP_SERVERS)
uv run python scripts/bench_pipeline.py produce --n 200000 --out out/produce.json
# stream processor must be running for the end-to-end and failure runs
uv run python scripts/bench_pipeline.py e2e --rate 1000 --duration 30 --out out/e2e.json
uv run python scripts/bench_pipeline.py e2e --rate 200 --duration 40 \
  --fault-cmd "docker compose kill -s KILL stream-processor" --fault-at 10 \
  --recover-cmd "docker compose start stream-processor" --recover-at 20 --drain-s 90
# API: start it with a high rate limit, then
API_RATE_LIMIT_PER_S=100000 API_RATE_LIMIT_BURST=100000 uv run python -m api.main
uv run --group loadtest locust -f loadtests/locustfile.py --headless \
  --host http://localhost:8000 -u 50 -r 20 -t 45s --csv out/u50 DashboardUser
uv run python scripts/summarize_benchmarks.py   # regenerate the tables below
```

Method notes: bars use fresh symbols on every run, because the stream
processor correctly drops bars older than a symbol's latest as late
(a first end-to-end rerun lost 20% of its events to exactly that, visible as
`outcome="late"` in the processor's metrics, before the symbols were made
unique per run). End-to-end latency is measured from just before `produce()`
of a raw bar to the arrival of its enriched event, on the local wall clock.
The collector reads without a consumer group: a group member that loses its
coordinator rejoins at "latest" and silently skips the outage, which made an
early broker-restart run look like 75% data loss until the collector was fixed.
The 200 to 3,000 events/s latency runs used an earlier collector that read as a
consumer-group member; with no restarts in the path it behaves the same.
The API runs one uvicorn process, the demo feed was writing 12 bars per second
concurrently, and the load generator shared the CPUs.

## Results

<!-- BEGIN GENERATED -->
_Machine for every result below: 2 vCPU, Linux-6.18.44-fc-v114-x86_64-with-glibc2.39, Python 3.12.3._

### Kafka producer

| Events | Partitions | Throughput | Delivered / failed | Ack latency p50 / p99 / max |
| --- | --- | --- | --- | --- |
| 200,000 | 3 | 120,346 events/s | 200,000 / 0 | 521 / 952 / 955 ms |

### End-to-end latency (Kafka to Kafka through the stream processor)

| Offered load | Processor poll timeout | Sent | Missing | Duplicates | Latency p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 200 events/s | default (0.5 s) | 6,000 | 0 | 0 | 295 ms | 528 ms | 568 ms | 732 ms |
| 1000 events/s | default (0.5 s) | 30,000 | 0 | 0 | 316 ms | 494 ms | 514 ms | 623 ms |
| 3000 events/s | default (0.5 s) | 60,000 | 0 | 0 | 162 ms | 314 ms | 596 ms | 726 ms |
| 200 events/s | 0.05 s | 6,000 | 0 | 0 | 36 ms | 63 ms | 272 ms | 570 ms |
| 1000 events/s | 0.05 s | 30,000 | 0 | 0 | 53 ms | 84 ms | 245 ms | 564 ms |

### Failure injection (200 events/s for 40 s)

| Fault | Consumer session timeout | Sent | Missing | Duplicates | Longest output gap | Latency p50 / max | First output after recovery step |
| --- | --- | --- | --- | --- | --- | --- | --- |
| stream processor `kill -9`, restarted 10 s later | 45 s | 8,000 | 0 | 11 | 45.2 s | 27,169 / 47,993 ms | 35.1 s |
| stream processor `kill -9`, restarted 10 s later | 10 s | 8,000 | 0 | 57 | 10.6 s | 50 / 11,695 ms | 0.6 s |
| Kafka broker stopped 10 s, then started (processor under a restart loop) | 10 s | 8,000 | 0 | 42 | 25.0 s | 15,830 / 39,074 ms | 15.0 s |

### API under load (Locust, mixed dashboard traffic, 45 s per level)

| Virtual users | Requests | Failures | Throughput | Median | p95 | p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 10 | 2,633 | 0 | 60 req/s | 13 ms | 42 ms | 73 ms |
| 50 | 6,094 | 0 | 138 req/s | 170 ms | 500 ms | 680 ms |
| 200 | 5,523 | 0 | 125 req/s | 720 ms | 4000 ms | 5400 ms |

#### Slowest routes at 50 users

| Route (50 users) | Requests | Median | p95 | p99 |
| --- | --- | --- | --- | --- |
| `/api/v1/stocks/{symbol}` | 1,309 | 330 ms | 640 ms | 820 ms |
| `/api/v1/status` | 1,003 | 300 ms | 540 ms | 700 ms |
| `/api/v1/stocks/{symbol}/indicators` | 511 | 210 ms | 380 ms | 510 ms |
| `/api/v1/stocks/{symbol}/history` | 646 | 160 ms | 350 ms | 470 ms |
| `/api/v1/stocks/{symbol}/predictions` | 344 | 110 ms | 320 ms | 410 ms |
| `/api/v1/model` | 167 | 62 ms | 200 ms | 300 ms |
| `/api/v1/stocks` | 1,609 | 79 ms | 200 ms | 310 ms |
| `/api/v1/anomalies` | 505 | 25 ms | 74 ms | 110 ms |

Rate limiter check (5 clients hammering one route for 20 s): 461 requests allowed and 11,772 rejected with 429 (about 20 req/s * 20 s + burst 60 = 460 expected allowed).

<!-- END GENERATED -->

## What the results show

**Producer.** One idempotent, `acks=all`, lz4-compressed producer sustains
about 120k events/s of roughly 320-byte events to a single local broker. The
ack latency in that table is saturation latency (events queue locally while the
test produces back-to-back), not what a paced producer sees. The default
simulator emits 12 symbols at one bar per second, three orders of magnitude
below that, so ingestion is not the constraint anywhere in this design.

**Stream processing.** No run lost an event, including at 3,000 events/s (the
sender, a single Python process, was the limit beyond that: its scheduling
slip reaches 213 ms p99 at that rate). The latency is dominated by batching,
not computation: at 200 events/s the median was 295 ms with the default
0.5 s `consume()` timeout, and 36 ms with 0.05 s, because `consume()` waits for
a full batch or the timeout. Latency falls as load rises under the default
(162 ms at 3,000 events/s) for the same reason. The cost of the shorter timeout
was not measured (smaller batches mean more commits and flushes per second),
so the default was left alone and `STREAM_POLL_TIMEOUT_S` is the documented
knob; it should be chosen against a CPU measurement on the target hardware.

**Failure behaviour.** In all three scenarios nothing was lost, which is the
at-least-once guarantee working: offsets are stored only after outputs are
delivered. Duplicates appear when a batch had been emitted but not yet
committed (11, 57 and 42 across the three runs), which is why the sinks
are idempotent. A first run exposed an operational weakness: after `kill -9`
(no clean group leave), the replacement waited for the dead member's session to
expire: output stopped for 45 s (35 s after the restart) and the worst event
took 48 s. The consumer `session.timeout.ms` was therefore lowered from 45 s to
10 s; the same fault then produced output again 0.6 s after the restart, and
the longest gap (10.6 s) is simply the time the processor was down. The trade-off is less tolerance
of long GC or network pauses before a rebalance. In the broker-restart run the
processor deliberately aborts when outputs cannot be delivered within its flush
timeout (`stream_processor.aborted`, batch not committed) and relies on its
supervisor (`restart: unless-stopped` in Compose, a shell loop here) to come
back; the first attempt without a supervisor correctly stayed down.

**API.** At 10 virtual users the median is 13 ms and p99 73 ms. Throughput
peaks around 140 requests/s at 50 users, where the CPUs were saturated by
Postgres, the feed, Locust and the single API process; at 200 users it does not
rise and latency grows to seconds, i.e. the service queues rather than sheds.
No request failed at any level. The slowest routes under load are the symbol
detail and `/status`, which both fan out to several queries; they are the first
candidates for caching or fewer round trips. More API workers, a pooled
Postgres on separate hardware and a production-sized dataset would change all
of this, which is why no capacity claim is made. The rate limiter behaves as
configured (about 20 requests/s plus the burst allowance), shedding the rest
with 429.

## Not measured

* Time from a bar to its appearance in the API or WebSocket (sinks and Redis
  are outside the end-to-end figure above); sink lag is visible through the
  lag exporter and dashboards from Phase 11.
* Inference under concurrent load (single-client latency is in
  `benchmarks/inference_latency.json`).
* Postgres or Redis outages, network partitions, disk-full, and
  multi-replica rebalancing.
* The WebSocket fan-out at scale.
* The cost of the shorter poll timeout, as above.
* The Docker images and Compose networking, which were not run here.
