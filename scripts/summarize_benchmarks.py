"""Render the benchmark tables in docs/PERFORMANCE.md from the committed raw results.

    uv run python scripts/summarize_benchmarks.py          # rewrite the generated block
    uv run python scripts/summarize_benchmarks.py --check  # fail if it is stale

Every number in the generated block comes from a file under docs/benchmarks/.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BENCH = ROOT / "docs" / "benchmarks"
DOC = ROOT / "docs" / "PERFORMANCE.md"
BEGIN, END = "<!-- BEGIN GENERATED -->", "<!-- END GENERATED -->"


def load(name: str) -> dict:  # type: ignore[type-arg]
    return json.loads((BENCH / name).read_text())  # type: ignore[no-any-return]


def f(x: float | None, nd: int = 1) -> str:
    return "n/a" if x is None else f"{x:,.{nd}f}"


def produce_table() -> str:
    d = load("kafka_produce.json")
    r, c, a = d["results"], d["config"], d["results"]["ack_latency"]
    return (
        "| Events | Partitions | Throughput | Delivered / failed | Ack latency p50 / p99 / max |\n"
        "| --- | --- | --- | --- | --- |\n"
        f"| {c['events']:,} | {c['partitions']} | {f(r['events_per_s'], 0)} events/s "
        f"| {r['delivered']:,} / {r['failed']} "
        f"| {f(a['p50_ms'], 0)} / {f(a['p99_ms'], 0)} / {f(a['max_ms'], 0)} ms |"
    )


E2E = [
    ("e2e_200eps.json", "default (0.5 s)"),
    ("e2e_1000eps.json", "default (0.5 s)"),
    ("e2e_3000eps.json", "default (0.5 s)"),
    ("e2e_200eps_poll50ms.json", "0.05 s"),
    ("e2e_1000eps_poll50ms.json", "0.05 s"),
]


def e2e_table() -> str:
    rows = [
        "| Offered load | Processor poll timeout | Sent | Missing | Duplicates "
        "| Latency p50 | p95 | p99 | max |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for name, poll in E2E:
        d = load(name)
        r, lat = d["results"], d["results"]["end_to_end_latency"]
        rows.append(
            f"| {d['config']['rate_per_s']:.0f} events/s | {poll} | {r['sent']:,} "
            f"| {r['missing']} | {r['duplicates']} | {f(lat['p50_ms'], 0)} ms "
            f"| {f(lat['p95_ms'], 0)} ms | {f(lat['p99_ms'], 0)} ms | {f(lat['max_ms'], 0)} ms |"
        )
    return "\n".join(rows)


FAULTS = [
    (
        "fault_processor_kill9_session45s.json",
        "stream processor `kill -9`, restarted 10 s later",
        "45 s",
    ),
    (
        "fault_processor_kill9_session10s.json",
        "stream processor `kill -9`, restarted 10 s later",
        "10 s",
    ),
    (
        "fault_broker_restart.json",
        "Kafka broker stopped 10 s, then started (processor under a restart loop)",
        "10 s",
    ),
]


def fault_table() -> str:
    rows = [
        "| Fault | Consumer session timeout | Sent | Missing | Duplicates "
        "| Longest output gap | Latency p50 / max | First output after recovery step |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for name, label, session in FAULTS:
        r = load(name)["results"]
        lat = r["end_to_end_latency"]
        rows.append(
            f"| {label} | {session} | {r['sent']:,} | {r['missing']} | {r['duplicates']} "
            f"| {f(r['max_arrival_gap_s'])} s | {f(lat['p50_ms'], 0)} / {f(lat['max_ms'], 0)} ms "
            f"| {f(r.get('first_enriched_after_recovery_s'))} s |"
        )
    return "\n".join(rows)


def api_table() -> str:
    rows = [
        "| Virtual users | Requests | Failures | Throughput | Median | p95 | p99 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for users in (10, 50, 200):
        with (BENCH / "api_load" / f"u{users}_stats.csv").open() as fh:
            agg = next(r for r in csv.DictReader(fh) if r["Name"] == "Aggregated")
        rows.append(
            f"| {users} | {int(agg['Request Count']):,} | {agg['Failure Count']} "
            f"| {float(agg['Requests/s']):.0f} req/s | {agg['50%']} ms | {agg['95%']} ms "
            f"| {agg['99%']} ms |"
        )
    return "\n".join(rows)


def api_routes_table() -> str:
    with (BENCH / "api_load" / "u50_stats.csv").open() as fh:
        data = [r for r in csv.DictReader(fh) if r["Name"] != "Aggregated"]
    rows = [
        "| Route (50 users) | Requests | Median | p95 | p99 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for r in sorted(data, key=lambda r: -float(r["95%"])):
        rows.append(
            f"| `{r['Name']}` | {int(r['Request Count']):,} | {r['50%']} ms | {r['95%']} ms "
            f"| {r['99%']} ms |"
        )
    return "\n".join(rows)


def limiter_line() -> str:
    d = json.loads((BENCH / "api_load" / "limiter_counts.json").read_text())
    return (
        f"{d['allowed_200']:,} requests allowed and {d['rejected_429']:,} rejected with 429 "
        f"({d['expected_allowed']} expected allowed)"
    )


def machine() -> str:
    h = load("kafka_produce.json")["host"]
    return f"{h['cpus']} vCPU, {h['platform']}, Python {h['python']}"


def render() -> str:
    parts = {
        "Machine": machine(),
        "### Kafka producer\n\n": produce_table(),
        "### End-to-end latency (Kafka to Kafka through the stream processor)\n\n": e2e_table(),
        "### Failure injection (200 events/s for 40 s)\n\n": fault_table(),
        "### API under load (Locust, mixed dashboard traffic, 45 s per level)\n\n": api_table(),
        "#### Slowest routes at 50 users\n\n": api_routes_table(),
    }
    out = [f"_Machine for every result below: {parts.pop('Machine')}._", ""]
    for head, body in parts.items():
        out += [head + body, ""]
    out += [f"Rate limiter check (5 clients hammering one route for 20 s): {limiter_line()}.", ""]
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    text = DOC.read_text()
    head, rest = text.split(BEGIN, 1)
    _old, tail = rest.split(END, 1)
    new = f"{head}{BEGIN}\n{render()}\n{END}{tail}"
    if args.check:
        if new != text:
            print(
                "docs/PERFORMANCE.md is stale; run scripts/summarize_benchmarks.py", file=sys.stderr
            )
            return 1
        return 0
    DOC.write_text(new)
    return 0


if __name__ == "__main__":
    sys.exit(main())
