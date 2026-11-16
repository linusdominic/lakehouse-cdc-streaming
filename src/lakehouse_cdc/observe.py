"""Consumer lag and end-to-end freshness.

Lag is measured in records: Kafka high watermark minus the offset the streaming
job has committed to the lake (ops.stream_batches), summed over partitions.
The Spark Kafka source does not commit to a consumer group, so broker-side
group lag does not exist for this job; this is the equivalent.

Freshness is measured per change event: the time from the commit in Postgres
(Debezium source.ts_ms, millisecond resolution) to the end of the micro-batch
that merged the event into silver. Redelivered copies of an event count once,
at their first delivery. Snapshot rows (op = 'r') are excluded because their
source timestamp is the snapshot time, not a commit time.
"""

from __future__ import annotations

import csv
import math
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from lakehouse_cdc.clients import Lake, kafka_offsets
from lakehouse_cdc.config import Settings
from lakehouse_cdc.contracts import BRONZE_EVENTS, CONTRACTS, OPS_BATCHES


def percentile(values: list[float], q: float) -> float:
    """Nearest-rank percentile, q in (0, 100]."""
    if not values:
        raise ValueError("percentile of empty list")
    ordered = sorted(values)
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]


def sample_lag(settings: Settings, out: Path, interval_s: float, duration_s: float | None) -> int:
    """Append a lag sample to `out` every interval until SIGTERM/SIGINT or duration."""
    lake = Lake(settings)
    stop = False

    def _stop(*_args) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    out.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    n = 0
    with out.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "kafka_end", "committed", "lag_records"])
        while not stop and (duration_s is None or time.monotonic() - started < duration_s):
            try:
                high = {tp: hi for tp, (_lo, hi) in kafka_offsets(settings.kafka_bootstrap).items()}
                done = lake.processed_offsets()
                end = sum(high.values())
                committed = sum(min(done.get(tp, 0), hi) for tp, hi in high.items())
                writer.writerow(
                    [
                        datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        end,
                        committed,
                        end - committed,
                    ]
                )
                f.flush()
                n += 1
            except Exception as exc:  # a sampler must not die because one probe failed
                print(f"lag sample failed: {exc}", flush=True)
            time.sleep(interval_s)
    return n


def lag_summary(samples_csv: Path) -> dict[str, float] | None:
    if not samples_csv.exists():
        return None
    with samples_csv.open() as f:
        lags = [float(r["lag_records"]) for r in csv.DictReader(f)]
    if not lags:
        return None
    return {
        "samples": len(lags),
        "p50": percentile(lags, 50),
        "p95": percentile(lags, 95),
        "max": max(lags),
        "last": lags[-1],
    }


def freshness_summary(lake: Lake, since: str | None = None) -> dict[str, float] | None:
    tables = ", ".join(f"'{t}'" for t in CONTRACTS)
    since_filter = f"AND e.source_ts >= from_iso8601_timestamp('{since}')" if since else ""
    row = lake.query(f"""
        WITH batches AS (
            SELECT query_id, batch_id, min(committed_at) AS committed_at
            FROM {OPS_BATCHES} GROUP BY 1, 2
        ),
        events AS (
            SELECT e.source_table, e.lsn, e.key,
                   min(e.source_ts) AS source_ts, min(b.committed_at) AS committed_at
            FROM {BRONZE_EVENTS} e
            JOIN batches b ON b.query_id = e.query_id AND b.batch_id = e.batch_id
            WHERE e.op <> 'r' AND e.source_table IN ({tables}) {since_filter}
            GROUP BY 1, 2, 3
        ),
        lat AS (
            SELECT date_diff('millisecond', source_ts, committed_at) / 1000.0 AS seconds FROM events
        )
        SELECT count(*),
               approx_percentile(seconds, 0.50),
               approx_percentile(seconds, 0.95),
               approx_percentile(seconds, 0.99),
               max(seconds)
        FROM lat
    """)[0]
    if not row[0]:
        return None
    return dict(zip(("events", "p50", "p95", "p99", "max"), (float(v) for v in row), strict=True))


def batch_summary(lake: Lake, since: str | None = None) -> dict[str, float] | None:
    since_filter = f"WHERE started_at >= from_iso8601_timestamp('{since}')" if since else ""
    row = lake.query(f"""
        WITH b AS (
            SELECT num_records, date_diff('millisecond', started_at, committed_at) / 1000.0 AS seconds
            FROM {OPS_BATCHES} {since_filter}
        )
        SELECT count(*), sum(num_records), sum(seconds),
               approx_percentile(seconds, 0.50), approx_percentile(seconds, 0.95), max(seconds),
               max(num_records)
        FROM b
    """)[0]
    if not row[0]:
        return None
    batches, records, busy, p50, p95, mx, biggest = (float(v) for v in row)
    return {
        "batches": batches,
        "records": records,
        "busy_seconds": busy,
        "records_per_busy_second": records / busy if busy else 0.0,
        "batch_p50": p50,
        "batch_p95": p95,
        "batch_max": mx,
        "largest_batch": biggest,
    }


def last_batch_state(lake: Lake) -> str:
    """What the newest micro-batch left on disk: bronze rows, and whether it finished.

    A batch that reached bronze but has no row in ops.stream_batches was
    interrupted before its silver merges completed.
    """
    row = lake.query(f"""
        SELECT batch_id, count(*) FROM {BRONZE_EVENTS}
        WHERE (query_id, batch_id) IN (
            SELECT query_id, batch_id FROM {BRONZE_EVENTS} ORDER BY ingested_at DESC LIMIT 1
        )
        GROUP BY 1
    """)
    if not row:
        return "bronze is empty"
    batch_id, rows = row[0]
    finished = lake.scalar(f"SELECT count(*) FROM {OPS_BATCHES} WHERE batch_id = {batch_id}")
    state = "recorded as complete" if finished else "NOT recorded as complete (interrupted)"
    return f"batch {batch_id}: {rows} rows in bronze; {state} in ops.stream_batches"


def format_report(settings: Settings, samples_csv: Path, since: str | None = None) -> str:
    lake = Lake(settings)
    lines = []
    lag = lag_summary(samples_csv)
    if lag:
        lines.append(
            f"consumer lag (records)   samples={lag['samples']:.0f} p50={lag['p50']:.0f} "
            f"p95={lag['p95']:.0f} max={lag['max']:.0f} last={lag['last']:.0f}"
        )
    else:
        lines.append(f"consumer lag (records)   no samples at {samples_csv}")
    fresh = freshness_summary(lake, since)
    if fresh:
        lines.append(
            f"freshness (seconds)      events={fresh['events']:.0f} p50={fresh['p50']:.1f} "
            f"p95={fresh['p95']:.1f} p99={fresh['p99']:.1f} max={fresh['max']:.1f}"
        )
    batches = batch_summary(lake, since)
    if batches:
        lines.append(
            f"micro-batches            count={batches['batches']:.0f} records={batches['records']:.0f} "
            f"p50={batches['batch_p50']:.1f}s p95={batches['batch_p95']:.1f}s max={batches['batch_max']:.1f}s "
            f"largest={batches['largest_batch']:.0f} records"
        )
        lines.append(
            f"streaming throughput     {batches['records_per_busy_second']:.0f} records/s "
            f"while a batch is running ({batches['busy_seconds']:.0f}s busy)"
        )
    return "\n".join(lines)
