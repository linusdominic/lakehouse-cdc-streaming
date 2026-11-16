"""Wait until everything committed in Postgres so far has reached silver.

1. Insert a marker row in Postgres. Debezium emits WAL changes in commit
   order, so once the marker is in the lake every earlier commit has at least
   reached Kafka.
2. Wait for the marker to show up in bronze.
3. Read the Kafka high watermarks (twice, to see them stable) and wait until
   the streaming job reports having committed past all of them. The job
   records its offsets after the last silver merge of a batch, so this covers
   silver as well as bronze.
"""

from __future__ import annotations

import time
import uuid

from lakehouse_cdc.clients import Lake, kafka_offsets, pg_connect
from lakehouse_cdc.config import Settings
from lakehouse_cdc.contracts import BRONZE_EVENTS


class DrainTimeout(RuntimeError):
    pass


def drain(settings: Settings, timeout_s: float = 600, poll_s: float = 2.0) -> float:
    """Block until the pipeline has caught up. Returns seconds waited."""
    started = time.monotonic()
    deadline = started + timeout_s
    marker = str(uuid.uuid4())
    with pg_connect(settings) as pg:
        pg.execute("INSERT INTO pipeline_marker (marker_id, note) VALUES (%s, 'drain')", (marker,))
    lake = Lake(settings)

    def _check_deadline(what: str) -> None:
        if time.monotonic() > deadline:
            raise DrainTimeout(f"timed out after {timeout_s:.0f}s waiting for {what}")

    while True:
        found = lake.scalar(f"""
            SELECT count(*) FROM {BRONZE_EVENTS}
            WHERE source_table = 'pipeline_marker' AND key LIKE '%{marker}%'
        """)
        if found:
            break
        _check_deadline("the marker row to reach bronze (is the streaming job running?)")
        time.sleep(poll_s)

    high = {tp: hi for tp, (_lo, hi) in kafka_offsets(settings.kafka_bootstrap).items()}
    while True:
        time.sleep(poll_s)
        again = {tp: hi for tp, (_lo, hi) in kafka_offsets(settings.kafka_bootstrap).items()}
        if again == high:
            break
        high = again
        _check_deadline("Kafka high watermarks to stop moving")

    while True:
        processed = lake.processed_offsets()
        behind = {
            tp: hi - processed.get(tp, 0) for tp, hi in high.items() if processed.get(tp, 0) < hi
        }
        if not behind:
            break
        _check_deadline(f"the streaming job to catch up ({sum(behind.values())} records behind)")
        time.sleep(poll_s)
    return time.monotonic() - started
