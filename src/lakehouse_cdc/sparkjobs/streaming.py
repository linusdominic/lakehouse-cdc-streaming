"""Structured Streaming job: Kafka (Debezium JSON) -> Iceberg bronze + silver.

One query, one foreachBatch. Per micro-batch, in order:

  1. parse the envelope, split valid events from dead letters
  2. bronze: insert-only MERGE on (topic, partition, offset)
  3. dead letter: same
  4. silver current state: one LSN-guarded MERGE per source table, and the
     SCD2 table for loans.status, run concurrently
  5. append one row of batch metadata to ops.stream_batches

The steps are separate Iceberg commits, so a crash can land between any two of
them. Recovery relies on two facts: Spark re-runs the failed batch with the
same Kafka offsets (checkpoint offset log), and every step is idempotent.
"""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_cdc.contracts import CONTRACTS, LOANS, OPS_BATCHES, TOPIC_PATTERN
from lakehouse_cdc.sparkjobs import sinks
from lakehouse_cdc.sparkjobs.envelope_sql import parse_envelope
from lakehouse_cdc.sparkjobs.tables import ensure_tables

CHECKPOINT_DIR = os.environ.get("CDC_CHECKPOINT_DIR", "/checkpoints/cdc")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("CDC_MAX_OFFSETS_PER_TRIGGER", "20000")
TRIGGER_INTERVAL = os.environ.get("CDC_TRIGGER_INTERVAL", "2 seconds")
SILVER_PARALLELISM = int(os.environ.get("CDC_SILVER_PARALLELISM", "5"))
FAILPOINT_FILE = Path(os.environ.get("CDC_FAILPOINT_FILE", "/checkpoints/_failpoint"))


def log(message: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat(timespec='milliseconds')} {message}", flush=True)


def failpoint(name: str) -> None:
    """Crash the driver at a named point if the failpoint file asks for it.

    Used by scripts/chaos.sh to die between two Iceberg commits of the same
    micro-batch, which a random kill only hits by luck. The file is removed
    first so the restarted job does not crash again.
    """
    try:
        armed = FAILPOINT_FILE.read_text().strip()
    except OSError:
        return
    if armed == name:
        FAILPOINT_FILE.unlink(missing_ok=True)
        log(f"[failpoint] {name}: exiting hard")
        os._exit(86)


def _query_id() -> str:
    """Stable id of this checkpoint, written by Spark before the first batch."""
    try:
        return json.loads((Path(CHECKPOINT_DIR) / "metadata").read_text())["id"]
    except (OSError, ValueError, KeyError):
        return "unknown"


def _map_sql(values: dict[str, int]) -> str:
    """A map<string,bigint> literal. Keys are topic and table names from our own config."""
    if not values:
        return "CAST(map() AS map<string,bigint>)"
    entries = ", ".join(f"'{k}', CAST({int(v)} AS BIGINT)" for k, v in sorted(values.items()))
    return f"map({entries})"


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    started = time.time()
    spark = batch_df.sparkSession
    query_id = _query_id()

    parsed = (
        parse_envelope(batch_df)
        .withColumn("query_id", F.lit(query_id))
        .withColumn("batch_id", F.lit(batch_id).cast("bigint"))
        .withColumn("ingested_at", F.current_timestamp())
        .persist()
    )
    try:
        # One pass over the batch for everything the driver needs to plan the rest.
        summary = (
            parsed.groupBy(
                "topic",
                "kafka_partition",
                "source_table",
                F.col("dlq_reason").isNotNull().alias("is_dead"),
                F.xxhash64("schema_json").alias("schema_hash"),
            )
            .agg(
                F.count("*").alias("n"),
                F.max("kafka_offset").alias("max_offset"),
                F.first("schema_json").alias("schema_json"),
            )
            .collect()
        )
        if not summary:
            return

        num_records = sum(r["n"] for r in summary)
        num_dead = sum(r["n"] for r in summary if r["is_dead"])
        table_counts: dict[str, int] = {}
        schemas: dict[str, list[str]] = {}
        end_offsets: dict[str, int] = {}
        for r in summary:
            tp = f"{r['topic']}/{r['kafka_partition']}"
            end_offsets[tp] = max(end_offsets.get(tp, 0), r["max_offset"] + 1)
            if r["is_dead"]:
                continue
            table_counts[r["source_table"]] = table_counts.get(r["source_table"], 0) + r["n"]
            if r["schema_json"]:
                schemas.setdefault(r["source_table"], []).append(r["schema_json"])

        valid = parsed.where("dlq_reason is null")
        stages: dict[str, float] = {"parse": round(time.time() - started, 2)}

        def timed(stage: str, fn, *args) -> None:
            t0 = time.time()
            fn(*args)
            stages[stage] = round(time.time() - t0, 2)

        timed("bronze", sinks.append_bronze, valid)
        failpoint("after_bronze")

        if num_dead:
            timed("dead_letter", sinks.append_dead_letter, parsed.where("dlq_reason is not null"))

        # The silver tables are independent of each other, so their merges run
        # concurrently. Each is its own Iceberg commit either way.
        def merge_table(name: str) -> None:
            timed(
                name,
                sinks.merge_current_state,
                valid.where(F.col("source_table") == name),
                CONTRACTS[name],
                schemas.get(name, []),
            )
            failpoint(f"after_silver_{name}")

        def merge_scd2() -> None:
            timed(
                "scd2",
                sinks.merge_status_history,
                valid.where(F.col("source_table") == LOANS.source_table),
            )

        with ThreadPoolExecutor(max_workers=SILVER_PARALLELISM) as pool:
            futures = [pool.submit(merge_table, n) for n in CONTRACTS if table_counts.get(n)]
            if table_counts.get(LOANS.source_table):
                futures.append(pool.submit(merge_scd2))
            for future in futures:
                future.result()  # re-raise the first failure and fail the batch
        failpoint("after_silver")

        # Built in SQL rather than with createDataFrame so the job never starts
        # Python worker processes; the container has no memory to spare for them.
        spark.sql(f"""
            SELECT '{query_id}'                                        AS query_id,
                   CAST({batch_id} AS BIGINT)                          AS batch_id,
                   timestamp_micros({int(started * 1_000_000)})        AS started_at,
                   timestamp_micros({int(time.time() * 1_000_000)})    AS committed_at,
                   CAST({num_records} AS BIGINT)                       AS num_records,
                   CAST({num_dead} AS BIGINT)                          AS num_dead_letter,
                   {_map_sql(table_counts)}                            AS table_counts,
                   {_map_sql(end_offsets)}                             AS end_offsets
        """).writeTo(OPS_BATCHES).append()

        log(
            f"[batch {batch_id}] records={num_records} dead_letter={num_dead} "
            f"tables={json.dumps(table_counts, sort_keys=True)} "
            f"seconds={time.time() - started:.2f} stages={json.dumps(stages)}"
        )
    finally:
        parsed.unpersist()


def main() -> int:
    spark = SparkSession.builder.appName("cdc-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        # Topics are created by Kafka Connect after this job starts.
        .option("kafka.metadata.max.age.ms", "5000")
        .option("kafka.fetch.max.wait.ms", "50")
        # If Kafka has dropped offsets the checkpoint still needs, stop. Silently
        # skipping them would leave silver wrong with nothing to show for it.
        .option("failOnDataLoss", "true")
        .load()
    )
    query = (
        source.writeStream.queryName("cdc_lakehouse")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    log(f"[stream] started query id={query.id} checkpoint={CHECKPOINT_DIR}")

    # There is no graceful shutdown path. Stopping the container is handled
    # exactly like a crash: the next start re-runs the unfinished batch.
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
