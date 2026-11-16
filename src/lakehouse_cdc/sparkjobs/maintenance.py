"""Iceberg table maintenance: compaction, snapshot expiry, orphan-file removal.

Run with the streaming job stopped (scripts/maintenance.sh does that). Running
it concurrently is possible but compaction and the streaming MERGE then race
on the same files and one of them retries or fails; see docs/runbook.md.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass

from pyspark.sql import SparkSession

from lakehouse_cdc.contracts import (
    BRONZE_DEAD_LETTER,
    BRONZE_EVENTS,
    CATALOG,
    CONTRACTS,
    OPS_BATCHES,
    SILVER_SCD2,
)

SILVER_TABLES = [c.silver_table for c in CONTRACTS.values()]
ALL_TABLES = [BRONZE_EVENTS, BRONZE_DEAD_LETTER, *SILVER_TABLES, SILVER_SCD2, OPS_BATCHES]
ORPHAN_PROCEDURE_MIN_AGE_HOURS = 24  # enforced by Iceberg's remove_orphan_files procedure


@dataclass
class TableStats:
    data_files: int
    delete_files: int
    data_bytes: int
    snapshots: int


def table_stats(spark: SparkSession, table: str) -> TableStats:
    files = spark.sql(f"""
        SELECT coalesce(sum(if(content = 0, 1, 0)), 0),
               coalesce(sum(if(content > 0, 1, 0)), 0),
               coalesce(sum(if(content = 0, file_size_in_bytes, 0)), 0)
        FROM {table}.files
    """).first()
    snapshots = spark.sql(f"SELECT count(*) FROM {table}.snapshots").first()[0]
    return TableStats(int(files[0]), int(files[1]), int(files[2]), int(snapshots))


def _short(table: str) -> str:
    return table.split(".", 1)[1]


def compact(spark: SparkSession, table: str) -> None:
    spark.sql(f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{_short(table)}',
            options => map(
                'min-input-files', '2',
                'delete-file-threshold', '1',
                'remove-dangling-deletes', 'true'
            )
        )
    """).collect()
    # Position deletes that still point at files the rewrite just replaced.
    spark.sql(f"""
        CALL {CATALOG}.system.rewrite_position_delete_files(
            table => '{_short(table)}', options => map('rewrite-all', 'true'))
    """).collect()
    spark.sql(f"CALL {CATALOG}.system.rewrite_manifests(table => '{_short(table)}')").collect()


def expire_snapshots(spark: SparkSession, table: str, retain_last: int) -> int:
    row = spark.sql(f"""
        CALL {CATALOG}.system.expire_snapshots(
            table => '{_short(table)}',
            older_than => TIMESTAMP '{time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())}',
            retain_last => {retain_last}
        )
    """).first()
    return int(sum(v or 0 for v in row.asDict().values()))


def remove_orphans(spark: SparkSession, table: str, min_age_minutes: int) -> int:
    """Delete files under the table location that no snapshot references.

    Orphans come from writers that died before committing (the chaos test
    produces them). The SQL procedure refuses an age below 24 hours because a
    file written by an in-flight commit looks exactly like an orphan. For a
    shorter age the Action API is used, which is only safe because nothing
    else is writing while this job runs.
    """
    if min_age_minutes >= ORPHAN_PROCEDURE_MIN_AGE_HOURS * 60:
        cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - min_age_minutes * 60))
        rows = spark.sql(f"""
            CALL {CATALOG}.system.remove_orphan_files(
                table => '{_short(table)}',
                older_than => TIMESTAMP '{cutoff}',
                prefix_listing => true)
        """).collect()
        return len(rows)

    jvm = spark._jvm
    jtable = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    cutoff_ms = int((time.time() - min_age_minutes * 60) * 1000)
    result = (
        jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
        .deleteOrphanFiles(jtable)
        .usePrefixListing(True)  # list through S3FileIO; no Hadoop S3 filesystem in the image
        .olderThan(cutoff_ms)
        .execute()
    )
    removed = 0
    it = result.orphanFileLocations().iterator()
    while it.hasNext():
        it.next()
        removed += 1
    return removed


def purge_tombstones(spark: SparkSession, table: str, retention_hours: int) -> None:
    """Drop soft-deleted rows older than the retention.

    The retention must exceed the longest replay you expect to absorb: once a
    tombstone is gone, a stale event for that key would re-insert the row.
    """
    spark.sql(f"""
        DELETE FROM {table}
        WHERE _is_deleted AND _source_ts < current_timestamp() - INTERVAL {retention_hours} HOURS
    """)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retain-snapshots", type=int, default=3)
    parser.add_argument("--orphan-min-age-minutes", type=int, default=3 * 24 * 60)
    parser.add_argument("--tombstone-retention-hours", type=int, default=7 * 24)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("cdc-maintenance").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    header = (
        f"{'table':<28} {'data files':>12} {'delete files':>13} {'data KB':>16} "
        f"{'snapshots':>11} {'files expired':>14} {'orphans removed':>16} {'seconds':>8}"
    )
    lines = [header, "-" * len(header)]
    ok = True
    for table in ALL_TABLES:
        t0 = time.time()
        before = table_stats(spark, table)
        if table in SILVER_TABLES:
            purge_tombstones(spark, table, args.tombstone_retention_hours)
        rows_before = spark.table(table).count()
        compact(spark, table)
        expired = expire_snapshots(spark, table, args.retain_snapshots)
        orphans = remove_orphans(spark, table, args.orphan_min_age_minutes)
        after = table_stats(spark, table)
        rows_after = spark.table(table).count()
        if rows_after != rows_before:
            ok = False
            print(f"ERROR {table}: maintenance changed the row count {rows_before} -> {rows_after}")
        lines.append(
            f"{_short(table):<28} "
            f"{before.data_files:>5} -> {after.data_files:<4} "
            f"{before.delete_files:>5} -> {after.delete_files:<4} "
            f"{before.data_bytes / 1e3:>7.0f} -> {after.data_bytes / 1e3:<6.0f} "
            f"{before.snapshots:>4} -> {after.snapshots:<4} "
            f"{expired:>14} {orphans:>16} {time.time() - t0:>8.1f}"
        )
    print("\n".join(lines), flush=True)
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
