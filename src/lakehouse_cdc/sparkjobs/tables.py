"""DDL for the Iceberg tables owned by the streaming job."""

from __future__ import annotations

from pyspark.sql import SparkSession

from lakehouse_cdc.contracts import (
    BRONZE_DEAD_LETTER,
    BRONZE_EVENTS,
    CATALOG,
    CONTRACTS,
    META_COLUMNS,
    OPS_BATCHES,
    SILVER_SCD2,
)

# Silver is merge-on-read: a micro-batch touches a few rows in many files, and
# copy-on-write would rewrite every touched file on every batch. The
# maintenance job folds the delete files back in. See ADR 0004.
_SILVER_PROPS = """
    'format-version' = '2',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
"""

_APPEND_PROPS = """
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
"""


def ensure_tables(spark: SparkSession) -> None:
    for namespace in ("bronze", "silver", "ops", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")

    # Partitioned by arrival day, not event day: appends only ever touch the
    # current partition, and replays are read back by when they arrived.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_EVENTS} (
            topic           string,
            kafka_partition int,
            kafka_offset    bigint,
            kafka_ts        timestamp,
            key             string,
            op              string,
            lsn             bigint,
            tx_id           bigint,
            source_table    string,
            source_ts       timestamp,
            connector_ts    timestamp,
            before          string,
            after           string,
            query_id        string,
            batch_id        bigint,
            ingested_at     timestamp
        ) USING iceberg
        PARTITIONED BY (source_table, days(kafka_ts))
        TBLPROPERTIES ({_APPEND_PROPS})
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_DEAD_LETTER} (
            topic           string,
            kafka_partition int,
            kafka_offset    bigint,
            kafka_ts        timestamp,
            key             string,
            raw_value       binary,
            reason          string,
            query_id        string,
            batch_id        bigint,
            ingested_at     timestamp
        ) USING iceberg
        PARTITIONED BY (days(kafka_ts))
        TBLPROPERTIES ({_APPEND_PROPS})
    """)

    for contract in CONTRACTS.values():
        cols = ",\n            ".join(
            f"`{name}` {type_}"
            for name, type_ in [(c.name, c.type) for c in contract.columns] + list(META_COLUMNS)
        )
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {contract.silver_table} (
            {cols}
            ) USING iceberg
            TBLPROPERTIES ({_SILVER_PROPS})
        """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_SCD2} (
            loan_id         bigint,
            status          string,
            valid_from      timestamp,
            valid_from_lsn  bigint,
            valid_to        timestamp,
            valid_to_lsn    bigint,
            is_current      boolean,
            _silver_ts      timestamp
        ) USING iceberg
        TBLPROPERTIES ({_SILVER_PROPS})
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {OPS_BATCHES} (
            query_id        string,
            batch_id        bigint,
            started_at      timestamp,
            committed_at    timestamp,
            num_records     bigint,
            num_dead_letter bigint,
            table_counts    map<string, bigint>,
            end_offsets     map<string, bigint>
        ) USING iceberg
        TBLPROPERTIES ({_APPEND_PROPS})
    """)
