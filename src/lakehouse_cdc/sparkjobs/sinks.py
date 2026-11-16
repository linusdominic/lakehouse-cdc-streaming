"""Idempotent Iceberg writes used inside foreachBatch.

Every write here can be repeated with the same input and leave the table
unchanged. That is what turns Spark's at-least-once micro-batch replay into
effectively-once tables; see ADR 0002.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from lakehouse_cdc.contracts import (
    BRONZE_DEAD_LETTER,
    BRONZE_EVENTS,
    DELETED_STATUS,
    META_COLUMN_NAMES,
    SILVER_SCD2,
    TableContract,
)
from lakehouse_cdc.schema_evolution import (
    SourceField,
    conversion_sql,
    plan_new_columns,
    row_fields_from_schema,
)

_BRONZE_COLS = (
    "topic", "kafka_partition", "kafka_offset", "kafka_ts", "key", "op", "lsn", "tx_id",
    "source_table", "source_ts", "connector_ts", "before", "after",
    "query_id", "batch_id", "ingested_at",
)  # fmt: skip
_DLQ_COLS = (
    "topic", "kafka_partition", "kafka_offset", "kafka_ts", "key", "raw_value", "reason",
    "query_id", "batch_id", "ingested_at",
)  # fmt: skip


def _append_once(df: DataFrame, table: str, columns: tuple[str, ...], view: str) -> None:
    """Insert rows whose (topic, partition, offset) is not already in the table.

    A re-executed micro-batch covers exactly the same offsets as the first
    attempt, so an insert-only MERGE on the Kafka coordinates makes the append
    idempotent. The lower bounds are constants so Iceberg can prune the target
    scan to the files that could overlap this batch (normally none).
    """
    spark = df.sparkSession
    bounds = df.agg(
        F.date_format(F.min("kafka_ts"), "yyyy-MM-dd HH:mm:ss.SSSSSS"), F.min("kafka_offset")
    ).first()
    if bounds is None or bounds[0] is None:
        return
    min_ts, min_offset = bounds
    df.select(*columns).createOrReplaceTempView(view)
    col_list = ", ".join(columns)
    spark.sql(f"""
        MERGE INTO {table} t
        USING {view} s
        ON  t.topic = s.topic
        AND t.kafka_partition = s.kafka_partition
        AND t.kafka_offset = s.kafka_offset
        AND t.kafka_ts >= TIMESTAMP '{min_ts}'
        AND t.kafka_offset >= {min_offset}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({", ".join("s." + c for c in columns)})
    """)


def append_bronze(valid: DataFrame) -> None:
    _append_once(valid, BRONZE_EVENTS, _BRONZE_COLS, "_bronze_batch")


def append_dead_letter(invalid: DataFrame) -> None:
    df = invalid.withColumnRenamed("dlq_reason", "reason")
    _append_once(df, BRONZE_DEAD_LETTER, _DLQ_COLS, "_dlq_batch")


def _data_columns(spark: SparkSession, table: str) -> list[tuple[str, str]]:
    return [
        (f.name, f.dataType.simpleString())
        for f in spark.table(table).schema.fields
        if f.name not in META_COLUMN_NAMES
    ]


def evolve_schema(
    spark: SparkSession, contract: TableContract, schema_jsons: list[str]
) -> dict[str, SourceField]:
    """Add columns that appeared at the source. Returns the observed source fields."""
    observed: dict[str, SourceField] = {}
    for schema_json in schema_jsons:
        observed.update(row_fields_from_schema(schema_json))
    table = contract.silver_table
    existing = [name for name, _ in _data_columns(spark, table)]
    for name, spark_type in plan_new_columns(existing, observed):
        print(f"[schema] {table}: ADD COLUMN {name} {spark_type}", flush=True)
        spark.sql(f"ALTER TABLE {table} ADD COLUMN `{name}` {spark_type}")
    return observed


def merge_current_state(
    events: DataFrame, contract: TableContract, schema_jsons: list[str]
) -> None:
    """Upsert one table's change events into its silver current-state table.

    1. Within the batch keep one event per key, the one with the highest LSN.
    2. MERGE on the key; a matched row is overwritten only if the incoming LSN
       is strictly greater than the stored one.
    3. Deletes are written as tombstones so the LSN guard also covers events
       that arrive after the delete.
    """
    spark = events.sparkSession
    table = contract.silver_table
    pk = contract.primary_key

    observed = evolve_schema(spark, contract, schema_jsons)
    data_cols = _data_columns(spark, table)

    all_strings = ", ".join(f"`{name}` string" for name, _ in data_cols)
    projected = events.withColumn("_row", F.from_json("row_image", all_strings)).selectExpr(
        *[
            f"{conversion_sql(name, type_, getattr(observed.get(name), 'logical_name', None), '_row')}"
            f" as `{name}`"
            for name, type_ in data_cols
        ],
        "lsn as _lsn",
        "op as _op",
        "op = 'd' as _is_deleted",
        "source_ts as _source_ts",
        "ingested_at as _silver_ts",
        "kafka_offset as _kafka_offset",
    )
    newest_first = Window.partitionBy(pk).orderBy(F.desc("_lsn"), F.desc("_kafka_offset"))
    latest = (
        projected.withColumn("_rn", F.row_number().over(newest_first))
        .where("_rn = 1")
        .drop("_rn", "_kafka_offset")
    )

    view = f"_silver_batch_{contract.source_table}"
    latest.createOrReplaceTempView(view)
    cols = [name for name, _ in data_cols] + sorted(META_COLUMN_NAMES)
    spark.sql(f"""
        MERGE INTO {table} t
        USING {view} s
        ON t.`{pk}` = s.`{pk}`
        WHEN MATCHED AND s._lsn > t._lsn THEN
            UPDATE SET {", ".join(f"t.`{c}` = s.`{c}`" for c in cols)}
        WHEN NOT MATCHED THEN
            INSERT ({", ".join(f"`{c}`" for c in cols)})
            VALUES ({", ".join(f"s.`{c}`" for c in cols)})
    """)


def merge_status_history(loan_events: DataFrame) -> None:
    """Maintain the SCD2 table over loans.status.

    Each status-changing event becomes a version keyed by (loan_id, LSN).
    For the loans touched in this batch, the stored versions and the new ones
    are combined and the validity intervals recomputed from scratch, so the
    result does not depend on the order in which the events arrived.
    """
    spark = loan_events.sparkSession
    changes = (
        loan_events.select(
            F.get_json_object("row_image", "$.loan_id").cast("bigint").alias("loan_id"),
            F.when(F.col("op") == "d", F.lit(DELETED_STATUS))
            .otherwise(F.get_json_object("after", "$.status"))
            .alias("status"),
            F.col("source_ts").alias("valid_from"),
            F.col("lsn").alias("valid_from_lsn"),
            F.col("op"),
            F.get_json_object("before", "$.status").alias("before_status"),
            F.col("ingested_at"),
        )
        .where("op in ('c', 'r', 'd') or not (before_status <=> status)")
        .drop("op", "before_status")
        .dropDuplicates(["loan_id", "valid_from_lsn"])
    )
    affected = changes.select("loan_id").distinct()
    stored = (
        spark.table(SILVER_SCD2)
        .join(F.broadcast(affected), "loan_id", "left_semi")
        .select("loan_id", "status", "valid_from", "valid_from_lsn", "_silver_ts")
    )
    combined = stored.unionByName(
        changes.withColumnRenamed("ingested_at", "_silver_ts")
    ).dropDuplicates(["loan_id", "valid_from_lsn"])

    by_lsn = Window.partitionBy("loan_id").orderBy("valid_from_lsn")
    versions = (
        combined.withColumn("valid_to", F.lead("valid_from").over(by_lsn))
        .withColumn("valid_to_lsn", F.lead("valid_from_lsn").over(by_lsn))
        .withColumn("is_current", F.col("valid_to_lsn").isNull())
    )
    versions.createOrReplaceTempView("_scd2_batch")
    spark.sql(f"""
        MERGE INTO {SILVER_SCD2} t
        USING _scd2_batch s
        ON t.loan_id = s.loan_id AND t.valid_from_lsn = s.valid_from_lsn
        WHEN MATCHED AND NOT (t.valid_to_lsn <=> s.valid_to_lsn) THEN UPDATE SET
            t.valid_to = s.valid_to,
            t.valid_to_lsn = s.valid_to_lsn,
            t.is_current = s.is_current,
            t._silver_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
            (loan_id, status, valid_from, valid_from_lsn, valid_to, valid_to_lsn, is_current, _silver_ts)
            VALUES (s.loan_id, s.status, s.valid_from, s.valid_from_lsn, s.valid_to, s.valid_to_lsn,
                    s.is_current, s._silver_ts)
    """)
