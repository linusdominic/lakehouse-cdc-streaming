"""Debezium envelope parsing in Spark SQL.

Mirrors lakehouse_cdc.envelope.parse_envelope. tests/spark/test_envelope_parity.py
runs both on the same records and compares the classification.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakehouse_cdc import envelope as E
from lakehouse_cdc.contracts import CONTRACTS

_ENVELOPE_DDL = "schema string, payload string"
_PAYLOAD_DDL = "op string, before string, after string, source string, ts_ms string"
_SOURCE_DDL = "lsn string, ts_ms string, `table` string, txId string"


def _is_object(c: Column) -> Column:
    return c.isNotNull() & c.startswith("{")


def _digits(c: Column) -> Column:
    return F.when(c.rlike("^[0-9]+$"), c.cast("bigint"))


def _missing_pk() -> Column:
    """True when the row image of a contracted table has no primary key value."""
    cond = F.lit(False)
    for name, contract in CONTRACTS.items():
        pk_value = F.get_json_object(F.col("row_image"), f"$.{contract.primary_key}")
        cond = cond | ((F.col("source_table") == name) & pk_value.isNull())
    return cond


def parse_envelope(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows -> one row per record with parsed fields and dlq_reason.

    dlq_reason is NULL for records that are valid change events.
    """
    df = kafka_df.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("key"),
        F.col("value").alias("raw_value"),
        F.from_json(F.col("value").cast("string"), _ENVELOPE_DDL).alias("_env"),
    )
    df = df.withColumn(
        "_payload",
        F.when(_is_object(F.col("_env.payload")), F.from_json("_env.payload", _PAYLOAD_DDL)),
    ).withColumn(
        "_source",
        F.when(_is_object(F.col("_payload.source")), F.from_json("_payload.source", _SOURCE_DDL)),
    )
    df = df.select(
        "topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        "key",
        "raw_value",
        F.col("_payload").isNotNull().alias("_has_payload"),
        F.col("_payload.op").alias("op"),
        _digits(F.col("_source.lsn")).alias("lsn"),
        _digits(F.col("_source.txId")).alias("tx_id"),
        F.when(F.col("_source.table") != "", F.col("_source.table")).alias("source_table"),
        F.timestamp_millis(_digits(F.col("_source.ts_ms"))).alias("source_ts"),
        F.timestamp_millis(_digits(F.col("_payload.ts_ms"))).alias("connector_ts"),
        F.when(_is_object(F.col("_payload.before")), F.col("_payload.before")).alias("before"),
        F.when(_is_object(F.col("_payload.after")), F.col("_payload.after")).alias("after"),
        F.col("_env.schema").alias("schema_json"),
    ).withColumn("row_image", F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after")))
    reason = (
        F.when(F.col("raw_value").isNull(), E.REASON_NULL_VALUE)
        .when(~F.col("_has_payload"), E.REASON_UNPARSEABLE)
        .when(F.col("op").isNull() | ~F.col("op").isin(*sorted(E.VALID_OPS)), E.REASON_UNKNOWN_OP)
        .when(F.col("lsn").isNull(), E.REASON_MISSING_LSN)
        .when(F.col("source_table").isNull(), E.REASON_MISSING_TABLE)
        .when(F.col("row_image").isNull(), E.REASON_MISSING_ROW_IMAGE)
        .when(_missing_pk(), E.REASON_MISSING_PRIMARY_KEY)
    )
    return df.withColumn("dlq_reason", reason).drop("_has_payload")
