"""Spark tests run inside the Spark image (make test-spark), not on the host.

They use a throwaway Hadoop catalog on local disk under the same catalog name
the jobs use, so the production sink code runs unchanged.
"""

from __future__ import annotations

import json
import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession

from lakehouse_cdc.contracts import CATALOG


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="iceberg-test-")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-cdc-tests")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"file://{warehouse}")
        .getOrCreate()
    )
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture()
def fresh_tables(spark):
    """Drop and recreate every pipeline table."""
    from lakehouse_cdc.sparkjobs.tables import ensure_tables

    for namespace in ("bronze", "silver", "ops"):
        if spark.catalog.databaseExists(f"{CATALOG}.{namespace}"):
            for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{namespace}").collect():
                spark.sql(f"DROP TABLE {CATALOG}.{namespace}.{row['tableName']} PURGE")
    ensure_tables(spark)


LOAN_FIELDS = [
    ("loan_id", "int64", None),
    ("borrower_id", "int64", None),
    ("product_type", "string", None),
    ("property_state", "string", None),
    ("original_principal", "string", None),
    ("principal_balance", "string", None),
    ("interest_rate", "string", None),
    ("term_months", "int32", None),
    ("origination_date", "int32", "io.debezium.time.Date"),
    ("next_due_date", "int32", "io.debezium.time.Date"),
    ("status", "string", None),
    ("created_at", "string", "io.debezium.time.ZonedTimestamp"),
    ("updated_at", "string", "io.debezium.time.ZonedTimestamp"),
]


def envelope_schema(fields) -> dict:
    row = {
        "type": "struct",
        "optional": True,
        "fields": [
            {"type": t, "optional": True, "field": n, **({"name": logical} if logical else {})}
            for n, t, logical in fields
        ],
    }
    return {
        "type": "struct",
        "fields": [
            {**row, "field": "before"},
            {**row, "field": "after"},
            {"type": "struct", "fields": [], "field": "source"},
            {"type": "string", "field": "op"},
        ],
    }


def loan_row(loan_id: int, status: str, balance: str = "1000.00") -> dict:
    return {
        "loan_id": loan_id,
        "borrower_id": 1,
        "product_type": "FIXED_30",
        "property_state": "CA",
        "original_principal": "1000.00",
        "principal_balance": balance,
        "interest_rate": "0.05125",
        "term_months": 360,
        "origination_date": 19723,
        "next_due_date": None,
        "status": status,
        "created_at": "2024-01-01T00:00:00.5Z",
        "updated_at": "2024-01-01T00:00:00.123456Z",
    }


def envelope(table, op, lsn, before=None, after=None, fields=LOAN_FIELDS, ts_ms=None) -> bytes:
    return json.dumps(
        {
            "schema": envelope_schema(fields),
            "payload": {
                "before": before,
                "after": after,
                "source": {
                    "lsn": lsn,
                    "table": table,
                    "ts_ms": ts_ms or 1_700_000_000_000 + lsn,
                    "txId": lsn,
                },
                "op": op,
                "ts_ms": 1_700_000_000_000 + lsn,
            },
        }
    ).encode()


def kafka_frame(spark, values, topic="cdc.public.loans", start_offset=0):
    """A DataFrame shaped like the Kafka source output."""
    rows = [
        (b"k", v, topic, 0, start_offset + i, 1_700_000_000.0 + i) for i, v in enumerate(values)
    ]
    return spark.createDataFrame(
        rows, "key binary, value binary, topic string, partition int, offset long, ts double"
    ).selectExpr(
        "key", "value", "topic", "partition", "offset", "cast(ts as timestamp) as timestamp"
    )


def run_batch(spark, values, batch_id=0, **kwargs):
    from lakehouse_cdc.sparkjobs.streaming import process_batch

    process_batch(kafka_frame(spark, values, **kwargs), batch_id)
