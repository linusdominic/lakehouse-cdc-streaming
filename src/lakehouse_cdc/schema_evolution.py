"""Additive schema evolution for silver tables (pure Python).

Debezium embeds the Kafka Connect schema in every message (schemas.enable=true).
The streaming job collects the distinct schemas seen per table in a micro-batch,
and this module decides which columns to add to the Iceberg table and with what
type.

Policy:
  * New source column -> ALTER TABLE ... ADD COLUMN, nullable, type mapped below.
  * Dropped source column -> nothing. The silver column stays and goes NULL.
  * Type change of an existing column -> not handled; the cast yields NULL or
    fails the batch. That is deliberate: it needs a human (docs/runbook.md).
"""

from __future__ import annotations

import json
from dataclasses import dataclass

# Connect logical type names emitted by Debezium for Postgres with
# time.precision.mode=adaptive and decimal.handling.mode=string.
LOGICAL_DATE = "io.debezium.time.Date"  # int32 days since epoch
LOGICAL_TIMESTAMP_MS = "io.debezium.time.Timestamp"  # int64 millis
LOGICAL_TIMESTAMP_US = "io.debezium.time.MicroTimestamp"  # int64 micros
LOGICAL_ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp"  # ISO-8601 string

_LOGICAL_TO_SPARK = {
    LOGICAL_DATE: "date",
    LOGICAL_TIMESTAMP_MS: "timestamp",
    LOGICAL_TIMESTAMP_US: "timestamp",
    LOGICAL_ZONED_TIMESTAMP: "timestamp",
}

_PRIMITIVE_TO_SPARK = {
    "int8": "int",
    "int16": "int",
    "int32": "int",
    "int64": "bigint",
    "float32": "double",
    "float64": "double",
    "boolean": "boolean",
    "string": "string",
}


@dataclass(frozen=True)
class SourceField:
    name: str
    connect_type: str
    logical_name: str | None = None

    @property
    def spark_type(self) -> str:
        """Target type for a column that is not in the contract.

        Anything without a safe mapping (bytes, struct, array, and NUMERIC,
        which arrives as a string under decimal.handling.mode=string) lands as
        string so that no data is lost; tightening the type is a contract change.
        """
        if self.logical_name in _LOGICAL_TO_SPARK:
            return _LOGICAL_TO_SPARK[self.logical_name]
        return _PRIMITIVE_TO_SPARK.get(self.connect_type, "string")


def row_fields_from_schema(schema_json: str | None) -> dict[str, SourceField]:
    """Extract the row fields from a Debezium envelope schema.

    Uses the "after" struct, falling back to "before" (they share one schema).
    Returns {} when the schema is absent or not an envelope.
    """
    if not schema_json:
        return {}
    try:
        schema = json.loads(schema_json)
    except ValueError:
        return {}
    if not isinstance(schema, dict):
        return {}
    by_name = {f.get("field"): f for f in schema.get("fields") or [] if isinstance(f, dict)}
    row = by_name.get("after") or by_name.get("before")
    if not row:
        return {}
    out: dict[str, SourceField] = {}
    for f in row.get("fields") or []:
        name = f.get("field")
        if name:
            out[name] = SourceField(name, f.get("type", "string"), f.get("name"))
    return out


def plan_new_columns(
    table_columns: list[str] | tuple[str, ...],
    observed: dict[str, SourceField],
) -> list[tuple[str, str]]:
    """Columns to add, as (name, spark_type), in the order the source declares them."""
    existing = {c.lower() for c in table_columns}
    return [(f.name, f.spark_type) for f in observed.values() if f.name.lower() not in existing]


def conversion_sql(column: str, target_type: str, logical_name: str | None, struct: str) -> str:
    """Spark SQL expression converting a Debezium JSON scalar to the target type.

    `struct` is the name of a struct column whose fields are all strings (the
    row image parsed with an all-string schema), so every conversion is a cast
    from the JSON text of the value.
    """
    ref = f"{struct}.`{column}`"
    t = target_type.lower()
    if t == "date":
        if logical_name in (None, LOGICAL_DATE):
            return f"date_from_unix_date(cast({ref} as int))"
        return f"cast({ref} as date)"
    if t.startswith("timestamp"):
        if logical_name == LOGICAL_TIMESTAMP_US:
            return f"timestamp_micros(cast({ref} as bigint))"
        if logical_name == LOGICAL_TIMESTAMP_MS:
            return f"timestamp_millis(cast({ref} as bigint))"
        return f"cast({ref} as timestamp)"
    return f"cast({ref} as {target_type})"
