import json

from lakehouse_cdc.schema_evolution import (
    SourceField,
    conversion_sql,
    plan_new_columns,
    row_fields_from_schema,
)


def schema(fields) -> str:
    row = {"type": "struct", "optional": True, "fields": fields}
    return json.dumps(
        {
            "type": "struct",
            "fields": [
                {**row, "field": "before"},
                {**row, "field": "after"},
                {"type": "string", "field": "op"},
            ],
        }
    )


FIELDS = [
    {"type": "int64", "optional": False, "field": "payment_id"},
    {"type": "string", "optional": False, "field": "amount"},
    {"type": "int32", "optional": False, "name": "io.debezium.time.Date", "field": "payment_date"},
    {
        "type": "string",
        "optional": False,
        "name": "io.debezium.time.ZonedTimestamp",
        "field": "created_at",
    },
    {"type": "string", "optional": True, "field": "channel"},
]


def test_extracts_row_fields_with_logical_types():
    fields = row_fields_from_schema(schema(FIELDS))
    assert list(fields) == ["payment_id", "amount", "payment_date", "created_at", "channel"]
    assert fields["payment_date"] == SourceField("payment_date", "int32", "io.debezium.time.Date")
    assert fields["channel"].logical_name is None


def test_bad_schemas_yield_nothing():
    assert row_fields_from_schema(None) == {}
    assert row_fields_from_schema("") == {}
    assert row_fields_from_schema("{not json") == {}
    assert row_fields_from_schema("[]") == {}
    assert row_fields_from_schema('{"type": "string"}') == {}


def test_type_mapping_for_new_columns():
    def spark_type(connect_type, logical=None):
        return SourceField("c", connect_type, logical).spark_type

    assert spark_type("int16") == "int"
    assert spark_type("int64") == "bigint"
    assert spark_type("boolean") == "boolean"
    assert spark_type("float64") == "double"
    assert spark_type("string") == "string"
    assert spark_type("int32", "io.debezium.time.Date") == "date"
    assert spark_type("int64", "io.debezium.time.MicroTimestamp") == "timestamp"
    assert spark_type("string", "io.debezium.time.ZonedTimestamp") == "timestamp"
    # No safe mapping: keep the raw text rather than guess.
    assert spark_type("bytes") == "string"
    assert spark_type("struct") == "string"


def test_only_missing_columns_are_planned():
    observed = row_fields_from_schema(schema(FIELDS))
    existing = ["payment_id", "amount", "payment_date", "created_at"]
    assert plan_new_columns(existing, observed) == [("channel", "string")]
    assert plan_new_columns([*existing, "channel"], observed) == []
    assert plan_new_columns([*existing, "CHANNEL"], observed) == []


def test_a_column_dropped_at_the_source_is_left_alone():
    observed = row_fields_from_schema(schema(FIELDS[:2]))
    assert plan_new_columns(["payment_id", "amount", "payment_date"], observed) == []


def test_conversion_sql():
    assert (
        conversion_sql("amount", "decimal(14,2)", None, "r") == "cast(r.`amount` as decimal(14,2))"
    )
    assert (
        conversion_sql("payment_date", "date", "io.debezium.time.Date", "r")
        == "date_from_unix_date(cast(r.`payment_date` as int))"
    )
    assert conversion_sql("d", "date", None, "r") == "date_from_unix_date(cast(r.`d` as int))"
    assert conversion_sql("t", "timestamp", None, "r") == "cast(r.`t` as timestamp)"
    assert (
        conversion_sql("t", "timestamp", "io.debezium.time.MicroTimestamp", "r")
        == "timestamp_micros(cast(r.`t` as bigint))"
    )
    assert (
        conversion_sql("t", "timestamp", "io.debezium.time.Timestamp", "r")
        == "timestamp_millis(cast(r.`t` as bigint))"
    )
