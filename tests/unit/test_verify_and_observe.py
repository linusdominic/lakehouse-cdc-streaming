import pytest

from lakehouse_cdc.contracts import CONTRACTS, META_COLUMN_NAMES
from lakehouse_cdc.observe import percentile
from lakehouse_cdc.verify import (
    Check,
    diff_keyed_hashes,
    format_report,
    pg_checksum_sql,
    pg_value_sql,
    trino_checksum_sql,
    trino_value_sql,
)


def test_percentile_nearest_rank():
    values = [float(v) for v in range(1, 101)]
    assert percentile(values, 50) == 50
    assert percentile(values, 95) == 95
    assert percentile(values, 100) == 100
    assert percentile([7.0], 95) == 7.0
    assert percentile([3.0, 1.0, 2.0], 50) == 2.0
    with pytest.raises(ValueError):
        percentile([], 50)


def test_diff_reports_each_kind_of_difference():
    source = {1: "a", 2: "b", 3: "c"}
    target = {2: "b", 3: "X", 4: "d"}
    assert diff_keyed_hashes(source, target) == [
        "1 keys in Postgres, missing in silver: 1",
        "1 keys in silver, not in Postgres: 4",
        "1 keys present in both, content differs: 3",
    ]
    assert diff_keyed_hashes(source, dict(source)) == []


def test_diff_truncates_long_lists():
    source = dict.fromkeys(range(25), "a")
    (line,) = diff_keyed_hashes(source, {}, limit=10)
    assert line.endswith("(+15 more)")


def test_timestamps_are_rendered_in_utc_on_both_sides():
    assert "AT TIME ZONE 'UTC'" in pg_value_sql("created_at", "timestamp with time zone")
    assert "AT TIME ZONE 'UTC'" in trino_value_sql("created_at", "timestamp(6) with time zone")


def test_nulls_get_an_explicit_marker_on_both_sides():
    for sql in (pg_value_sql("c", "text"), trino_value_sql("c", "varchar")):
        assert sql.startswith("coalesce(") and "∅" in sql


def test_checksum_sql_covers_every_column_and_skips_tombstones():
    columns = [("payment_id", "bigint"), ("amount", "numeric"), ("channel", "text")]
    pg = pg_checksum_sql("payments", columns)
    lake = trino_checksum_sql("lake.silver.payments", columns)
    for name, _ in columns:
        assert f'"{name}"' in pg and f'"{name}"' in lake
    assert "NOT _is_deleted" in lake


def test_report_summary_line():
    report = format_report([Check("a", True, "ok"), Check("b", False, "bad", ["key 1"])])
    assert "FAIL  b" in report and "        key 1" in report
    assert report.endswith("RECONCILIATION FAILED: 1/2 checks passed")
    assert format_report([Check("a", True, "ok")]).endswith(
        "RECONCILIATION PASSED: 1/1 checks passed"
    )


def test_contracts_are_well_formed():
    for contract in CONTRACTS.values():
        assert contract.primary_key in contract.column_names
        assert contract.columns[0].name == contract.primary_key
        assert not META_COLUMN_NAMES & set(contract.column_names)
        assert len(set(contract.column_names)) == len(contract.columns)
