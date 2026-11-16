"""Reconciliation: Postgres (source of truth) against the lake, through Trino.

Checks, all of which must pass:

  current state   for each replicated table, the row count and an
                  order-independent checksum over every column match between
                  Postgres and the silver table (tombstones excluded)
  status history  for each loan, the sequence of statuses in the SCD2 table
                  equals the sequence the application wrote to
                  loan_status_history; exactly one current version per loan;
                  loans deleted at the source end in DELETED
  bronze          one row per Kafka record: no duplicate (topic, partition,
                  offset), and bronze + dead letter = records in Kafka
  dead letter     row count equals the number of poison records injected
                  (only when artifacts/injections.jsonl exists)

The column list comes from Postgres at run time, so a column added mid-run is
part of the checksum without any change here.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from lakehouse_cdc.clients import Lake, kafka_offsets, pg_connect
from lakehouse_cdc.config import Settings
from lakehouse_cdc.contracts import (
    BRONZE_DEAD_LETTER,
    BRONZE_EVENTS,
    CONTRACTS,
    DELETED_STATUS,
    SILVER_SCD2,
    TableContract,
)
from lakehouse_cdc.inject import read_ledger

NULL_MARKER = "∅"
SEPARATOR = "|"


@dataclass
class Check:
    name: str
    passed: bool
    detail: str
    diagnostics: list[str] = field(default_factory=list)


# -- canonical row text ------------------------------------------------------
#
# Both engines render every value to the same text, the texts are joined with a
# separator, and the row is hashed with MD5. Per-row hashes are combined with
# SUM, so the checksum does not depend on row order.


def pg_value_sql(column: str, data_type: str) -> str:
    c = f'"{column}"'
    if data_type == "timestamp with time zone":
        text = f"to_char({c} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || ' UTC'"
    elif data_type == "date":
        text = f"to_char({c}, 'YYYY-MM-DD')"
    else:
        text = f"{c}::text"
    return f"coalesce({text}, '{NULL_MARKER}')"


def trino_value_sql(column: str, data_type: str) -> str:
    c = f'"{column}"'
    if data_type.startswith("timestamp") and "with time zone" in data_type:
        text = f"cast({c} AT TIME ZONE 'UTC' AS varchar)"
    elif data_type.startswith("varchar"):
        text = c
    else:
        text = f"cast({c} AS varchar)"
    return f"coalesce({text}, '{NULL_MARKER}')"


def pg_row_hash_sql(columns: list[tuple[str, str]]) -> str:
    parts = f" || '{SEPARATOR}' || ".join(pg_value_sql(n, t) for n, t in columns)
    return f"md5({parts})"


def trino_row_hash_sql(columns: list[tuple[str, str]]) -> str:
    parts = f" || '{SEPARATOR}' || ".join(trino_value_sql(n, t) for n, t in columns)
    return f"lower(to_hex(md5(to_utf8({parts}))))"


def pg_checksum_sql(table: str, columns: list[tuple[str, str]]) -> str:
    h = pg_row_hash_sql(columns)
    return f"""
        SELECT count(*),
               coalesce(sum(('x' || substr(h, 1, 15))::bit(60)::bigint), 0),
               coalesce(sum(('x' || substr(h, 17, 15))::bit(60)::bigint), 0)
        FROM (SELECT {h} AS h FROM public."{table}") t
    """


def trino_checksum_sql(table: str, columns: list[tuple[str, str]]) -> str:
    h = trino_row_hash_sql(columns)
    return f"""
        SELECT count(*),
               coalesce(sum(cast(from_base(substr(h, 1, 15), 16) AS decimal(38, 0))), 0),
               coalesce(sum(cast(from_base(substr(h, 17, 15), 16) AS decimal(38, 0))), 0)
        FROM (SELECT {h} AS h FROM {table} WHERE NOT _is_deleted) t
    """


def diff_keyed_hashes(source: dict, target: dict, limit: int = 10) -> list[str]:
    """Human-readable differences between two {key: hash} maps."""
    missing = sorted(set(source) - set(target))
    extra = sorted(set(target) - set(source))
    different = sorted(k for k in set(source) & set(target) if source[k] != target[k])
    out = []
    for label, keys in (
        ("in Postgres, missing in silver", missing),
        ("in silver, not in Postgres", extra),
        ("present in both, content differs", different),
    ):
        if keys:
            shown = ", ".join(str(k) for k in keys[:limit])
            more = f" (+{len(keys) - limit} more)" if len(keys) > limit else ""
            out.append(f"{len(keys)} keys {label}: {shown}{more}")
    return out


# -- checks ------------------------------------------------------------------


def _pg_columns(pg, table: str) -> list[tuple[str, str]]:
    return [
        (r[0], r[1])
        for r in pg.execute(
            "SELECT column_name, data_type FROM information_schema.columns"
            " WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position",
            (table,),
        )
    ]


def _lake_columns(lake: Lake, table: str) -> dict[str, str]:
    rows = lake.query(
        "SELECT column_name, data_type FROM lake.information_schema.columns"
        f" WHERE table_schema = 'silver' AND table_name = '{table}'"
    )
    return dict(rows)


def check_table(pg, lake: Lake, contract: TableContract) -> Check:
    name = contract.source_table
    pg_cols = _pg_columns(pg, name)
    lake_types = _lake_columns(lake, name)
    absent = [c for c, _ in pg_cols if c not in lake_types]
    if absent:
        return Check(f"silver.{name}", False, f"columns missing in silver: {', '.join(absent)}")
    lake_cols = [(c, lake_types[c]) for c, _ in pg_cols]

    src = pg.execute(pg_checksum_sql(name, pg_cols)).fetchone()
    dst = lake.query(trino_checksum_sql(contract.silver_table, lake_cols))[0]
    src = tuple(int(v) for v in src)
    dst = tuple(int(v) for v in dst)
    passed = src == dst
    detail = (
        f"rows pg={src[0]} silver={dst[0]}  "
        f"checksum pg={src[1]:x}:{src[2]:x} silver={dst[1]:x}:{dst[2]:x}  "
        f"columns={len(pg_cols)}"
    )
    diagnostics: list[str] = []
    if not passed:
        pk = contract.primary_key
        src_rows = dict(
            pg.execute(f'SELECT "{pk}", {pg_row_hash_sql(pg_cols)} FROM public."{name}"')
        )
        dst_rows = dict(
            lake.query(
                f'SELECT "{pk}", {trino_row_hash_sql(lake_cols)} FROM {contract.silver_table}'
                " WHERE NOT _is_deleted"
            )
        )
        diagnostics = diff_keyed_hashes(src_rows, dst_rows)
    return Check(f"silver.{name}", passed, detail, diagnostics)


def check_status_history(pg, lake: Lake) -> Check:
    expected = dict(
        pg.execute(
            "SELECT loan_id, string_agg(to_status, '>' ORDER BY history_id)"
            " FROM loan_status_history GROUP BY loan_id"
        )
    )
    live_loans = {r[0] for r in pg.execute("SELECT loan_id FROM loans")}
    rows = lake.query(f"""
        SELECT loan_id,
               array_join(array_agg(status ORDER BY valid_from_lsn), '>'),
               count_if(is_current),
               element_at(array_agg(status ORDER BY valid_from_lsn), -1),
               count_if(valid_to IS NULL) + count_if(valid_to_lsn IS NULL)
        FROM {SILVER_SCD2}
        GROUP BY loan_id
    """)
    actual: dict[int, str] = {}
    problems: list[str] = []
    deleted = 0
    for loan_id, path, n_current, last_status, n_open in rows:
        if n_current != 1 or n_open != 2:
            problems.append(f"loan {loan_id}: {n_current} current versions")
        if loan_id in live_loans:
            actual[loan_id] = path
        else:
            deleted += 1
            if last_status != DELETED_STATUS:
                problems.append(f"loan {loan_id}: deleted at source but SCD2 ends in {last_status}")
    expected_live = {k: v for k, v in expected.items() if k in live_loans}
    problems += diff_keyed_hashes(expected_live, actual)
    versions = sum(p.count(">") + 1 for p in actual.values())
    return Check(
        "silver.loan_status_scd2",
        not problems,
        f"loans compared={len(expected_live)} versions={versions} "
        f"deleted loans closed with {DELETED_STATUS}={deleted}",
        problems[:12],
    )


def check_bronze(lake: Lake, settings: Settings) -> list[Check]:
    total, distinct = lake.query(f"""
        SELECT count(*), count(DISTINCT (topic, kafka_partition, kafka_offset)) FROM {BRONZE_EVENTS}
    """)[0]
    dead, dead_distinct = lake.query(f"""
        SELECT count(*), count(DISTINCT (topic, kafka_partition, kafka_offset))
        FROM {BRONZE_DEAD_LETTER}
    """)[0]
    in_kafka = sum(hi - lo for lo, hi in kafka_offsets(settings.kafka_bootstrap).values())
    checks = [
        Check(
            "bronze: no duplicate offsets",
            total == distinct and dead == dead_distinct,
            f"cdc_events rows={total} distinct offsets={distinct}; "
            f"dead_letter rows={dead} distinct offsets={dead_distinct}",
        ),
        Check(
            "bronze: complete vs Kafka",
            total + dead == in_kafka,
            f"kafka records={in_kafka} bronze={total} dead_letter={dead}",
        ),
    ]
    injected = read_ledger()
    if injected:
        expected = injected.get("poison", 0)
        reasons = lake.query(
            f"SELECT reason, count(*) FROM {BRONZE_DEAD_LETTER} GROUP BY 1 ORDER BY 1"
        )
        checks.append(
            Check(
                "dead letter: matches injected poison",
                dead == expected,
                f"injected={expected} dead_letter={dead} "
                + " ".join(f"{reason}={n}" for reason, n in reasons),
            )
        )
    return checks


def run_checks(settings: Settings) -> list[Check]:
    lake = Lake(settings)
    checks: list[Check] = []
    # One repeatable-read transaction so every Postgres query sees the same state.
    with pg_connect(settings, autocommit=False) as pg:
        pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for contract in CONTRACTS.values():
            checks.append(check_table(pg, lake, contract))
        checks.append(check_status_history(pg, lake))
    checks.extend(check_bronze(lake, settings))
    return checks


def format_report(checks: list[Check]) -> str:
    lines = []
    for c in checks:
        lines.append(f"{'PASS' if c.passed else 'FAIL'}  {c.name:<38} {c.detail}")
        lines.extend(f"        {d}" for d in c.diagnostics)
    failed = sum(not c.passed for c in checks)
    lines.append("")
    lines.append(
        f"RECONCILIATION {'PASSED' if not failed else 'FAILED'}: "
        f"{len(checks) - failed}/{len(checks)} checks passed"
    )
    return "\n".join(lines)
