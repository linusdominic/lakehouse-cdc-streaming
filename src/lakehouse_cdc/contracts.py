"""Table contracts for the silver layer.

The contract is the declared schema used to create each silver table. After
creation the Iceberg table schema is the source of truth: columns added at the
source are appended by the streaming job (see schema_evolution.py), so the
contract is a floor, not a ceiling.

Pure module: imported by the Spark jobs (Python 3.10 in the Spark image) and
by the host-side tooling. Keep it free of third-party imports.
"""

from __future__ import annotations

from dataclasses import dataclass

CATALOG = "lake"
BRONZE_EVENTS = f"{CATALOG}.bronze.cdc_events"
BRONZE_DEAD_LETTER = f"{CATALOG}.bronze.dead_letter"
OPS_BATCHES = f"{CATALOG}.ops.stream_batches"
SILVER_SCD2 = f"{CATALOG}.silver.loan_status_scd2"

# Metadata columns carried on every silver current-state table.
META_COLUMNS: tuple[tuple[str, str], ...] = (
    ("_lsn", "bigint"),  # Postgres LSN of the change that produced this row version
    ("_op", "string"),  # Debezium op of that change: c, u, d, r
    ("_is_deleted", "boolean"),  # soft-delete tombstone, see ADR 0003
    ("_source_ts", "timestamp"),  # commit time in Postgres (source.ts_ms)
    ("_silver_ts", "timestamp"),  # wall clock when the merge that wrote the row ran
)
META_COLUMN_NAMES = frozenset(name for name, _ in META_COLUMNS)

# Synthetic terminal status written to the SCD2 table when a loan is hard-deleted.
DELETED_STATUS = "DELETED"


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # Spark SQL type


@dataclass(frozen=True)
class TableContract:
    source_table: str  # table name in Postgres schema "public"
    primary_key: str
    columns: tuple[Column, ...]

    @property
    def silver_table(self) -> str:
        return f"{CATALOG}.silver.{self.source_table}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


def _cols(*pairs: tuple[str, str]) -> tuple[Column, ...]:
    return tuple(Column(n, t) for n, t in pairs)


BORROWERS = TableContract(
    source_table="borrowers",
    primary_key="borrower_id",
    columns=_cols(
        ("borrower_id", "bigint"),
        ("full_name", "string"),
        ("email", "string"),
        ("state_code", "string"),
        ("credit_score", "int"),
        ("annual_income", "decimal(14,2)"),
        ("is_active", "boolean"),
        ("created_at", "timestamp"),
        ("updated_at", "timestamp"),
    ),
)

LOANS = TableContract(
    source_table="loans",
    primary_key="loan_id",
    columns=_cols(
        ("loan_id", "bigint"),
        ("borrower_id", "bigint"),
        ("product_type", "string"),
        ("property_state", "string"),
        ("original_principal", "decimal(14,2)"),
        ("principal_balance", "decimal(14,2)"),
        ("interest_rate", "decimal(7,5)"),
        ("term_months", "int"),
        ("origination_date", "date"),
        ("next_due_date", "date"),
        ("status", "string"),
        ("created_at", "timestamp"),
        ("updated_at", "timestamp"),
    ),
)

PAYMENTS = TableContract(
    source_table="payments",
    primary_key="payment_id",
    columns=_cols(
        ("payment_id", "bigint"),
        ("loan_id", "bigint"),
        ("payment_date", "date"),
        ("amount", "decimal(14,2)"),
        ("principal_component", "decimal(14,2)"),
        ("interest_component", "decimal(14,2)"),
        ("fee_component", "decimal(14,2)"),
        ("status", "string"),
        ("created_at", "timestamp"),
        ("updated_at", "timestamp"),
    ),
)

LOAN_STATUS_HISTORY = TableContract(
    source_table="loan_status_history",
    primary_key="history_id",
    columns=_cols(
        ("history_id", "bigint"),
        ("loan_id", "bigint"),
        ("from_status", "string"),
        ("to_status", "string"),
        ("reason", "string"),
        ("changed_at", "timestamp"),
    ),
)

CONTRACTS: dict[str, TableContract] = {
    c.source_table: c for c in (BORROWERS, LOANS, PAYMENTS, LOAN_STATUS_HISTORY)
}

# Captured by Debezium and landed in bronze, but not modelled in silver.
BRONZE_ONLY_TABLES = frozenset({"pipeline_marker"})

TOPIC_PREFIX = "cdc"
TOPIC_PATTERN = r"cdc\.public\..*"


def topic_for(source_table: str) -> str:
    return f"{TOPIC_PREFIX}.public.{source_table}"
