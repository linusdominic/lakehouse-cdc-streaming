"""Runs a workload plan against Postgres, one plan transaction per commit."""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Iterable

import psycopg

from lakehouse_cdc.generator.planner import Txn

ID_COLUMNS = (
    ("borrowers", "borrower_id"),
    ("loans", "loan_id"),
    ("payments", "payment_id"),
    ("loan_status_history", "history_id"),
)


def current_id_base(conn: psycopg.Connection) -> int:
    """Highest id in use across the tables; the plan allocates above it."""
    selects = " UNION ALL ".join(
        f"SELECT max({col}) AS m FROM {table}" for table, col in ID_COLUMNS
    )
    row = conn.execute(f"SELECT coalesce(max(m), 0) FROM ({selects}) t").fetchone()
    return int(row[0])


def existing_borrowers(conn: psycopg.Connection) -> tuple[int, ...]:
    return tuple(r[0] for r in conn.execute("SELECT borrower_id FROM borrowers ORDER BY 1"))


def channel_exists(conn: psycopg.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM information_schema.columns"
        " WHERE table_schema = 'public' AND table_name = 'payments' AND column_name = 'channel'"
    ).fetchone()
    return row is not None


def execute(
    conn: psycopg.Connection,
    txns: Iterable[Txn],
    rate: float | None = None,
    hooks: dict[int, Callable[[], None]] | None = None,
    progress_every: int = 5000,
) -> Counter:
    """Execute transactions in plan order. Returns a count of transactions by kind.

    `rate` caps transactions per second. `hooks` maps an operation index to a
    callable run just before that operation (used for fault injection).
    """
    counts: Counter = Counter()
    hooks = hooks or {}
    # Do not wait for the WAL flush on every commit. The WAL content, and so
    # the CDC stream, is the same; this only lifts the rate a laptop disk allows.
    conn.execute("SET synchronous_commit = off")
    started = time.monotonic()
    for i, txn in enumerate(txns):
        if i in hooks:
            hooks[i]()
        if rate:
            delay = started + i / rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        # Pipeline mode sends the whole transaction in one network round trip.
        with conn.pipeline(), conn.transaction():
            for statement in txn.statements:
                conn.execute(statement.sql, statement.params)
        counts[txn.kind] += 1
        if progress_every and (i + 1) % progress_every == 0:
            elapsed = time.monotonic() - started
            print(
                f"  {i + 1} transactions in {elapsed:.1f}s ({(i + 1) / elapsed:.0f}/s)", flush=True
            )
    return counts
