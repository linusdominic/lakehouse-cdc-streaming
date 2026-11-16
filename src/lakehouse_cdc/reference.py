"""Reference model of the silver semantics (pure Python).

The rules, stated once:

Current state (one row per primary key):
  * An incoming change is applied only if its LSN is strictly greater than the
    LSN stored for that key. Equal LSN means a redelivery of the same change.
  * Deletes are applied as tombstones (_is_deleted = true) and keep their LSN.
    A hard delete would forget the LSN, and a stale update delivered after the
    delete would then re-insert the row.

Because "apply" is max-by-LSN per key, the result is independent of delivery
order and of duplicates. tests/unit asserts that property directly on this
model; tests/spark asserts that the Spark MERGE produces the same state as
this model for the same events.

Status history (SCD2 over loans.status):
  * A version starts at every event that changes the status: inserts and
    snapshot reads, updates where before.status != after.status, and deletes
    (which start a synthetic DELETED version).
  * Versions are keyed by (loan_id, valid_from_lsn) and ordered by LSN;
    valid_to of a version is valid_from of the next one. The newest is current.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from lakehouse_cdc.contracts import DELETED_STATUS
from lakehouse_cdc.envelope import ChangeEvent


@dataclass(frozen=True)
class RowState:
    lsn: int
    op: str
    is_deleted: bool
    data: dict[str, Any]


def apply_event(state: dict[Any, RowState], event: ChangeEvent, primary_key: str) -> None:
    """Apply one change to the current-state map, in place."""
    image = event.row_image
    key = image[primary_key]
    stored = state.get(key)
    if stored is not None and event.lsn <= stored.lsn:
        return
    state[key] = RowState(lsn=event.lsn, op=event.op, is_deleted=event.op == "d", data=dict(image))


def reduce_current_state(events: Iterable[ChangeEvent], primary_key: str) -> dict[Any, RowState]:
    state: dict[Any, RowState] = {}
    for event in events:
        apply_event(state, event, primary_key)
    return state


def live_rows(state: dict[Any, RowState]) -> dict[Any, dict[str, Any]]:
    return {k: v.data for k, v in state.items() if not v.is_deleted}


@dataclass(frozen=True)
class StatusVersion:
    loan_id: int
    status: str
    valid_from_lsn: int
    valid_from_ms: int | None
    valid_to_lsn: int | None
    valid_to_ms: int | None

    @property
    def is_current(self) -> bool:
        return self.valid_to_lsn is None


def is_status_change(event: ChangeEvent) -> bool:
    if event.op in ("c", "r", "d"):
        return True
    # Updates need the before image, which requires REPLICA IDENTITY FULL on loans.
    before_status = event.before.get("status") if event.before else None
    after_status = event.after.get("status") if event.after else None
    return before_status != after_status


def build_status_history(loan_events: Iterable[ChangeEvent]) -> list[StatusVersion]:
    """SCD2 versions for all loans, from loan change events in any order."""
    points: dict[tuple[int, int], tuple[str, int | None]] = {}
    for event in loan_events:
        if not is_status_change(event):
            continue
        loan_id = event.row_image["loan_id"]
        status = DELETED_STATUS if event.op == "d" else event.after["status"]  # type: ignore[index]
        points[(loan_id, event.lsn)] = (status, event.source_ts_ms)

    versions: list[StatusVersion] = []
    ordered = sorted(points.items())
    for i, ((loan_id, lsn), (status, ts_ms)) in enumerate(ordered):
        nxt = ordered[i + 1] if i + 1 < len(ordered) else None
        same_loan = nxt is not None and nxt[0][0] == loan_id
        versions.append(
            StatusVersion(
                loan_id=loan_id,
                status=status,
                valid_from_lsn=lsn,
                valid_from_ms=ts_ms,
                valid_to_lsn=nxt[0][1] if same_loan else None,  # type: ignore[index]
                valid_to_ms=nxt[1][1] if same_loan else None,  # type: ignore[index]
            )
        )
    return versions
