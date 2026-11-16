# ADR 0006: SCD2 status history derived from the change stream

Status: accepted

## Context

Delinquency reporting needs the status of a loan at any point in time, and
roll rates between statuses. The current-state table only has the latest
status.

## Decision

`silver.loan_status_scd2` is built from the CDC events of `loans`:

- A version starts at every event that changes the status: an insert or
  snapshot read, an update where `before.status` differs from `after.status`,
  and a delete, which starts a synthetic `DELETED` version.
- A version is identified by `(loan_id, valid_from_lsn)`.
- For each micro-batch, the loans that have new versions are recomputed: the
  stored versions and the new ones are unioned, deduplicated on the key, and
  `valid_to` is derived with `lead()` over the LSN order. A `MERGE` inserts
  the new versions and updates the `valid_to` of the ones they close.

## Reasons

- Recomputing per affected key makes the result independent of arrival order.
  The usual "close the current row, insert a new one" pattern is only correct
  when events arrive in order.
- Using the change stream rather than the application's own
  `loan_status_history` table means the history is correct even for writers
  that forget to insert the audit row. It also gives the verifier two
  independent sources to compare: the SCD2 table must show the same status
  sequence per loan as `loan_status_history`.

## Consequences

- Detecting "the status changed" on an update needs the before image, so
  `loans` has `REPLICA IDENTITY FULL`. That writes the old row to the WAL on
  every update of `loans`. It is set on that one table only. Without it every
  update of a loan would be treated as a status change and the table would
  have consecutive versions with the same status.
- `valid_from` is the commit time from Debezium, at millisecond resolution.
  Two status changes of one loan in the same millisecond produce a
  zero-length version. Ordering is by LSN, so it is still correct.
- A loan that was hard-deleted keeps its history and ends in `DELETED`.
