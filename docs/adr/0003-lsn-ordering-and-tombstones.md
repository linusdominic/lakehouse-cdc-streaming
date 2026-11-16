# ADR 0003: Order by LSN, keep deletes as tombstones

Status: accepted

## Context

Silver must converge to the source state when events arrive duplicated or out
of order. That needs a total order per key and a rule for deletes.

## Decision

- The order is the Postgres LSN of the change (`source.lsn` in the Debezium
  envelope), stored on every silver row as `_lsn`.
- Within a micro-batch, keep one event per key: the one with the highest LSN.
- Apply to the table only if the incoming LSN is strictly greater than the
  stored one.
- A delete does not remove the row. It sets `_is_deleted = true` and stores
  the delete's LSN. Readers filter on `_is_deleted`.

## Why LSN and not a timestamp

`source.ts_ms` is the commit time in milliseconds. Two changes to one row
inside the same transaction, or in two transactions within a millisecond,
share a timestamp, and the generator produces both on purpose
(`burst_update_borrower`). An application `updated_at` column is worse: it is
set by the client, deletes do not carry one, and clocks disagree across hosts.
The LSN is assigned by the database, unique per change and strictly
increasing along the WAL.

Is the event LSN a valid order per key when transactions interleave? Yes. A
row is locked by the transaction that modifies it until that transaction
commits, so a second transaction can only write the same row after the first
one has committed, at a higher LSN. Across different keys event LSNs are not
in commit order, but the merge never compares LSNs across keys.

## Why tombstones

With hard deletes the sequence `delete (LSN 70)`, then a late
`update (LSN 60)` re-inserts the row: after the delete there is no stored LSN
left to compare against. The tombstone keeps it. This exact case is a test
(`test_stale_update_after_delete_does_not_resurrect`).

## Consequences

- Silver tables carry dead rows. The maintenance job purges tombstones older
  than a retention (7 days by default). The retention has to be longer than
  the oldest replay the pipeline may ever see; a replay older than that can
  resurrect a row.
- The scheme assumes one Postgres timeline. After a failover to a replica
  that was promoted, or a restore from backup, LSNs can go backwards relative
  to what silver has stored and newer changes would be rejected. That needs a
  re-snapshot or an epoch column in front of the LSN. Not implemented; listed
  under known limitations.
- Snapshot rows (`op = 'r'`) carry the LSN at which the snapshot was taken,
  which is lower than any change streamed afterwards, so they order correctly.
