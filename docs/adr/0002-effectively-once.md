# ADR 0002: Effectively-once from a checkpoint plus idempotent sinks

Status: accepted

## Context

One micro-batch writes to up to eight Iceberg tables. Iceberg commits are
atomic per table; there is no transaction across tables. The job can die
between any two commits. Kafka Connect can redeliver records (new offsets,
same content) and an operator can rewind a connector.

## Decision

Do not try to make the batch atomic. Make every write safe to repeat.

- The Structured Streaming checkpoint records the Kafka offset range of each
  batch before the batch runs. After a crash the same batch id is re-run with
  exactly the same offsets.
- Bronze and the dead-letter table use an insert-only `MERGE` keyed on
  `(topic, partition, offset)`. A re-run inserts nothing. The `ON` clause
  carries constant lower bounds on `kafka_ts` and `kafka_offset` so Iceberg
  can prune the target scan; in the normal case no existing file overlaps.
- Silver uses a `MERGE` keyed on the primary key that only updates when
  `source._lsn > target._lsn`. A re-run, a redelivered record and a stale
  record all fail that condition and change nothing.
- The SCD2 table is recomputed for the affected loans from stored versions
  plus the new ones, keyed on `(loan_id, valid_from_lsn)`. Recomputing with
  the same inputs gives the same rows.
- `ops.stream_batches` is a plain append written last. A crash after it but
  before Spark marks the batch done produces a second row for the same batch;
  readers take the earliest per `(query_id, batch_id)`.

## How it is tested

- `tests/spark/test_silver_merge.py` re-executes a batch and asserts nothing
  changes.
- `make chaos` kills the job with SIGKILL mid-stream, then uses failpoints to
  die exactly after the bronze commit and in the middle of the silver merges,
  restarts each time, and runs the reconciliation.
- The reconciliation checks that bronze has exactly one row per Kafka record.

## Consequences

- Between a crash and the restart, bronze can be ahead of silver for one
  batch. Readers that need a consistent cut across tables should read at the
  offsets recorded in `ops.stream_batches`, or wait for the batch to finish.
- The bronze insert is a join against the target instead of a blind append.
  That is the price of not trusting "this batch has not been written before".
  The alternative, storing the batch id in the snapshot summary and skipping
  on match, is cheaper but breaks when snapshots are expired or a compaction
  commit lands in between.
- There is no graceful shutdown path. Stopping the container is the same code
  path as a crash, which keeps the recovery path exercised.
