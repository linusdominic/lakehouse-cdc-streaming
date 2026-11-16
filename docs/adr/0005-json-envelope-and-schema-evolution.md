# ADR 0005: JSON envelope with embedded schema, additive schema evolution

Status: accepted for the local stack; Avro with a schema registry in production

## Context

The streaming job has to know when a source table gains a column, and what
type it is, without a deploy.

## Decision

- Debezium writes JSON with `schemas.enable=true`, so every message carries
  its Kafka Connect schema.
- Bronze stores `before` and `after` as JSON strings. It never needs a schema
  change, and nothing is lost if silver gets a type wrong.
- Per micro-batch, the job collects the distinct schemas per table. A field
  that the silver table does not have is added with `ALTER TABLE ADD COLUMN`
  before the merge. The type comes from the Connect schema
  (`schema_evolution.py`); anything without a safe mapping lands as `string`.
- Dropped source columns stay in silver and go null. Type changes and renames
  are not handled automatically.

## Reasons

- Embedded schemas need no extra service, which keeps the local stack inside
  its memory budget.
- Additive changes are the common case and safe to automate. A rename is
  indistinguishable from a drop plus an add, and a type change can lose data;
  both should stop a human, not be guessed at.

## Consequences

- Messages are several times larger than they need to be: the schema is
  repeated in every record. With Avro and a registry the record carries a
  4-byte schema id instead. That is the production choice; the job would read
  the writer schema from the registry instead of from the message, and
  `schema_evolution.py` stays the same.
- `decimal.handling.mode=string` keeps decimals exact and readable, but a new
  `NUMERIC` source column arrives typed as `string`. Promoting it to
  `decimal(p,s)` is a contract change in `contracts.py`.
- `ADD COLUMN ... DEFAULT x` at the source back-fills existing rows without
  writing them to the WAL. Silver would show NULL where Postgres shows the
  default, and the reconciliation would fail. The fix is a Debezium
  incremental snapshot of the table (runbook). The demo adds the column
  without a default.
