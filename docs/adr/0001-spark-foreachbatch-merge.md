# ADR 0001: Spark Structured Streaming with foreachBatch MERGE

Status: accepted

## Context

CDC events from Kafka have to become two things in Iceberg: an append-only raw
log (bronze) and current-state tables that match the source row for row
(silver). The silver write is an upsert with a per-row ordering condition, and
a single source topic set feeds several tables.

Options considered:

1. Spark Structured Streaming, `foreachBatch`, one `MERGE INTO` per table.
2. Flink with the Iceberg sink in upsert mode.
3. The Kafka Connect Iceberg sink.

## Decision

Option 1.

## Reasons

- The merge condition is the whole correctness story (apply only if the
  incoming LSN is greater than the stored one, keep tombstones). `MERGE INTO`
  expresses that directly and the same statement can be tested on a laptop
  against a local catalog. Flink's upsert mode writes an equality delete plus
  an insert for every event; it has no "only if newer" condition, so a
  duplicate or late event overwrites newer data unless ordering is solved
  upstream of the sink.
- The Kafka Connect Iceberg sink is the least code for bronze, and I would use
  it for an append-only landing zone. It does not do conditional upserts, so
  silver would still need a second engine. One engine for both layers is less
  to operate.
- `foreachBatch` gives a DataFrame per micro-batch, so one stream can fan out
  to bronze, a dead-letter table, four silver tables and the SCD2 table with
  ordinary batch code.
- The target environment is a Spark shop (Databricks or EMR). The job runs
  there with a catalog and checkpoint path change.

## Consequences

- Latency is bounded below by the micro-batch: several Iceberg commits per
  batch, a few seconds each on this stack. Sub-second freshness is not
  available from this design. If that became a requirement, Flink with an
  explicit dedupe-by-LSN operator in front of the sink is the alternative.
- Every sink inside `foreachBatch` must be idempotent, because Spark's
  guarantee for `foreachBatch` is at-least-once. See ADR 0002.
- One query for all tables means one slow table delays the others. Splitting
  into a query per table is a configuration change (one checkpoint each), not
  a redesign.
