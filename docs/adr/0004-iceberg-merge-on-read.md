# ADR 0004: Iceberg tables, merge-on-read for silver

Status: accepted

## Context

The lake needs row-level upserts from Spark, reads from Trino, schema
evolution without rewriting data, and a catalog that maps to AWS Glue.

## Decision

Apache Iceberg (format v2) through a REST catalog locally. Silver tables use
merge-on-read for `MERGE`, `UPDATE` and `DELETE`. Bronze is append-only.

## Why Iceberg

- Spark and Trino both read and write it natively, and neither needs the
  other to be running. Delta would be the default on Databricks; outside it,
  Iceberg has the broader engine support, and the REST catalog protocol means
  the local setup and Glue or Unity look the same to the jobs.
- Schema evolution is by column id, so `ADD COLUMN` is a metadata change. The
  mid-run schema change in the demo relies on that.
- Snapshots give time travel and rollback. `make verify-negative` restores a
  corrupted table with `rollback_to_snapshot`.

## Why merge-on-read for silver

A micro-batch touches a small fraction of the rows in a table, spread over
many files. Copy-on-write would rewrite every touched data file on every
batch, so write cost grows with table size instead of batch size.
Merge-on-read writes the new row versions plus small position-delete files.

The cost moves to readers, who apply the delete files, and to the file count:
every batch adds a data file and a delete file per table. That is why the
maintenance job is part of the design and not an afterthought. In the demo
run the silver tables went from 14-19 data files and as many delete files to
one data file and no delete files each (numbers in the README).

Copy-on-write is the right choice for tables that are read far more than they
are written and where batches are large relative to the table. Gold is built
that way (dbt `table` materialisation, full rebuild).

## Partitioning

- Bronze: `(source_table, days(kafka_ts))`. Arrival time, not event time, so
  appends only touch the current partition and late events do not scatter
  small files across old days.
- Silver: unpartitioned at this size. A `MERGE` by primary key cannot prune on
  a date partition anyway. At scale the tables would be bucketed on the
  primary key so each batch only touches the buckets it has keys for.

## Consequences

- Maintenance is mandatory: compaction, snapshot expiry, orphan removal.
- Compaction and the streaming `MERGE` conflict when they touch the same
  files. The demo pauses the stream for maintenance. See the runbook for the
  options when pausing is not acceptable.
