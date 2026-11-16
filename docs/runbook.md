# Runbook

For whoever is on call for this pipeline. Commands assume the local stack;
the AWS equivalents are noted where they differ.

Quick orientation:

```
make ps                      # containers and memory
make logs                    # streaming job, one line per micro-batch
make report                  # lag, freshness, batch durations
make verify                  # drain + reconcile against Postgres
uv run cdc last-batch        # did the newest batch complete
make trino                   # SQL shell on the lake
```

Useful queries:

```sql
-- what the stream has committed, newest first
SELECT batch_id, committed_at, num_records, num_dead_letter, table_counts
FROM lake.ops.stream_batches ORDER BY committed_at DESC LIMIT 20;

-- dead letters by reason
SELECT reason, count(*), max(kafka_ts) FROM lake.bronze.dead_letter GROUP BY 1;

-- file counts per table
SELECT content, count(*), sum(file_size_in_bytes) FROM lake.silver."loans$files" GROUP BY 1;
```

## 1. Consumer lag is growing

Symptom: `make report` shows lag rising, freshness p95 climbing.

1. Is the job running? `make ps`. If the `streaming` container has exited,
   read the tail of `docker compose logs streaming` and go to the matching
   section below. To restart: `docker compose up -d streaming`. It resumes
   from the checkpoint; the interrupted batch is re-run and that is safe.
2. Running but slow: look at the `stages=` field in the batch log lines.
   - `parse` high: Kafka fetch or JSON parsing. Check Redpanda health
     (`docker compose exec redpanda rpk cluster health`).
   - one table high and rising batch over batch: small files. Go to section 4.
   - everything high: the container is out of memory headroom and swapping.
     `docker stats`; if the streaming container sits at its limit, raise
     `mem_limit` and `spark.driver.memory` together.
3. Running, healthy, and still behind: the input rate exceeds what one
   micro-batch loop can commit. The batch size adapts on its own (a batch
   takes everything available up to `CDC_MAX_OFFSETS_PER_TRIGGER`), and larger
   batches are more efficient per record, so a temporary burst clears by
   itself. A sustained overload needs more cores, or the split described in
   the README under "What changes at 100x".

Do not delete the checkpoint to "get unstuck". A new checkpoint starts from
the earliest offset and reprocesses the whole topic. The result is still
correct, because the sinks are idempotent, but it takes as long as a full
backfill.

## 2. Replication slot bloat (Postgres disk filling up)

Symptom: Postgres disk usage grows; `pg_wal` is large.

```sql
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))        AS retained,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconfirmed
FROM pg_replication_slots;
```

Cause: the slot `lakehouse_cdc` keeps WAL until Debezium confirms it. If the
connector is down, paused, or stuck, WAL accumulates.

1. Connector state:
   `curl -s localhost:18083/connectors/loans-cdc/status`. If a task is
   `FAILED`, the trace is in the response. Restart the task:
   `curl -X POST localhost:18083/connectors/loans-cdc/tasks/0/restart`.
2. `active = true` and `retained` still growing while the tables are quiet:
   the slot only advances when the connector sees changes for its tables.
   Heartbeats are enabled (`heartbeat.interval.ms=10000`) to cover this. On a
   database where the captured tables are quiet and other tables are busy,
   also set `heartbeat.action.query` so the heartbeat itself writes WAL the
   connector can confirm.
3. The backstop is `max_slot_wal_keep_size` (1GB locally). When the limit is
   hit Postgres invalidates the slot (`wal_status = 'lost'`) instead of
   filling the disk. That protects the OLTP database at the cost of the
   pipeline: the connector cannot resume and needs a new snapshot.
4. Recovering from a lost slot: drop the slot, delete and re-create the
   connector with a new `slot.name` so it takes a fresh snapshot
   (`snapshot.mode=initial`). The snapshot rows are merged by key; rows
   deleted at the source while the slot was lost are not seen as deletes, so
   run `make verify` afterwards. It reports them as "in silver, not in
   Postgres". Tombstone those keys by hand or rebuild the silver table from
   the snapshot.

Never drop the slot of a healthy connector to free disk. It guarantees data
loss in the lake.

## 3. A schema change broke the pipeline

What is handled without intervention: a new nullable column at the source.
The log shows `[schema] lake.silver.<table>: ADD COLUMN <name> <type>`.

What is not:

| Change at the source | What happens | What to do |
| --- | --- | --- |
| `ADD COLUMN ... DEFAULT x` (or a later `UPDATE` that bypasses nothing but touches every row) | Existing rows get the default without WAL events. `make verify` fails with "content differs". | Trigger a Debezium incremental snapshot of the table (signal table or Kafka signal). The re-read rows carry a current LSN and merge normally. |
| Column type change | The cast in silver yields NULL or the batch fails with a cast error. Bronze is unaffected. | Stop the stream. `ALTER TABLE ... ALTER COLUMN` in Iceberg if the promotion is legal (int to bigint, decimal precision widening), otherwise add a new column and backfill from bronze. Update `contracts.py`. Restart. |
| Column rename | Seen as one column going NULL and a new one appearing. | Decide whether to rename the Iceberg column (`ALTER TABLE ... RENAME COLUMN`, metadata only) before the first event with the new name arrives, or to backfill the new column from bronze. |
| Column drop | The silver column stays and goes NULL for new versions. | Nothing urgent. Drop it from silver and the contract when consumers are ready. |
| New table | Lands in bronze only if it is in the publication and the connector's include list. | Add a contract, add the table to the publication and include list. |
| `TRUNCATE` | Debezium emits an event with `op = 't'`; it goes to the dead-letter table as `unknown_op`. Silver keeps the rows. | Decide deliberately. Tombstone the table's rows in silver if the truncate was intended. |

A failing batch does not lose data: the offsets are not committed, and the
same batch is retried on restart. If a single bad record keeps failing the
batch and it is not caught by the envelope checks, the options are to fix the
code and restart, or, as a last resort, to move the record aside by extending
the dead-letter rules in `envelope.py` and `envelope_sql.py` (the parity test
keeps them in step).

## 4. Small-file explosion

Symptom: batch durations creep up; Trino queries on silver slow down;
`"<table>$files"` shows hundreds of files of a few KB.

Cause: every micro-batch adds one data file and one position-delete file per
table it touches (merge-on-read), plus one data file per touched partition in
bronze.

1. `make maintenance`. It pauses the stream, compacts every table, rewrites
   position deletes and manifests, expires snapshots (keeps the last 3),
   removes orphan files, and resumes the stream. It prints file counts before
   and after and fails if any table's row count changed.
2. Schedule it. Hourly for silver is a reasonable start; the right interval
   is the one that keeps the per-table file count in the low hundreds.
3. If the stream cannot be paused: run compaction concurrently with
   `partial-progress.enabled=true` and accept that either side may hit a
   commit conflict and retry. The streaming `MERGE` retries are bounded by
   `commit.retry.num-retries`; a batch that exhausts them fails and is re-run,
   which is safe. Keep the orphan-file age at the 24-hour default in that
   mode. The 1-minute age used by `make maintenance` is only safe because
   nothing else is writing.
4. Fewer, larger batches also help: raise `CDC_TRIGGER_INTERVAL`. That trades
   freshness for file count.

## 5. Dead-letter records are appearing

`SELECT reason, count(*) FROM lake.bronze.dead_letter GROUP BY 1`.

- `unparseable_envelope`, `null_value`: something other than Debezium is
  writing to a `cdc.public.*` topic, or the converter configuration changed
  (for example `schemas.enable=false`). Check who has write access.
- `unknown_op`: a truncate (`t`) or a logical decoding message (`m`). See the
  table in section 3.
- `missing_primary_key`: a table without a primary key or with a changed key
  was added to the publication.

Records in the dead-letter table keep the raw bytes and the Kafka
coordinates. After fixing the cause they can be re-published to the topic
with `uv run cdc replay <topic> --partition P --from-offset A --to-offset B`.

## 6. Reconciliation fails

`make verify` prints which keys differ, per table.

1. Make sure it was run against a drained pipeline (`make verify` drains
   first; `uv run cdc verify` alone does not).
2. "in Postgres, missing in silver" or "content differs": look the key up in
   bronze to see what was delivered.
   ```sql
   SELECT kafka_ts, op, lsn, kafka_partition, kafka_offset, after
   FROM lake.bronze.cdc_events
   WHERE source_table = 'payments' AND key LIKE '%"payment_id":12345%'
   ORDER BY lsn;
   ```
   If the newest event in bronze matches Postgres but silver does not, the
   merge is wrong: that is a bug, keep the evidence. If bronze does not have
   the change at all, the loss is upstream (connector, slot, section 2).
3. If someone modified silver by hand, roll the table back:
   `ALTER TABLE lake.silver.<t> EXECUTE rollback_to_snapshot(<id>)`, with the
   id from `"<t>$snapshots"`.
4. Silver can always be rebuilt from bronze: bronze holds every delivered
   event with its LSN, and the merge is order-independent.
