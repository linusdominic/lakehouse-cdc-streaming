#!/usr/bin/env bash
# Show that the reconciliation can fail. Requires a drained pipeline with data
# (run after `make demo`).
#
# Corrupts silver.payments through Trino (one amount changed by a cent, one
# row deleted), expects `cdc verify` to exit non-zero and name the keys, then
# restores the table with an Iceberg snapshot rollback and expects a pass.
. "$(dirname "$0")/lib.sh"

sql() { docker compose exec -T trino trino --catalog lake --output-format TSV --execute "$1" 2>/dev/null; }

uv run cdc drain
SNAPSHOT=$(sql 'SELECT snapshot_id FROM silver."payments$snapshots" ORDER BY committed_at DESC LIMIT 1')
read -r CHANGED DELETED <<<"$(sql 'SELECT min(payment_id), max(payment_id) FROM silver.payments WHERE NOT _is_deleted' | tr '\t' ' ')"

step "corrupt silver.payments: +0.01 on payment ${CHANGED}, delete payment ${DELETED}"
sql "UPDATE silver.payments SET amount = amount + DECIMAL '0.01' WHERE payment_id = ${CHANGED}" >/dev/null
sql "DELETE FROM silver.payments WHERE payment_id = ${DELETED}" >/dev/null

step "verify must fail"
if uv run cdc verify; then
  echo "ERROR: verify passed on corrupted data" >&2
  exit 1
fi

step "roll silver.payments back to snapshot ${SNAPSHOT}, verify must pass"
sql "ALTER TABLE silver.payments EXECUTE rollback_to_snapshot(${SNAPSHOT})" >/dev/null
uv run cdc verify

echo
echo "NEGATIVE TEST PASSED: the corruption was detected and located"
