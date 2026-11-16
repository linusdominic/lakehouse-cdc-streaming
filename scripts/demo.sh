#!/usr/bin/env bash
# End-to-end demo. Requires `make up`.
#
#   1. run the OLTP workload against Postgres, with duplicates, out-of-order
#      replays, poison records, hard deletes and a mid-run ADD COLUMN
#   2. wait for the pipeline to drain, reconcile Postgres against silver
#   3. report consumer lag and freshness
#   4. compact / expire / remove orphans, then reconcile again
#   5. build and test the dbt gold layer
. "$(dirname "$0")/lib.sh"
trap stop_sampler EXIT

if ! streaming_running; then start_streaming; fi

step "1/5 workload: ${DEMO_OPS} transactions, seed ${DEMO_SEED}, up to ${DEMO_RATE}/s"
uv run cdc sample-lag --out artifacts/lag-demo.csv >/dev/null &
SAMPLER=$!
uv run cdc generate --seed "$DEMO_SEED" --ops "$DEMO_OPS" --rate "$DEMO_RATE" --label demo

step "2/5 drain and reconcile"
uv run cdc drain
stop_sampler
uv run cdc verify

step "3/5 lag and freshness"
uv run cdc report --lag-csv artifacts/lag-demo.csv

step "4/5 table maintenance, then reconcile again"
./scripts/maintenance.sh
uv run cdc drain
uv run cdc verify

step "5/5 dbt build (gold layer)"
DBT_PROFILES_DIR="$PWD/dbt" uv run dbt build --project-dir dbt | grep -E 'Found|Finished|Completed|Done\.|ERROR|FAIL|WARN ' || true
[ "${PIPESTATUS[0]}" = "0" ]

echo
echo "DEMO PASSED"
