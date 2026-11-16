#!/usr/bin/env bash
# Chaos test. Requires `make up`. While the workload is running:
#
#   A. SIGKILL the streaming container, leave it down while a backlog builds
#   B. replay a Kafka offset range (duplicates, shuffled) into two topics
#   C. crash the job right after the bronze commit of a batch, before silver
#   D. crash the job in the middle of the silver merges of a batch
#
# Then drain and reconcile. The test passes only if every fault actually fired
# and the reconciliation is clean.
. "$(dirname "$0")/lib.sh"
trap 'stop_sampler; kill "${GEN:-}" 2>/dev/null || true' EXIT

CONTAINER="${COMPOSE_PROJECT_NAME}-streaming-1"
STARTED=$(date -u +%Y-%m-%dT%H:%M:%SZ)

arm_failpoint() {
  docker compose exec -T streaming sh -c "echo $1 > /checkpoints/_failpoint"
}

# wait_for_exit <expected code> <timeout seconds>
wait_for_exit() {
  local deadline=$((SECONDS + $2)) code
  while streaming_running; do
    if [ $SECONDS -ge $deadline ]; then
      echo "ERROR: streaming job did not exit within $2s" >&2
      return 1
    fi
    sleep 1
  done
  code=$(docker inspect -f '{{.State.ExitCode}}' "$CONTAINER")
  echo "streaming job exited with code $code"
  [ "$code" = "$1" ] || { echo "ERROR: expected exit code $1" >&2; return 1; }
}

if ! streaming_running; then start_streaming; fi

step "workload: ${CHAOS_OPS} transactions, seed ${CHAOS_SEED}, up to ${CHAOS_RATE}/s (background)"
uv run cdc sample-lag --out artifacts/lag-chaos.csv >/dev/null &
SAMPLER=$!
uv run cdc generate --seed "$CHAOS_SEED" --ops "$CHAOS_OPS" --rate "$CHAOS_RATE" --label chaos \
  > artifacts/chaos-generate.log 2>&1 &
GEN=$!
wait_for_new_batch 120

step "A. SIGKILL the streaming container, keep it down for 10s"
docker compose kill -s SIGKILL streaming >/dev/null 2>&1
wait_for_exit 137 30
sleep 10
start_streaming
wait_for_new_batch 180
echo "restarted from the checkpoint; next batch committed"

step "B. replay Kafka offsets 0..1500 of every partition of the loans and payments topics"
uv run cdc replay cdc.public.loans --from-offset 0 --to-offset 1500 --seed "$CHAOS_SEED"
uv run cdc replay cdc.public.payments --from-offset 0 --to-offset 1500 --seed "$CHAOS_SEED"

step "C. failpoint: exit after the bronze commit, before any silver merge"
arm_failpoint after_bronze
wait_for_exit 86 120
echo "on disk now:   $(uv run cdc last-batch)"
start_streaming
wait_for_new_batch 180
echo "after restart: $(uv run cdc last-batch)"

step "D. failpoint: exit after the loans merge, while the other silver merges are in flight"
arm_failpoint after_silver_loans
wait_for_exit 86 120
echo "on disk now:   $(uv run cdc last-batch)"
start_streaming
wait_for_new_batch 180
echo "after restart: $(uv run cdc last-batch)"

step "wait for the workload to finish"
wait "$GEN"
GEN=""
grep -E '"(transactions|transactions_per_second|replayed_records|poison_records)"' artifacts/chaos-generate.log

step "drain and reconcile"
uv run cdc drain
stop_sampler
uv run cdc verify

step "lag and freshness during the chaos run"
uv run cdc report --lag-csv artifacts/lag-chaos.csv --since "$STARTED"

echo
echo "CHAOS TEST PASSED"
