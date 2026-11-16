# Shared helpers for the demo and chaos scripts. Source, do not execute.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
[ -f .env ] || cp .env.example .env
set -a; . ./.env; set +a
mkdir -p artifacts

step() { printf '\n== %s\n' "$*"; }

streaming_running() {
  [ "$(docker inspect -f '{{.State.Running}}' "${COMPOSE_PROJECT_NAME}-streaming-1" 2>/dev/null)" = "true" ]
}

# Number of micro-batches the streaming job has logged since the container was created.
batches_logged() {
  docker compose logs --no-log-prefix streaming 2>/dev/null | grep -c '^[0-9T:.+-]* \[batch ' || true
}

# wait_for_new_batch <timeout seconds>: block until one more batch completes.
wait_for_new_batch() {
  local before deadline
  before=$(batches_logged)
  deadline=$((SECONDS + $1))
  while [ "$(batches_logged)" -le "$before" ]; do
    if [ $SECONDS -ge $deadline ]; then
      echo "ERROR: no micro-batch completed within $1s" >&2
      return 1
    fi
    sleep 1
  done
}

start_streaming() {
  docker compose up -d streaming >/dev/null 2>&1
}

# Stop the background lag sampler started with: uv run cdc sample-lag ... & SAMPLER=$!
stop_sampler() {
  if [ -n "${SAMPLER:-}" ] && kill -0 "$SAMPLER" 2>/dev/null; then
    kill -TERM "$SAMPLER" 2>/dev/null || true
    wait "$SAMPLER" 2>/dev/null || true
  fi
  SAMPLER=""
}
