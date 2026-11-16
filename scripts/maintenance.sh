#!/usr/bin/env bash
# Pause the streaming job, run Iceberg maintenance, resume.
#
# The stream is paused for two reasons: the one-shot job needs the memory, and
# with no writer running it is safe to remove orphan files younger than the
# 24 hours Iceberg normally insists on (see sparkjobs/maintenance.py).
. "$(dirname "$0")/lib.sh"

docker compose stop streaming >/dev/null 2>&1
trap start_streaming EXIT
docker compose --profile jobs run --rm maintenance --orphan-min-age-minutes 1 "$@" 2>&1 \
  | grep -vE ' (INFO|WARN) |^Container | Container ' || true
exit "${PIPESTATUS[0]}"
