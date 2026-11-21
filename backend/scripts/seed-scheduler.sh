#!/usr/bin/env bash
# Local development bootstrap. Idempotently creates/repairs the reservation
# cleaner config without overwriting a valid administrator-set timeout.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runtime="${CONTAINER_RUNTIME:-}"

if [[ -z "$runtime" ]]; then
  if command -v podman >/dev/null 2>&1 && podman ps -a --format '{{.Names}}' 2>/dev/null | grep -Eq '^backend[_-]postgres-scheduler'; then
    runtime=podman
  else
    runtime=docker
  fi
fi

pick_container() {
  local override="$1"; shift
  if [[ -n "$override" ]]; then
    [[ "$("$runtime" inspect -f '{{.State.Running}}' "$override" 2>/dev/null)" == "true" ]] || {
      echo "container $override not found or not running" >&2
      return 1
    }
    echo "$override"
    return 0
  fi
  local candidate
  for candidate in "$@"; do
    if [[ "$("$runtime" inspect -f '{{.State.Running}}' "$candidate" 2>/dev/null)" == "true" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

scheduler_db="$(pick_container "${SCHEDULER_DB_CONTAINER:-}" \
  backend-postgres-scheduler-1 backend_postgres-scheduler-1 backend_postgres-scheduler_1)" || {
  echo "postgres-scheduler container not found; set SCHEDULER_DB_CONTAINER" >&2
  exit 1
}

"$runtime" exec -i "$scheduler_db" psql -U ticketbox -d ticketbox_scheduler \
  -v ON_ERROR_STOP=1 < "$here/seed-scheduler.sql"

scheduler="$(pick_container "${SCHEDULER_CONTAINER:-}" \
  backend-scheduler-service-1 backend_scheduler-service-1 backend_scheduler-service_1)" || true
if [[ -n "$scheduler" ]]; then
  "$runtime" restart "$scheduler" >/dev/null
  echo "scheduler config written and scheduler-service restarted"
else
  echo "scheduler config written; scheduler-service not found, start/restart it before relying on cleanup" >&2
fi
