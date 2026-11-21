#!/usr/bin/env bash
# Development/test-only, event-scoped baseline initialization.
# Usage: seed-baseline.sh <event-uuid>[,<event-uuid>...]
set -euo pipefail

ids="${1:-${EVENT_IDS:-}}"
if [[ -z "$ids" ]]; then
  echo "usage: $0 <event-uuid>[,<event-uuid>...]" >&2
  exit 2
fi

if [[ "$ids" == *$'\n'* || "$ids" == *$'\r'* ]]; then
  echo "EVENT_IDS must be a single comma-separated line" >&2
  exit 2
fi
uuid_re='[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
if [[ ! "$ids" =~ ^${uuid_re}(,${uuid_re})*$ ]]; then
  echo "invalid event id in EVENT_IDS" >&2
  exit 2
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runtime="${CONTAINER_RUNTIME:-}"
if [[ -z "$runtime" ]]; then
  if command -v podman >/dev/null 2>&1 && podman ps -a --format '{{.Names}}' 2>/dev/null | grep -Eq '^backend[_-]postgres-event'; then
    runtime=podman
  else
    runtime=docker
  fi
fi

pick_event_container() {
  if [[ -n "${EVENT_DB_CONTAINER:-}" ]]; then
    [[ "$("$runtime" inspect -f '{{.State.Running}}' "$EVENT_DB_CONTAINER" 2>/dev/null)" == "true" ]] || {
      echo "container $EVENT_DB_CONTAINER not found or not running" >&2
      return 1
    }
    echo "$EVENT_DB_CONTAINER"
    return 0
  fi
  local candidate
  for candidate in backend-postgres-event-1 backend_postgres-event-1 backend_postgres-event_1; do
    if [[ "$("$runtime" inspect -f '{{.State.Running}}' "$candidate" 2>/dev/null)" == "true" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

event_db="$(pick_event_container)" || {
  echo "postgres-event container not found; set EVENT_DB_CONTAINER" >&2
  exit 1
}

{
  printf '%s\n' "SET ticketbox.dev_baseline='on';" "SET ticketbox.event_ids = :'event_ids';"
  cat "$here/dev_baseline.sql"
} | "$runtime" exec -i "$event_db" psql -U ticketbox -d ticketbox_event \
  -v ON_ERROR_STOP=1 -v event_ids="$ids" -f -

echo "baseline invariant passed for $ids"
