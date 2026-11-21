#!/bin/bash
set -e

SERVICES=("user:5433:ticketbox_user" "booking:5435:ticketbox_booking" "event:5434:ticketbox_event" "notification:5436:ticketbox_notification" "payment:5437:ticketbox_payment" "saga:5438:ticketbox_saga" "scheduler:5439:ticketbox_scheduler")

for entry in "${SERVICES[@]}"; do
    IFS=: read -r svc port db <<< "$entry"
    echo "Running migrations for $svc service..."
    if [[ "$svc" == "user" ]]; then
        echo "User M6 requires paused auth/profile writes and a private AUTH_PREFLIGHT_DATABASE_URL."
        : "${AUTH_PREFLIGHT_DATABASE_URL:?Set private User preflight DSN; audit before migration}"
        # An empty database has no users to audit. Only an explicitly verified
        # empty schema may bootstrap; connection failures and dirty versions stop.
        version=$(go run ./services/user/cmd/auth_preflight --migration-state)
        if [[ "$version" == "FRESH" ]]; then
            migrate -path "services/user/migrations" -database "$AUTH_PREFLIGHT_DATABASE_URL" goto 1
            version=1
        fi
        go run ./services/user/cmd/auth_preflight
        if [[ "$version" == "1" ]]; then
            migrate -path "services/user/migrations" -database "$AUTH_PREFLIGHT_DATABASE_URL" goto 2
        fi
        go run ./services/user/cmd/auth_preflight --apply-canonical
        migrate -path "services/user/migrations" -database "$AUTH_PREFLIGHT_DATABASE_URL" up
        echo "User migrations complete."
        continue
    fi
    migrate -path "services/$svc/migrations" \
        -database "postgres://ticketbox:${POSTGRES_PASSWORD:-ticketbox_secret}@localhost:$port/$db?sslmode=disable" \
        up
    echo "$svc migrations complete."
done
