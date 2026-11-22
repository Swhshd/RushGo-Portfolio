# RushGo Backend Development

[简体中文](README.md) | [English](README.en.md)

The backend workspace contains eight Go services: Gateway, User, Event, Booking, Payment, Saga, Notification, and Scheduler. It also contains shared gRPC contracts and infrastructure components. This guide describes the local development stack, initialization order, and machine-specific prerequisites. [Project overview](../README.en.md) · [Frontend development](../frontend/README.en.md) · [Read-only demo](../DEMO.en.md) · [Project status](../PROJECT_STATUS.en.md)

## Prerequisites and configuration

| Purpose | Repository configuration |
| --- | --- |
| Go build and User database preflight | Go 1.25.5, declared in [go.work](go.work) |
| Services and infrastructure | Docker Engine and Docker Compose v2, or the Podman/WSL workflow used by the project Makefile |
| Databases | PostgreSQL 16, with a separate database for each service that persists business state |
| Cache and messaging | Redis 7; Kafka and ZooKeeper use the Confluent 7.6.0 image tag |
| Database migrations | The migrate CLI, Bash, and Make. User migrations also run a Go preflight program. |
| Browser frontend | Node.js is not needed for the backend. For Next.js, use the versions listed in [frontend guide](../frontend/README.en.md). |

Compose publishes the Gateway HTTP port on 8000. The Payment webhook binds only to 127.0.0.1:8081. Default database host ports are User 5433, Event 5434, Booking 5435, Notification 5436, Payment 5437, Saga 5438, and Scheduler 5439. Redis uses 6379, Kafka 9092, and Kafka UI 8080. Kafka and Postgres are intended for the local development network.

The local Compose stack creates seven separate databases, but they use the same PostgreSQL owner account for initialization, service connections, and migrations. The repository does not provide an initialization script for separate SQL login roles or GRANTs per local service, so this container configuration does not provide least-privilege deployment roles. Compose `POSTGRES_USER` and `POSTGRES_DB` values are the source of configuration. The migration script makes fixed assumptions about connection users and database names for services other than User. Keep these in sync if changing the Compose database identities.

From the repository root, open PowerShell and enter the backend directory. On first setup, copy the template. Keep an existing `.env` file if you already have one.

~~~powershell
Set-Location backend
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
~~~

Edit `backend/.env`. Replace the example values for POSTGRES_PASSWORD and JWT_SECRET with random values for local use only. The database password and JWT text in this template are placeholders. `.env` is ignored by Git. Do not commit local credentials.

Compose also requires RUSHGO_GRPC_CERT_ROOT to point to a private certificate root outside the repository. The container runtime must be able to mount that directory. The base Compose file reads the `user`, `event`, `booking`, `payment`, `saga`, `scheduler`, and `gateway` subdirectories. Notification currently has no certificate mount and needs the private override described below.

Use Stripe TEST account configuration only. When Payment starts, it reads the current TEST account through the Stripe SDK to bind the local credentials to that account. This startup request contacts Stripe but does not create a payment. No usable payment credentials are included, and Payment was not started for this documentation work. Authentication email and Notification delivery require a compatible SMTP service. For mail delivery, use a local mail catcher or a provider sandbox. The repository has no SMTP dry-run mode and includes no usable mail credentials. The root read-only discovery demo does not connect to backend, payment, or mail services.

## Starting the complete local stack

Start the infrastructure and apply database migrations before starting the application services. Do not run the User M6 migration while account profile writes are active.

Set the certificate root, provide the local-only Compose override described below, and start the databases, Redis, and Kafka. Keep the same `-f` file arguments for every Compose operation in this setup. The commands use default database host ports because the current migration script hard-codes them.

~~~powershell
$env:RUSHGO_GRPC_CERT_ROOT = Read-Host 'Private gRPC certificate root visible to the container runtime'
$env:RUSHGO_COMPOSE_OVERRIDE = Read-Host 'Path to the local private Compose override'
if (-not (Test-Path -LiteralPath $env:RUSHGO_COMPOSE_OVERRIDE)) { throw 'Private Compose override not found' }
$composeFiles = @('-f', 'docker-compose.yml', '-f', $env:RUSHGO_COMPOSE_OVERRIDE)
docker compose @composeFiles config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Compose configuration failed validation' }
docker compose @composeFiles up -d postgres-user postgres-event postgres-booking postgres-notification postgres-payment postgres-saga postgres-scheduler redis kafka
docker compose @composeFiles ps
~~~

Compose starts ZooKeeper alongside Kafka. Wait until the databases and Kafka report `healthy` before migrating. User migration requires AUTH_PREFLIGHT_DATABASE_URL to point to the User database. Other migrations read POSTGRES_PASSWORD from the Bash environment. Compose started from PowerShell reads interpolation values from `backend/.env`; Make and Bash do not load that file automatically. Obtain both values from a private local configuration source before running `make migrate` in Bash. Take the User database username and name from Compose `postgres-user.environment.POSTGRES_USER` and `POSTGRES_DB`, and the password from local configuration. For other services, keep database identities aligned with the fixed connection strings in `scripts/migrate.sh`. The default User database maps to localhost:5433. Avoid placing secret values in shell command history.

~~~bash
read -rsp 'POSTGRES_PASSWORD: ' POSTGRES_PASSWORD
printf '\n'
export POSTGRES_PASSWORD
read -rsp 'AUTH_PREFLIGHT_DATABASE_URL: ' AUTH_PREFLIGHT_DATABASE_URL
printf '\n'
export AUTH_PREFLIGHT_DATABASE_URL
make migrate
unset POSTGRES_PASSWORD AUTH_PREFLIGHT_DATABASE_URL
~~~

Install the migrate CLI before running the command, and run it from the `backend` directory. The script applies migrations for User, Booking, Event, Notification, Payment, Saga, and Scheduler in that order. User preflight checks migration versions and normalized email conflicts. A conflict, unversioned existing table, or unexpected migration state stops the procedure for manual review. Do not bypass the preflight or use local migration scripts on production data.

An empty database has no accounts, admin, or event seed data. `make seed-scheduler` only adds Scheduler booking-cleanup job configuration. It does not create events or users. Event `make seed-baseline` accepts only existing event UUIDs and marks baseline data for ticket tiers that are new and fully available. Create initial events and accounts through the actual management API and account flow.

After the infrastructure and migrations are ready, application services can be started. Payment startup contacts the Stripe TEST account. Run the next step only if that external account read is approved and private local TEST configuration is available. Payment also requires the Stripe TEST endpoint secret and the expected TEST account ID. The Stripe endpoint is the local `127.0.0.1:8081/webhooks/stripe`. Accessing that local webhook from a remote test service requires a separate test forwarding setup.

~~~powershell
docker compose @composeFiles up --build -d
docker compose @composeFiles ps
docker compose @composeFiles logs --tail 100 gateway user-service event-service booking-service payment-service saga-service scheduler-service notification-service
~~~

Check the Gateway public event query at <http://127.0.0.1:8000/api/events>. A successful HTTP JSON response confirms that Gateway and the current query dependencies are reachable. A new database returns an empty event list until events are created.

### Current Compose configuration and the Notification entry point

The base [docker-compose.yml](docker-compose.yml) does not provide all settings required by Notification's startup code, so Notification exits during initialization when run with the base file alone. Notification startup also pings Redis. Set `REDIS_URL=redis://redis:6379` inside the container. The default loopback address would refer to the container itself. Notification also requires the User gRPC address `user-service:50051`, server name `user-service`, complete client and server mTLS credentials, and SMTP configuration. The current Compose service has no certificate mount and receives none of these address, server-name, or SMTP environment variables.

Save the following fragment as a private Compose file outside the repository and set RUSHGO_COMPOSE_OVERRIDE to its path. Merge it with the base Compose file, then use the `composeFiles` variable above for `up`, `ps`, `logs`, and `down`. It also requires SMTP sandbox values and RUSHGO_STRIPE_EXPECTED_ACCOUNT in `backend/.env`. Keep SMTP login, password, sender, and frontend origin private in `.env`.

~~~yaml
services:
  notification-service:
    depends_on:
      redis:
        condition: service_healthy
    environment:
      REDIS_URL: redis://redis:6379
      USER_SERVICE_ADDR: user-service:50051
      RUSHGO_USER_GRPC_SERVER_NAME: user-service
      RUSHGO_GRPC_SERVER_CERT_FILE: /run/secrets/rushgo-grpc/server-cert.pem
      RUSHGO_GRPC_SERVER_KEY_FILE: /run/secrets/rushgo-grpc/server-key.pem
      RUSHGO_GRPC_CLIENT_CA_FILE: /run/secrets/rushgo-grpc/ca.pem
      RUSHGO_GRPC_CLIENT_CERT_FILE: /run/secrets/rushgo-grpc/client-cert.pem
      RUSHGO_GRPC_CLIENT_KEY_FILE: /run/secrets/rushgo-grpc/client-key.pem
      RUSHGO_GRPC_SERVER_CA_FILE: /run/secrets/rushgo-grpc/ca.pem
      SMTP_HOST: ${SMTP_HOST:?set a local mail catcher or SMTP sandbox}
      SMTP_PORT: ${SMTP_PORT:?set 465 or 587}
      SMTP_TLS_MODE: ${SMTP_TLS_MODE:?set implicit or starttls}
      SMTP_USERNAME: ${SMTP_USERNAME:?set private SMTP credentials}
      SMTP_PASSWORD: ${SMTP_PASSWORD:?set private SMTP credentials}
      SMTP_FROM: ${SMTP_FROM:?set a sandbox sender}
      AUTH_FRONTEND_ORIGIN: ${AUTH_FRONTEND_ORIGIN:?set the local frontend origin}
    volumes:
      - type: bind
        source: ${RUSHGO_GRPC_CERT_ROOT:?set the private certificate root}/notification
        target: /run/secrets/rushgo-grpc
        read_only: true

  gateway:
    environment:
      NOTIFICATION_SERVICE_ADDR: notification-service:50051
      RUSHGO_NOTIFICATION_GRPC_SERVER_NAME: notification-service

  payment-service:
    environment:
      RUSHGO_STRIPE_EXPECTED_ACCOUNT: ${RUSHGO_STRIPE_EXPECTED_ACCOUNT:?set the expected TEST account ID}
~~~

SMTP has no dry-run switch, and Notification may deliver queued mail after it starts. Use a local mail catcher or dedicated sandbox with test recipients only. Payment reads the Stripe account at startup. It can only initialize when private TEST credentials and the webhook endpoint secret are configured.

User can start for existing-account login with all SMTP settings empty, but it cannot deliver registration verification or account emails. Notification's SMTP sender initialization requires a host, username, password, sender address, either port 465 with implicit TLS or port 587 with STARTTLS, and a valid frontend origin. When SMTP is configured, User also requires AUTH_FRONTEND_ORIGIN.

With User, Gateway, and mail configuration ready, a regular buyer account can be created through `/register` in the frontend. Registration sends a verification email through SMTP. A new database has no default user. The scheduler and baseline seed scripts do not create application accounts. Newly registered accounts have the `user` role. The repository does not provide a supported command to create or elevate an administrator account. An administrator account must be prepared through a separate controlled process. Do not add guessed credentials or local-start instructions that directly alter production roles using SQL.

Compose configures Gateway addresses for User, Event, Booking, Payment, and Scheduler, along with Redis and gRPC client credentials. These services must be reachable for the complete API. Notification is optional in the Gateway configuration. Applying the private override above enables Notification-related administrative event sources.

Payment initializes its database, Kafka, Booking/Event authority, and mTLS during startup. It reads the TEST account identity using STRIPE_SECRET_KEY. The private override supplies the expected TEST account ID so startup can reject an unexpected account. The webhook uses STRIPE_SECRET_WEBHOOK to validate local callbacks at `/webhooks/stripe`. This account read contacts the Stripe API even when no payment is submitted. Before enabling the full Payment service, confirm that this TEST account read is approved and load TEST keys and the webhook secret from the private local `.env`. This documentation pass did not perform the account read.

## gRPC mTLS certificates

The certificate root is not included in the repository, and the project provides no general-purpose CA or service certificate generator for local services. Tests create temporary test certificates, but those are not a certificate issuance tool for the development stack. Use an approved private local certificate issuance process and supply the files through Compose bind mounts. Do not disable mTLS to bypass validation.

Compose mounts the subdirectories at `/run/secrets/rushgo-grpc`. The table below lists files for the default `local` environment. Event, Saga, and Scheduler use plaintext for ordinary RPCs in this environment. Separate internal authorization interfaces have their own transport requirements. In an external runtime environment, ordinary RPCs also require the appropriate server credentials and client server names. Do not reuse this table without checking the environment. [Transport selection](pkg/grpcsecurity/internal_transport.go)

| Subdirectory | Client identity files | Server identity files |
| --- | --- | --- |
| gateway | client-cert.pem, client-key.pem, ca.pem | None |
| user | None | server-cert.pem, server-key.pem, ca.pem |
| event | None | server-cert.pem, server-key.pem, ca.pem |
| booking | client-cert.pem, client-key.pem, ca.pem | server-cert.pem, server-key.pem |
| payment | client-cert.pem, client-key.pem, ca.pem | server-cert.pem, server-key.pem |
| saga | client-cert.pem, client-key.pem, ca.pem | server-cert.pem, server-key.pem |
| scheduler | client-cert.pem, client-key.pem, ca.pem | None |
| notification | client-cert.pem, client-key.pem, ca.pem | server-cert.pem, server-key.pem |

The base Compose file has no Notification bind mount. The table lists the files required by the current Notification implementation, so the override must mount them explicitly. Compose client/server environment variables point to `client-cert.pem`, `client-key.pem`, `server-cert.pem`, `server-key.pem`, and `ca.pem` as appropriate.

Client certificates need URI SANs that match the service identities checked by the code. For example, Gateway uses `spiffe://rushgo.internal/gateway` and Booking uses `spiffe://rushgo.internal/booking`. Other client identities use `user`, `event`, `payment`, `saga`, `scheduler`, and `notification`. Server certificates establish the TLS server identity, and their DNS SANs must match the server names configured in Compose. Current configuration uses `user-service`, `event-service`, `booking-service`, `payment-service`, and `saga-service`; the private Notification configuration uses `notification-service`. Servers check the client identity against the URI SAN in the client certificate. The mounted `ca.pem` supplies the trust chain. In Linux containers, private key files must be regular files and must not be readable or writable by group or other users. Preserve the existing certificate validation and key-file permission checks.

Ticket authority and some internal RPCs run on Event, Saga, and other service ports. They rely on the matching CA trust and service identity. Services may reject connections at startup or during a handshake if certificates are expired, URI SANs or DNS SANs do not match, keys do not correspond, or files are mounted at the wrong paths.

## Migrations and optional initialization

`make migrate` connects to databases through host ports published by the default Compose file. Those ports are hard-coded in [scripts/migrate.sh](scripts/migrate.sh). The local port override changes PostgreSQL host ports to 15433 through 15439, while the migration script still connects to 5433 through 5439. Do not run `make migrate` with `docker-compose.local.yml`. The script has no migration-port parameter, so this port mismatch needs a separate future fix.

The optional `docker-compose.local.yml` overrides host mappings to work around specific port conflicts. It uses the `!override` tag, which requires a Compose version that supports that tag. This file is not needed for a standard startup, and its PostgreSQL ports do not match the current `migrate.sh`. Resolve the migration script's fixed-port assumption before choosing another port setup.

Initialize Scheduler's booking-cleanup job configuration with the command below. The script checks and repairs invalid defaults. If Scheduler is already running, it also restarts the service. Run this only against a disposable development database.

~~~bash
make seed-scheduler
~~~

When an event already exists and its ticket tiers and seats meet the script's requirements, run the Event lifecycle baseline helper for explicitly identified event IDs. This updates baseline markers for ticket tiers belonging to those events. It does not create or reset ticket tiers, seats, events, or orders.

~~~bash
event_id='00000000-0000-4000-8000-000000000001'
EVENT_IDS="$event_id" make seed-baseline
~~~

Replace the example with the real UUID of an event you created. The script does not create events and accepts only ticket tiers that satisfy the full inventory and seat requirements.

When database state, migrations, or optional scripts change, read the corresponding SQL and Bash scripts first. Run them against local development databases only.

## Frontend connection, logs, and shutdown

The frontend is a separate Next.js project. In a new PowerShell window, start it from the repository root and point its API at Gateway.

~~~powershell
Set-Location frontend
npm ci
$env:NEXT_PUBLIC_API_URL = 'http://127.0.0.1:8000'
npm run dev -- --hostname 127.0.0.1 --port 3000
~~~

Open <http://127.0.0.1:3000/>. For full-stack settings, feature boundaries, and the UI demo, see the [frontend guide](../frontend/README.en.md) and the [root DEMO](../DEMO.en.md).

Every Compose command must use the same base files and private override. Use the `composeFiles` variable created during startup for status and logs.

Stop the stack while preserving the local database and Redis volumes.

~~~powershell
docker compose @composeFiles down
~~~

Do not add a volume-deletion option unless you have confirmed that local data can be discarded permanently. `docker compose down` stops the Compose containers and retains named volumes.

The optional `docker-compose.observability.yml` adds a local OpenTelemetry Collector, Prometheus, Tempo, and Grafana. Include it only when you need the observability UI, and keep the same Compose file set for startup and subsequent management commands.

~~~powershell
$observabilityComposeFiles = $composeFiles + @('-f', 'docker-compose.observability.yml')
docker compose @observabilityComposeFiles up --build -d
docker compose @observabilityComposeFiles ps
docker compose @observabilityComposeFiles logs -f
docker compose @observabilityComposeFiles down
~~~

## Build

Install Go 1.25.5 or newer. From the `backend` directory, build each Go workspace module in PowerShell.

~~~powershell
$modules = @('pkg', 'services/booking', 'services/event', 'services/gateway', 'services/notification', 'services/payment', 'services/saga', 'services/scheduler', 'services/user')
foreach ($module in $modules) {
    Push-Location $module
    try {
        go build ./...
        if ($LASTEXITCODE -ne 0) { throw "go build failed for $module" }
    }
    finally {
        Pop-Location
    }
}
~~~

`make build` builds each service entry point from the Makefile. `make test` runs tests for all Go services. Some integration tests require separate databases, Kafka, or test credentials; review their requirements before running them. This documentation update checked Compose configuration but did not start backend containers or run the complete backend integration flow. See [project status](../PROJECT_STATUS.en.md) for the boundaries of the browser demo and full business E2E verification.
