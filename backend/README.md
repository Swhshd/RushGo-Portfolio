# RushGo backend

The backend workspace contains the Go services, shared packages and Protocol Buffer contracts used by the RushGo application. It has eight services: Gateway, User, Event, Booking, Notification, Payment, Saga and Scheduler.

## Build

Install Go 1.25.5 or newer to match `go.work`. From this directory, build each workspace module with PowerShell:

```powershell
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
```

The `Makefile` also provides `make build`, `make test`, `make up`, `make down` and migration helpers for a Bash/Make environment. `make test` runs each service's full Go test suite. Many integration tests need local PostgreSQL or Kafka, so use the specific package tests when those services are unavailable.

## Full local service stack

The Compose stack starts the Go services with PostgreSQL, Redis and Kafka. It needs a private mTLS certificate directory outside this repository. Set `RUSHGO_GRPC_CERT_ROOT` to the parent directory containing the service-specific identities expected by the Compose mounts. The repository does not include certificates or a general-purpose certificate provisioning command. Keep private keys out of the checkout.

Copy `.env.example` to `.env` and provide the local settings required by the Compose services. Do not reuse these development defaults outside a disposable local environment. Payment integration requires Stripe TEST credentials and a matching local webhook configuration. For the browser demo, use the explicitly started read-only fixture described in the repository-root `DEMO.md`. That view does not verify the backend purchase path.

With Docker Compose installed and the private mTLS material prepared, start the services from this directory:

```sh
docker compose up --build -d
```

On a host where the default database ports are occupied, add the local override:

```sh
docker compose -f docker-compose.yml -f docker-compose.local.yml up --build -d
```

The authenticated User migration has preflight requirements. Review `scripts/migrate.sh` and its `AUTH_PREFLIGHT_DATABASE_URL` setup before running `make migrate`.

Stop the local services while retaining their database volumes with:

```sh
docker compose down
```

The optional `docker-compose.observability.yml` adds the local OpenTelemetry Collector, Prometheus, Tempo and Grafana stack.

## Validation scope

For the scope of the browser demo, unit checks and outstanding end-to-end verification, see [project status](../PROJECT_STATUS.md).
