# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS source
WORKDIR /app
COPY pkg ./pkg
COPY services/saga ./services/saga
COPY services/booking ./services/booking
COPY services/event ./services/event
COPY services/payment ./services/payment

FROM source AS production-builder
ARG RECOVERY_BUILD_TAGS=""
RUN cd services/saga && GOPROXY=https://goproxy.cn,direct CGO_ENABLED=0 GOOS=linux GOWORK=off go build -mod=readonly -tags "$RECOVERY_BUILD_TAGS" -o /bin/saga-service ./cmd

FROM source AS ticket-e2e-builder
ENV GOPROXY=https://goproxy.cn,direct GOSUMDB=sum.golang.org
ARG TICKET_E2E_SOURCE_HEAD
ARG TICKET_E2E_SOURCE_FINGERPRINT
ARG TICKET_E2E_PROJECT
COPY services/user ./services/user
COPY services/gateway ./services/gateway
COPY services/scheduler ./services/scheduler
COPY scripts/ticket-e2e ./scripts/ticket-e2e
RUN --mount=type=cache,id=ticket-e2e-gomod,target=/go/pkg/mod,sharing=locked \
	--mount=type=cache,id=ticket-e2e-gocache-go125,target=/go/cache,sharing=locked \
	test "${#TICKET_E2E_SOURCE_HEAD}" -eq 40 && \
	test "${#TICKET_E2E_SOURCE_FINGERPRINT}" -eq 64 && \
	printf '%s\n' "$TICKET_E2E_PROJECT" | grep -Eq '^rushgo-ticket-e2e-[0-9]{8}-[0-9a-f]{12}$' && \
	GOWORK=off go run ./scripts/ticket-e2e/fingerprint.go --root /app --prefix backend --expected "${TICKET_E2E_SOURCE_FINGERPRINT}" && \
	cd services/saga && GOPROXY=https://goproxy.cn,direct GOSUMDB=sum.golang.org GOMODCACHE=/go/pkg/mod GOCACHE=/go/cache GOMAXPROCS=2 CGO_ENABLED=0 GOOS=linux GOWORK=off go build -p 1 -mod=readonly -tags ticket_e2e \
	-ldflags "-X github.com/ticketbox/pkg/grpcsecurity.ticketE2EBuildSourceHead=${TICKET_E2E_SOURCE_HEAD} -X github.com/ticketbox/pkg/grpcsecurity.ticketE2EBuildSourceFingerprint=${TICKET_E2E_SOURCE_FINGERPRINT} -X github.com/ticketbox/pkg/grpcsecurity.ticketE2EBuildProject=${TICKET_E2E_PROJECT}" \
	-o /bin/saga-service-ticket-e2e ./cmd

FROM alpine:3.19 AS ticket-e2e
RUN apk --no-cache add ca-certificates
COPY --from=ticket-e2e-builder /bin/saga-service-ticket-e2e /bin/saga-service
COPY services/saga/migrations /migrations
ENTRYPOINT ["/bin/saga-service"]

FROM alpine:3.19 AS production
RUN apk --no-cache add ca-certificates
COPY --from=production-builder /bin/saga-service /bin/saga-service
COPY services/saga/migrations /migrations

ENTRYPOINT ["/bin/saga-service"]
