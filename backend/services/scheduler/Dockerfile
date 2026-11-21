FROM golang:1.25-alpine AS builder
WORKDIR /app
ARG RECOVERY_BUILD_TAGS=""
COPY pkg ./pkg
COPY services/scheduler ./services/scheduler
COPY services/booking/go.mod services/booking/go.sum ./services/booking/

RUN cd services/scheduler && GOPROXY=https://goproxy.cn,direct GOWORK=off go mod download && \
    CGO_ENABLED=0 GOOS=linux GOWORK=off go build -tags "$RECOVERY_BUILD_TAGS" -o /bin/scheduler-service ./cmd

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /bin/scheduler-service /bin/scheduler-service
COPY services/scheduler/migrations /migrations

ENTRYPOINT ["/bin/scheduler-service"]
