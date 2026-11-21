FROM golang:1.25-alpine AS builder
WORKDIR /app
ARG RECOVERY_BUILD_TAGS=""

COPY pkg/ ./pkg/
COPY services/booking/ ./services/booking/

RUN cd services/booking && GOPROXY=https://goproxy.cn,direct GOWORK=off go mod download && \
    CGO_ENABLED=0 GOOS=linux GOWORK=off go build -tags "$RECOVERY_BUILD_TAGS" -o /bin/booking-service ./cmd

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /bin/booking-service /bin/booking-service
COPY services/booking/migrations /migrations

ENTRYPOINT ["/bin/booking-service"]
