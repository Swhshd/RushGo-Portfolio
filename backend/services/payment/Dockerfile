FROM golang:1.25-alpine AS builder
WORKDIR /app
ARG RECOVERY_BUILD_TAGS=""
COPY pkg ./pkg
COPY services/payment ./services/payment

RUN cd services/payment && GOPROXY=https://goproxy.cn,direct GOWORK=off go mod download && \
    CGO_ENABLED=0 GOOS=linux GOWORK=off go build -tags "$RECOVERY_BUILD_TAGS" -o /bin/payment-service ./cmd

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /bin/payment-service /bin/payment-service
COPY services/payment/migrations /migrations

ENTRYPOINT ["/bin/payment-service"]
