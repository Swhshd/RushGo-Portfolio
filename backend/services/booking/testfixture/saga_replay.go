// Package testfixture wires production boundaries for cross-service integration
// tests without exposing service internals to another production module.
package testfixture

import (
	"github.com/jackc/pgx/v5/pgxpool"
	bookinggrpc "github.com/ticketbox/booking/internal/grpc"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"go.uber.org/zap"
)

func SagaReplayServer(pool *pgxpool.Pool) bookingv1.BookingServiceServer {
	logger := zap.NewNop()
	return bookinggrpc.NewBookingServer(service.NewBookingService(repository.NewPostgresBookingRepository(pool), nil, nil, logger), logger)
}

func RequestReplayServer(pool *pgxpool.Pool, event eventv1.EventServiceClient, saga sagav1.SagaOrchestratorServiceClient) bookingv1.BookingServiceServer {
	logger := zap.NewNop()
	return bookinggrpc.NewBookingServer(service.NewBookingService(repository.NewPostgresBookingRepository(pool), event, saga, logger), logger)
}
