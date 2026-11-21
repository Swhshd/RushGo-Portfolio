// Package testfixture provides production service wiring for isolated tests.
package testfixture

import (
	"github.com/jackc/pgx/v5/pgxpool"
	eventgrpc "github.com/ticketbox/event/internal/grpc"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/event/internal/service"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"go.uber.org/zap"
)

func SagaReplayServer(pool *pgxpool.Pool) eventv1.EventServiceServer {
	logger := zap.NewNop()
	return eventgrpc.NewEventServer(service.NewEventService(repository.NewPostgresEventRepository(pool), repository.NewPostgresTicketTierRepository(pool), repository.NewPostgresSeatRepository(pool), logger), logger)
}
