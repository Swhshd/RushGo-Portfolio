package main

import (
	"context"
	eventgrpc "github.com/ticketbox/event/internal/grpc"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/event/internal/service"
	"github.com/ticketbox/pkg/originalissuance"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"go.uber.org/zap"
)

func main() {
	originalissuance.Main(func(ctx context.Context) error {
		pool, err := originalissuance.Open(ctx, "event", true)
		if err != nil {
			return err
		}
		defer pool.Close()
		svc := service.NewEventService(nil, nil, repository.NewPostgresSeatRepository(pool), zap.NewNop())
		return originalissuance.Serve(ctx, "event", eventv1.EventService_ServiceDesc, eventgrpc.NewEventServer(svc, zap.NewNop()), eventv1.EventService_VerifyBookingTicketEligibility_FullMethodName)
	})
}
