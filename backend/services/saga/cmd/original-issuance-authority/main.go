package main

import (
	"context"
	"github.com/ticketbox/pkg/originalissuance"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	sagagrpc "github.com/ticketbox/saga/internal/grpc"
	"github.com/ticketbox/saga/internal/repository"
	"github.com/ticketbox/saga/internal/service"
	"go.uber.org/zap"
)

func main() {
	originalissuance.Main(func(ctx context.Context) error {
		pool, err := originalissuance.Open(ctx, "saga", true)
		if err != nil {
			return err
		}
		defer pool.Close()
		repo := repository.NewSagaRepository(pool)
		svc := service.NewSagaService(zap.NewNop(), repo, nil, nil, nil, nil)
		return originalissuance.Serve(ctx, "saga", sagav1.SagaOrchestratorService_ServiceDesc, sagagrpc.NewSagaOrchestratorServer(svc, zap.NewNop()), sagav1.SagaOrchestratorService_VerifyTicketIssuanceSaga_FullMethodName)
	})
}
