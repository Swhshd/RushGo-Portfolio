package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	schedulerv1 "github.com/ticketbox/pkg/proto/scheduler/v1"
	"github.com/ticketbox/scheduler/internal/domain"
	"github.com/ticketbox/scheduler/internal/repository"
	service_scheduler "github.com/ticketbox/scheduler/internal/service/scheduler"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SchedulerServiceServer struct {
	schedulerv1.UnimplementedSchedulerServiceServer
	service *service_scheduler.SchedulerService
	logger  *zap.Logger
}

func NewSchedulerServiceServer(service *service_scheduler.SchedulerService, logger *zap.Logger) *SchedulerServiceServer {
	return &SchedulerServiceServer{
		service: service,
		logger:  logger,
	}
}

func (s *SchedulerServiceServer) ListActiveSchedulers(ctx context.Context, req *schedulerv1.ListActiveSchedulersRequest) (*schedulerv1.ListActiveSchedulersResponse, error) {
	res, err := s.service.ListSchedulersConfig(ctx)
	if err != nil {
		s.logger.Sugar().Error("[Scheduler service][LisActiveSchedulers] Fail to get list schedulers config", zap.Error(err))
		return nil, err
	}
	schedulerCfgs := make([]*schedulerv1.SchedulerItem, 0, len(res))
	for _, cfg := range res {
		schedulerCfgs = append(schedulerCfgs, &schedulerv1.SchedulerItem{
			Id:              cfg.Id.String(),
			IsEnabled:       cfg.IsEnabled,
			Version:         int32(cfg.Version),
			Name:            cfg.Name,
			JobType:         string(cfg.JobType),
			CronExpression:  cfg.IntervalExpression,
			Timeout:         int32(cfg.Timeout / time.Second),
			CreatedAt:       timestamppb.New(cfg.CreatedAt),
			UpdatedAt:       timestamppb.New(cfg.UpdatedAt),
			Valid:           cfg.Valid,
			ValidationError: cfg.ValidationError,
		})
	}
	return &schedulerv1.ListActiveSchedulersResponse{
		Schedulers: schedulerCfgs,
	}, nil
}

func (s *SchedulerServiceServer) UpdateSchedulerById(ctx context.Context, req *schedulerv1.UpdateSchedulerByIdRequest) (*emptypb.Empty, error) {
	schedulerCfgId, err := uuid.Parse(req.Id)
	if err != nil {
		s.logger.Sugar().Error("[Scheduler service][UpdateSchedulerById] Scheduler config id is invalid", zap.Error(err))
		return nil, fmt.Errorf("scheduler config id is invalid: %w", err)
	}
	err = s.service.UpdateSchedulerConfigById(ctx, schedulerCfgId, domain.SchedulerConfig{
		IsEnabled:          req.IsEnable,
		IntervalExpression: req.CronExpression,
		Timeout:            time.Duration(req.Timeout) * time.Second,
		Version:            int(req.ExpectedVersion),
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *SchedulerServiceServer) CreateSchedulerConfig(ctx context.Context, req *schedulerv1.CreateSchedulerConfigRequest) (*schedulerv1.SchedulerItem, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "scheduler config is required")
	}
	target := domain.SchedulerConfig{
		Name: req.Name, JobType: domain.JobType(req.JobType),
		IntervalExpression: req.CronExpression, Timeout: time.Duration(req.Timeout) * time.Second,
	}
	if err := repository.ValidateCreateConfig(target); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid scheduler job configuration")
	}
	created, err := s.service.CreateSchedulerConfig(ctx, target)
	if err != nil {
		if errors.Is(err, repository.ErrConfigConflict) {
			return nil, status.Error(codes.AlreadyExists, "a scheduler job with this name already exists")
		}
		s.logger.Error("scheduler job creation failed", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to create scheduler job")
	}
	return &schedulerv1.SchedulerItem{
		Id: created.Id.String(), Name: created.Name, JobType: string(created.JobType),
		CronExpression: created.IntervalExpression, IsEnabled: created.IsEnabled,
		Version: int32(created.Version), Timeout: int32(created.Timeout / time.Second),
		CreatedAt: timestamppb.New(created.CreatedAt), UpdatedAt: timestamppb.New(created.UpdatedAt),
		Valid: created.Valid,
	}, nil
}
