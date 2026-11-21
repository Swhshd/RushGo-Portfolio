package service_scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/pkg/outbound"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/scheduler/internal/domain"
	cronjob "github.com/ticketbox/scheduler/internal/jobs"
	"github.com/ticketbox/scheduler/internal/repository"
	"go.uber.org/zap"
)

type SchedulerService struct {
	logger              *zap.Logger
	schedulerConfigRepo repository.SchedulerRepository
	outboundEventRepo   repository.OutboundEventRepository
	uow                 repository.UnitOfWork
	cronjobManager      *cronjob.CronJobManager
	reliabilityHook     *reliabilityhook.Controller
}

// ConfigureReliabilityHook enables private barriers for an isolated Recovery
// candidate. A disabled hook leaves normal Scheduler behavior unchanged.
func (s *SchedulerService) ConfigureReliabilityHook(hook *reliabilityhook.Controller) error {
	if s == nil || hook == nil {
		return fmt.Errorf("missing reliability hook")
	}
	s.reliabilityHook = hook
	return nil
}

func NewSchedulerService(logger *zap.Logger, schedulerConfigRepo repository.SchedulerRepository, outboundEventRepo repository.OutboundEventRepository, uow repository.UnitOfWork, cronjobManager *cronjob.CronJobManager) *SchedulerService {
	return &SchedulerService{
		logger:              logger,
		schedulerConfigRepo: schedulerConfigRepo,
		outboundEventRepo:   outboundEventRepo,
		uow:                 uow,
		cronjobManager:      cronjobManager,
	}
}

func (s *SchedulerService) ListSchedulersConfig(ctx context.Context) ([]domain.SchedulerConfig, error) {
	return s.schedulerConfigRepo.ListSchedulersConfig(ctx)
}

// CreateSchedulerConfig persists a supported job configuration and its
// version-1 outbox event atomically. New configurations are always disabled;
// enabling them remains an explicit follow-up admin action.
func (s *SchedulerService) CreateSchedulerConfig(ctx context.Context, target domain.SchedulerConfig) (*domain.SchedulerConfig, error) {
	target.IsEnabled = false
	target.Version = 1
	if err := repository.ValidateCreateConfig(target); err != nil {
		return nil, err
	}
	target.Id = uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("ticketbox:scheduler:create:%s:%s", target.JobType, target.Name)))

	eventID := SchedulerOutboundID(target.Id, target.Version)
	var persisted *domain.SchedulerConfig
	var inserted bool
	err := s.uow.Execute(ctx, func(repos *repository.UowRepos) error {
		config, created, err := repos.SchedulerConfigRepo().CreateIfAbsent(ctx, target)
		if err != nil {
			return err
		}
		persisted, inserted = config, created
		if !created {
			return nil
		}
		payload, err := json.Marshal(*config)
		if err != nil {
			return err
		}
		return repos.OutboundRepo().Create(ctx, outbound.OutboundEvent{
			Id: eventID, Topic: topics.TopicSchedulerConfigEvent,
			EventType: topics.TypeSchedulerConfigChanged,
			Payload:   payload, Status: outbound.OutboundStatusPending,
		})
	})
	if errors.Is(err, repository.ErrCommitUnknown) {
		readCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if s.verifyCreatedPair(readCtx, target) == nil {
			return s.schedulerConfigRepo.GetById(readCtx, target.Id)
		}
	}
	if err != nil {
		return nil, err
	}
	if !inserted {
		if err := s.verifyCreatedPair(ctx, target); err != nil {
			return nil, err
		}
		return persisted, nil
	}
	return persisted, nil
}

func (s *SchedulerService) verifyCreatedPair(ctx context.Context, target domain.SchedulerConfig) error {
	c, err := s.schedulerConfigRepo.GetById(ctx, target.Id)
	if err != nil {
		return err
	}
	if !c.Valid || c.Name != target.Name || c.JobType != target.JobType || c.Version != 1 || c.IsEnabled || !sameConfig(*c, target) {
		return repository.ErrConfigConflict
	}
	e, err := s.outboundEventRepo.GetByID(ctx, SchedulerOutboundID(target.Id, 1))
	if err != nil {
		return err
	}
	var frozen domain.SchedulerConfig
	if e.Topic != topics.TopicSchedulerConfigEvent || e.EventType != topics.TypeSchedulerConfigChanged || json.Unmarshal(e.Payload, &frozen) != nil ||
		frozen.Id != target.Id || frozen.Name != target.Name || frozen.JobType != target.JobType || frozen.Version != 1 || frozen.IsEnabled || !sameConfig(frozen, target) {
		return repository.ErrConfigConflict
	}
	return nil
}

func (s *SchedulerService) UpdateSchedulerConfigById(ctx context.Context, id uuid.UUID, target domain.SchedulerConfig) error {
	if err := repository.ValidateConfig(target); err != nil {
		return err
	}
	current, err := s.schedulerConfigRepo.GetById(ctx, id)
	if err != nil {
		return err
	}
	if !current.Valid {
		return errors.New(current.ValidationError)
	}
	expected := target.Version
	if expected == 0 {
		// The compatibility RPC has no expected-version field. An identical
		// latest target with its durable event is a replay, not a new update.
		if sameConfig(*current, target) {
			return s.verifyPair(ctx, id, current.Version, target)
		}
		return repository.ErrConfigConflict
	}
	targetVersion := expected + 1
	eventID := SchedulerOutboundID(id, targetVersion)
	if current.Version == targetVersion {
		return s.verifyPair(ctx, id, targetVersion, target)
	}
	if current.Version != expected {
		return repository.ErrConfigConflict
	}
	err = s.uow.Execute(ctx, func(repos *repository.UowRepos) error {
		updatedConfig, err := repos.SchedulerConfigRepo().UpdateByVersion(ctx, id, expected, target)
		if err != nil {
			s.logger.Sugar().Error("[SchedulerService][UpdateSchedulerConfigById] Fail to update scheduler config", zap.Error(err))
			return err
		}
		if updatedConfig == nil {
			s.logger.Sugar().Error("[SchedulerService][UpdateSchedulerConfigById] Scheduler config didn't change, no need to create outbound event")
			return nil
		}
		if err := s.awaitUnitOfWorkBoundary(ctx, updatedConfig, expected, eventID); err != nil {
			return err
		}

		s.logger.Info("[SchedulerService][UpdateSchedulerConfigById] Scheduler config updated", zap.String("id", updatedConfig.Id.String()), zap.String("name", updatedConfig.Name), zap.Bool("is_enabled", updatedConfig.IsEnabled), zap.String("interval_expression", updatedConfig.IntervalExpression), zap.Duration("timeout", updatedConfig.Timeout))
		payload, err := json.Marshal(*updatedConfig)
		if err != nil {
			s.logger.Sugar().Error("[SchedulerService][UpdateSchedulerConfigById] Fail to marshal target scheduler config", zap.Error(err))
			return err
		}

		err = repos.OutboundRepo().Create(ctx, outbound.OutboundEvent{
			Id:        eventID,
			Topic:     topics.TopicSchedulerConfigEvent,
			EventType: topics.TypeSchedulerConfigChanged,
			Payload:   payload,
			Status:    outbound.OutboundStatusPending,
		})
		if err != nil {
			s.logger.Sugar().Error("[SchedulerService][UpdateSchedulerConfigById] Fail to create a outbound event", zap.Error(err))
			return err
		}
		return nil
	})
	if errors.Is(err, repository.ErrCommitUnknown) || errors.Is(err, repository.ErrConfigConflict) {
		readCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if s.verifyPair(readCtx, id, targetVersion, target) == nil {
			return nil
		}
	}
	return err
}

func SchedulerOutboundID(id uuid.UUID, version int) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("ticketbox:scheduler:%s:%d", id, version)))
}
func sameConfig(a, b domain.SchedulerConfig) bool {
	return a.IsEnabled == b.IsEnabled && a.Timeout == b.Timeout && a.IntervalExpression == b.IntervalExpression
}
func (s *SchedulerService) verifyPair(ctx context.Context, id uuid.UUID, version int, target domain.SchedulerConfig) error {
	c, err := s.schedulerConfigRepo.GetById(ctx, id)
	if err != nil {
		return err
	}
	if !c.Valid || c.Version != version || !sameConfig(*c, target) {
		return repository.ErrConfigConflict
	}
	e, err := s.outboundEventRepo.GetByID(ctx, SchedulerOutboundID(id, version))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return repository.ErrConfigConflict
		}
		return err
	}
	var frozen domain.SchedulerConfig
	if e.Topic != topics.TopicSchedulerConfigEvent || e.EventType != topics.TypeSchedulerConfigChanged || json.Unmarshal(e.Payload, &frozen) != nil || frozen.Id != id || frozen.Version != version || frozen.Name != c.Name || frozen.JobType != c.JobType || !sameConfig(frozen, target) {
		return repository.ErrConfigConflict
	}
	return nil
}

func (s *SchedulerService) HandleSchedulerConfigChanged(ctx context.Context, payload domain.SchedulerConfig) error {
	return s.cronjobManager.ReRegisterJob(ctx, payload)
}
