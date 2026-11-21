package cronjob

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	cronv3 "github.com/robfig/cron/v3"
	"github.com/ticketbox/pkg/reliabilityhook"
	"go.uber.org/zap"

	"github.com/ticketbox/scheduler/internal/domain"
	"github.com/ticketbox/scheduler/internal/repository"
)

// defaultJobTimeout is used when a job config carries no positive timeout,
// otherwise context.WithTimeout(0) would expire immediately.
const defaultJobTimeout = 30 * time.Second

type CronJobManager struct {
	logger          *zap.Logger
	cron            *cronv3.Cron
	jobs            map[string]Job
	jobEntryID      map[string]cronv3.EntryID
	jobConfig       map[string]domain.SchedulerConfig
	mu              sync.Mutex
	isStarted       bool
	withSecond      bool
	withErrHandler  func(ctx context.Context, err error) error
	schedulerRepo   repository.SchedulerRepository
	reliabilityHook *reliabilityhook.Controller
}

// ConfigureReliabilityHook enables the isolated config-replacement boundary.
// Normal Scheduler behavior remains unchanged when the controller is disabled.
func (c *CronJobManager) ConfigureReliabilityHook(hook *reliabilityhook.Controller) error {
	if c == nil || hook == nil {
		return fmt.Errorf("missing reliability hook")
	}
	c.reliabilityHook = hook
	return nil
}

func NewCronjobManager(logger *zap.Logger, schedulerRepo repository.SchedulerRepository, opts ...Option) *CronJobManager {
	cronjobManager := &CronJobManager{
		jobs:          make(map[string]Job),
		jobConfig:     make(map[string]domain.SchedulerConfig),
		mu:            sync.Mutex{},
		jobEntryID:    make(map[string]cronv3.EntryID),
		logger:        logger,
		schedulerRepo: schedulerRepo,
	}

	for _, opt := range opts {
		opt(cronjobManager)
	}

	cronOpts := []cronv3.Option{}

	if cronjobManager.withSecond {
		cronOpts = append(cronOpts, cronv3.WithSeconds())
	}

	cronjobManager.cron = cronv3.New(cronOpts...)

	return cronjobManager

}

// Start is non-blocking (robfig cron spawns its own goroutine), so callers
// must NOT wrap it in an extra `go func()`. It is idempotent and safe to
// call once after all jobs are registered.
func (c *CronJobManager) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isStarted {
		return
	}
	c.cron.Start()
	c.isStarted = true
	c.logger.Info("[CronJobManager][Start] Cronjob manager starts successfully")

}

func (c *CronJobManager) LoadJobSchedulerConfigs(ctx context.Context) error {
	jobConfigs, err := c.schedulerRepo.ListSchedulersConfig(ctx)
	if err != nil {
		return fmt.Errorf("[CronJobManager][LoadJobSchedulerConfigs] Load job schedule config fail: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.jobConfig = make(map[string]domain.SchedulerConfig, len(jobConfigs))
	for _, jc := range jobConfigs {
		if !jc.Valid {
			c.logger.Warn("invalid scheduler config excluded")
			continue
		}
		c.jobConfig[jc.Name] = jc
	}
	c.logger.Info("[CronJobManager][LoadJobSchedulerConfigs] Scheduler configs load successfully",
		zap.Int("count", len(jobConfigs)))
	return nil
}

func (c *CronJobManager) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.isStarted {
		return nil
	}
	c.isStarted = false
	return c.cron.Stop().Err()
}

func (c *CronJobManager) RegisterJob(_ context.Context, job Job) error {
	jobType := job.Name()
	c.mu.Lock()
	c.jobs[jobType] = job
	names := make([]string, 0)
	for name, cfg := range c.jobConfig {
		if configJobType(cfg) == jobType {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		c.mu.Unlock()
		return fmt.Errorf("[CronJobManager][RegisterJob] Job config of this job is not exist: %s", jobType)
	}
	sort.Strings(names)
	for _, name := range names {
		jc := c.jobConfig[name]
		if err := repository.ValidateConfig(jc); err != nil {
			c.mu.Unlock()
			return err
		}
		if !jc.IsEnabled {
			c.logger.Info("[CronJobManager][RegisterJob] Job is disabled, skip scheduling", zap.String("job", name))
			continue
		}
		entryID, err := c.cron.AddFunc(jc.IntervalExpression, jobHandle(job, c.logger, jc.Timeout, c.withErrHandler))
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("[CronJobManager][RegisterJob] Register job fail: %w", err)
		}
		c.jobEntryID[name] = entryID
		c.logger.Info("[CronJobManager][RegisterJob] Job registered successfully",
			zap.String("job", name), zap.String("job_type", jobType),
			zap.String("cron", jc.IntervalExpression), zap.Duration("timeout", jc.Timeout), zap.Bool("isEnabled", jc.IsEnabled))
	}
	c.mu.Unlock()
	return nil
}

func configJobType(cfg domain.SchedulerConfig) string {
	if cfg.JobType != "" {
		return string(cfg.JobType)
	}
	// Keep direct unit fixtures and legacy in-memory callers source compatible.
	return cfg.Name
}

func jobHandle(job Job, logger *zap.Logger, timeout time.Duration, handlerErr func(ctx context.Context, err error) error) func() {
	if timeout <= 0 {
		timeout = defaultJobTimeout
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := job.Run(ctx); err != nil {
			logger.Error("Job run failed",
				zap.String("job", job.Name()),
				zap.Error(err))
			if handlerErr != nil {
				if handleErr := handlerErr(ctx, err); handleErr != nil {
					logger.Error("Job error handler failed",
						zap.String("job", job.Name()),
						zap.Error(handleErr))
				}
			}
			return
		}
		logger.Info("Job completed successfully", zap.String("job", job.Name()))
	}
}

func (c *CronJobManager) ReRegisterJob(ctx context.Context, jc domain.SchedulerConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentJobConfig, exists := c.jobConfig[jc.Name]
	if exists {
		if jc.Id != currentJobConfig.Id || jc.Name != currentJobConfig.Name || configJobType(jc) != configJobType(currentJobConfig) {
			return fmt.Errorf("scheduler identity changed")
		}
		if jc.Version <= currentJobConfig.Version {
			// NOOP: stale event, keep the current schedule untouched.
			c.logger.Info("[CronJobManager][ReRegisterJob] The target job config is not latest version, skip",
				zap.String("job", jc.Name),
				zap.Int("currentVersion", currentJobConfig.Version),
				zap.Int("targetVersion", jc.Version))
			return nil
		}
	} else {
		// A Kafka group member may miss version 1 and first observe a later
		// event. Treat the event only as a lookup hint; always load and apply
		// the validated current row so a payload cannot invent a version or
		// schedule for an unknown local config.
		if c.schedulerRepo == nil {
			return fmt.Errorf("scheduler repository unavailable for unknown config %s", jc.Name)
		}
		authoritative, err := c.schedulerRepo.GetById(ctx, jc.Id)
		if err != nil {
			return fmt.Errorf("load authoritative scheduler config: %w", err)
		}
		if authoritative == nil || !authoritative.Valid || authoritative.Id != jc.Id || authoritative.Name != jc.Name || configJobType(*authoritative) != configJobType(jc) {
			return fmt.Errorf("scheduler identity changed or authoritative config is invalid")
		}
		if jc.Version < 1 || authoritative.Version < jc.Version {
			return fmt.Errorf("scheduler event version is not present in the authoritative config")
		}
		jc = *authoritative
	}
	if err := repository.ValidateConfig(jc); err != nil {
		return err
	}
	job, ok := c.jobs[configJobType(jc)]
	if !ok {
		return fmt.Errorf("[CronJobManager][ReRegisterJob] Fail to get job implementation: %s", configJobType(jc))
	}
	if c.reliabilityHook != nil && c.reliabilityHook.Enabled() {
		eventID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf(
			"ticketbox:scheduler:%s:%d", jc.Id, jc.Version)))
		_, _, err := c.reliabilityHook.AwaitSchedulerBoundary(ctx,
			"scheduler.config_replacement_reload", eventID.String(),
			"scheduler_config_before_apply", map[string]string{
				"scheduler_config_id": jc.Id.String(),
				"outbound_event_id":   eventID.String(),
				"version":             fmt.Sprint(jc.Version),
				"prior_version":       fmt.Sprint(currentJobConfig.Version),
			})
		if err != nil {
			return fmt.Errorf("scheduler replacement boundary: %w", err)
		}
	}
	var entryID cronv3.EntryID
	if jc.IsEnabled {
		var err error
		entryID, err = c.cron.AddFunc(jc.IntervalExpression, jobHandle(job, c.logger, jc.Timeout, c.withErrHandler))
		if err != nil {
			return fmt.Errorf("register replacement: %w", err)
		}
	}
	if old, ok := c.jobEntryID[jc.Name]; ok {
		c.cron.Remove(old)
		delete(c.jobEntryID, jc.Name)
	}
	jc.Valid = true
	c.jobConfig[jc.Name] = jc
	if jc.IsEnabled {
		c.jobEntryID[jc.Name] = entryID
	}
	c.logger.Info("[CronJobManager][ReRegisterJob] Job re-registered successfully",
		zap.String("job", jc.Name),
		zap.String("cron", jc.IntervalExpression),
		zap.Duration("timeout", jc.Timeout),
		zap.Bool("isEnabled", jc.IsEnabled))
	return nil
}

func (c *CronJobManager) Reload(ctx context.Context) error {
	configs, err := c.schedulerRepo.ListSchedulersConfig(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, cfg := range configs {
		if !cfg.Valid {
			failures = append(failures, fmt.Errorf("invalid scheduler config"))
			continue
		}
		if err := c.ReRegisterJob(ctx, cfg); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
func (c *CronJobManager) StartReload(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := c.Reload(ctx); err != nil {
			c.logger.Warn("scheduler reload retained prior schedule")
		}
		timer := time.NewTimer(30 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
