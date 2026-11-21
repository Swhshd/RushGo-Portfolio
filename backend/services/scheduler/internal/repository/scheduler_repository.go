package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	cron "github.com/robfig/cron/v3"
	"github.com/ticketbox/scheduler/internal/domain"
)

type SchedulerConfigRepo struct{ q Querier }

func NewSchedulerConfigRepo(q Querier) *SchedulerConfigRepo { return &SchedulerConfigRepo{q: q} }

var ErrConfigConflict = errors.New("scheduler config version conflict")
var ErrConfigNotFound = errors.New("scheduler config not found")

const configColumns = `id,name,job_type,timeout,version,interval_expression,is_enable,created_at,updated_at`

var schedulerNamePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,99}$`)

func ValidateCreateConfig(c domain.SchedulerConfig) error {
	if c.Name != strings.TrimSpace(c.Name) || !schedulerNamePattern.MatchString(c.Name) {
		return errors.New("invalid scheduler job name")
	}
	if c.JobType != domain.JobTypeReservationCleaner {
		return errors.New("unsupported scheduler job type")
	}
	if c.Timeout < time.Second || c.Timeout > 600*time.Second || c.Timeout%time.Second != 0 {
		return errors.New("invalid scheduler timeout")
	}
	if err := ValidateConfig(c); err != nil {
		return err
	}
	return nil
}

func ValidateConfig(c domain.SchedulerConfig) error {
	if c.Timeout <= 0 || c.Timeout%time.Second != 0 || c.Timeout/time.Second > math.MaxInt32 {
		return errors.New("invalid scheduler timeout")
	}
	if _, err := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(c.IntervalExpression); err != nil {
		return errors.New("invalid scheduler expression")
	}
	return nil
}
func scanConfig(row pgx.Row) (*domain.SchedulerConfig, error) {
	c := &domain.SchedulerConfig{}
	var timeout, version *int32
	var enabled *bool
	var jobType string
	err := row.Scan(&c.Id, &c.Name, &jobType, &timeout, &version, &c.IntervalExpression, &enabled, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrConfigNotFound
		}
		return nil, err
	}
	c.JobType = domain.JobType(jobType)
	if timeout != nil {
		c.Timeout = time.Duration(*timeout) * time.Second
	}
	if version != nil {
		c.Version = int(*version)
	}
	if enabled != nil {
		c.IsEnabled = *enabled
	}
	if timeout == nil || version == nil || enabled == nil {
		c.ValidationError = "nullable legacy scheduler config"
		return c, nil
	}
	if c.Version < 1 || c.Version == math.MaxInt32 || !schedulerNamePattern.MatchString(c.Name) || c.JobType != domain.JobTypeReservationCleaner {
		c.ValidationError = "invalid scheduler identity/version"
		return c, nil
	}
	if err := ValidateConfig(*c); err != nil {
		c.ValidationError = err.Error()
		return c, nil
	}
	c.Valid = true
	return c, nil
}

// CreateIfAbsent uses the existing unique name constraint as the stable
// idempotency identity. A retry with the same initial config returns the
// persisted row; reusing a name for different settings is a conflict.
func (s *SchedulerConfigRepo) CreateIfAbsent(ctx context.Context, c domain.SchedulerConfig) (*domain.SchedulerConfig, bool, error) {
	if c.Id == uuid.Nil || c.Version != 1 || c.IsEnabled {
		return nil, false, errors.New("invalid initial scheduler config")
	}
	if err := ValidateCreateConfig(c); err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	created, err := scanConfig(s.q.QueryRow(ctx, `INSERT INTO scheduler_configs(id,name,job_type,timeout,version,interval_expression,is_enable)
		VALUES($1,$2,$3,$4,$5,$6,false) ON CONFLICT(name) DO NOTHING RETURNING `+configColumns,
		c.Id, c.Name, string(c.JobType), int32(c.Timeout/time.Second), int32(c.Version), c.IntervalExpression))
	if err == nil {
		return created, true, nil
	}
	if !errors.Is(err, ErrConfigNotFound) {
		return nil, false, err
	}
	existing, err := scanConfig(s.q.QueryRow(ctx, `SELECT `+configColumns+` FROM scheduler_configs WHERE name=$1`, c.Name))
	if err != nil {
		return nil, false, err
	}
	if !existing.Valid || existing.Id != c.Id || existing.JobType != c.JobType || existing.Version != 1 || existing.IsEnabled ||
		existing.Timeout != c.Timeout || existing.IntervalExpression != c.IntervalExpression {
		return nil, false, ErrConfigConflict
	}
	return existing, false, nil
}

func (s *SchedulerConfigRepo) GetById(ctx context.Context, id uuid.UUID) (*domain.SchedulerConfig, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return scanConfig(s.q.QueryRow(ctx, `SELECT `+configColumns+` FROM scheduler_configs WHERE id=$1`, id))
}
func (s *SchedulerConfigRepo) UpdateById(ctx context.Context, id uuid.UUID, target domain.SchedulerConfig) (*domain.SchedulerConfig, error) {
	c, err := s.GetById(ctx, id)
	if err != nil {
		return nil, err
	}
	if !c.Valid {
		return nil, errors.New(c.ValidationError)
	}
	return s.UpdateByVersion(ctx, id, c.Version, target)
}
func (s *SchedulerConfigRepo) UpdateByVersion(ctx context.Context, id uuid.UUID, expected int, target domain.SchedulerConfig) (*domain.SchedulerConfig, error) {
	if id == uuid.Nil || expected < 1 || expected >= math.MaxInt32-1 {
		return nil, ErrConfigConflict
	}
	if err := ValidateConfig(target); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	c, err := scanConfig(s.q.QueryRow(ctx, `UPDATE scheduler_configs SET is_enable=$2,interval_expression=$3,timeout=$4,updated_at=clock_timestamp(),version=version+1 WHERE id=$1 AND version=$5 RETURNING `+configColumns, id, target.IsEnabled, target.IntervalExpression, int32(target.Timeout/time.Second), expected))
	if errors.Is(err, ErrConfigNotFound) {
		return nil, ErrConfigConflict
	}
	return c, err
}
func (s *SchedulerConfigRepo) ListSchedulersConfig(ctx context.Context) ([]domain.SchedulerConfig, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := s.q.Query(ctx, `SELECT `+configColumns+` FROM scheduler_configs ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	configs := []domain.SchedulerConfig{}
	for rows.Next() {
		c, err := scanConfig(rows)
		if err != nil {
			return nil, fmt.Errorf("read scheduler config: %w", err)
		}
		configs = append(configs, *c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return configs, nil
}
