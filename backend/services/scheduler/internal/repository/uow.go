package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UnitOfWorkRepository interface {
	OutboundRepo() OutboundEventRepository
	SchedulerConfigRepo() SchedulerRepository
}

type UnitOfWork interface {
	Execute(ctx context.Context, fn func(repos *UowRepos) error) error
}

type UnitOfWorkImpl struct {
	pool *pgxpool.Pool
}

func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWorkImpl {
	return &UnitOfWorkImpl{
		pool: pool,
	}
}

func (u *UnitOfWorkImpl) Execute(ctx context.Context, fn func(repos *UowRepos) error) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()

	repos := &UowRepos{
		schedulerConfigRepo: NewSchedulerConfigRepo(tx),
		outboundEventRepo:   NewOutboundEventRepository(tx),
	}

	err = fn(repos)
	if err != nil {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if rollbackErr := tx.Rollback(cleanup); rollbackErr != nil {
			return errors.Join(err, fmt.Errorf("scheduler rollback: %w", rollbackErr))
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		var pgErr *pgconn.PgError
		if errors.Is(err, pgx.ErrTxCommitRollback) || errors.As(err, &pgErr) {
			return fmt.Errorf("scheduler commit rolled back: %w", err)
		}
		return errors.Join(ErrCommitUnknown, err)
	}
	return nil
}

var ErrCommitUnknown = errors.New("scheduler commit outcome unknown; reread config and outbound identity")

type UowRepos struct {
	schedulerConfigRepo *SchedulerConfigRepo
	outboundEventRepo   *OutBoundEventRepo
}

func (u *UowRepos) OutboundRepo() OutboundEventRepository {
	return u.outboundEventRepo
}

func (u *UowRepos) SchedulerConfigRepo() SchedulerRepository {
	return u.schedulerConfigRepo
}
