package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/pkg/outbound"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/scheduler/internal/domain"
)

type OutboundClaim struct {
	Event      outbound.OutboundEvent
	Token      uuid.UUID
	Fence      int64
	LeaseUntil time.Time
	Attempts   int
	CreatedAt  time.Time
}
type OutboundWorkRepository interface {
	GetByConfigVersion(context.Context, uuid.UUID, int) (*outbound.OutboundEvent, error)
	ClaimOutbound(context.Context, int, time.Duration) ([]OutboundClaim, error)
	FinishOutbound(context.Context, OutboundClaim, bool, string, time.Time) (bool, error)
}

func (o *OutBoundEventRepo) GetByConfigVersion(ctx context.Context, id uuid.UUID, version int) (*outbound.OutboundEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return scanOutbound(o.q.QueryRow(ctx, `SELECT `+outboundColumns+` FROM outbound_events WHERE config_id=$1 AND config_version=$2`, id, version))
}
func (o *OutBoundEventRepo) ClaimOutbound(ctx context.Context, limit int, lease time.Duration) ([]OutboundClaim, error) {
	if limit < 1 || limit > 4 || lease < time.Second || lease > time.Minute {
		return nil, errors.New("invalid Scheduler claim bounds")
	}
	beginner, ok := o.q.(interface {
		Begin(context.Context) (pgx.Tx, error)
	})
	if !ok {
		return nil, errors.New("outbound work requires transaction owner")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackOutbound(tx)
	// Classify a bounded legacy page using its immutable payload and a unique
	// config/version fact. Retain the historical event UUID rather than inventing
	// a new identity. Unproven/conflicting rows receive an explicit diagnostic.
	legacyRows, err := tx.Query(ctx, `SELECT `+outboundColumns+` FROM outbound_events WHERE config_id IS NULL AND status='pending' AND last_error IS NULL ORDER BY created_at,id LIMIT 16 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return nil, err
	}
	var legacy []outbound.OutboundEvent
	for legacyRows.Next() {
		e, err := scanOutbound(legacyRows)
		if err != nil {
			legacyRows.Close()
			return nil, err
		}
		legacy = append(legacy, *e)
	}
	err = legacyRows.Err()
	legacyRows.Close()
	if err != nil {
		return nil, err
	}
	for _, e := range legacy {
		var cfg domain.SchedulerConfig
		var fields struct {
			Id                 *uuid.UUID
			Name               *string
			Version            *int64
			Timeout            *int64
			IntervalExpression *string
			IsEnabled          *bool
		}
		typed := json.Unmarshal(e.Payload, &fields) == nil && fields.Id != nil && fields.Name != nil && fields.Version != nil && fields.Timeout != nil && fields.IntervalExpression != nil && fields.IsEnabled != nil
		valid := typed && e.PublishAt == nil && e.Topic == topics.TopicSchedulerConfigEvent && e.EventType == topics.TypeSchedulerConfigChanged && json.Unmarshal(e.Payload, &cfg) == nil && cfg.Id != uuid.Nil && cfg.Name != "" && cfg.Version > 0 && cfg.Version < math.MaxInt32 && ValidateConfig(cfg) == nil
		var matching, unique bool
		if valid {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, fmt.Sprintf("scheduler-outbound:%s:%d", cfg.Id, cfg.Version)); err != nil {
				return nil, err
			}
			err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM scheduler_configs WHERE id=$1 AND name=$2 AND version>=$3),
			NOT EXISTS(SELECT 1 FROM outbound_events WHERE id<>$4 AND (config_id=$1 AND config_version=$3 OR (topic=$5 AND event_type=$6 AND payload->>'Id'=$1::text AND payload->>'Version'=$3::text)))`, cfg.Id, cfg.Name, cfg.Version, e.Id, e.Topic, e.EventType).Scan(&matching, &unique)
			if err != nil {
				return nil, err
			}
			valid = matching && unique
		}
		if valid {
			_, err = tx.Exec(ctx, `UPDATE outbound_events SET config_id=$2,config_version=$3,due_at=clock_timestamp() WHERE id=$1`, e.Id, cfg.Id, cfg.Version)
		} else {
			_, err = tx.Exec(ctx, `UPDATE outbound_events SET last_error='legacy_outbound_attention' WHERE id=$1`, e.Id)
		}
		if err != nil {
			return nil, err
		}
	}
	rows, err := tx.Query(ctx, `SELECT id FROM outbound_events WHERE config_id IS NOT NULL AND status='pending' AND due_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY due_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	claims := make([]OutboundClaim, 0, len(ids))
	for _, id := range ids {
		c := OutboundClaim{Token: uuid.New()}
		err := tx.QueryRow(ctx, `UPDATE outbound_events SET claim_token=$2,claim_fence=claim_fence+1,attempts=attempts+1,lease_until=clock_timestamp()+$3::double precision*interval '1 second'
 WHERE id=$1 RETURNING id,topic,event_type,status,published_at,payload,claim_fence,lease_until,attempts,created_at`, id, c.Token, lease.Seconds()).Scan(&c.Event.Id, &c.Event.Topic, &c.Event.EventType, &c.Event.Status, &c.Event.PublishAt, &c.Event.Payload, &c.Fence, &c.LeaseUntil, &c.Attempts, &c.CreatedAt)
		if err != nil {
			return nil, err
		}
		claims = append(claims, c)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claims, nil
}
func rollbackOutbound(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func (o *OutBoundEventRepo) FinishOutbound(ctx context.Context, c OutboundClaim, success bool, category string, next time.Time) (bool, error) {
	if c.Event.Id == uuid.Nil || c.Token == uuid.Nil || c.Fence < 1 || (!success && (category == "" || next.IsZero())) {
		return false, errors.New("invalid Scheduler result")
	}
	beginner, ok := o.q.(interface {
		Begin(context.Context) (pgx.Tx, error)
	})
	if !ok {
		return false, errors.New("outbound work requires transaction owner")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackOutbound(tx)
	var id uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM outbound_events WHERE id=$1 AND config_id IS NOT NULL FOR UPDATE`, c.Event.Id).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	var applied bool
	if err := tx.QueryRow(ctx, `SELECT claim_token IS NULL AND claim_fence=$2 AND (( $3 AND status='published' AND published_at IS NOT NULL) OR (NOT $3 AND status='pending' AND last_error=$4 AND due_at=$5)) FROM outbound_events WHERE id=$1`, id, c.Fence, success, category, next).Scan(&applied); err != nil {
		return false, err
	}
	if applied {
		return true, nil
	}
	tag, err := tx.Exec(ctx, `UPDATE outbound_events SET status=CASE WHEN $4 THEN 'published'::outbound_status ELSE status END,
 published_at=CASE WHEN $4 THEN clock_timestamp() ELSE published_at END,claim_token=NULL,lease_until=NULL,
 due_at=CASE WHEN $4 THEN due_at ELSE $6 END,last_error=CASE WHEN $4 THEN NULL ELSE $5 END
 WHERE id=$1 AND status='pending' AND config_id IS NOT NULL AND claim_token=$2 AND claim_fence=$3 AND lease_until>clock_timestamp()`, id, c.Token, c.Fence, success, category, next)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
