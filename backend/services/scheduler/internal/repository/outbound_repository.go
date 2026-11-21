package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/pkg/outbound"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/scheduler/internal/domain"
)

type OutBoundEventRepo struct{ q Querier }

func NewOutboundEventRepository(q Querier) *OutBoundEventRepo { return &OutBoundEventRepo{q: q} }
func (o *OutBoundEventRepo) Create(ctx context.Context, e outbound.OutboundEvent) error {
	if e.Id == uuid.Nil {
		return errors.New("explicit scheduler outbound identity required")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var cfg domain.SchedulerConfig
	if json.Unmarshal(e.Payload, &cfg) == nil && cfg.Id != uuid.Nil && cfg.Version > 0 && e.Topic == topics.TopicSchedulerConfigEvent && e.EventType == topics.TypeSchedulerConfigChanged {
		if err := ValidateConfig(cfg); err != nil {
			return err
		}
		stable := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("ticketbox:scheduler:%s:%d", cfg.Id, cfg.Version)))
		if stable != e.Id {
			return errors.New("outbound config identity mismatch")
		}
		_, err := o.q.Exec(ctx, `INSERT INTO outbound_events(id,topic,event_type,status,payload,created_at,config_id,config_version,due_at) VALUES($1,$2,$3,$4,$5,clock_timestamp(),$6,$7,clock_timestamp())`, e.Id, e.Topic, e.EventType, e.Status, e.Payload, cfg.Id, cfg.Version)
		return err
	}
	_, err := o.q.Exec(ctx, `INSERT INTO outbound_events(id,topic,event_type,status,payload,created_at) VALUES($1,$2,$3,$4,$5,clock_timestamp())`, e.Id, e.Topic, e.EventType, e.Status, e.Payload)
	return err
}
func scanOutbound(row pgx.Row) (*outbound.OutboundEvent, error) {
	e := &outbound.OutboundEvent{}
	var status *string
	if err := row.Scan(&e.Id, &e.Topic, &e.EventType, &status, &e.PublishAt, &e.Payload); err != nil {
		return nil, err
	}
	if status != nil {
		e.Status = outbound.OutboundStatus(*status)
	}
	return e, nil
}

const outboundColumns = `id,topic,event_type,status,published_at,payload`

func (o *OutBoundEventRepo) GetByID(ctx context.Context, id uuid.UUID) (*outbound.OutboundEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return scanOutbound(o.q.QueryRow(ctx, `SELECT `+outboundColumns+` FROM outbound_events WHERE id=$1`, id))
}
func (o *OutBoundEventRepo) GetListOutboundPending(ctx context.Context, limit, offset int) ([]outbound.OutboundEvent, error) {
	if limit < 1 || limit > 1000 || offset < 0 {
		return nil, errors.New("invalid outbound page")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := o.q.Query(ctx, `SELECT `+outboundColumns+` FROM outbound_events WHERE status=$1 AND config_id IS NULL ORDER BY created_at,id LIMIT $2 OFFSET $3`, string(outbound.OutboundStatusPending), limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []outbound.OutboundEvent{}
	for rows.Next() {
		e, err := scanOutbound(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, *e)
	}
	return events, rows.Err()
}
func (o *OutBoundEventRepo) MarkEventAsPublished(ctx context.Context, id uuid.UUID) error {
	return o.MarkEventsAsPublished(ctx, []uuid.UUID{id})
}
func (o *OutBoundEventRepo) MarkEventsAsPublished(ctx context.Context, ids []uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var managed bool
	if err := o.q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM outbound_events WHERE id=ANY($1) AND config_id IS NOT NULL)`, ids).Scan(&managed); err != nil {
		return err
	}
	if managed {
		return errors.New("managed outbound requires fenced result")
	}
	_, err := o.q.Exec(ctx, `UPDATE outbound_events SET published_at=clock_timestamp(),status=$2 WHERE id=ANY($1) AND config_id IS NULL AND status='pending'`, ids, string(outbound.OutboundStatusPublished))
	return err
}
