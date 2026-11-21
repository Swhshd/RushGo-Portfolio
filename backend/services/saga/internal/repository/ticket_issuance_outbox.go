package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/saga/internal/domain"
)

func insertTicketIssuanceOutbox(ctx context.Context, tx pgx.Tx, saga *domain.Saga) error {
	if saga == nil || !saga.RecoveryManaged || saga.Input == nil || saga.ID == uuid.Nil || saga.BookingID == uuid.Nil ||
		saga.Input.BookingID != saga.BookingID || saga.Input.EventID == uuid.Nil || saga.Generation != 1 || saga.PaymentID == uuid.Nil {
		return ErrRecoveryAttention
	}
	issuanceID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:saga:"+saga.ID.String()+":ticket-issuance:v1"))
	event := domain.TicketIssuanceRequestedV1{
		Version: 1, TicketIssuanceID: issuanceID, SagaID: saga.ID, BookingID: saga.BookingID,
		PaymentID: saga.PaymentID, EventID: saga.Input.EventID, Generation: saga.Generation,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO saga_ticket_issuance_outbox(id,saga_id,booking_id,payment_id,event_id,lifecycle_generation,event_type,event_key,payload)
	 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(saga_id) DO NOTHING`, issuanceID, saga.ID, saga.BookingID, saga.PaymentID, saga.Input.EventID, saga.Generation, topics.TypeTicketIssuanceRequestedV1, saga.BookingID.String(), payload); err != nil {
		return err
	}
	var storedID uuid.UUID
	var storedType string
	var storedPayload []byte
	if err := tx.QueryRow(ctx, `SELECT id,event_type,payload FROM saga_ticket_issuance_outbox WHERE saga_id=$1`, saga.ID).Scan(&storedID, &storedType, &storedPayload); err != nil {
		return err
	}
	var storedEvent domain.TicketIssuanceRequestedV1
	decoder := json.NewDecoder(bytes.NewReader(storedPayload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&storedEvent); err != nil {
		return fmt.Errorf("decode stored Ticket issuance outbox payload: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("stored Ticket issuance outbox payload contains trailing data")
	}
	if storedID != issuanceID || storedType != topics.TypeTicketIssuanceRequestedV1 || storedEvent != event {
		return ErrRecoveryConflict
	}
	return nil
}

func (r *SagaRepository) ClaimTicketIssuanceOutbox(ctx context.Context, limit int, lease time.Duration) ([]domain.TicketIssuanceOutboxEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if limit < 1 || limit > 100 || lease < time.Second || lease > time.Minute {
		return nil, ErrInvalidRecoveryInput
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `SELECT id,saga_id,booking_id,event_type,event_key,payload,created_at,attempts
	 FROM saga_ticket_issuance_outbox WHERE published_at IS NULL AND next_attempt_at<=clock_timestamp()
	 AND (claim_until IS NULL OR claim_until<=clock_timestamp()) ORDER BY next_attempt_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	var events []domain.TicketIssuanceOutboxEvent
	for rows.Next() {
		var event domain.TicketIssuanceOutboxEvent
		if err := rows.Scan(&event.ID, &event.SagaID, &event.BookingID, &event.EventType, &event.EventKey, &event.Payload, &event.CreatedAt, &event.Attempts); err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for i := range events {
		events[i].ClaimToken = uuid.New()
		if _, err := tx.Exec(ctx, `UPDATE saga_ticket_issuance_outbox SET claim_token=$2,claim_until=clock_timestamp()+$3*interval '1 second',attempts=attempts+1 WHERE id=$1`, events[i].ID, events[i].ClaimToken, lease.Seconds()); err != nil {
			return nil, err
		}
		events[i].Attempts++
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("ticket issuance outbox claim commit outcome unknown: %w", err)
	}
	return events, nil
}

func (r *SagaRepository) CompleteTicketIssuanceOutbox(ctx context.Context, id, token uuid.UUID) (bool, error) {
	if id == uuid.Nil || token == uuid.Nil {
		return false, ErrInvalidRecoveryInput
	}
	tag, err := r.pool.Exec(ctx, `UPDATE saga_ticket_issuance_outbox SET published_at=clock_timestamp(),claim_token=NULL,claim_until=NULL,last_error_category=NULL
	 WHERE id=$1 AND claim_token=$2 AND claim_until>clock_timestamp() AND published_at IS NULL`, id, token)
	return err == nil && tag.RowsAffected() == 1, err
}

func (r *SagaRepository) RetryTicketIssuanceOutbox(ctx context.Context, id, token uuid.UUID, next time.Time) (bool, error) {
	if id == uuid.Nil || token == uuid.Nil || next.IsZero() {
		return false, ErrInvalidRecoveryInput
	}
	tag, err := r.pool.Exec(ctx, `UPDATE saga_ticket_issuance_outbox SET next_attempt_at=$3,claim_token=NULL,claim_until=NULL,last_error_category='publish_unknown'
	 WHERE id=$1 AND claim_token=$2 AND claim_until>clock_timestamp() AND published_at IS NULL`, id, token, next.UTC())
	return err == nil && tag.RowsAffected() == 1, err
}

var _ TicketIssuanceOutboxRepository = (*SagaRepository)(nil)
