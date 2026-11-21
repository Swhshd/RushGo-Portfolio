package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/payment/internal/domain"
)

const refundAuditColumns = `record_id,schema_version,payment_id,booking_id,generation,command_digest,observation_digest,
	application_attempt_id,record_kind,refund_decision_id,provider,intended_account,intended_livemode,scope_source,
	target_amount_cents,currency,payment_intent_id,idempotency_key_digest,claim_token_digest,claim_fence,
	provider_request_id,provider_refund_id,provider_event_id,observed_at,payload,recorded_at`

type refundAuditDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type refundPreSendTx interface {
	refundAuditDB
	Commit(context.Context) error
	Rollback(context.Context) error
}

// AppendRefundAudit inserts one immutable typed record. Replays are accepted
// only when stable content matches; local/database observation times do not
// make an otherwise identical retry conflict.
func (p *PostgresPaymentRepository) AppendRefundAudit(ctx context.Context, record domain.RefundAuditRecord) error {
	return appendRefundAuditRecord(ctx, p.pool, record)
}

func appendRefundAuditRecord(ctx context.Context, db refundAuditDB, record domain.RefundAuditRecord) error {
	if err := domain.ValidateRefundAuditRecord(record); err != nil {
		return err
	}
	payload, err := refundAuditPayload(record)
	if err != nil {
		return err
	}
	tag, err := db.Exec(ctx, `INSERT INTO payment_refund_audit(
		record_id,schema_version,payment_id,booking_id,generation,command_digest,observation_digest,application_attempt_id,record_kind,
		refund_decision_id,provider,intended_account,intended_livemode,scope_source,target_amount_cents,currency,
		payment_intent_id,idempotency_key_digest,claim_token_digest,claim_fence,provider_request_id,provider_refund_id,
		provider_event_id,observed_at,payload)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULLIF($11,''),NULLIF($12,''),$13,NULLIF($14,''),$15,NULLIF($16,''),
		NULLIF($17,''),$18,$19,$20,NULLIF($21,''),NULLIF($22,''),NULLIF($23,''),$24,$25)
		ON CONFLICT(record_id) DO NOTHING`,
		record.RecordID, record.Version, nullableUUID(record.PaymentID), nullableUUID(record.BookingID), nullableInt64(record.Generation),
		nullableDigest(record.CommandDigest), nullableDigest(record.ObservationDigest), nullableUUID(record.AttemptID), string(record.Kind), nullableUUID(record.RefundDecisionID),
		record.Provider, record.IntendedAccount, record.IntendedLiveMode, record.ScopeSource, record.TargetAmountCents, record.Currency,
		record.PaymentIntentID, nullableDigest(record.IdempotencyKeyDigest), nullableDigest(record.ClaimTokenDigest), nullableInt64(record.ClaimFence),
		record.ProviderRequestID, record.ProviderRefundID, record.ProviderEventID, record.ObservedAt, payload)
	if err != nil {
		return wrapRefundAuditStorageError("append", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	stored, err := scanRefundAuditRecord(db.QueryRow(ctx, `SELECT `+refundAuditColumns+` FROM payment_refund_audit WHERE record_id=$1`, record.RecordID))
	if err != nil {
		return wrapRefundAuditStorageError("read back", err)
	}
	storedStable, err := domain.RefundAuditStableContent(*stored)
	if err != nil {
		return fmt.Errorf("compare stored refund audit record: %w", err)
	}
	wantStable, err := domain.RefundAuditStableContent(record)
	if err != nil {
		return fmt.Errorf("compare new refund audit record: %w", err)
	}
	if !bytes.Equal(storedStable, wantStable) {
		return fmt.Errorf("%w: record identity %s", domain.ErrRefundAuditConflict, record.RecordID)
	}
	return nil
}

// ListRefundAudit is a bounded read-only query over indexed identity selectors.
// Errors are classified as AUDIT_UNAVAILABLE by callers; an empty successful
// result is the distinct LEGACY_UNVERIFIED case.
func (p *PostgresPaymentRepository) ListRefundAudit(ctx context.Context, query domain.RefundAuditQuery) ([]domain.RefundAuditRecord, error) {
	if err := query.Validate(); err != nil {
		return nil, err
	}
	selectors := make([]string, 0, 7)
	args := make([]any, 0, 10)
	add := func(clause string, value any) {
		args = append(args, value)
		selectors = append(selectors, strings.Replace(clause, "?", fmt.Sprintf("$%d", len(args)), 1))
	}
	if query.PaymentID != uuid.Nil {
		add("payment_id=?", query.PaymentID)
	}
	if query.BookingID != uuid.Nil && query.Generation > 0 {
		args = append(args, query.BookingID, query.Generation)
		selectors = append(selectors, fmt.Sprintf("(booking_id=$%d AND generation=$%d)", len(args)-1, len(args)))
	}
	if query.CommandDigest != ([32]byte{}) {
		add("command_digest=?", query.CommandDigest[:])
	}
	if query.AttemptID != uuid.Nil {
		add("application_attempt_id=?", query.AttemptID)
	}
	if query.ProviderRequestID != "" {
		add("provider_request_id=?", query.ProviderRequestID)
	}
	if query.ProviderRefundID != "" {
		add("provider_refund_id=?", query.ProviderRefundID)
	}
	if query.ProviderEventID != "" {
		add("provider_event_id=?", query.ProviderEventID)
	}
	if len(selectors) == 0 {
		return nil, domain.ErrInvalidRefundAuditRecord
	}
	where := strings.Join(selectors, " AND ")
	if query.Kind != "" {
		args = append(args, string(query.Kind))
		where += fmt.Sprintf(" AND record_kind=$%d", len(args))
	}
	limit := query.Limit
	if limit == 0 {
		limit = 200
	}
	args = append(args, limit)
	rows, err := p.pool.Query(ctx, `SELECT `+refundAuditColumns+` FROM payment_refund_audit WHERE `+where+
		fmt.Sprintf(" ORDER BY recorded_at DESC,record_id LIMIT $%d", len(args)), args...)
	if err != nil {
		return nil, wrapRefundAuditStorageError("query", err)
	}
	defer rows.Close()
	records := make([]domain.RefundAuditRecord, 0)
	for rows.Next() {
		record, err := scanRefundAuditRecord(rows)
		if err != nil {
			return nil, wrapRefundAuditStorageError("scan", err)
		}
		records = append(records, *record)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapRefundAuditStorageError("iterate", err)
	}
	return records, nil
}

func refundAuditPayload(record domain.RefundAuditRecord) ([]byte, error) {
	switch record.Kind {
	case domain.RefundAuditKindPrepared:
		return json.Marshal(record.Prepared)
	case domain.RefundAuditKindResponse:
		return json.Marshal(record.Response)
	case domain.RefundAuditKindVerifiedEvent:
		return json.Marshal(record.VerifiedEvent)
	default:
		return nil, domain.ErrInvalidRefundAuditRecord
	}
}

func scanRefundAuditRecord(row pgx.Row) (*domain.RefundAuditRecord, error) {
	var record domain.RefundAuditRecord
	var paymentID, bookingID, attemptID, decisionID *uuid.UUID
	var generation, claimFence *int64
	var commandDigest, observationDigest, keyDigest, tokenDigest []byte
	var provider, account, scopeSource, currency, intentID, requestID, refundID, eventID *string
	var intendedMode *bool
	var targetAmount *int64
	var observedAt *time.Time
	var kind string
	var payload []byte
	if err := row.Scan(
		&record.RecordID, &record.Version, &paymentID, &bookingID, &generation, &commandDigest, &observationDigest, &attemptID, &kind,
		&decisionID, &provider, &account, &intendedMode, &scopeSource, &targetAmount, &currency, &intentID,
		&keyDigest, &tokenDigest, &claimFence, &requestID, &refundID, &eventID, &observedAt, &payload, &record.RecordedAt,
	); err != nil {
		return nil, err
	}
	if paymentID != nil {
		record.PaymentID = *paymentID
	}
	if bookingID != nil {
		record.BookingID = *bookingID
	}
	if generation != nil {
		record.Generation = *generation
	}
	if err := copyAuditDigest(&record.CommandDigest, commandDigest); err != nil {
		return nil, err
	}
	if err := copyAuditDigest(&record.ObservationDigest, observationDigest); err != nil {
		return nil, err
	}
	if attemptID != nil {
		record.AttemptID = *attemptID
	}
	if decisionID != nil {
		record.RefundDecisionID = *decisionID
	}
	record.Kind = domain.RefundAuditKind(kind)
	if provider != nil {
		record.Provider = *provider
	}
	if account != nil {
		record.IntendedAccount = *account
	}
	record.IntendedLiveMode = intendedMode
	if scopeSource != nil {
		record.ScopeSource = *scopeSource
	}
	record.TargetAmountCents = targetAmount
	if currency != nil {
		record.Currency = *currency
	}
	if intentID != nil {
		record.PaymentIntentID = *intentID
	}
	if err := copyAuditDigest(&record.IdempotencyKeyDigest, keyDigest); err != nil {
		return nil, err
	}
	if err := copyAuditDigest(&record.ClaimTokenDigest, tokenDigest); err != nil {
		return nil, err
	}
	if claimFence != nil {
		record.ClaimFence = *claimFence
	}
	if requestID != nil {
		record.ProviderRequestID = *requestID
	}
	if refundID != nil {
		record.ProviderRefundID = *refundID
	}
	if eventID != nil {
		record.ProviderEventID = *eventID
	}
	record.ObservedAt = observedAt
	if err := decodeRefundAuditPayload(record.Kind, payload, &record); err != nil {
		return nil, err
	}
	if err := domain.ValidateRefundAuditRecord(record); err != nil {
		return nil, fmt.Errorf("invalid stored refund audit record: %w", err)
	}
	return &record, nil
}

func decodeRefundAuditPayload(kind domain.RefundAuditKind, payload []byte, record *domain.RefundAuditRecord) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	switch kind {
	case domain.RefundAuditKindPrepared:
		record.Prepared = &domain.RefundAuditPrepared{}
		if err := decoder.Decode(record.Prepared); err != nil {
			return fmt.Errorf("decode prepared audit payload: %w", err)
		}
	case domain.RefundAuditKindResponse:
		record.Response = &domain.RefundAuditResponse{}
		if err := decoder.Decode(record.Response); err != nil {
			return fmt.Errorf("decode response audit payload: %w", err)
		}
	case domain.RefundAuditKindVerifiedEvent:
		record.VerifiedEvent = &domain.VerifiedRefundEventAudit{}
		if err := decoder.Decode(record.VerifiedEvent); err != nil {
			return fmt.Errorf("decode verified event audit payload: %w", err)
		}
	default:
		return domain.ErrInvalidRefundAuditRecord
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("invalid trailing refund audit payload data")
	}
	return nil
}

func copyAuditDigest(target *[32]byte, source []byte) error {
	if len(source) == 0 {
		return nil
	}
	if len(source) != len(target) {
		return fmt.Errorf("invalid refund audit digest length")
	}
	copy(target[:], source)
	return nil
}

func nullableDigest(digest [32]byte) []byte {
	if digest == ([32]byte{}) {
		return nil
	}
	return digest[:]
}

func nullableUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}

func nullableInt64(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

func wrapRefundAuditStorageError(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
		return fmt.Errorf("%w: %s", domain.ErrRefundAuditUnavailable, operation)
	}
	return fmt.Errorf("refund audit %s: %w", operation, err)
}
