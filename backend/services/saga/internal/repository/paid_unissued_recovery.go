package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/saga/internal/domain"
)

var ErrPaidUnissuedRecoveryConflict = errors.New("paid-unissued recovery state conflict")

type PaidUnissuedSagaState struct {
	Saga              *domain.Saga
	OutboxCount       int
	OutboxPublished   int
	OutboxPaymentID   *uuid.UUID
	OutboxEventID     *uuid.UUID
	OutboxPublishedAt *time.Time
	OutboxClaimActive bool
}

type PaidUnissuedRecoveryCommand struct {
	SagaID, BookingID, OperationKey uuid.UUID
	Operator, Reason                string
	ExpectedProviderAccount         string
	Mode                            string
	Eligible                        bool
	ResultCode                      string
	PaymentID                       *uuid.UUID
	PaymentIntentID                 string
	Expected                        PaidUnissuedSagaState
	AuthoritySnapshot               json.RawMessage
}

type PaidUnissuedRecoveryAuditResult struct {
	AuditID    uuid.UUID
	ResultCode string
	Eligible   bool
	Requeued   bool
	Replayed   bool
}

type PaidUnissuedRecoveryRepository interface {
	GetPaidUnissuedSagaState(context.Context, uuid.UUID) (*PaidUnissuedSagaState, error)
	LookupPaidUnissuedRecoveryAudit(context.Context, PaidUnissuedRecoveryCommand) (*PaidUnissuedRecoveryAuditResult, error)
	RecordPaidUnissuedRecovery(context.Context, PaidUnissuedRecoveryCommand) (*PaidUnissuedRecoveryAuditResult, error)
}

type paidUnissuedLocalSnapshot struct {
	SagaID           string `json:"saga_id"`
	BookingID        string `json:"booking_id"`
	Status           string `json:"status"`
	RecoveryState    string `json:"recovery_state"`
	Direction        string `json:"direction"`
	CurrentStepIndex int    `json:"current_step_index"`
	ErrorCategory    string `json:"error_category"`
	PaymentID        string `json:"payment_id,omitempty"`
	PaymentIntentID  string `json:"payment_intent_id,omitempty"`
	ClaimFence       int64  `json:"claim_fence"`
	ClaimActive      bool   `json:"claim_active"`
	OutboxCount      int    `json:"outbox_count"`
	OutboxPublished  int    `json:"outbox_published"`
	FailureAttempts  int    `json:"failure_attempts"`
}

func (r *SagaRepository) GetPaidUnissuedSagaState(ctx context.Context, id uuid.UUID) (*PaidUnissuedSagaState, error) {
	if id == uuid.Nil {
		return nil, ErrInvalidRecoveryInput
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	v, err := loadSaga(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	state := &PaidUnissuedSagaState{Saga: v}
	var paymentIDText, eventIDText *string
	var publishedAt *time.Time
	var active bool
	if err := tx.QueryRow(ctx, `SELECT count(*)::int, count(*) FILTER (WHERE published_at IS NOT NULL)::int,
		(SELECT payment_id::text FROM saga_ticket_issuance_outbox WHERE saga_id=$1 ORDER BY created_at,id LIMIT 1),
		(SELECT id::text FROM saga_ticket_issuance_outbox WHERE saga_id=$1 ORDER BY created_at,id LIMIT 1),
		(SELECT published_at FROM saga_ticket_issuance_outbox WHERE saga_id=$1 ORDER BY created_at,id LIMIT 1),
		COALESCE(bool_or(claim_until > clock_timestamp()), false)
		FROM saga_ticket_issuance_outbox WHERE saga_id=$1`, id).Scan(&state.OutboxCount, &state.OutboxPublished, &paymentIDText, &eventIDText, &publishedAt, &active); err != nil {
		return nil, err
	}
	if err := parsePaidUnissuedOutboxIdentity(paymentIDText, eventIDText, publishedAt, state); err != nil {
		return nil, err
	}
	state.OutboxClaimActive = active
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit paid-unissued Saga read: %w", err)
	}
	return state, nil
}

func (r *SagaRepository) LookupPaidUnissuedRecoveryAudit(ctx context.Context, command PaidUnissuedRecoveryCommand) (*PaidUnissuedRecoveryAuditResult, error) {
	if err := validatePaidUnissuedCommand(command); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	fingerprint := paidUnissuedRequestFingerprint(command)
	var result PaidUnissuedRecoveryAuditResult
	var storedFingerprint []byte
	err := r.pool.QueryRow(ctx, `SELECT id,result_code,eligible,requeued,request_fingerprint
		FROM saga_paid_unissued_recovery_audit WHERE operation_key=$1 AND operation_mode=$2`, command.OperationKey, command.Mode).
		Scan(&result.AuditID, &result.ResultCode, &result.Eligible, &result.Requeued, &storedFingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if string(storedFingerprint) != string(fingerprint[:]) {
		return nil, ErrPaidUnissuedRecoveryConflict
	}
	result.Replayed = true
	return &result, nil
}

func (r *SagaRepository) RecordPaidUnissuedRecovery(ctx context.Context, command PaidUnissuedRecoveryCommand) (*PaidUnissuedRecoveryAuditResult, error) {
	if err := validatePaidUnissuedCommand(command); err != nil {
		return nil, err
	}
	if len(command.AuthoritySnapshot) == 0 || !json.Valid(command.AuthoritySnapshot) || command.AuthoritySnapshot[0] != '{' {
		return nil, ErrInvalidRecoveryInput
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	fingerprint := paidUnissuedRequestFingerprint(command)

	if replay, err := paidUnissuedAuditByKey(ctx, tx, command, fingerprint); err != nil || replay != nil {
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit paid-unissued replay read: %w", err)
		}
		return replay, nil
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, command.BookingID.String()); err != nil {
		return nil, err
	}
	var lockedID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM sagas WHERE id=$1 FOR UPDATE`, command.SagaID).Scan(&lockedID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	// Recheck after the row lock: a concurrent duplicate operation may have
	// committed its audit while this transaction waited.
	if replay, err := paidUnissuedAuditByKey(ctx, tx, command, fingerprint); err != nil || replay != nil {
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit paid-unissued replay read: %w", err)
		}
		return replay, nil
	}

	state, err := loadPaidUnissuedSagaState(ctx, tx, lockedID)
	if err != nil {
		return nil, err
	}
	before := paidUnissuedSnapshot(state, time.Now())
	after := before
	resultCode, eligible, requeued := command.ResultCode, command.Eligible, false
	if eligible {
		if !paidUnissuedExpectedStateMatches(command.Expected, *state) {
			resultCode, eligible = "state_changed_during_qualification", false
		} else if err := validatePaidUnissuedAttentionSaga(state, command); err != nil {
			resultCode, eligible = "recovery_state_not_eligible", false
		} else if command.Mode != "execute" {
			resultCode = "dry_run_eligible"
			after = before
		} else {
			paymentID := *command.PaymentID
			tag, err := tx.Exec(ctx, `UPDATE sagas
				SET recovery_state='ready', error_category=NULL, next_attempt_at=clock_timestamp(),
					payment_id=COALESCE(payment_id,$2), payment_intent_id=COALESCE(NULLIF(payment_intent_id,''),NULLIF($3,'')),
					claim_token=NULL, lease_until=NULL, claim_fence=claim_fence+1
				WHERE id=$1 AND recovery_managed AND recovery_state='attention'
					AND (lease_until IS NULL OR lease_until <= clock_timestamp())
					AND (payment_id IS NULL OR payment_id=$2)
					AND (payment_intent_id IS NULL OR payment_intent_id='' OR payment_intent_id=$3)`,
				command.SagaID, paymentID, command.PaymentIntentID)
			if err != nil {
				return nil, err
			}
			if tag.RowsAffected() != 1 {
				resultCode, eligible = "state_changed_during_requeue", false
			} else {
				requeued = true
				state, err = loadPaidUnissuedSagaState(ctx, tx, lockedID)
				if err != nil {
					return nil, err
				}
				after = paidUnissuedSnapshot(state, time.Now())
				resultCode = "requeued"
			}
		}
	}
	if !eligible && resultCode == "" {
		resultCode = "not_eligible"
	}

	beforeJSON, err := json.Marshal(before)
	if err != nil {
		return nil, err
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		return nil, err
	}
	var payment any
	if command.PaymentID != nil {
		payment = *command.PaymentID
	}
	result := &PaidUnissuedRecoveryAuditResult{AuditID: uuid.New(), ResultCode: resultCode, Eligible: eligible, Requeued: requeued}
	_, err = tx.Exec(ctx, `INSERT INTO saga_paid_unissued_recovery_audit
		(id,operation_key,operation_mode,saga_id,booking_id,operator_identity,reason,expected_provider_account,payment_id,result_code,eligible,requeued,request_fingerprint,state_before,state_after,authority_snapshot)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15::jsonb,$16::jsonb)`,
		result.AuditID, command.OperationKey, command.Mode, command.SagaID, command.BookingID, command.Operator, command.Reason,
		command.ExpectedProviderAccount, payment, result.ResultCode, result.Eligible, result.Requeued, fingerprint[:],
		string(beforeJSON), string(afterJSON), string(command.AuthoritySnapshot))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("paid-unissued recovery audit/requeue commit outcome unknown: %w", err)
	}
	return result, nil
}

func loadPaidUnissuedSagaState(ctx context.Context, q sagaQuerier, id uuid.UUID) (*PaidUnissuedSagaState, error) {
	v, err := loadSaga(ctx, q, id)
	if err != nil {
		return nil, err
	}
	state := &PaidUnissuedSagaState{Saga: v}
	var paymentIDText, eventIDText *string
	var publishedAt *time.Time
	if err := q.QueryRow(ctx, `SELECT count(*)::int, count(*) FILTER (WHERE published_at IS NOT NULL)::int,
		(SELECT payment_id::text FROM saga_ticket_issuance_outbox WHERE saga_id=$1 ORDER BY created_at,id LIMIT 1),
		(SELECT id::text FROM saga_ticket_issuance_outbox WHERE saga_id=$1 ORDER BY created_at,id LIMIT 1),
		(SELECT published_at FROM saga_ticket_issuance_outbox WHERE saga_id=$1 ORDER BY created_at,id LIMIT 1),
		COALESCE(bool_or(claim_until > clock_timestamp()), false)
		FROM saga_ticket_issuance_outbox WHERE saga_id=$1`, id).Scan(&state.OutboxCount, &state.OutboxPublished, &paymentIDText, &eventIDText, &publishedAt, &state.OutboxClaimActive); err != nil {
		return nil, err
	}
	if err := parsePaidUnissuedOutboxIdentity(paymentIDText, eventIDText, publishedAt, state); err != nil {
		return nil, err
	}
	return state, nil
}

func parsePaidUnissuedOutboxIdentity(paymentIDText, eventIDText *string, publishedAt *time.Time, state *PaidUnissuedSagaState) error {
	if paymentIDText != nil {
		parsed, err := uuid.Parse(*paymentIDText)
		if err != nil {
			return ErrPaidUnissuedRecoveryConflict
		}
		state.OutboxPaymentID = &parsed
	}
	if eventIDText != nil {
		parsed, err := uuid.Parse(*eventIDText)
		if err != nil {
			return ErrPaidUnissuedRecoveryConflict
		}
		state.OutboxEventID = &parsed
	}
	if publishedAt != nil {
		observed := publishedAt.UTC()
		state.OutboxPublishedAt = &observed
	}
	return nil
}

func paidUnissuedAuditByKey(ctx context.Context, tx pgx.Tx, command PaidUnissuedRecoveryCommand, fingerprint [32]byte) (*PaidUnissuedRecoveryAuditResult, error) {
	var result PaidUnissuedRecoveryAuditResult
	var storedFingerprint []byte
	err := tx.QueryRow(ctx, `SELECT id,result_code,eligible,requeued,request_fingerprint
		FROM saga_paid_unissued_recovery_audit WHERE operation_key=$1 AND operation_mode=$2`, command.OperationKey, command.Mode).
		Scan(&result.AuditID, &result.ResultCode, &result.Eligible, &result.Requeued, &storedFingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if string(storedFingerprint) != string(fingerprint[:]) {
		return nil, ErrPaidUnissuedRecoveryConflict
	}
	result.Replayed = true
	return &result, nil
}

func validatePaidUnissuedCommand(command PaidUnissuedRecoveryCommand) error {
	if command.SagaID == uuid.Nil || command.BookingID == uuid.Nil || command.OperationKey == uuid.Nil ||
		command.Operator == "" || len(command.Operator) > 128 || command.Reason == "" || len(command.Reason) > 500 ||
		command.ExpectedProviderAccount == "" || len(command.ExpectedProviderAccount) > 128 ||
		(command.Mode != "dry_run" && command.Mode != "execute") {
		return ErrInvalidRecoveryInput
	}
	return nil
}

func paidUnissuedRequestFingerprint(command PaidUnissuedRecoveryCommand) [32]byte {
	request := struct {
		SagaID, BookingID, OperationKey   uuid.UUID
		Operator, Reason, ProviderAccount string
	}{command.SagaID, command.BookingID, command.OperationKey, command.Operator, command.Reason, command.ExpectedProviderAccount}
	raw, _ := json.Marshal(request)
	return sha256.Sum256(raw)
}

func paidUnissuedExpectedStateMatches(expected, current PaidUnissuedSagaState) bool {
	if expected.Saga == nil || current.Saga == nil {
		return false
	}
	a, b := expected.Saga, current.Saga
	return a.ID == b.ID && a.BookingID == b.BookingID && a.Name == b.Name && a.Status == b.Status && a.RecoveryState == b.RecoveryState &&
		a.Direction == b.Direction && a.CurrentStepIndex == b.CurrentStepIndex && a.ErrorCategory == b.ErrorCategory &&
		a.Generation == b.Generation && a.RecoveryManaged == b.RecoveryManaged && sameSagaInput(a.Input, b.Input) && samePaidUnissuedSteps(a.Steps, b.Steps) &&
		a.PaymentID == b.PaymentID && a.PaymentIntentId == b.PaymentIntentId && a.ClaimFence == b.ClaimFence &&
		a.WakeVersion == b.WakeVersion && a.ConsumedWakeVersion == b.ConsumedWakeVersion && a.Attempts == b.Attempts &&
		a.FailureAttempts == b.FailureAttempts && sameUUIDPointer(a.LastObligationID, b.LastObligationID) &&
		sameUUIDPointer(a.ClaimToken, b.ClaimToken) && sameTimePointer(a.LeaseUntil, b.LeaseUntil) &&
		sameTimePointer(a.NextAttemptAt, b.NextAttemptAt) && expected.OutboxCount == current.OutboxCount &&
		expected.OutboxPublished == current.OutboxPublished && sameUUIDPointer(expected.OutboxPaymentID, current.OutboxPaymentID) &&
		expected.OutboxClaimActive == current.OutboxClaimActive
}

func sameSagaInput(a, b *domain.SagaInput) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.BookingID != b.BookingID || a.UserID != b.UserID || a.EventID != b.EventID || a.Generation != b.Generation ||
		a.AmountCents != b.AmountCents || a.Currency != b.Currency || a.BookingMode != b.BookingMode ||
		!a.ReservationDeadline.Equal(b.ReservationDeadline) || a.Fingerprint != b.Fingerprint || len(a.SeatIDs) != len(b.SeatIDs) {
		return false
	}
	for i := range a.SeatIDs {
		if a.SeatIDs[i] != b.SeatIDs[i] {
			return false
		}
	}
	return true
}

func samePaidUnissuedSteps(a, b []domain.SagaStep) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.ID != y.ID || x.SagaID != y.SagaID || x.Name != y.Name || x.Order != y.Order || x.ShouldPauseForPayment != y.ShouldPauseForPayment ||
			x.Status != y.Status || x.ExecuteState != y.ExecuteState || x.CompensateState != y.CompensateState ||
			!bytes.Equal(x.ExecuteEvidence, y.ExecuteEvidence) || !bytes.Equal(x.CompensateEvidence, y.CompensateEvidence) ||
			!sameTimePointer(x.ExecutedAt, y.ExecutedAt) || !sameTimePointer(x.CompenstatedAt, y.CompenstatedAt) {
			return false
		}
	}
	return true
}

func sameUUIDPointer(a, b *uuid.UUID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameTimePointer(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func validatePaidUnissuedAttentionSaga(state *PaidUnissuedSagaState, command PaidUnissuedRecoveryCommand) error {
	if state == nil || state.Saga == nil {
		return ErrPaidUnissuedRecoveryConflict
	}
	v := state.Saga
	if v.ID != command.SagaID || v.BookingID != command.BookingID || !v.RecoveryManaged || v.Generation != 1 || v.Input == nil ||
		v.Status != domain.SAGA_PROCESSING || v.RecoveryState != "attention" || v.Direction != "forward" ||
		v.CurrentStepIndex != 4 || (v.ErrorCategory != "paid_unissued_binding_missing" && v.ErrorCategory != "invalid_persisted_graph") ||
		v.LastObligationID != nil || state.OutboxCount != 0 || state.OutboxClaimActive || command.PaymentID == nil ||
		v.PaymentID != uuid.Nil ||
		(v.PaymentIntentId != "" && v.PaymentIntentId != command.PaymentIntentID) {
		return ErrPaidUnissuedRecoveryConflict
	}
	if v.ClaimToken != nil && v.LeaseUntil != nil && v.LeaseUntil.After(time.Now()) {
		return ErrPaidUnissuedRecoveryConflict
	}
	for i, step := range v.Steps {
		if i < 4 {
			if step.ExecuteState != "completed" || step.CompensateState != "pending" {
				return ErrPaidUnissuedRecoveryConflict
			}
		} else if step.ExecuteState != "intended" || step.CompensateState != "pending" {
			return ErrPaidUnissuedRecoveryConflict
		}
	}
	copy := *v
	copy.RecoveryState = "ready"
	copy.ErrorCategory = ""
	if err := validateManagedGraph(&copy); err != nil {
		return ErrPaidUnissuedRecoveryConflict
	}
	return nil
}

func paidUnissuedSnapshot(state *PaidUnissuedSagaState, now time.Time) paidUnissuedLocalSnapshot {
	snapshot := paidUnissuedLocalSnapshot{}
	if state == nil || state.Saga == nil {
		return snapshot
	}
	v := state.Saga
	snapshot = paidUnissuedLocalSnapshot{
		SagaID: v.ID.String(), BookingID: v.BookingID.String(), Status: string(v.Status), RecoveryState: v.RecoveryState,
		Direction: v.Direction, CurrentStepIndex: v.CurrentStepIndex, ErrorCategory: v.ErrorCategory,
		PaymentIntentID: v.PaymentIntentId, ClaimFence: v.ClaimFence,
		ClaimActive: v.ClaimToken != nil && v.LeaseUntil != nil && v.LeaseUntil.After(now),
		OutboxCount: state.OutboxCount, OutboxPublished: state.OutboxPublished, FailureAttempts: v.FailureAttempts,
	}
	if v.PaymentID != uuid.Nil {
		snapshot.PaymentID = v.PaymentID.String()
	}
	return snapshot
}

var _ PaidUnissuedRecoveryRepository = (*SagaRepository)(nil)
