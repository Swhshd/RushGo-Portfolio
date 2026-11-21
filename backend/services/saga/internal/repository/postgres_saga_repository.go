package repository

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/saga/internal/domain"
)

type SagaRepository struct{ pool *pgxpool.Pool }

func NewSagaRepository(pool *pgxpool.Pool) *SagaRepository { return &SagaRepository{pool: pool} }

type sagaQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

const sagaColumns = `id,booking_id,name,status,current_step_index,payment_intent_id,payment_id,last_obligation_id,recovery_managed,lifecycle_generation,input_snapshot,input_fingerprint,recovery_state,direction,next_attempt_at,error_category,wake_version,consumed_wake_version,attempts,failure_attempts,claim_token,claim_fence,lease_until`
const stepColumns = `id,saga_id,name,executed_at,compensated_at,status,"order",should_pause_for_payment,execute_state,compensate_state,execute_evidence,compensate_evidence`

func scanSaga(row pgx.Row) (*domain.Saga, error) {
	v := &domain.Saga{}
	v.NullableFields = true
	var status, intent, direction, category *string
	var paymentID, lastObligation *uuid.UUID
	var index *int
	var generation *int64
	var managed bool
	var raw, hash []byte
	if err := row.Scan(&v.ID, &v.BookingID, &v.Name, &status, &index, &intent, &paymentID, &lastObligation, &managed, &generation, &raw, &hash, &v.RecoveryState, &direction, &v.NextAttemptAt, &category, &v.WakeVersion, &v.ConsumedWakeVersion, &v.Attempts, &v.FailureAttempts, &v.ClaimToken, &v.ClaimFence, &v.LeaseUntil); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan saga: %w", err)
	}
	if status != nil {
		v.Status = domain.SagaStatus(*status)
		v.NullableStatus = &v.Status
	}
	if index != nil {
		v.CurrentStepIndex = *index
		v.NullableCurrentStepIndex = index
	}
	if intent != nil {
		v.PaymentIntentId = *intent
	}
	if paymentID != nil {
		v.PaymentID = *paymentID
	}
	v.LastObligationID = lastObligation
	if generation != nil {
		v.Generation = *generation
	}
	v.RecoveryManaged = managed
	if direction != nil {
		v.Direction = *direction
	}
	if category != nil {
		v.ErrorCategory = *category
	}
	if len(raw) > 0 {
		input, err := domain.DecodeInput(raw)
		if err != nil || string(input.Fingerprint[:]) != string(hash) || input.BookingID != v.BookingID || (managed && input.Generation != v.Generation) {
			v.RecoveryState = "attention"
			v.ErrorCategory = "invalid_persisted_input"
		} else {
			v.Input = &input
		}
	} else if managed {
		v.RecoveryState = "attention"
		v.ErrorCategory = "missing_persisted_input"
	}
	if status == nil {
		v.RecoveryState = "attention"
		v.ErrorCategory = "missing_status"
	} else if index == nil {
		v.RecoveryState = "attention"
		v.ErrorCategory = "missing_step_index"
	} else if !managed {
		v.RecoveryState = "attention"
		v.ErrorCategory = "legacy_input_unproven"
	}
	if managed && v.NextAttemptAt == nil && v.Status != domain.SAGA_COMPLETED && v.Status != domain.SAGA_ROLLED_BACK {
		v.RecoveryState = "attention"
		v.ErrorCategory = "missing_due_time"
	}
	return v, nil
}
func loadSteps(ctx context.Context, q sagaQuerier, v *domain.Saga) error {
	rows, err := q.Query(ctx, `SELECT `+stepColumns+` FROM saga_steps WHERE saga_id=$1 ORDER BY "order" NULLS LAST,id`, v.ID)
	if err != nil {
		return fmt.Errorf("get saga steps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var st domain.SagaStep
		var status *string
		var order *int32
		var pause *bool
		if err := rows.Scan(&st.ID, &st.SagaID, &st.Name, &st.ExecutedAt, &st.CompenstatedAt, &status, &order, &pause, &st.ExecuteState, &st.CompensateState, &st.ExecuteEvidence, &st.CompensateEvidence); err != nil {
			return fmt.Errorf("scan saga step: %w", err)
		}
		if status != nil {
			st.Status = domain.SagaStepStatus(*status)
			st.NullableStatus = &st.Status
		}
		if order != nil {
			if *order < 0 || *order > 4 {
				v.RecoveryState = "attention"
				v.ErrorCategory = "invalid_step_order"
			} else {
				st.Order = int16(*order)
			}
			st.NullableOrder = order
		}
		if pause != nil {
			st.ShouldPauseForPayment = *pause
			st.NullablePause = pause
		}
		if status == nil || order == nil || pause == nil {
			v.RecoveryState = "attention"
			v.ErrorCategory = "missing_step_fields"
		}
		st.NullableFields = true
		v.Steps = append(v.Steps, st)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate saga steps: %w", err)
	}
	return nil
}
func loadSaga(ctx context.Context, q sagaQuerier, id uuid.UUID) (*domain.Saga, error) {
	v, err := scanSaga(q.QueryRow(ctx, `SELECT `+sagaColumns+` FROM sagas WHERE id=$1`, id))
	if err != nil {
		return nil, err
	}
	if err := loadSteps(ctx, q, v); err != nil {
		return nil, err
	}
	return v, nil
}
func (r *SagaRepository) GetSagaById(ctx context.Context, id uuid.UUID) (*domain.Saga, error) {
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
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit saga read: %w", err)
	}
	return v, nil
}
func (r *SagaRepository) GetSagaByBookingId(ctx context.Context, booking uuid.UUID) (*domain.Saga, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `SELECT id FROM sagas WHERE booking_id=$1 ORDER BY id`, booking)
	if err != nil {
		return nil, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, ErrNotFound
	}
	if len(ids) != 1 {
		return nil, ErrRecoveryAttention
	}
	v, err := loadSaga(ctx, tx, ids[0])
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit saga read: %w", err)
	}
	return v, nil
}
func sagaStatusValue(status domain.SagaStatus) any {
	if status == "" {
		return nil
	}
	return string(status)
}
func stepStatusValue(status domain.SagaStepStatus) any {
	if status == "" {
		return nil
	}
	return string(status)
}

func stepOrderValue(st domain.SagaStep) any {
	if st.NullableFields {
		return st.NullableOrder
	}
	return st.Order
}
func stepPauseValue(st domain.SagaStep) any {
	if st.NullableFields {
		return st.NullablePause
	}
	return st.ShouldPauseForPayment
}
func sagaIndexValue(v *domain.Saga) any {
	if v.NullableFields && v.NullableCurrentStepIndex == nil {
		return nil
	}
	return v.CurrentStepIndex
}
func (r *SagaRepository) Create(ctx context.Context, v *domain.Saga) error {
	if v == nil || v.ID == uuid.Nil || v.BookingID == uuid.Nil {
		return ErrInvalidRecoveryInput
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, v.BookingID.String()); err != nil {
		return err
	}
	var managed bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sagas WHERE booking_id=$1 AND recovery_managed)`, v.BookingID).Scan(&managed); err != nil {
		return err
	}
	if managed {
		return ErrRecoveryConflict
	}
	if _, err := tx.Exec(ctx, `INSERT INTO sagas(id,booking_id,name,status,current_step_index,payment_intent_id) VALUES($1,$2,$3,$4,$5,NULLIF($6,''))`, v.ID, v.BookingID, v.Name, sagaStatusValue(v.Status), sagaIndexValue(v), v.PaymentIntentId); err != nil {
		return err
	}
	for _, st := range v.Steps {
		if st.SagaID != v.ID || st.ID == uuid.Nil {
			return ErrInvalidRecoveryInput
		}
		if err := insertLegacyStep(ctx, tx, st); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("create saga commit outcome unknown: %w", err)
	}
	return nil
}
func insertLegacyStep(ctx context.Context, tx pgx.Tx, st domain.SagaStep) error {
	_, err := tx.Exec(ctx, `INSERT INTO saga_steps(id,saga_id,name,status,"order",should_pause_for_payment,executed_at,compensated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, st.ID, st.SagaID, st.Name, stepStatusValue(st.Status), stepOrderValue(st), stepPauseValue(st), st.ExecutedAt, st.CompenstatedAt)
	return err
}
func (r *SagaRepository) UpsertBatchSagaSteps(ctx context.Context, steps []domain.SagaStep) error {
	if len(steps) == 0 {
		return nil
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	ids := map[uuid.UUID]bool{}
	var parents []uuid.UUID
	for _, st := range steps {
		if st.ID == uuid.Nil || st.SagaID == uuid.Nil {
			return ErrInvalidRecoveryInput
		}
		if !ids[st.SagaID] {
			ids[st.SagaID] = true
			parents = append(parents, st.SagaID)
		}
	}
	sort.Slice(parents, func(i, j int) bool { return parents[i].String() < parents[j].String() })
	for _, id := range parents {
		var managed bool
		if err := tx.QueryRow(ctx, `SELECT recovery_managed FROM sagas WHERE id=$1 FOR UPDATE`, id).Scan(&managed); err != nil {
			return err
		}
		if managed {
			return ErrRecoveryConflict
		}
	}
	for _, st := range steps {
		tag, err := tx.Exec(ctx, `INSERT INTO saga_steps(id,saga_id,name,status,"order",should_pause_for_payment,executed_at,compensated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status,executed_at=COALESCE(EXCLUDED.executed_at,saga_steps.executed_at),compensated_at=COALESCE(EXCLUDED.compensated_at,saga_steps.compensated_at) WHERE NOT saga_steps.recovery_managed AND saga_steps.saga_id=EXCLUDED.saga_id AND saga_steps.name=EXCLUDED.name AND saga_steps."order" IS NOT DISTINCT FROM EXCLUDED."order" AND saga_steps.should_pause_for_payment IS NOT DISTINCT FROM EXCLUDED.should_pause_for_payment`, st.ID, st.SagaID, st.Name, stepStatusValue(st.Status), stepOrderValue(st), stepPauseValue(st), st.ExecutedAt, st.CompenstatedAt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrRecoveryConflict
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("upsert step commit outcome unknown: %w", err)
	}
	return nil
}
func (r *SagaRepository) UpdateSaga(ctx context.Context, v *domain.Saga, stepID *uuid.UUID) error {
	if v == nil {
		return ErrInvalidRecoveryInput
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var managed bool
	if err := tx.QueryRow(ctx, `SELECT recovery_managed FROM sagas WHERE id=$1 FOR UPDATE`, v.ID).Scan(&managed); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if managed {
		return ErrRecoveryConflict
	}
	if _, err := tx.Exec(ctx, `UPDATE sagas SET name=$2,status=$3,current_step_index=$4,payment_intent_id=NULLIF($5,'') WHERE id=$1`, v.ID, v.Name, sagaStatusValue(v.Status), sagaIndexValue(v), v.PaymentIntentId); err != nil {
		return err
	}
	if stepID != nil {
		var st *domain.SagaStep
		for i := range v.Steps {
			if v.Steps[i].ID == *stepID {
				st = &v.Steps[i]
				break
			}
		}
		if st == nil || st.SagaID != v.ID {
			return ErrInvalidRecoveryInput
		}
		tag, err := tx.Exec(ctx, `UPDATE saga_steps SET executed_at=$3,compensated_at=$4,status=$5 WHERE id=$1 AND saga_id=$2`, *stepID, v.ID, st.ExecutedAt, st.CompenstatedAt, stepStatusValue(st.Status))
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrNotFound
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("update saga commit outcome unknown: %w", err)
	}
	return nil
}

// Legacy adapters cannot change a managed workflow behind its durable executor.
func (r *SagaRepository) TransitionWaitingSaga(ctx context.Context, booking uuid.UUID, status domain.SagaStatus) (bool, error) {
	if status != domain.SAGA_PROCESSING && status != domain.SAGA_ROLLED_BACK && status != domain.SAGA_ROLLING_BACK {
		return false, ErrInvalidRecoveryInput
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var id uuid.UUID
	var managed bool
	var current *string
	err = tx.QueryRow(ctx, `SELECT id,recovery_managed,status::text FROM sagas WHERE booking_id=$1 FOR UPDATE`, booking).Scan(&id, &managed, &current)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if managed {
		return false, ErrRecoveryConflict
	}
	if current == nil {
		return false, ErrRecoveryAttention
	}
	if *current != "WAIT_FOR_PAYMENT" {
		return *current == "ROLLING_BACK" && status == domain.SAGA_ROLLING_BACK, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE sagas SET status=$2 WHERE id=$1`, id, string(status)); err != nil {
		return false, err
	}
	if status == domain.SAGA_ROLLED_BACK {
		if _, err := tx.Exec(ctx, `UPDATE saga_steps SET status='COMPENSATED',compensated_at=NOW() WHERE saga_id=$1 AND "order"=0 AND status='COMPLETED'`, id); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `UPDATE sagas SET current_step_index=0 WHERE id=$1`, id); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("legacy transition commit outcome unknown: %w", err)
	}
	return true, nil
}
func (r *SagaRepository) ResetProcessingSaga(ctx context.Context, booking uuid.UUID) (bool, error) {
	tag, err := r.pool.Exec(ctx, `UPDATE sagas SET status='WAIT_FOR_PAYMENT' WHERE booking_id=$1 AND status='PROCESSING' AND NOT recovery_managed`, booking)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
