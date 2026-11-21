package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	sagapkg "github.com/ticketbox/pkg/saga"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/recoverycomponent"
)

var orderNames = []sagapkg.SagaStepName{sagapkg.RESERVED_SEAT_STEP, sagapkg.CREATE_PAYMENT_INTENT_STEP, sagapkg.UPDATE_SEAT_BOOKED, sagapkg.UPDATE_BOOKING_STATUS_CONFIRMED, sagapkg.UPDATE_AVAILABLE_SEAT_NUMBER}

func validateNewSteps(steps []domain.SagaStep) error {
	if len(steps) != 5 {
		return ErrInvalidRecoveryInput
	}
	ids := map[uuid.UUID]bool{}
	for i, st := range steps {
		if st.ID == uuid.Nil || ids[st.ID] || st.Order != int16(i) || st.Name != string(orderNames[i]) || st.ShouldPauseForPayment != (i == 1) || st.Status != domain.SAGA_STEP_PENDING || st.ExecutedAt != nil || st.CompenstatedAt != nil {
			return ErrInvalidRecoveryInput
		}
		ids[st.ID] = true
	}
	return nil
}
func (r *SagaRepository) CreateOrGetSaga(ctx context.Context, input domain.SagaInput, steps []domain.SagaStep) (*domain.Saga, bool, error) {
	normalized, raw, err := domain.CanonicalInput(input)
	if err != nil {
		return nil, false, ErrInvalidRecoveryInput
	}
	if input.Fingerprint != ([32]byte{}) && input.Fingerprint != normalized.Fingerprint {
		return nil, false, ErrRecoveryConflict
	}
	input = normalized
	if err := validateNewSteps(steps); err != nil {
		return nil, false, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(context.Background())
	// Absent legacy rows and first managed insert share the same local identity lock.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, input.BookingID.String()); err != nil {
		return nil, false, err
	}
	rows, err := tx.Query(ctx, `SELECT id FROM sagas WHERE booking_id=$1 ORDER BY id FOR UPDATE`, input.BookingID)
	if err != nil {
		return nil, false, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, false, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(ids) > 1 {
		if _, err := tx.Exec(ctx, `UPDATE sagas SET recovery_state='attention',error_category='duplicate_booking_history' WHERE booking_id=$1`, input.BookingID); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		return nil, false, ErrRecoveryAttention
	}
	created := len(ids) == 0
	id := uuid.New()
	if created {
		if _, err := tx.Exec(ctx, `INSERT INTO sagas(id,booking_id,name,status,current_step_index,recovery_managed,lifecycle_generation,input_snapshot,input_fingerprint,recovery_state,direction,next_attempt_at) VALUES($1,$2,'ORDER_SAGA','PENDING',0,TRUE,1,$3,$4,'ready','forward',NOW())`, id, input.BookingID, raw, input.Fingerprint[:]); err != nil {
			return nil, false, err
		}
		for _, st := range steps {
			st.SagaID = id
			if err := insertManagedStep(ctx, tx, st); err != nil {
				return nil, false, err
			}
		}
	} else {
		id = ids[0]
		existing, err := loadSaga(ctx, tx, id)
		if err != nil {
			return nil, false, err
		}
		if existing.RecoveryManaged {
			if existing.Input == nil || existing.Input.Fingerprint != input.Fingerprint || existing.Input.BookingID != input.BookingID {
				return nil, false, ErrRecoveryConflict
			}
			if err := validateManagedGraph(existing); err != nil {
				return nil, false, err
			}
		} else {
			// Input here is an authoritative Booking snapshot supplied by the service.
			// Only the old completed S0/S1 waiting form has an unambiguous upgrade.
			valid := existing.Input != nil && existing.Input.Fingerprint == input.Fingerprint && existing.Status == domain.SAGA_WAIT_FOR_PAYMENT && existing.NullableCurrentStepIndex != nil && existing.CurrentStepIndex == 1 && len(existing.Steps) == 2
			for i, st := range existing.Steps {
				if i >= 2 || st.NullableStatus == nil || st.NullableOrder == nil || st.NullablePause == nil || st.Order != int16(i) || st.Name != string(orderNames[i]) || st.ShouldPauseForPayment != (i == 1) || st.Status != domain.SAGA_STEP_COMPLETED || st.ExecutedAt == nil || st.CompenstatedAt != nil {
					valid = false
				}
			}
			if !valid {
				if _, err := tx.Exec(ctx, `UPDATE sagas SET recovery_state='attention',error_category='legacy_graph_unproven' WHERE id=$1`, id); err != nil {
					return nil, false, err
				}
				if err := tx.Commit(ctx); err != nil {
					return nil, false, err
				}
				return nil, false, ErrRecoveryAttention
			}
			if _, err := tx.Exec(ctx, `UPDATE saga_steps SET recovery_managed=TRUE,execute_state='completed' WHERE saga_id=$1`, id); err != nil {
				return nil, false, err
			}
			for _, st := range steps[2:] {
				st.SagaID = id
				if err := insertManagedStep(ctx, tx, st); err != nil {
					return nil, false, err
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE sagas SET recovery_managed=TRUE,lifecycle_generation=1,input_snapshot=$2,input_fingerprint=$3,recovery_state='ready',direction='forward',current_step_index=2,next_attempt_at=NOW(),error_category=NULL WHERE id=$1`, id, raw, input.Fingerprint[:]); err != nil {
				return nil, false, err
			}
		}
	}
	v, err := loadSaga(ctx, tx, id)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("Saga create/adopt commit outcome unknown: %w", err)
	}
	return v, created, nil
}
func insertManagedStep(ctx context.Context, tx pgx.Tx, st domain.SagaStep) error {
	_, err := tx.Exec(ctx, `INSERT INTO saga_steps(id,saga_id,name,status,"order",should_pause_for_payment,recovery_managed) VALUES($1,$2,$3,'PENDING',$4,$5,TRUE)`, st.ID, st.SagaID, st.Name, st.Order, st.ShouldPauseForPayment)
	return err
}
func validateManagedGraph(v *domain.Saga) error {
	if v.Input == nil || v.RecoveryState != "ready" || len(v.Steps) != 5 || v.NullableStatus == nil || v.NullableCurrentStepIndex == nil || (v.Direction != "forward" && v.Direction != "compensate") {
		return ErrRecoveryAttention
	}
	for i, st := range v.Steps {
		if st.SagaID != v.ID || st.ID == uuid.Nil || st.NullableStatus == nil || st.NullableOrder == nil || st.NullablePause == nil || st.Order != int16(i) || st.Name != string(orderNames[i]) || st.ShouldPauseForPayment != (i == 1) || (st.ExecuteState == "completed" && st.ExecutedAt == nil) || (st.CompensateState == "completed" && st.CompenstatedAt == nil) {
			return ErrRecoveryAttention
		}
		if v.Direction == "forward" && i < v.CurrentStepIndex && st.ExecuteState != "completed" {
			return ErrRecoveryAttention
		}
		if (st.ExecuteState == "completed") != (st.ExecutedAt != nil) || (st.CompensateState == "completed") != (st.CompenstatedAt != nil) {
			return ErrRecoveryAttention
		}
		if v.Direction == "forward" {
			if st.CompensateState != "pending" || st.CompenstatedAt != nil {
				return ErrRecoveryAttention
			}
			expected := map[string]domain.SagaStepStatus{"pending": domain.SAGA_STEP_PENDING, "intended": domain.SAGA_STEP_EXECUTING, "completed": domain.SAGA_STEP_COMPLETED, "retryable": domain.SAGA_STEP_FAILED, "attention": domain.SAGA_STEP_FAILED}[st.ExecuteState]
			if st.Status != expected || expected == "" || (i == v.CurrentStepIndex && st.ExecuteState == "completed") || (i > v.CurrentStepIndex && st.ExecuteState != "pending") {
				return ErrRecoveryAttention
			}
		} else {
			if i > v.CurrentStepIndex && st.ExecuteState != "pending" && st.CompensateState != "completed" {
				return ErrRecoveryAttention
			}
			if st.Status == domain.SAGA_STEP_COMPLETED && st.ExecuteState != "completed" {
				return ErrRecoveryAttention
			}
			if st.CompensateState == "completed" && st.Status != domain.SAGA_STEP_COMPENSATED {
				return ErrRecoveryAttention
			}
			if i == v.CurrentStepIndex && st.CompensateState == "completed" {
				return ErrRecoveryAttention
			}
			expected := map[string]domain.SagaStepStatus{"pending": map[string]domain.SagaStepStatus{"pending": domain.SAGA_STEP_PENDING, "intended": domain.SAGA_STEP_EXECUTING, "completed": domain.SAGA_STEP_COMPLETED, "retryable": domain.SAGA_STEP_FAILED, "attention": domain.SAGA_STEP_FAILED}[st.ExecuteState], "intended": domain.SAGA_STEP_COMPENSATING, "completed": domain.SAGA_STEP_COMPENSATED, "retryable": domain.SAGA_STEP_FAILED, "attention": domain.SAGA_STEP_FAILED}[st.CompensateState]
			if expected == "" || st.Status != expected {
				return ErrRecoveryAttention
			}
		}
	}
	if v.Direction == "forward" {
		if v.CurrentStepIndex < 0 || v.CurrentStepIndex > 5 {
			return ErrRecoveryAttention
		}
		if (v.Status == domain.SAGA_COMPLETED) != (v.CurrentStepIndex == 5) {
			return ErrRecoveryAttention
		}
		if v.Status == domain.SAGA_WAIT_FOR_PAYMENT && v.CurrentStepIndex != 2 {
			return ErrRecoveryAttention
		}
	} else {
		if v.CurrentStepIndex < -1 || v.CurrentStepIndex > 4 {
			return ErrRecoveryAttention
		}
		if (v.Status == domain.SAGA_ROLLED_BACK) != (v.CurrentStepIndex == -1) {
			return ErrRecoveryAttention
		}
	}
	return nil
}

func (r *SagaRepository) ClaimDueSagas(ctx context.Context, limit int, lease time.Duration) ([]domain.SagaClaim, error) {
	return r.claimSagas(ctx, limit, lease, nil)
}
func (r *SagaRepository) ClaimSaga(ctx context.Context, id uuid.UUID, lease time.Duration) (*domain.SagaClaim, error) {
	if id == uuid.Nil {
		return nil, ErrInvalidRecoveryInput
	}
	claims, err := r.claimSagas(ctx, 1, lease, &id)
	if err != nil || len(claims) == 0 {
		return nil, err
	}
	return &claims[0], nil
}
func (r *SagaRepository) claimSagas(ctx context.Context, limit int, lease time.Duration, target *uuid.UUID) ([]domain.SagaClaim, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if limit < 1 || limit > 4 || lease < time.Second || lease > time.Minute {
		return nil, ErrInvalidRecoveryInput
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `SELECT id,(claim_token IS NOT NULL AND lease_until IS NOT NULL AND lease_until<=clock_timestamp()) FROM sagas WHERE recovery_managed AND recovery_state='ready' AND status IN ('PENDING','PROCESSING','ROLLING_BACK','FAIL','WAIT_FOR_PAYMENT') AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp()) AND ($2::uuid IS NULL OR id=$2) ORDER BY next_attempt_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit, target)
	if err != nil {
		return nil, err
	}
	type dueSaga struct {
		id        uuid.UUID
		reclaimed bool
	}
	var ids []dueSaga
	for rows.Next() {
		var item dueSaga
		if err := rows.Scan(&item.id, &item.reclaimed); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	claims := []domain.SagaClaim{}
	for _, item := range ids {
		v, err := loadSaga(ctx, tx, item.id)
		if err != nil {
			return nil, err
		}
		if err := validateManagedGraph(v); err != nil {
			if _, err := tx.Exec(ctx, `UPDATE sagas SET recovery_state='attention',error_category='invalid_persisted_graph',claim_token=NULL,lease_until=NULL WHERE id=$1`, item.id); err != nil {
				return nil, err
			}
			continue
		}
		c := domain.SagaClaim{SagaID: item.id, Token: uuid.New(), Saga: v, Input: *v.Input, Direction: v.Direction, Reclaimed: item.reclaimed}
		err = tx.QueryRow(ctx, `UPDATE sagas SET claim_token=$2,claim_fence=claim_fence+1,lease_until=clock_timestamp()+$3*interval '1 second',attempts=attempts+1,status=CASE WHEN direction='compensate' THEN 'ROLLING_BACK'::saga_status ELSE 'PROCESSING'::saga_status END WHERE id=$1 RETURNING claim_fence,lease_until`, item.id, c.Token, lease.Seconds()).Scan(&c.Fence, &c.LeaseUntil)
		if err != nil {
			return nil, err
		}
		if v.Direction == "compensate" {
			v.Status = domain.SAGA_ROLLING_BACK
		} else {
			v.Status = domain.SAGA_PROCESSING
		}
		claims = append(claims, c)
		v.Attempts++
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("Saga claim commit outcome unknown: %w", err)
	}
	return claims, nil
}
func ownedSagaClaim(ctx context.Context, tx pgx.Tx, c domain.SagaClaim) (*domain.Saga, bool, error) {
	var id uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM sagas WHERE id=$1 FOR UPDATE`, c.SagaID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var owned bool
	if err := tx.QueryRow(ctx, `SELECT COALESCE(claim_token=$2 AND claim_fence=$3 AND lease_until>clock_timestamp() AND recovery_state='ready',FALSE) FROM sagas WHERE id=$1`, c.SagaID, c.Token, c.Fence).Scan(&owned); err != nil {
		return nil, false, err
	}
	if !owned {
		return nil, false, nil
	}
	v, err := loadSaga(ctx, tx, id)
	if err == nil {
		err = validateManagedGraph(v)
	}
	return v, err == nil, err
}
func (r *SagaRepository) RenewClaim(ctx context.Context, c domain.SagaClaim, lease time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if lease < time.Second || lease > time.Minute {
		return false, ErrInvalidRecoveryInput
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	_, owned, err := ownedSagaClaim(ctx, tx, c)
	if err != nil || !owned {
		return false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE sagas SET lease_until=clock_timestamp()+$2*interval '1 second' WHERE id=$1 AND lease_until>clock_timestamp()`, c.SagaID, lease.Seconds())
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("renew Saga commit outcome unknown: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
func claimStep(v *domain.Saga, id uuid.UUID, direction string) (*domain.SagaStep, error) {
	if direction != v.Direction || (direction != "forward" && direction != "compensate") || v.CurrentStepIndex < 0 || v.CurrentStepIndex >= len(v.Steps) {
		return nil, ErrRecoveryConflict
	}
	st := &v.Steps[v.CurrentStepIndex]
	if st.ID != id {
		return nil, ErrRecoveryConflict
	}
	return st, nil
}
func (r *SagaRepository) PersistStepIntent(ctx context.Context, c domain.SagaClaim, id uuid.UUID, direction string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	v, owned, err := ownedSagaClaim(ctx, tx, c)
	if err != nil || !owned {
		return false, err
	}
	st, err := claimStep(v, id, direction)
	if err != nil {
		return false, err
	}
	state, status := "execute_state", "EXECUTING"
	if direction == "compensate" {
		state, status = "compensate_state", "COMPENSATING"
	}
	existing := st.ExecuteState
	if direction == "compensate" {
		existing = st.CompensateState
	}
	if existing == "completed" || existing == "attention" {
		return false, ErrRecoveryConflict
	}
	tag, err := tx.Exec(ctx, `UPDATE saga_steps SET `+state+`='intended',status=$3 WHERE id=$1 AND saga_id=$2 AND EXISTS(SELECT 1 FROM sagas WHERE id=$2 AND lease_until>clock_timestamp())`, id, v.ID, status)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("Saga intent commit outcome unknown: %w", err)
	}
	return true, nil
}
func (r *SagaRepository) PersistStepResult(ctx context.Context, c domain.SagaClaim, result domain.StepResult) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if result.State != "completed" && result.State != "retryable" && result.State != "attention" {
		return false, ErrInvalidRecoveryInput
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	v, owned, err := ownedSagaClaim(ctx, tx, c)
	if err != nil || !owned {
		return false, err
	}
	st, err := claimStep(v, result.StepID, result.Direction)
	if err != nil {
		return false, err
	}
	prefix, timestamp, status := "execute", "executed_at", "COMPLETED"
	prior := st.ExecuteState
	if result.Direction == "compensate" {
		prefix, timestamp, status = "compensate", "compensated_at", "COMPENSATED"
		prior = st.CompensateState
	}
	if prior != "intended" {
		return false, ErrRecoveryConflict
	}
	if result.State != "completed" {
		status = "FAILED"
	}
	var evidence any
	if len(result.Evidence) > 0 {
		evidence = result.Evidence
	}
	if _, err := tx.Exec(ctx, `UPDATE saga_steps SET `+prefix+`_state=$3,`+prefix+`_evidence=$4,error_category=NULLIF($5,''),status=$6,`+timestamp+`=CASE WHEN $3='completed' THEN COALESCE(`+timestamp+`,NOW()) ELSE `+timestamp+` END WHERE id=$1 AND saga_id=$2`, st.ID, v.ID, result.State, evidence, result.ErrorCategory, status); err != nil {
		return false, err
	}
	index := v.CurrentStepIndex
	workflow, recovery := "PROCESSING", "ready"
	release := result.State != "completed"
	next := result.NextAttemptAt
	if next.IsZero() {
		next = time.Now().Add(time.Second)
	}
	if v.Direction == "compensate" {
		workflow = "ROLLING_BACK"
	}
	if result.State == "completed" {
		if v.Direction == "forward" {
			index++
			if st.ShouldPauseForPayment {
				workflow = "WAIT_FOR_PAYMENT"
				next = time.Now().Add(15 * time.Minute)
				if v.Input != nil && !v.Input.ReservationDeadline.IsZero() {
					next = v.Input.ReservationDeadline
				}
				release = true
				if v.WakeVersion > v.ConsumedWakeVersion {
					workflow = "PROCESSING"
					next = time.Now()
				}
			}
			if index == 5 {
				if v.PaymentID == uuid.Nil {
					return false, ErrPaidUnissuedBindingMissing
				}
				if err := insertTicketIssuanceOutbox(ctx, tx, v); err != nil {
					return false, err
				}
				workflow = "COMPLETED"
				release = true
			}
		} else {
			index--
			for index >= 0 && v.Steps[index].CompensateState == "completed" {
				index--
			}
			if index < 0 {
				workflow = "ROLLED_BACK"
				release = true
			}
		}
	} else if result.State == "attention" {
		recovery = "attention"
	}
	tag, err := tx.Exec(ctx, `UPDATE sagas SET current_step_index=$2,status=$3,recovery_state=$4,next_attempt_at=$5,error_category=NULLIF($6,''),claim_token=CASE WHEN $7 THEN NULL ELSE claim_token END,lease_until=CASE WHEN $7 THEN NULL ELSE lease_until END,consumed_wake_version=CASE WHEN $8 THEN wake_version ELSE consumed_wake_version END,failure_attempts=CASE WHEN $9='completed' OR $6='financial_wait' THEN 0 ELSE failure_attempts+1 END WHERE id=$1 AND lease_until>clock_timestamp()`, v.ID, index, workflow, recovery, next, result.ErrorCategory, release, st.ShouldPauseForPayment && result.State == "completed", result.State)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	preCommitTokenDigest := sha256.Sum256([]byte(c.Token.String()))
	recoverycomponent.PreCommitControl(recoverycomponent.PreCommitEvent{
		ContractVersion:  recoverycomponent.PreCommitContractVersion,
		TransactionState: "OPEN_UNCOMMITTED",
		SagaID:           v.ID.String(),
		StepID:           st.ID.String(),
		Direction:        result.Direction,
		ResultState:      result.State,
		PriorIndex:       v.CurrentStepIndex,
		NextIndex:        index,
		ClaimFence:       c.Fence,
		ClaimTokenSHA256: hex.EncodeToString(preCommitTokenDigest[:]),
	})
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("Saga result commit outcome unknown: %w", err)
	}
	claimAction := "retained"
	if release {
		claimAction = "released"
	}
	tokenDigest := sha256.Sum256([]byte(c.Token.String()))
	recoverycomponent.PostCommit(recoverycomponent.PostCommitEvent{
		ContractVersion:      recoverycomponent.PostCommitContractVersion,
		CommitOutcome:        "COMMIT_ACKED",
		SagaID:               v.ID.String(),
		StepID:               st.ID.String(),
		Direction:            result.Direction,
		ResultState:          result.State,
		PriorIndex:           v.CurrentStepIndex,
		NextIndex:            index,
		NextStatus:           workflow,
		RecoveryState:        recovery,
		ClaimAction:          claimAction,
		ClaimFence:           c.Fence,
		ClaimTokenSHA256:     hex.EncodeToString(tokenDigest[:]),
		TicketOutboxExpected: result.State == "completed" && v.Direction == "forward" && index == 5,
	})
	return true, nil
}
func (r *SagaRepository) WakeSaga(ctx context.Context, booking uuid.UUID, generation int64, payment *uuid.UUID, reason string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if booking == uuid.Nil || generation != 1 || reason == "" || (payment != nil && *payment == uuid.Nil) {
		return ErrInvalidRecoveryInput
	}
	tag, err := r.pool.Exec(ctx, `UPDATE sagas SET payment_id=COALESCE(payment_id,$3),wake_reason=$4,wake_version=wake_version+1,next_attempt_at=NOW(),status=CASE WHEN status='WAIT_FOR_PAYMENT' THEN 'PROCESSING'::saga_status ELSE status END WHERE booking_id=$1 AND lifecycle_generation=$2 AND recovery_managed AND recovery_state='ready' AND status NOT IN ('COMPLETED','ROLLED_BACK') AND ($3::uuid IS NULL OR payment_id IS NULL OR payment_id=$3)`, booking, generation, payment, reason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrRecoveryConflict
	}
	return nil
}

var _ RecoveryRepository = (*SagaRepository)(nil)
