package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/payment/internal/domain"
)

type refundPreSendRow struct {
	PaymentID            uuid.UUID
	ClaimToken           *uuid.UUID
	ClaimFence           int64
	LeaseValid           bool
	OperationKind        *string
	BookingID            uuid.UUID
	UserID               uuid.UUID
	Generation           int64
	ProviderAccount      string
	LiveMode             bool
	AmountCents          int64
	Currency             string
	PaymentStatus        string
	ProviderOutcome      string
	PaymentIntentID      string
	RequestHash          []byte
	FrozenCreate         []byte
	CreateKey            string
	CancelKey            string
	RefundKey            string
	RefundState          string
	RefundID             *string
	RefundProviderStatus *string
	RefundDecisionID     *uuid.UUID
	RefundAmountCents    *int64
	RefundCurrency       *string
	RefundReason         *string
}

// CheckRefundPreSend rechecks the current Payment command and lease after any
// external inspection. It holds a row lock only while reading the authority;
// callers make no Provider call until this method has committed and returned.
func (p *PostgresPaymentRepository) CheckRefundPreSend(ctx context.Context, c domain.FinancialClaim) (RefundPreSendResult, error) {
	if c.Kind != "refund" || c.PaymentID == uuid.Nil || c.Token == uuid.Nil || c.Fence < 1 || c.View == nil || c.Refund == nil {
		return RefundPreSendResult{}, ErrInvalidFinancialIdentity
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var tx refundPreSendTx
	var err error
	if p.beginRefundPreSendTx != nil {
		tx, err = p.beginRefundPreSendTx(ctx)
	} else {
		var rawTx pgx.Tx
		rawTx, err = p.pool.Begin(ctx)
		tx = rawTx
	}
	if err != nil {
		return RefundPreSendResult{}, err
	}
	defer tx.Rollback(ctx)

	// This row lock is the Payment activation barrier. It precedes the Payment
	// row lock in the new worker so activation cannot cross this decision.
	var admissionActive bool
	if err := tx.QueryRow(ctx, `SELECT active FROM payment_admission_mode WHERE singleton=TRUE FOR SHARE`).Scan(&admissionActive); err != nil {
		return RefundPreSendResult{}, fmt.Errorf("lock payment admission mode: %w", err)
	}

	// Lock first. Lease validity is evaluated in a later statement, after any
	// wait for the row lock, so clock_timestamp() cannot go stale while waiting.
	var lockedID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM payments WHERE id=$1 AND managed FOR UPDATE`, c.PaymentID).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		result := RefundPreSendResult{Decision: RefundPreSendNotAllowed, Reason: "payment_unavailable"}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return RefundPreSendResult{}, fmt.Errorf("refund pre-send check outcome unknown: %w", commitErr)
		}
		return result, nil
	}
	if err != nil {
		return RefundPreSendResult{}, err
	}

	var row refundPreSendRow
	err = tx.QueryRow(ctx, `SELECT id,claim_token,claim_fence,
		COALESCE(lease_until>clock_timestamp(),false),operation_kind,
		booking_id,user_id,payment_generation,provider_account,livemode,price,currency,status,provider_outcome,
		COALESCE(payment_intent_id,''),request_hash,frozen_create,create_key,cancel_key,refund_key,
		refund_state,refund_provider_id,refund_provider_status,refund_decision_id,refund_amount_cents,refund_currency,refund_reason
		FROM payments WHERE id=$1 AND managed`, c.PaymentID).Scan(
		&row.PaymentID, &row.ClaimToken, &row.ClaimFence, &row.LeaseValid, &row.OperationKind,
		&row.BookingID, &row.UserID, &row.Generation, &row.ProviderAccount, &row.LiveMode, &row.AmountCents,
		&row.Currency, &row.PaymentStatus, &row.ProviderOutcome, &row.PaymentIntentID, &row.RequestHash,
		&row.FrozenCreate, &row.CreateKey, &row.CancelKey, &row.RefundKey, &row.RefundState, &row.RefundID,
		&row.RefundProviderStatus, &row.RefundDecisionID, &row.RefundAmountCents, &row.RefundCurrency, &row.RefundReason)
	if err != nil {
		return RefundPreSendResult{}, err
	}

	result := RefundPreSendResult{Decision: RefundPreSendAllow}
	switch {
	case row.ClaimToken == nil || *row.ClaimToken != c.Token || row.ClaimFence != c.Fence || !row.LeaseValid || row.OperationKind == nil || *row.OperationKind != c.Kind:
		result = RefundPreSendResult{Decision: RefundPreSendStaleOwner, Reason: "owner_or_lease"}
	case row.RefundState == "succeeded":
		result = RefundPreSendResult{Decision: RefundPreSendSucceeded}
	default:
		if reason := refundPreSendCommandMismatch(row, c); reason != "" {
			result = RefundPreSendResult{Decision: RefundPreSendCommandMismatch, Reason: reason}
		} else if row.RefundID != nil && *row.RefundID != "" {
			result = RefundPreSendResult{Decision: RefundPreSendExistingResult, RefundID: *row.RefundID, Reason: "refund_id_present"}
		} else if row.RefundProviderStatus != nil && *row.RefundProviderStatus != "" || row.RefundState == "provider_pending" {
			result = RefundPreSendResult{Decision: RefundPreSendExistingResult, Reason: "provider_result_present"}
		} else if row.PaymentStatus != string(domain.PaymentSuccess) || row.ProviderOutcome != "collected" || row.RefundState != "claimed" {
			result = RefundPreSendResult{Decision: RefundPreSendNotAllowed, Reason: "current_state_not_claimed"}
		}
	}
	if result.Decision == RefundPreSendAllow && row.RefundReason != nil && *row.RefundReason == domain.RefundReasonConfirmedUnusedTicket {
		var committedAdmission bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM payment_admission_permits
			WHERE payment_id=$1 AND state='COMMITTED')`, c.PaymentID).Scan(&committedAdmission); err != nil {
			return RefundPreSendResult{}, fmt.Errorf("recheck committed Payment admission before unused-ticket refund PREPARED: %w", err)
		}
		if committedAdmission {
			result = RefundPreSendResult{Decision: RefundPreSendAdmissionConflict, Reason: "committed_admission"}
		}
	}
	if result.Decision == RefundPreSendAllow && admissionActive {
		var pendingAdmission bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM payment_admission_permits
			WHERE payment_id=$1 AND state='PENDING')`, c.PaymentID).Scan(&pendingAdmission); err != nil {
			return RefundPreSendResult{}, fmt.Errorf("recheck pending Payment admission before PREPARED: %w", err)
		}
		if pendingAdmission {
			// The committed positive result means no PREPARED audit record was
			// written; the worker can perform a dedicated neutral claim defer.
			result = RefundPreSendResult{Decision: RefundPreSendCoordinationWait, Reason: "pending_admission"}
		}
	}
	if result.Decision == RefundPreSendAllow {
		attemptID := uuid.New()
		prepared, err := domain.NewRefundPreparedAudit(c, attemptID)
		if err != nil {
			return RefundPreSendResult{}, fmt.Errorf("prepare refund audit record: %w", err)
		}
		if err := appendRefundAuditRecord(ctx, tx, prepared); err != nil {
			return RefundPreSendResult{}, fmt.Errorf("persist refund preparation before Provider call: %w", err)
		}
		result.AttemptID = attemptID
	}

	if err := tx.Commit(ctx); err != nil {
		return RefundPreSendResult{}, fmt.Errorf("refund pre-send check outcome unknown: %w", err)
	}
	if result.Decision == RefundPreSendCoordinationWait {
		result.coordinationWaitProof = true
	}
	return result, nil
}

func refundPreSendCommandMismatch(row refundPreSendRow, c domain.FinancialClaim) string {
	view := c.View
	if row.PaymentID != c.PaymentID || c.Operation.PaymentID != row.PaymentID ||
		row.BookingID != c.Operation.Identity.BookingID || row.UserID != c.Operation.Identity.UserID ||
		row.Generation != c.Operation.Identity.Generation || row.ProviderAccount != c.Operation.Identity.ProviderAccount ||
		row.LiveMode != c.Operation.Identity.LiveMode || row.AmountCents != c.Operation.Identity.AmountCents || row.Currency != c.Operation.Identity.Currency ||
		view == nil || view.Payment == nil || view.Payment.ID != row.PaymentID || view.Payment.BookingId == nil || *view.Payment.BookingId != row.BookingID ||
		view.Payment.UserId != row.UserID || view.Payment.Price < 0 || int64(view.Payment.Price) != row.AmountCents || view.Payment.Currency != row.Currency ||
		view.Payment.PaymentIntentId == "" || view.Payment.PaymentIntentId != row.PaymentIntentID || view.Payment.Status != domain.PaymentSuccess ||
		view.Generation != row.Generation || view.ProviderAccount != row.ProviderAccount || view.LiveMode == nil || *view.LiveMode != row.LiveMode ||
		view.ProviderOutcome != "collected" {
		return "payment_identity"
	}

	wantCreateKey := fmt.Sprintf("ticketbox:payment:%s:g%d:create", row.PaymentID, row.Generation)
	wantCancelKey := fmt.Sprintf("ticketbox:payment:%s:g%d:cancel", row.PaymentID, row.Generation)
	wantRefundKey := fmt.Sprintf("ticketbox:payment:%s:g%d:refund", row.PaymentID, row.Generation)
	if row.CreateKey != wantCreateKey || row.CancelKey != wantCancelKey || row.RefundKey != wantRefundKey ||
		c.Key != wantRefundKey || c.Operation.Key != wantRefundKey || c.Refund.Key != wantRefundKey {
		return "stable_key"
	}

	if c.Refund.PaymentID != row.PaymentID || c.Refund.Generation != row.Generation || c.Refund.DecisionID == uuid.Nil ||
		row.RefundDecisionID == nil || *row.RefundDecisionID != c.Refund.DecisionID ||
		c.Refund.AmountCents != row.AmountCents || row.RefundAmountCents == nil || *row.RefundAmountCents != c.Refund.AmountCents ||
		c.Refund.Currency != row.Currency || row.RefundCurrency == nil || *row.RefundCurrency != c.Refund.Currency ||
		c.Refund.Reason == "" || row.RefundReason == nil || *row.RefundReason != c.Refund.Reason ||
		view.RefundAmountCents != c.Refund.AmountCents {
		return "refund_command"
	}

	var storedHash [32]byte
	if len(row.RequestHash) != len(storedHash) || !bytes.Equal(row.RequestHash, c.Operation.Identity.Fingerprint[:]) {
		return "frozen_identity"
	}
	copy(storedHash[:], row.RequestHash)
	currentIdentity := domain.FinancialIdentity{
		BookingID: row.BookingID, UserID: row.UserID, Generation: row.Generation, ProviderAccount: row.ProviderAccount,
		LiveMode: row.LiveMode, AmountCents: row.AmountCents, Currency: row.Currency,
	}
	currentHash, err := domain.FinancialFingerprint(currentIdentity, row.FrozenCreate)
	if err != nil || currentHash != storedHash {
		return "frozen_identity"
	}
	claimHash, err := domain.FinancialFingerprint(c.Operation.Identity, c.Operation.FrozenParams)
	if err != nil || claimHash != c.Operation.Identity.Fingerprint {
		return "frozen_identity"
	}
	return ""
}
