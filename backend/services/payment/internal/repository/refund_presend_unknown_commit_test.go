package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ticketbox/payment/internal/domain"
)

func TestRefundPreSendSimulatedCommitAckLossNeverReturnsAllow(t *testing.T) {
	claim := refundPreSendUnknownCommitClaim()
	tx := &refundPreSendUnknownCommitTx{claim: claim}
	repo := &PostgresPaymentRepository{
		beginRefundPreSendTx: func(context.Context) (refundPreSendTx, error) { return tx, nil },
	}

	result, err := repo.CheckRefundPreSend(context.Background(), claim)
	if err == nil || result.Decision == RefundPreSendAllow || result.AttemptID != uuid.Nil {
		t.Fatalf("SIMULATED_COMMIT_ACK_LOSS result=%+v err=%v; want no allow/no attempt identity", result, err)
	}
	if !tx.auditInsertAttempted || !tx.commitCalled || !tx.serverMayHaveCommitted {
		t.Fatalf("SIMULATED_COMMIT_ACK_LOSS did not reach the unknown-commit boundary: %+v", tx)
	}
}

func TestConfirmedUnusedRefundPreSendRejectsCommittedAdmissionWithoutPreparing(t *testing.T) {
	claim := refundPreSendUnknownCommitClaim()
	claim.Refund.Reason = domain.RefundReasonConfirmedUnusedTicket
	tx := &confirmedUnusedCommittedAdmissionTx{claim: claim}
	repo := &PostgresPaymentRepository{
		beginRefundPreSendTx: func(context.Context) (refundPreSendTx, error) { return tx, nil },
	}

	result, err := repo.CheckRefundPreSend(context.Background(), claim)
	if err != nil || result.Decision != RefundPreSendAdmissionConflict || result.Reason != "committed_admission" || result.AttemptID != uuid.Nil {
		t.Fatalf("committed Admission result=%+v err=%v; want conflict with no PREPARED attempt", result, err)
	}
	if tx.auditInsertAttempted || !tx.commitCalled || !tx.committedAdmissionRead {
		t.Fatalf("committed Admission guard did not commit a no-PREPARED conflict: %+v", tx)
	}
}

type confirmedUnusedCommittedAdmissionTx struct {
	claim                  domain.FinancialClaim
	committedAdmissionRead bool
	auditInsertAttempted   bool
	commitCalled           bool
}

func (tx *confirmedUnusedCommittedAdmissionTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(query, "FROM payment_admission_mode"):
		// The guard must inspect prior COMMITTED history even if activation is
		// currently false; legacy refund reasons retain their old behavior.
		return refundPreSendUnknownCommitRow{values: []any{false}}
	case strings.Contains(query, "SELECT id FROM payments WHERE"):
		return refundPreSendUnknownCommitRow{values: []any{tx.claim.PaymentID}}
	case strings.Contains(query, "state='COMMITTED'"):
		tx.committedAdmissionRead = true
		return refundPreSendUnknownCommitRow{values: []any{true}}
	case strings.Contains(query, "state='PENDING'"):
		return refundPreSendUnknownCommitRow{values: []any{false}}
	default:
		claim := tx.claim
		createKey := "ticketbox:payment:" + claim.PaymentID.String() + ":g1:create"
		cancelKey := "ticketbox:payment:" + claim.PaymentID.String() + ":g1:cancel"
		operation := "refund"
		reason := claim.Refund.Reason
		row := []any{
			claim.PaymentID, &claim.Token, claim.Fence, true, &operation,
			claim.Operation.Identity.BookingID, claim.Operation.Identity.UserID, claim.Operation.Identity.Generation,
			claim.Operation.Identity.ProviderAccount, claim.Operation.Identity.LiveMode, claim.Operation.Identity.AmountCents,
			claim.Operation.Identity.Currency, string(domain.PaymentSuccess), "collected", claim.View.Payment.PaymentIntentId,
			claim.Operation.Identity.Fingerprint[:], []byte(claim.Operation.FrozenParams), createKey, cancelKey, claim.Refund.Key,
			"claimed", (*string)(nil), (*string)(nil), &claim.Refund.DecisionID, &claim.Refund.AmountCents,
			&claim.Refund.Currency, &reason,
		}
		return refundPreSendUnknownCommitRow{values: row}
	}
}

func (tx *confirmedUnusedCommittedAdmissionTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	if !strings.Contains(query, "INSERT INTO payment_refund_audit") {
		return pgconn.CommandTag{}, fmt.Errorf("unexpected transaction statement")
	}
	tx.auditInsertAttempted = true
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *confirmedUnusedCommittedAdmissionTx) Commit(context.Context) error {
	tx.commitCalled = true
	return nil
}

func (tx *confirmedUnusedCommittedAdmissionTx) Rollback(context.Context) error { return nil }

type refundPreSendUnknownCommitTx struct {
	claim                  domain.FinancialClaim
	queryCount             int
	auditInsertAttempted   bool
	commitCalled           bool
	serverMayHaveCommitted bool
}

func (tx *refundPreSendUnknownCommitTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.queryCount++
	if strings.Contains(query, "FROM payment_admission_mode") {
		return refundPreSendUnknownCommitRow{values: []any{false}}
	}
	if strings.Contains(query, "SELECT id FROM payments WHERE") {
		return refundPreSendUnknownCommitRow{values: []any{tx.claim.PaymentID}}
	}
	claim := tx.claim
	createKey := "ticketbox:payment:" + claim.PaymentID.String() + ":g1:create"
	cancelKey := "ticketbox:payment:" + claim.PaymentID.String() + ":g1:cancel"
	refundKey := claim.Refund.Key
	operation := "refund"
	row := []any{
		claim.PaymentID, &claim.Token, claim.Fence, true, &operation,
		claim.Operation.Identity.BookingID, claim.Operation.Identity.UserID, claim.Operation.Identity.Generation,
		claim.Operation.Identity.ProviderAccount, claim.Operation.Identity.LiveMode, claim.Operation.Identity.AmountCents,
		claim.Operation.Identity.Currency, string(domain.PaymentSuccess), "collected", claim.View.Payment.PaymentIntentId,
		claim.Operation.Identity.Fingerprint[:], []byte(claim.Operation.FrozenParams), createKey, cancelKey, refundKey,
		"claimed", (*string)(nil), (*string)(nil), &claim.Refund.DecisionID,
		&claim.Refund.AmountCents, &claim.Refund.Currency, &claim.Refund.Reason,
	}
	return refundPreSendUnknownCommitRow{values: row}
}

func (tx *refundPreSendUnknownCommitTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	if !strings.Contains(query, "INSERT INTO payment_refund_audit") {
		return pgconn.CommandTag{}, fmt.Errorf("unexpected transaction statement")
	}
	tx.auditInsertAttempted = true
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *refundPreSendUnknownCommitTx) Commit(context.Context) error {
	tx.commitCalled = true
	// Simulate the server committing the inserted preparation while the
	// acknowledgement is lost before the client can know the outcome.
	tx.serverMayHaveCommitted = tx.auditInsertAttempted
	return errors.New("connection lost waiting for COMMIT acknowledgement")
}

func (tx *refundPreSendUnknownCommitTx) Rollback(context.Context) error { return nil }

type refundPreSendUnknownCommitRow struct {
	values []any
	err    error
}

func (row refundPreSendUnknownCommitRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(dest) != len(row.values) {
		return fmt.Errorf("fake row has %d values for %d scan targets", len(row.values), len(dest))
	}
	for i, value := range row.values {
		target := reflect.ValueOf(dest[i])
		if target.Kind() != reflect.Pointer || target.IsNil() {
			return fmt.Errorf("scan target %d is not a non-nil pointer", i)
		}
		valueTarget := target.Elem()
		if value == nil {
			valueTarget.Set(reflect.Zero(valueTarget.Type()))
			continue
		}
		source := reflect.ValueOf(value)
		if source.Type().AssignableTo(valueTarget.Type()) {
			valueTarget.Set(source)
		} else if source.Type().ConvertibleTo(valueTarget.Type()) {
			valueTarget.Set(source.Convert(valueTarget.Type()))
		} else {
			return fmt.Errorf("value %d type %s cannot scan into %s", i, source.Type(), valueTarget.Type())
		}
	}
	return nil
}

func refundPreSendUnknownCommitClaim() domain.FinancialClaim {
	paymentID, bookingID, userID := uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":700,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	identity := domain.FinancialIdentity{
		BookingID: bookingID, UserID: userID, Generation: 1, ProviderAccount: "acct_unknown_commit_fixture",
		AmountCents: 700, Currency: "usd",
	}
	identity.Fingerprint, _ = domain.FinancialFingerprint(identity, frozen)
	refundKey := "ticketbox:payment:" + paymentID.String() + ":g1:refund"
	booking := bookingID
	mode := identity.LiveMode
	return domain.FinancialClaim{
		PaymentID: paymentID, Token: uuid.New(), Fence: 4, Kind: "refund", Key: refundKey,
		Operation: domain.CreateOperation{Identity: identity, PaymentID: paymentID, Key: refundKey, FrozenParams: frozen},
		View: &domain.FinancialView{
			Payment: &domain.Payment{ID: paymentID, UserId: userID, BookingId: &booking, Price: 700, Currency: "usd",
				PaymentIntentId: "pi_unknown_commit_fixture", Status: domain.PaymentSuccess},
			Generation: 1, ProviderAccount: identity.ProviderAccount, LiveMode: &mode, ProviderOutcome: "collected",
			RefundAmountCents: 700,
		},
		Refund: &domain.RefundObligation{
			PaymentID: paymentID, DecisionID: uuid.New(), Generation: 1, AmountCents: 700,
			Currency: "usd", Reason: "EXPIRED", Key: refundKey,
		},
		Attempts: 1,
	}
}
