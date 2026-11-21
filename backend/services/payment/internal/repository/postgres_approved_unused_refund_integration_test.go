package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

func TestRequireApprovedUnusedRefundAdmissionAndIdempotencyIntegration(t *testing.T) {
	pool, _ := paymentFinancialFixture(t)
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(pool)

	confirmed := func() (domain.CreateOperation, ApprovedUnusedRefundIdentity) {
		t.Helper()
		return seedConfirmedUnusedRefundPayment(t, ctx, pool, repo)
	}
	obligation := func(op domain.CreateOperation) domain.RefundObligation {
		return domain.RefundObligation{PaymentID: op.PaymentID, DecisionID: uuid.New(), Generation: 1,
			AmountCents: op.Identity.AmountCents, Currency: "usd", Reason: domain.RefundReasonConfirmedUnusedTicket,
			Key: "ticketbox:payment:" + op.PaymentID.String() + ":g1:refund"}
	}

	// The admission latch is created inactive by migration. Do not accept a new
	// decision until the Booking/Payment admission fence has been activated.
	inactiveOp, inactiveIdentity := confirmed()
	inactiveObligation := obligation(inactiveOp)
	if _, err := repo.RequireApprovedUnusedRefund(ctx, inactiveObligation, inactiveIdentity); !errors.Is(err, admission.ErrUnavailable) {
		t.Fatalf("inactive admission accepted approved unused refund: %v", err)
	}
	inactiveView, err := repo.GetFinancial(ctx, inactiveOp.PaymentID)
	if err != nil || inactiveView.RefundDecisionID != nil || inactiveView.BookingDisposition != "CONFIRMED" {
		t.Fatalf("inactive rejection changed Payment obligation/disposition: view=%+v err=%v", inactiveView, err)
	}
	if err := repo.ActivatePaymentAdmission(ctx); err != nil {
		t.Fatal(err)
	}

	acceptedOp, acceptedIdentity := confirmed()
	acceptedObligation := obligation(acceptedOp)
	accepted, err := repo.RequireApprovedUnusedRefund(ctx, acceptedObligation, acceptedIdentity)
	if err != nil || accepted.RefundDecisionID == nil || *accepted.RefundDecisionID != acceptedObligation.DecisionID ||
		accepted.RefundState != "pending" || accepted.BookingDisposition != "CONFIRMED" || accepted.Payment.Status != domain.PaymentSuccess {
		t.Fatalf("accepted unused refund=%+v err=%v", accepted, err)
	}

	// Exact delivery replay must remain idempotent for both pending and a later
	// completed refund, preserving the original collected Payment projection.
	replayed, err := repo.RequireApprovedUnusedRefund(ctx, acceptedObligation, acceptedIdentity)
	if err != nil || replayed.RefundDecisionID == nil || *replayed.RefundDecisionID != acceptedObligation.DecisionID ||
		replayed.RefundState != "pending" || replayed.BookingDisposition != "CONFIRMED" || replayed.Payment.Status != domain.PaymentSuccess {
		t.Fatalf("pending exact replay=%+v err=%v", replayed, err)
	}
	changedDecision := acceptedObligation
	changedDecision.DecisionID = uuid.New()
	if _, err := repo.RequireApprovedUnusedRefund(ctx, changedDecision, acceptedIdentity); !errors.Is(err, ErrFinancialConflict) {
		t.Fatalf("same payment with a different immutable Booking decision error=%v; want financial conflict", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET refund_state='succeeded',refund_provider_id='re_unused_full',
		refund_provider_status='succeeded',refund_completed_at=clock_timestamp(),reconciliation_state='resolved' WHERE id=$1`, acceptedOp.PaymentID); err != nil {
		t.Fatal(err)
	}
	replayed, err = repo.RequireApprovedUnusedRefund(ctx, acceptedObligation, acceptedIdentity)
	if err != nil || replayed.RefundState != "succeeded" || replayed.Payment.Status != domain.PaymentSuccess ||
		replayed.BookingDisposition != "CONFIRMED" || replayed.RefundAmountCents != acceptedObligation.AmountCents {
		t.Fatalf("completed exact replay=%+v err=%v", replayed, err)
	}

	// A committed admission is an immutable scan outcome. It blocks a later
	// confirmed-unused refund before any refund decision is persisted.
	committedOp, committedIdentity := confirmed()
	permitIdentity := unusedRefundAdmissionIdentity(committedOp)
	permit, err := repo.GrantAdmission(ctx, permitIdentity, domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible, DisputeState: domain.DisputeEvidenceNoDispute,
		RefundState: domain.RefundEvidenceNoRefund, ObservedAt: time.Now().UTC(), Source: "stripe_test_api",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SettleAdmission(ctx, permitIdentity, permit.Token, permit.Fence, admission.Committed); err != nil {
		t.Fatal(err)
	}
	blockedObligation := obligation(committedOp)
	if _, err := repo.RequireApprovedUnusedRefund(ctx, blockedObligation, committedIdentity); !errors.Is(err, ErrFinancialConflict) {
		t.Fatalf("refund after COMMITTED admission error=%v; want financial conflict", err)
	}
	blockedView, err := repo.GetFinancial(ctx, committedOp.PaymentID)
	if err != nil || blockedView.RefundDecisionID != nil || blockedView.RefundState != "not_required" || blockedView.BookingDisposition != "CONFIRMED" {
		t.Fatalf("COMMITTED conflict persisted an obligation: view=%+v err=%v", blockedView, err)
	}
}

func seedConfirmedUnusedRefundPayment(t *testing.T, ctx context.Context, pool *pgxpool.Pool, repo *PostgresPaymentRepository) (domain.CreateOperation, ApprovedUnusedRefundIdentity) {
	t.Helper()
	op := financialFixtureOperation()
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	intentID := "pi_confirmed_unused_" + uuid.NewString()
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		Account: op.Identity.ProviderAccount, Status: "succeeded", IntentID: intentID, PaymentID: op.PaymentID,
		BookingID: op.Identity.BookingID, Generation: op.Identity.Generation, AmountCents: op.Identity.AmountCents,
		Currency: op.Identity.Currency, Collected: true, ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET reconciliation_state='sale_verified',booking_disposition='CONFIRMED' WHERE id=$1`, op.PaymentID); err != nil {
		t.Fatal(err)
	}
	return op, ApprovedUnusedRefundIdentity{
		BookingID: op.Identity.BookingID, UserID: op.Identity.UserID, Generation: op.Identity.Generation,
		ProviderAccount: op.Identity.ProviderAccount, LiveMode: op.Identity.LiveMode,
		AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency, PaymentIntentID: intentID,
	}
}

func unusedRefundAdmissionIdentity(op domain.CreateOperation) admission.Identity {
	actor := uuid.New()
	keyDigest := sha256.Sum256([]byte("unused refund admission key:" + uuid.NewString()))
	fingerprint := sha256.Sum256([]byte("unused refund admission request:" + uuid.NewString()))
	return admission.Identity{RequestID: admission.RequestIDFor(actor, keyDigest), PaymentID: op.PaymentID,
		BookingID: op.Identity.BookingID, TicketID: uuid.New(), EventID: uuid.New(), UserID: op.Identity.UserID,
		ActorID: actor, Generation: op.Identity.Generation, KeyDigest: keyDigest, Fingerprint: fingerprint}
}
