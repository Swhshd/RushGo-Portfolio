package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/pkg/admission"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestApprovedUnusedRefundFinancialWorkerCompletesFullRefundAndReplayIntegration(t *testing.T) {
	fixture := newApprovedUnusedRefundWorkerFixture(t, 2, false)
	ctx := context.Background()
	accepted, err := fixture.accept(ctx)
	if err != nil || accepted.RefundState != "pending" || accepted.BookingDisposition != "CONFIRMED" {
		t.Fatalf("accept approved unused-refund decision: view=%+v err=%v", accepted, err)
	}
	if _, err := fixture.worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}

	view, err := fixture.repo.GetFinancial(ctx, fixture.operation.PaymentID)
	if err != nil || view.RefundState != "succeeded" || view.ReconciliationState != "resolved" ||
		view.RefundID != fixture.provider.refundID || view.RefundAmountCents != fixture.operation.Identity.AmountCents ||
		view.RefundDecisionID == nil || *view.RefundDecisionID != fixture.decisionID ||
		view.RefundReason != domain.RefundReasonConfirmedUnusedTicket || view.Payment.Status != domain.PaymentSuccess ||
		view.BookingDisposition != "CONFIRMED" {
		t.Fatalf("completed confirmed-unused refund projection=%+v err=%v", view, err)
	}

	var preparedCount int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit
		WHERE payment_id=$1 AND record_kind='PREPARED'`, fixture.operation.PaymentID).Scan(&preparedCount); err != nil || preparedCount != 1 {
		t.Fatalf("durable PREPARED records=%d err=%v; want one", preparedCount, err)
	}
	fixture.provider.mu.Lock()
	creates, inspected, keys, claims := fixture.provider.createCalls, fixture.provider.inspectCalls, append([]string(nil), fixture.provider.keys...), append([]domain.FinancialClaim(nil), fixture.provider.claims...)
	fixture.provider.mu.Unlock()
	stableKey := "ticketbox:payment:" + fixture.operation.PaymentID.String() + ":g1:refund"
	if creates != 1 || inspected != 2 || len(keys) != 1 || keys[0] != stableKey || len(claims) != 1 ||
		claims[0].Refund == nil || claims[0].Refund.Reason != domain.RefundReasonConfirmedUnusedTicket || claims[0].Refund.Key != stableKey {
		t.Fatalf("FinancialWorker Provider calls: creates=%d inspections=%d keys=%v claims=%+v", creates, inspected, keys, claims)
	}

	// Booking may redeliver the same immutable approved decision after the
	// worker completes. Payment returns the persisted outcome without creating
	// a second obligation or another Provider call.
	replayed, err := fixture.accept(ctx)
	if err != nil || replayed.RefundState != "succeeded" || replayed.RefundDecisionID == nil || *replayed.RefundDecisionID != fixture.decisionID {
		t.Fatalf("completed decision replay=%+v err=%v", replayed, err)
	}
	fixture.provider.mu.Lock()
	deferredCreates := fixture.provider.createCalls
	fixture.provider.mu.Unlock()
	if deferredCreates != 1 {
		t.Fatalf("decision replay issued extra Provider refunds: %d", deferredCreates)
	}
}

func TestApprovedUnusedRefundFinancialWorkerWaitsForPendingAdmissionIntegration(t *testing.T) {
	fixture := newApprovedUnusedRefundWorkerFixture(t, 2, false)
	ctx := context.Background()
	permitIdentity := approvedUnusedRefundAdmissionIdentity(fixture.operation)
	permit, err := fixture.repo.GrantAdmission(ctx, permitIdentity, approvedUnusedRefundProviderEvidence())
	if err != nil || permit.State != admission.Pending {
		t.Fatalf("create pending Admission permit=%+v err=%v", permit, err)
	}
	if _, err := fixture.accept(ctx); err != nil {
		t.Fatalf("accept refund while prior Admission is pending: %v", err)
	}
	if processed, err := fixture.worker.RunOnce(ctx, 1); err != nil || processed != 0 {
		t.Fatalf("worker with PENDING Admission processed=%d err=%v; want no claim", processed, err)
	}
	assertApprovedUnusedRefundWorkerCounts(t, fixture, 0, 0)
	var state string
	if err := fixture.pool.QueryRow(ctx, `SELECT refund_state FROM payments WHERE id=$1`, fixture.operation.PaymentID).Scan(&state); err != nil || state != "pending" {
		t.Fatalf("refund state while Admission pending=%q err=%v", state, err)
	}

	if _, err := fixture.repo.SettleAdmission(ctx, permitIdentity, permit.Token, permit.Fence, admission.Aborted); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE payments SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1`, fixture.operation.PaymentID); err != nil {
		t.Fatal(err)
	}
	if processed, err := fixture.worker.RunOnce(ctx, 1); err != nil || processed != 1 {
		t.Fatalf("worker after Admission abort processed=%d err=%v", processed, err)
	}
	assertApprovedUnusedRefundWorkerCounts(t, fixture, 1, 2)
	view, err := fixture.repo.GetFinancial(ctx, fixture.operation.PaymentID)
	if err != nil || view.RefundState != "succeeded" || view.BookingDisposition != "CONFIRMED" {
		t.Fatalf("refund after Admission abort=%+v err=%v", view, err)
	}
}

func TestApprovedUnusedRefundFinancialWorkerBlocksCommittedAdmissionIntegration(t *testing.T) {
	fixture := newApprovedUnusedRefundWorkerFixture(t, 2, false)
	ctx := context.Background()
	permitIdentity := approvedUnusedRefundAdmissionIdentity(fixture.operation)
	permit, err := fixture.repo.GrantAdmission(ctx, permitIdentity, approvedUnusedRefundProviderEvidence())
	if err != nil || permit.State != admission.Pending {
		t.Fatalf("create pending Admission permit=%+v err=%v", permit, err)
	}
	if _, err := fixture.accept(ctx); err != nil {
		t.Fatalf("accept refund before pending permit outcome: %v", err)
	}
	if _, err := fixture.repo.SettleAdmission(ctx, permitIdentity, permit.Token, permit.Fence, admission.Committed); err != nil {
		t.Fatal(err)
	}
	if processed, err := fixture.worker.RunOnce(ctx, 1); err != nil || processed != 1 {
		t.Fatalf("worker after Admission commit processed=%d err=%v", processed, err)
	}
	var state, lastError string
	if err := fixture.pool.QueryRow(ctx, `SELECT refund_state,COALESCE(last_error,'') FROM payments WHERE id=$1`, fixture.operation.PaymentID).
		Scan(&state, &lastError); err != nil || state != "attention" || lastError != "committed_admission_conflicts_with_unused_refund" {
		t.Fatalf("COMMITTED Admission refund state=%q lastError=%q err=%v", state, lastError, err)
	}
	var preparedCount int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit
		WHERE payment_id=$1 AND record_kind='PREPARED'`, fixture.operation.PaymentID).Scan(&preparedCount); err != nil || preparedCount != 0 {
		t.Fatalf("COMMITTED Admission PREPARED rows=%d err=%v; want none", preparedCount, err)
	}
	assertApprovedUnusedRefundWorkerCounts(t, fixture, 0, 1)
}

func TestApprovedUnusedRefundFinancialWorkerRetryReusesStableKeyAfterUnknownOutcomeIntegration(t *testing.T) {
	fixture := newApprovedUnusedRefundWorkerFixture(t, 3, true)
	ctx := context.Background()
	if _, err := fixture.accept(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := fixture.pool.QueryRow(ctx, `SELECT refund_state FROM payments WHERE id=$1`, fixture.operation.PaymentID).Scan(&state); err != nil || state != "retryable" {
		t.Fatalf("first unknown Provider outcome state=%q err=%v; want retryable", state, err)
	}
	var preparedCount int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit
		WHERE payment_id=$1 AND record_kind='PREPARED'`, fixture.operation.PaymentID).Scan(&preparedCount); err != nil || preparedCount != 1 {
		t.Fatalf("PREPARED records after unknown outcome=%d err=%v; want one", preparedCount, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE payments SET due_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1`, fixture.operation.PaymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.worker.RunOnce(ctx, 1); err != nil {
		t.Fatal(err)
	}
	view, err := fixture.repo.GetFinancial(ctx, fixture.operation.PaymentID)
	if err != nil || view.RefundState != "succeeded" {
		t.Fatalf("retry after unknown Provider outcome=%+v err=%v", view, err)
	}
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit
		WHERE payment_id=$1 AND record_kind='PREPARED'`, fixture.operation.PaymentID).Scan(&preparedCount); err != nil || preparedCount != 2 {
		t.Fatalf("PREPARED records after retry=%d err=%v; want two attempt records", preparedCount, err)
	}
	fixture.provider.mu.Lock()
	creates, keys := fixture.provider.createCalls, append([]string(nil), fixture.provider.keys...)
	fixture.provider.mu.Unlock()
	wantKey := "ticketbox:payment:" + fixture.operation.PaymentID.String() + ":g1:refund"
	if creates != 2 || len(keys) != 2 || keys[0] != wantKey || keys[1] != wantKey {
		t.Fatalf("retry changed Provider idempotency identity: creates=%d keys=%v", creates, keys)
	}
}

type approvedUnusedRefundWorkerFixture struct {
	pool       *pgxpool.Pool
	repo       *repository.PostgresPaymentRepository
	operation  domain.CreateOperation
	decisionID uuid.UUID
	service    *PaymentService
	worker     *FinancialWorker
	provider   *approvedUnusedRefundWorkerProvider
}

func newApprovedUnusedRefundWorkerFixture(t *testing.T, successOnInspection int, failFirstCreate bool) *approvedUnusedRefundWorkerFixture {
	t.Helper()
	requireFinancialWorkerRuntimeDatabase(t)
	pool := retryPaymentFixture(t)
	repo := repository.NewPostgresPaymentRepository(pool)
	ctx := context.Background()
	paymentID, bookingID, buyerID := uuid.New(), uuid.New(), uuid.New()
	frozen := json.RawMessage(`{"version":1,"amount":725,"currency":"usd","automatic_payment_methods":{"enabled":true,"allow_redirects":"always"}}`)
	op := domain.CreateOperation{
		PaymentID: paymentID, Key: "ticketbox:payment:" + paymentID.String() + ":g1:create", FrozenParams: frozen,
		Identity: domain.FinancialIdentity{BookingID: bookingID, UserID: buyerID, Generation: 1,
			ProviderAccount: "acct_fixture", AmountCents: 725, Currency: "usd"},
	}
	op.Identity.Fingerprint, _ = domain.FinancialFingerprint(op.Identity, frozen)
	if _, _, err := repo.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	intentID := "pi_unused_refund_worker_" + paymentID.String()
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		PaymentID: paymentID, BookingID: bookingID, Generation: 1, Account: op.Identity.ProviderAccount,
		IntentID: intentID, AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency,
		Status: "succeeded", Collected: true, ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE payments SET reconciliation_state='sale_verified',booking_disposition='CONFIRMED' WHERE id=$1`, paymentID); err != nil {
		t.Fatal(err)
	}
	if err := repo.ActivatePaymentAdmission(ctx); err != nil {
		t.Fatal(err)
	}
	decisionID, eventID, approverID := uuid.New(), uuid.New(), uuid.New()
	approvedAt := time.Now().UTC().Add(-time.Minute)
	proof := &bookingv1.ApprovedUnusedRefundDecision{
		RefundDecisionId: decisionID.String(), Status: "APPROVED", BuyerId: buyerID.String(),
		BookingId: bookingID.String(), EventId: eventID.String(), PaymentId: paymentID.String(),
		PaymentIntentId: intentID, ProviderAccount: op.Identity.ProviderAccount, Livemode: false,
		Generation: op.Identity.Generation, AmountCents: op.Identity.AmountCents, Currency: op.Identity.Currency,
		ApprovedAt: timestamppb.New(approvedAt), EventStartsAt: timestamppb.New(approvedAt.Add(2 * time.Hour)), ApproverId: approverID.String(),
	}
	provider := &approvedUnusedRefundWorkerProvider{
		operation: op, refundID: "re_unused_worker_" + paymentID.String(),
		successOnInspection: successOnInspection, failFirstCreate: failFirstCreate,
	}
	service := NewPaymentService(repo, zap.NewNop(), nil)
	service.SetFinancialAuthority(nil, op.Identity.ProviderAccount, false)
	service.SetApprovedUnusedRefundDecisionReader(&approvedUnusedRefundAuthorityStub{decision: proof})
	service.provider = provider
	return &approvedUnusedRefundWorkerFixture{
		pool: pool, repo: repo, operation: op, decisionID: decisionID,
		service: service, worker: NewFinancialWorker(service), provider: provider,
	}
}

func (f *approvedUnusedRefundWorkerFixture) accept(ctx context.Context) (*domain.FinancialView, error) {
	return f.service.AcceptApprovedUnusedRefundDecision(ctx, f.operation.Identity.BookingID, f.decisionID, f.operation.PaymentID, f.operation.Identity.Generation)
}

type approvedUnusedRefundWorkerProvider struct {
	paymentgateway.FinancialProvider
	mu                  sync.Mutex
	operation           domain.CreateOperation
	refundID            string
	successOnInspection int
	failFirstCreate     bool
	inspectCalls        int
	createCalls         int
	keys                []string
	claims              []domain.FinancialClaim
}

func (p *approvedUnusedRefundWorkerProvider) InspectSucceededRefunds(_ context.Context, account string, live bool, intentID string) (*domain.ProviderObservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inspectCalls++
	if p.inspectCalls >= p.successOnInspection {
		return p.observation(account, live, intentID, "succeeded", p.operation.Identity.AmountCents, p.refundID), nil
	}
	return p.observation(account, live, intentID, "no_refunds", 0, ""), nil
}

func (p *approvedUnusedRefundWorkerProvider) CreateFullRefund(ctx context.Context, claim domain.FinancialClaim, intentID string) (*domain.ProviderObservation, error) {
	p.mu.Lock()
	p.createCalls++
	createNumber := p.createCalls
	p.keys = append(p.keys, claim.Key)
	p.claims = append(p.claims, claim)
	p.mu.Unlock()

	if p.failFirstCreate && createNumber == 1 {
		return nil, errors.New("test provider transport outcome unknown")
	}
	amount := claim.Refund.AmountCents
	if capture := domain.RefundResponseCaptureFromContext(ctx); capture != nil {
		capture.RecordResponse(domain.RefundAuditResponse{
			Classification: domain.RefundAuditResponseObserved, RefundID: p.refundID,
			PaymentIntentID: intentID, ProviderStatus: "pending", Currency: claim.Refund.Currency,
			AmountCents: &amount, SummarySource: "provider_object",
		})
	}
	return p.observation(claim.Operation.Identity.ProviderAccount, claim.Operation.Identity.LiveMode, intentID,
		"pending", 0, p.refundID), nil
}

func (p *approvedUnusedRefundWorkerProvider) observation(account string, live bool, intentID, status string, succeededCents int64, refundID string) *domain.ProviderObservation {
	return &domain.ProviderObservation{
		PaymentID: p.operation.PaymentID, BookingID: p.operation.Identity.BookingID,
		Generation: p.operation.Identity.Generation, Account: account, LiveMode: live,
		IntentID: intentID, RefundID: refundID, Status: status,
		AmountCents: p.operation.Identity.AmountCents, Currency: p.operation.Identity.Currency,
		Collected: true, SucceededRefundCents: succeededCents, ObservedAt: time.Now().UTC(),
	}
}

func approvedUnusedRefundAdmissionIdentity(op domain.CreateOperation) admission.Identity {
	actorID := uuid.New()
	keyDigest := sha256.Sum256([]byte("confirmed unused refund admission key:" + uuid.NewString()))
	fingerprint := sha256.Sum256([]byte("confirmed unused refund admission request:" + uuid.NewString()))
	return admission.Identity{
		RequestID: admission.RequestIDFor(actorID, keyDigest), PaymentID: op.PaymentID,
		BookingID: op.Identity.BookingID, TicketID: uuid.New(), EventID: uuid.New(), UserID: op.Identity.UserID,
		ActorID: actorID, Generation: op.Identity.Generation, KeyDigest: keyDigest, Fingerprint: fingerprint,
	}
}

func approvedUnusedRefundProviderEvidence() domain.AdmissionProviderEvidence {
	return domain.AdmissionProviderEvidence{
		EligibilityState: domain.AdmissionEligibilityEligible, DisputeState: domain.DisputeEvidenceNoDispute,
		RefundState: domain.RefundEvidenceNoRefund, ObservedAt: time.Now().UTC(), Source: "stripe_test_api",
	}
}

func assertApprovedUnusedRefundWorkerCounts(t *testing.T, fixture *approvedUnusedRefundWorkerFixture, wantCreates, wantInspections int) {
	t.Helper()
	fixture.provider.mu.Lock()
	defer fixture.provider.mu.Unlock()
	if fixture.provider.createCalls != wantCreates || fixture.provider.inspectCalls != wantInspections {
		t.Fatalf("Provider calls create=%d inspect=%d; want create=%d inspect=%d", fixture.provider.createCalls, fixture.provider.inspectCalls, wantCreates, wantInspections)
	}
}
