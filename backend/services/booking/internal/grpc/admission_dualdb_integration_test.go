//go:build admission_integration

package grpc

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/admissiontest"
	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

const (
	dualDBPaymentBinaryEnv = "ADMISSION_PAYMENT_TEST_BINARY"
	dualDBOwnerManifestEnv = "ADMISSION_PAYMENT_OWNER_MANIFEST"
	dualDBChildEnvBooking  = "RUSHGO_ADMISSION_PAYMENT_PROCESS"
	dualDBConfigEnvBooking = "RUSHGO_ADMISSION_PAYMENT_CONFIG"
	dualDBTestOptInValue   = "1"
	dualDBBookingServer    = "booking-admission.test"
	dualDBPaymentServer    = "payment-admission.test"
	dualDBProviderAccount  = "acct_admission_fixture"
)

type dualDBFixture struct {
	BookingID, EventID, UserID, StaffID uuid.UUID
	PaymentID, SagaID, ItemID           uuid.UUID
	TierID                              uuid.UUID
	TicketIDs                           []uuid.UUID
	SeatIDs                             []uuid.UUID
	Credentials                         []string
}

type dualDBEvidenceAuthority struct{}

func (dualDBEvidenceAuthority) CheckTicketAuthorities(context.Context, domain.TicketIssuanceEvent, domain.TicketIssuanceCandidate) (domain.TicketAuthorityEvidence, error) {
	now := time.Now().UTC()
	return domain.TicketAuthorityEvidence{
		SagaEligible: true, EventEligible: true, PaymentDecisionKnown: true, PaymentEligible: true,
		DisputeState: domain.DisputeNoDispute, RefundState: domain.RefundNoRefund,
		ObservedAt: now, PaymentQueryStartedAt: now, PaymentQueryWallStartedAt: now.Round(0), Source: "stripe_test_api",
	}, nil
}

type dualDBPaymentConfig struct {
	Version                int    `json:"version"`
	DatabaseURL            string `json:"database_url"`
	BookingAddress         string `json:"booking_address"`
	BookingServerName      string `json:"booking_server_name"`
	BookingClientCert      string `json:"booking_client_cert"`
	BookingClientKey       string `json:"booking_client_key"`
	PaymentServerCert      string `json:"payment_server_cert"`
	PaymentServerKey       string `json:"payment_server_key"`
	CAFile                 string `json:"ca_file"`
	PaymentID              string `json:"payment_id"`
	BookingID              string `json:"booking_id"`
	UserID                 string `json:"user_id"`
	AmountCents            int64  `json:"amount_cents"`
	PaymentIntentID        string `json:"payment_intent_id"`
	ProviderAccount        string `json:"provider_account"`
	SeedFinancial          bool   `json:"seed_financial"`
	DropPrepareReplyOnce   bool   `json:"drop_prepare_reply_once"`
	DropReconcileReplyOnce bool   `json:"drop_reconcile_reply_once"`
}

type dualDBReady struct {
	GRPCAddress    string `json:"grpc_address"`
	ControlAddress string `json:"control_address"`
}

type dualDBControlResponse struct {
	OK            bool   `json:"ok"`
	Processed     int    `json:"processed,omitempty"`
	ProviderCalls int64  `json:"provider_calls,omitempty"`
	RefundCalls   int64  `json:"refund_calls,omitempty"`
	Error         string `json:"error,omitempty"`
}

type dualDBPaymentProcess struct {
	command *exec.Cmd
	ready   dualDBReady
	done    <-chan struct{}
	waitErr error
	client  *http.Client
}

// TestAdmissionDualDatabaseMTLSProcessRecoveryAndRefundFence exercises real
// Booking and Payment repositories in independently owned PostgreSQLs. The
// Payment authority runs in a separately compiled OS process, and both
// admission RPC directions use verified mTLS. Its provider is a local double.
func TestAdmissionDualDatabaseMTLSProcessRecoveryAndRefundFence(t *testing.T) {
	admissiontest.RequireOptIn(t)
	bookingDSN := os.Getenv("BOOKING_TEST_DATABASE_URL")
	paymentDSN := os.Getenv("REFUND_COMPONENT_DATABASE_URL")
	admissiontest.VerifyBookingPostgres(t, bookingDSN)
	admissiontest.VerifyRefundComponentPostgres(t, paymentDSN, os.Getenv(dualDBOwnerManifestEnv))
	admissiontest.RequireDistinctPostgresEndpoints(t, bookingDSN, paymentDSN)
	paymentBinary := strings.TrimSpace(os.Getenv(dualDBPaymentBinaryEnv))
	if paymentBinary == "" {
		t.Fatal("BLOCKED: ADMISSION_PAYMENT_TEST_BINARY must point to the independently compiled Payment gRPC test binary")
	}
	if info, err := os.Stat(paymentBinary); err != nil || info.IsDir() {
		t.Fatal("BLOCKED: independently compiled Payment test binary is missing")
	}

	bookingSchema := admissiontest.OpenMigrationSchema(t, bookingDSN, "../../migrations", 12)
	paymentSchema := admissiontest.OpenMigrationSchema(t, paymentDSN, "../../../payment/migrations", 12)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	fixture := seedDualDBBookingRows(t, bookingSchema.Pool)
	ca := admissiontest.NewPKI(t, "RushGo admission dual database integration CA")
	certDir := t.TempDir()
	caPath := admissiontest.WriteCA(t, certDir, ca.CAPEM)
	bookingServerLeaf := ca.NewLeaf(t, dualDBBookingServer, grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageServerAuth)
	bookingClientLeaf := ca.NewLeaf(t, "", grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageClientAuth)
	paymentServerLeaf := ca.NewLeaf(t, dualDBPaymentServer, grpcsecurity.ServiceIdentityPayment, x509.ExtKeyUsageServerAuth)
	paymentClientLeaf := ca.NewLeaf(t, "", grpcsecurity.ServiceIdentityPayment, x509.ExtKeyUsageClientAuth)
	wrongClientLeaf := ca.NewLeaf(t, "", grpcsecurity.ServiceIdentityGateway, x509.ExtKeyUsageClientAuth)
	paymentServerCert, paymentServerKey := admissiontest.WriteLeaf(t, certDir, "payment-server", paymentServerLeaf)
	paymentClientCert, paymentClientKey := admissiontest.WriteLeaf(t, certDir, "payment-client", paymentClientLeaf)

	bookingServerCredentials, err := grpcsecurity.NewServerCredentials(bookingServerLeaf.CertificatePEM, bookingServerLeaf.PrivateKeyPEM, ca.CAPEM, []string{grpcsecurity.ServiceIdentityPayment, grpcsecurity.ServiceIdentityGateway})
	if err != nil {
		t.Fatal("create Booking admission mTLS server credentials:", err)
	}
	bookingListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen for isolated Booking admission RPC:", err)
	}
	bookingRepo := repository.NewPostgresBookingRepository(bookingSchema.Pool)
	bookingRPC := grpc.NewServer(grpc.Creds(bookingServerCredentials))
	admissionv1.RegisterBookingAdmissionAuthorityServer(bookingRPC, NewAdmissionServer(bookingRepo))
	bookingServeErr := make(chan error, 1)
	go func() { bookingServeErr <- bookingRPC.Serve(bookingListener) }()
	t.Cleanup(func() {
		bookingRPC.Stop()
		_ = bookingListener.Close()
	})

	config := dualDBPaymentConfig{
		Version: 1, DatabaseURL: paymentSchema.DSN,
		BookingAddress: bookingListener.Addr().String(), BookingServerName: dualDBBookingServer,
		BookingClientCert: paymentClientCert, BookingClientKey: paymentClientKey,
		PaymentServerCert: paymentServerCert, PaymentServerKey: paymentServerKey, CAFile: caPath,
		PaymentID: fixture.PaymentID.String(), BookingID: fixture.BookingID.String(), UserID: fixture.UserID.String(),
		AmountCents: 2175, PaymentIntentID: "pi_admission_integration_local", ProviderAccount: dualDBProviderAccount,
		SeedFinancial: true, DropPrepareReplyOnce: true,
	}
	configPath := filepath.Join(certDir, "payment-process.json")
	paymentProcess := startDualDBPaymentProcess(t, paymentBinary, configPath, config)
	t.Cleanup(func() { paymentProcess.stop(t) })
	var bookingClient *grpc.ClientConn
	t.Cleanup(func() {
		if bookingClient != nil {
			_ = bookingClient.Close()
		}
	})
	bookingClient = newDualDBClient(t, paymentProcess.ready.GRPCAddress, dualDBPaymentServer, bookingClientLeaf, ca.CAPEM, grpcsecurity.ServiceIdentityBooking)
	coordinator := admission.NewCoordinatorClient(bookingClient)

	// Activation follows the service order: both mTLS listeners are serving,
	// then Booking's SQL guard is latched before Payment's admission latch.
	if _, err := bookingSchema.Pool.Exec(ctx, `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal("activate Booking test admission guard:", err)
	}
	if response := paymentProcess.post(t, "/v1/activate"); !response.OK {
		t.Fatal("activate Payment test admission guard")
	}

	// A post-commit Prepare reply is deliberately dropped by the Payment
	// process. Booking's intent survives without an attached permit, while the
	// durable Payment grant is recovered after an actual OS process kill.
	keyA := "dualdb-abort-before-attach"
	identityA := dualDBIdentity(fixture, 0, keyA)
	if err := bookingRepo.EnsureAdmissionIntent(ctx, identityA); err != nil {
		t.Fatal("persist pre-grant Booking intent:", err)
	}
	if _, err := coordinator.PrepareAdmission(ctx, identityA); err == nil {
		t.Fatal("PrepareAdmission response loss was not observed")
	}
	permitA := readDualDBPermit(t, paymentSchema.Pool, identityA.RequestID)
	t.Logf("ADMISSION_BOUNDARY phase=payment_grant_observed observed_utc=%s", time.Now().UTC().Format(time.RFC3339Nano))
	if permitA.State != string(admission.Pending) || permitA.Token == uuid.Nil || permitA.Fence <= 0 {
		t.Fatalf("dropped grant response did not leave a durable pending Payment permit: %+v", permitA)
	}
	intentA := readDualDBIntent(t, bookingSchema.Pool, identityA.RequestID)
	if intentA.State != string(admission.Pending) || intentA.Token != uuid.Nil || intentA.Fence != 0 {
		t.Fatalf("Booking intent unexpectedly attached a lost response: %+v", intentA)
	}
	if got := paymentProcess.get(t, "/v1/provider-count").ProviderCalls; got != 1 {
		t.Fatalf("Payment provider double calls after lost grant ack=%d, want 1", got)
	}
	if _, err := paymentSchema.Pool.Exec(ctx, `UPDATE payment_admission_permits SET next_attempt_at=clock_timestamp()-INTERVAL '1 second' WHERE request_id=$1`, identityA.RequestID); err != nil {
		t.Fatal("make only the owned pending permit due for recovery:", err)
	}
	paymentProcess.kill(t)
	t.Logf("ADMISSION_BOUNDARY phase=payment_process_terminated observed_utc=%s", time.Now().UTC().Format(time.RFC3339Nano))

	config.SeedFinancial = false
	config.DropPrepareReplyOnce = false
	config.DropReconcileReplyOnce = true
	paymentProcess = startDualDBPaymentProcess(t, paymentBinary, configPath, config)
	_ = bookingClient.Close()
	bookingClient = newDualDBClient(t, paymentProcess.ready.GRPCAddress, dualDBPaymentServer, bookingClientLeaf, ca.CAPEM, grpcsecurity.ServiceIdentityBooking)
	coordinator = admission.NewCoordinatorClient(bookingClient)
	manager := service.NewTicketManager(bookingRepo, dualDBEvidenceAuthority{})
	manager.SetAdmissionCoordinator(coordinator)

	recovery := paymentProcess.post(t, "/v1/recover-once")
	t.Logf("ADMISSION_BOUNDARY phase=recovery_response observed_utc=%s", time.Now().UTC().Format(time.RFC3339Nano))
	if !recovery.OK || recovery.Processed != 1 {
		t.Fatalf("Payment restart recovery result=%+v; want one durable pending permit processed", recovery)
	}
	permitA = readDualDBPermit(t, paymentSchema.Pool, identityA.RequestID)
	intentA = readDualDBIntent(t, bookingSchema.Pool, identityA.RequestID)
	if permitA.State != string(admission.Aborted) || intentA.State != string(admission.Aborted) || intentA.Token != permitA.Token || intentA.Fence != permitA.Fence {
		t.Fatalf("restart recovery did not persist matching abort tombstones: Payment=%+v Booking=%+v", permitA, intentA)
	}
	assertDualDBTicketState(t, bookingSchema.Pool, fixture.TicketIDs[0], "ISSUED")
	assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[0], 0, 0)
	if _, err := manager.CheckIn(ctx, fixture.EventID, fixture.StaffID, fixture.Credentials[0], keyA); err == nil {
		t.Fatal("a later Booking process crossed the durable admission abort tombstone")
	}
	assertDualDBTicketState(t, bookingSchema.Pool, fixture.TicketIDs[0], "ISSUED")
	assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[0], 0, 0)
	if got := paymentProcess.get(t, "/v1/provider-count").ProviderCalls; got != 0 {
		t.Fatalf("recovery or tombstone replay called the eligibility provider after restart: %d", got)
	}

	// Create a second, independent Ticket permit. A refund obligation arriving
	// after the permit is durably pending is held by the real refund selector;
	// the normal Booking transaction then commits and reconciliation releases
	// the worker. The first Reconcile reply is dropped after Payment commits it.
	keyB := "dualdb-admission-first-refund-waits"
	identityB := dualDBIdentity(fixture, 1, keyB)
	if err := bookingRepo.EnsureAdmissionIntent(ctx, identityB); err != nil {
		t.Fatal("persist second Booking admission intent:", err)
	}
	permitB, err := coordinator.PrepareAdmission(ctx, identityB)
	if err != nil {
		t.Fatal("prepare second real Payment permit:", err)
	}
	if permitB.State != admission.Pending {
		t.Fatalf("new second Payment permit state=%s, want PENDING", permitB.State)
	}
	if retry, err := coordinator.PrepareAdmission(ctx, identityB); err != nil || retry.Token != permitB.Token || retry.Fence != permitB.Fence {
		t.Fatalf("same identity did not return its immutable permit: retry=%+v err=%v", retry, err)
	}
	if _, err := coordinator.ReconcileAdmission(ctx, identityB, permitB.Token, permitB.Fence+1); err == nil {
		t.Fatal("stale Payment permit fence was accepted")
	}
	if !paymentProcess.post(t, "/v1/require-refund").OK {
		t.Fatal("persist valid isolated refund obligation while admission is pending")
	}
	waiting := paymentProcess.post(t, "/v1/refund-once")
	if !waiting.OK || waiting.Processed != 0 || waiting.RefundCalls != 0 {
		t.Fatalf("refund worker crossed a pending admission: %+v", waiting)
	}
	assertPaymentPreparedCount(t, paymentSchema.Pool, fixture.PaymentID, 0)

	result, err := manager.CheckIn(ctx, fixture.EventID, fixture.StaffID, fixture.Credentials[1], keyB)
	t.Logf("ADMISSION_BOUNDARY phase=booking_checkin_response observed_utc=%s", time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil || result.ResultCode != "accepted" || result.ReasonCode != "checked_in" {
		t.Fatalf("real Booking check-in result=%+v err=%v", result, err)
	}
	if result.CheckedInAt == nil {
		t.Fatal("accepted check-in has no durable timestamp")
	}
	permitBState := readDualDBPermit(t, paymentSchema.Pool, identityB.RequestID)
	intentB := readDualDBIntent(t, bookingSchema.Pool, identityB.RequestID)
	if permitBState.State != string(admission.Committed) || intentB.State != string(admission.Committed) || intentB.Token != permitB.Token || intentB.Fence != permitB.Fence {
		t.Fatalf("lost reconciliation acknowledgement did not retain matching durable commit: Payment=%+v Booking=%+v", permitBState, intentB)
	}
	assertDualDBTicketState(t, bookingSchema.Pool, fixture.TicketIDs[1], "CHECKED_IN")
	assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[1], 1, 1)

	// Replay the completed operation and verify payload conflict and stale-fence
	// behavior against the same durable identity, not a mock client.
	replayed, err := manager.CheckIn(ctx, fixture.EventID, fixture.StaffID, fixture.Credentials[1], keyB)
	if err != nil || !replayed.Replayed || replayed.ResultCode != "accepted" {
		t.Fatalf("same-key completed scan replay=%+v err=%v", replayed, err)
	}
	if _, err := manager.CheckIn(ctx, fixture.EventID, fixture.StaffID, differentCredential(fixture.Credentials[1]), keyB); !errors.Is(err, repository.ErrTicketIdempotencyConflict) {
		t.Fatalf("same key with different scan payload error=%v, want idempotency conflict", err)
	}
	assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[1], 1, 1)

	refundDone := paymentProcess.post(t, "/v1/refund-once")
	if !refundDone.OK || refundDone.Processed != 1 || refundDone.RefundCalls != 1 {
		t.Fatalf("refund did not resume after durable Booking completion: %+v", refundDone)
	}
	assertPaymentPreparedCount(t, paymentSchema.Pool, fixture.PaymentID, 1)
	t.Logf("ADMISSION_BOUNDARY phase=refund_prepared_observed observed_utc=%s", time.Now().UTC().Format(time.RFC3339Nano))
	var responseAuditCount int
	if err := paymentSchema.Pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit WHERE payment_id=$1 AND record_kind='RESPONSE'`, fixture.PaymentID).Scan(&responseAuditCount); err != nil || responseAuditCount != 1 {
		t.Fatalf("local provider response audit count=%d err=%v, want 1", responseAuditCount, err)
	}

	// Refund PREPARED is durable history. A third Ticket on this Payment must
	// fail closed and must not create an admission permit or scan/audit fact.
	keyC := "dualdb-refund-first-denies-admission"
	identityC := dualDBIdentity(fixture, 2, keyC)
	result, err = manager.CheckIn(ctx, fixture.EventID, fixture.StaffID, fixture.Credentials[2], keyC)
	if err == nil || result.ResultCode == "accepted" {
		t.Fatalf("admission after refund PREPARED was allowed: result=%+v err=%v", result, err)
	}
	var permitCount int
	if err := paymentSchema.Pool.QueryRow(ctx, `SELECT count(*) FROM payment_admission_permits WHERE request_id=$1`, identityC.RequestID).Scan(&permitCount); err != nil || permitCount != 0 {
		t.Fatalf("refund-first request has %d Payment permits err=%v; want none", permitCount, err)
	}
	assertDualDBTicketState(t, bookingSchema.Pool, fixture.TicketIDs[2], "ISSUED")
	assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[2], 0, 0)
	assertDualDBScanFacts(t, bookingSchema.Pool, fixture.TicketIDs[1], 1, 1)

	assertWrongMTLSIdentityRejected(t, bookingListener.Addr().String(), paymentClientLeaf, wrongClientLeaf, ca.CAPEM, permitB)
	writeDualDBEvidence(t, bookingSchema.Name, paymentSchema.Name, permitA, intentA, permitBState, intentB, waiting, refundDone, responseAuditCount)
}

func seedDualDBBookingRows(t testing.TB, pool *pgxpool.Pool) dualDBFixture {
	t.Helper()
	f := dualDBFixture{
		BookingID: uuid.New(), EventID: uuid.New(), UserID: uuid.New(), StaffID: uuid.New(),
		PaymentID: uuid.New(), SagaID: uuid.New(), ItemID: uuid.New(), TierID: uuid.New(),
		TicketIDs: []uuid.UUID{uuid.New(), uuid.New(), uuid.New()},
		SeatIDs:   []uuid.UUID{uuid.New(), uuid.New(), uuid.New()},
	}
	f.Credentials = make([]string, len(f.TicketIDs))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `INSERT INTO bookings(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,lifecycle_phase,decision_kind,decision_id,currency,booking_mode,reservation_deadline,saga_id)
		VALUES($1,$2,$3,'CONFIRMED',2175,1,true,1,'TERMINAL','confirm',$4,'usd','pessimistic',clock_timestamp()+INTERVAL '1 hour',$5)`, f.BookingID, f.UserID, f.EventID, uuid.New(), f.SagaID); err != nil {
		t.Fatal("seed isolated managed Booking:", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO booking_items(id,booking_id,ticket_tier_id,quantity,unit_price_cents,seat_ids) VALUES($1,$2,$3,3,725,$4)`,
		f.ItemID, f.BookingID, f.TierID, f.SeatIDs); err != nil {
		t.Fatal("seed isolated Booking item:", err)
	}
	for index, ticketID := range f.TicketIDs {
		credentialBytes := make([]byte, 32)
		if _, err := rand.Read(credentialBytes); err != nil {
			t.Fatal("generate isolated Ticket credential:", err)
		}
		credential := base64.RawURLEncoding.EncodeToString(credentialBytes)
		f.Credentials[index] = credential
		digest := sha256.Sum256([]byte(credential))
		if _, err := pool.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation,status)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,'ISSUED')`, ticketID, f.BookingID, f.ItemID, f.SagaID, f.PaymentID, f.EventID, f.TierID, f.SeatIDs[index]); err != nil {
			t.Fatal("seed isolated ISSUED Ticket:", err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO ticket_credentials(ticket_id,token_digest,expires_at,credential_version)
			VALUES($1,$2,clock_timestamp()+INTERVAL '5 minutes',1)`, ticketID, digest[:]); err != nil {
			t.Fatal("seed isolated Ticket credential:", err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO event_scanner_assignments(event_id,staff_user_id,active,granted_by,granted_at)
		VALUES($1,$2,true,$3,clock_timestamp())`, f.EventID, f.StaffID, uuid.New()); err != nil {
		t.Fatal("seed isolated event scanner assignment:", err)
	}
	return f
}

func dualDBIdentity(f dualDBFixture, ticketIndex int, key string) admission.Identity {
	keyDigest := sha256.Sum256([]byte(key))
	credentialDigest := sha256.Sum256([]byte(f.Credentials[ticketIndex]))
	request := make([]byte, 0, 48)
	request = append(request, f.EventID[:]...)
	request = append(request, credentialDigest[:]...)
	fingerprint := sha256.Sum256(request)
	identity := admission.Identity{
		RequestID: admission.RequestIDFor(f.StaffID, keyDigest), PaymentID: f.PaymentID,
		BookingID: f.BookingID, TicketID: f.TicketIDs[ticketIndex], EventID: f.EventID,
		UserID: f.UserID, ActorID: f.StaffID, Generation: 1,
		KeyDigest: keyDigest, Fingerprint: fingerprint,
	}
	return identity
}

type dualDBPermit struct {
	State string
	Token uuid.UUID
	Fence int64
}

type dualDBIntent struct {
	State  string
	Token  uuid.UUID
	Fence  int64
	Result *string
}

func readDualDBPermit(t testing.TB, pool *pgxpool.Pool, requestID uuid.UUID) dualDBPermit {
	t.Helper()
	var result dualDBPermit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, `SELECT state,token,fence FROM payment_admission_permits WHERE request_id=$1`, requestID).Scan(&result.State, &result.Token, &result.Fence); err != nil {
		t.Fatal("read durable Payment permit:", err)
	}
	return result
}

func readDualDBIntent(t testing.TB, pool *pgxpool.Pool, requestID uuid.UUID) dualDBIntent {
	t.Helper()
	var result dualDBIntent
	var token *uuid.UUID
	var fence *int64
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, `SELECT state,permit_token,permit_fence,result_code FROM ticket_admission_intents WHERE request_id=$1`, requestID).
		Scan(&result.State, &token, &fence, &result.Result); err != nil {
		t.Fatal("read durable Booking intent:", err)
	}
	if token != nil {
		result.Token = *token
	}
	if fence != nil {
		result.Fence = *fence
	}
	return result
}

func assertDualDBTicketState(t testing.TB, pool *pgxpool.Pool, ticketID uuid.UUID, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got string
	if err := pool.QueryRow(ctx, `SELECT status FROM tickets WHERE id=$1`, ticketID).Scan(&got); err != nil {
		t.Fatal("read Ticket state:", err)
	}
	if got != want {
		t.Fatalf("Ticket %s state=%s, want %s", ticketID, got, want)
	}
}

func assertDualDBScanFacts(t testing.TB, pool *pgxpool.Pool, ticketID uuid.UUID, wantAudits, wantReceipts int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var audits, receipts, completed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1`, ticketID).Scan(&audits); err != nil {
		t.Fatal("read check-in audit count:", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_scan_requests WHERE ticket_id=$1`, ticketID).Scan(&receipts); err != nil {
		t.Fatal("read scan receipt count:", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_operation_idempotency WHERE ticket_id=$1 AND operation='CHECK_IN' AND completed_at IS NOT NULL`, ticketID).Scan(&completed); err != nil {
		t.Fatal("read completed scan idempotency count:", err)
	}
	if audits != wantAudits || receipts != wantReceipts || completed != wantReceipts {
		t.Fatalf("Ticket %s persisted check-in facts audit/receipt/completed=%d/%d/%d, want %d/%d/%d", ticketID, audits, receipts, completed, wantAudits, wantReceipts, wantReceipts)
	}
}

func assertPaymentPreparedCount(t testing.TB, pool *pgxpool.Pool, paymentID uuid.UUID, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_refund_audit WHERE payment_id=$1 AND record_kind='PREPARED'`, paymentID).Scan(&count); err != nil {
		t.Fatal("read durable refund PREPARED count:", err)
	}
	if count != want {
		t.Fatalf("Payment %s PREPARED count=%d, want %d", paymentID, count, want)
	}
}

func differentCredential(original string) string {
	decoded, _ := base64.RawURLEncoding.DecodeString(original)
	for i := range decoded {
		decoded[i] ^= 0x5a
	}
	return base64.RawURLEncoding.EncodeToString(decoded)
}

func newDualDBClient(t testing.TB, address, serverName string, client admissiontest.Leaf, caPEM []byte, identity string) *grpc.ClientConn {
	t.Helper()
	credentials, err := grpcsecurity.NewClientCredentials(client.CertificatePEM, client.PrivateKeyPEM, caPEM, serverName, identity)
	if err != nil {
		t.Fatal("create isolated mTLS gRPC client credentials:", err)
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials))
	if err != nil {
		t.Fatal("create isolated mTLS gRPC client:", err)
	}
	conn.Connect()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for conn.GetState() != connectivity.Ready {
		state := conn.GetState()
		if !conn.WaitForStateChange(ctx, state) {
			_ = conn.Close()
			t.Fatal("isolated mTLS gRPC connection did not become ready")
		}
	}
	return conn
}

func assertWrongMTLSIdentityRejected(t testing.TB, address string, authorized, wrong admissiontest.Leaf, caPEM []byte, permit admission.Permit) {
	t.Helper()
	request := &admissionv1.ResolutionRequest{Identity: admission.IdentityToProto(permit.Identity), Token: permit.Token.String(), Fence: permit.Fence}
	control := newDualDBClient(t, address, dualDBBookingServer, authorized, caPEM, grpcsecurity.ServiceIdentityPayment)
	defer control.Close()
	controlCtx, controlCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer controlCancel()
	outcome, controlErr := admissionv1.NewBookingAdmissionAuthorityClient(control).ResolveAdmission(controlCtx, request)
	if controlErr != nil || outcome.GetState() != string(admission.Committed) {
		t.Fatalf("authorized control must read the exact committed permit: %v", controlErr)
	}
	clientCredentials, err := grpcsecurity.NewClientCredentials(wrong.CertificatePEM, wrong.PrivateKeyPEM, caPEM, dualDBBookingServer, grpcsecurity.ServiceIdentityGateway)
	if err != nil {
		t.Fatal("create wrong-identity test credentials:", err)
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(clientCredentials))
	if err != nil {
		t.Fatal("create wrong-identity gRPC client:", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	_, err = admissionv1.NewBookingAdmissionAuthorityClient(conn).ResolveAdmission(ctx, request, grpc.WaitForReady(true))
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong SPIFFE identity must be denied before business logic, got %v", err)
	}
}

func startDualDBPaymentProcess(t testing.TB, binary, configPath string, config dualDBPaymentConfig) *dualDBPaymentProcess {
	t.Helper()
	logDirectory := strings.TrimSpace(os.Getenv("ADMISSION_TEST_EVIDENCE_DIR"))
	if logDirectory == "" {
		logDirectory = t.TempDir()
	}
	childLog, logErr := os.OpenFile(filepath.Join(logDirectory, "payment-process-"+uuid.NewString()+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if logErr != nil {
		t.Fatal("create synthetic Payment process evidence log")
	}
	t.Cleanup(func() { _ = childLog.Close() })
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal("encode Payment process config:", err)
	}
	if err := os.WriteFile(configPath, configBytes, 0o600); err != nil {
		t.Fatal("write test-only Payment process config:", err)
	}
	command := exec.Command(binary, "-test.v", "-test.run=^TestAdmissionPaymentProcess$")
	command.Env = append(os.Environ(),
		admissiontest.OptInEnv+"="+admissiontest.OptInValue,
		dualDBChildEnvBooking+"="+dualDBTestOptInValue,
		dualDBConfigEnvBooking+"="+configPath,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal("open Payment test process stdout:", err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal("open Payment test process stderr:", err)
	}
	if err := command.Start(); err != nil {
		t.Fatal("start independent Payment gRPC test process:", err)
	}
	done := make(chan struct{})
	process := &dualDBPaymentProcess{command: command, done: done, client: &http.Client{Timeout: 8 * time.Second}}
	go func() {
		process.waitErr = command.Wait()
		close(done)
	}()
	go func() { _, _ = io.Copy(childLog, stderr) }()
	ready := make(chan dualDBReady, 1)
	readErr := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		readyPublished := false
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = io.WriteString(childLog, line+"\n")
			if !strings.HasPrefix(line, "ADMISSION_PAYMENT_READY ") {
				continue
			}
			if readyPublished {
				continue
			}
			var value dualDBReady
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "ADMISSION_PAYMENT_READY ")), &value); err != nil {
				readErr <- errors.New("Payment test process published invalid readiness JSON")
				return
			}
			ready <- value
			readyPublished = true
		}
		if readyPublished {
			return
		}
		if err := scanner.Err(); err != nil {
			readErr <- errors.New("read Payment test process readiness output")
			return
		}
		readErr <- errors.New("Payment test process exited before readiness")
	}()
	select {
	case value := <-ready:
		if value.GRPCAddress == "" || value.ControlAddress == "" {
			_ = command.Process.Kill()
			<-done
			t.Fatal("Payment test process published empty listener address")
		}
		process.ready = value
		return process
	case err := <-readErr:
		select {
		case <-done:
			t.Fatalf("%v (exit=%v)", err, process.waitErr)
		case <-time.After(2 * time.Second):
			_ = command.Process.Kill()
			<-done
			t.Fatal(err)
		}
	case <-done:
		t.Fatalf("Payment test process exited before readiness: %v", process.waitErr)
	case <-time.After(20 * time.Second):
		_ = command.Process.Kill()
		<-done
		t.Fatal("Payment test process readiness deadline exceeded")
	}
	return nil
}

func (p *dualDBPaymentProcess) post(t testing.TB, path string) dualDBControlResponse {
	t.Helper()
	response, err := p.client.Post("http://"+p.ready.ControlAddress+path, "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("test-only loopback Payment control %s failed", path)
	}
	defer response.Body.Close()
	var body dualDBControlResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode test-only Payment control %s response", path)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("test-only Payment control %s returned status %d: %s", path, response.StatusCode, body.Error)
	}
	return body
}

func (p *dualDBPaymentProcess) get(t testing.TB, path string) dualDBControlResponse {
	t.Helper()
	response, err := p.client.Get("http://" + p.ready.ControlAddress + path)
	if err != nil {
		t.Fatalf("test-only loopback Payment control %s failed", path)
	}
	defer response.Body.Close()
	var body dualDBControlResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode test-only Payment control %s response", path)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("test-only Payment control %s returned status %d: %s", path, response.StatusCode, body.Error)
	}
	return body
}

func (p *dualDBPaymentProcess) stop(t testing.TB) {
	t.Helper()
	if p == nil || p.command == nil || p.command.Process == nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	_, _ = p.client.Post("http://"+p.ready.ControlAddress+"/v1/shutdown", "application/json", bytes.NewReader(nil))
	select {
	case <-p.done:
	case <-time.After(6 * time.Second):
		_ = p.command.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
			t.Error("Payment test process did not stop after kill")
		}
	}
}

func (p *dualDBPaymentProcess) kill(t testing.TB) {
	t.Helper()
	if p == nil || p.command == nil || p.command.Process == nil {
		t.Fatal("cannot kill missing Payment process")
	}
	select {
	case <-p.done:
		t.Fatal("Payment process exited before the requested crash boundary")
	default:
	}
	if err := p.command.Process.Kill(); err != nil {
		t.Fatalf("terminate Payment process at pending-permit boundary: %v", err)
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("killed Payment process did not exit")
	}
}

func writeDualDBEvidence(t testing.TB, bookingSchema, paymentSchema string, permitA dualDBPermit, intentA dualDBIntent, permitB dualDBPermit, intentB dualDBIntent, waiting, refundDone dualDBControlResponse, responseAuditCount int) {
	t.Helper()
	directory := strings.TrimSpace(os.Getenv("ADMISSION_TEST_EVIDENCE_DIR"))
	if directory == "" {
		return
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal("create admission test evidence directory:", err)
	}
	evidence := struct {
		Status             string                `json:"status"`
		Coverage           []string              `json:"coverage"`
		NotCovered         []string              `json:"not_covered"`
		BookingSchema      string                `json:"booking_schema"`
		PaymentSchema      string                `json:"payment_schema"`
		AbortPermit        dualDBPermit          `json:"abort_permit"`
		AbortIntent        dualDBIntent          `json:"abort_intent"`
		CommittedPermit    dualDBPermit          `json:"committed_permit"`
		CommittedIntent    dualDBIntent          `json:"committed_intent"`
		RefundWait         dualDBControlResponse `json:"refund_wait"`
		RefundAfterCommit  dualDBControlResponse `json:"refund_after_commit"`
		ResponseAuditCount int                   `json:"refund_response_audit_count"`
		RecordedAtUTC      time.Time             `json:"recorded_at_utc"`
	}{
		Status: "OBSERVED_BY_TEST_SOURCE; execution status must come from captured go test exit",
		Coverage: []string{
			"dual isolated PostgreSQL schemas use actual Booking and Payment migrations",
			"real Booking TicketManager and PostgreSQL check-in intent/audit/scan/idempotency writes",
			"real Payment AdmissionCoordinator and real Payment financial eligibility service with local provider double",
			"verified mTLS PrepareAdmission and ResolveAdmission RPCs between independently running test process and Booking test process",
			"Payment grant response lost after database commit; Payment process killed and restarted; durable abort tombstone and recovery",
			"Payment ReconcileAdmission response lost after commit; Booking ticket, audit, receipt, and Payment permit remain committed",
			"same-key retry, same-key/different-payload conflict, stale token fence rejection, wrong mTLS SPIFFE identity rejection",
			"refund selector defers while Payment permit is pending; local refund worker resumes after Booking outcome commits and writes PREPARED/RESPONSE audit",
			"refund-prepared financial state prevents a later admission permit and check-in",
		},
		NotCovered: []string{
			"48-case CP6 matrix is not exhaustive: real concurrent scanners/refund workers, lock timeout/deadlock, network partitions, revocation/cancellation, unknown commit injection, multiple Payments and activation-crossing old transaction remain separate evidence",
			"test Provider is local only; no Stripe request or external idempotency guarantee is exercised",
			"test does not claim production readiness or live deployment evidence",
		},
		BookingSchema: bookingSchema, PaymentSchema: paymentSchema,
		AbortPermit: permitA, AbortIntent: intentA, CommittedPermit: permitB, CommittedIntent: intentB,
		RefundWait: waiting, RefundAfterCommit: refundDone, ResponseAuditCount: responseAuditCount,
		RecordedAtUTC: time.Now().UTC(),
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal("encode public admission integration evidence:", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "admission-dualdb-evidence.json"), append(encoded, '\n'), 0o600); err != nil {
		t.Fatal("write public admission integration evidence:", err)
	}
}
