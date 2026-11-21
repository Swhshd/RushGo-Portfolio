//go:build admission_integration

package grpc

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/admissiontest"
	"github.com/ticketbox/pkg/grpcsecurity"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The two bounded barriers force overlap after authority observation and after
// both real Payment grant attempts. They do not change production SQL or RPC behavior.
type scannerConcurrentAuthority struct {
	observed chan struct{}
	release  chan struct{}
}

func (a *scannerConcurrentAuthority) CheckTicketAuthorities(ctx context.Context, event domain.TicketIssuanceEvent, candidate domain.TicketIssuanceCandidate) (domain.TicketAuthorityEvidence, error) {
	evidence, err := (dualDBEvidenceAuthority{}).CheckTicketAuthorities(ctx, event, candidate)
	if err != nil {
		return evidence, err
	}
	// Always include sub-microsecond precision so replay equality exercises the
	// database round trip, independent of the host clock's resolution.
	evidence.ObservedAt = evidence.ObservedAt.Truncate(time.Microsecond).Add(789 * time.Nanosecond)
	a.observed <- struct{}{}
	select {
	case <-a.release:
		return evidence, nil
	case <-ctx.Done():
		return domain.TicketAuthorityEvidence{}, ctx.Err()
	}
}

type scannerConcurrentCoordinator struct {
	admission.Coordinator
	granted  chan struct{}
	release  chan struct{}
	ticketID uuid.UUID
}

func (c *scannerConcurrentCoordinator) PrepareAdmission(ctx context.Context, identity admission.Identity) (admission.Permit, error) {
	permit, err := c.Coordinator.PrepareAdmission(ctx, identity)
	if identity.TicketID != c.ticketID {
		return permit, err
	}
	c.granted <- struct{}{}
	select {
	case <-c.release:
		return permit, err
	case <-ctx.Done():
		return admission.Permit{}, ctx.Err()
	}
}

// User/revocation and Event/Saga are contract doubles. JWT signatures are
// actually verified; the production Booking principal authenticator is used.
type scannerConcurrentUsers struct {
	userv1.UserServiceClient
	secret []byte
}

func (u scannerConcurrentUsers) ValidateToken(_ context.Context, req *userv1.ValidateTokenRequest, _ ...grpc.CallOption) (*userv1.ValidateTokenResponse, error) {
	claims, err := validateAdminOutboxJWT(req.AccessToken, u.secret)
	if err != nil {
		return &userv1.ValidateTokenResponse{Valid: false}, nil
	}
	return &userv1.ValidateTokenResponse{Valid: true, UserId: claims.UserID, Role: claims.Role}, nil
}

type scannerConcurrentRevocations struct{}

func (scannerConcurrentRevocations) IsRevoked(context.Context, string) (bool, error) {
	return false, nil
}

func scannerConcurrentToken(staff uuid.UUID, secret []byte) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims, _ := json.Marshal(adminOutboxJWTClaims{Issuer: "ticketbox", UserID: staff.String(), Role: "user", IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(5 * time.Minute).Unix()})
	message := header + "." + base64.RawURLEncoding.EncodeToString(claims)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(message))
	return message + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestAdmissionConcurrentScannerRPC(t *testing.T) {
	admissiontest.RequireOptIn(t)
	bookingDSN, paymentDSN := os.Getenv("BOOKING_TEST_DATABASE_URL"), os.Getenv("REFUND_COMPONENT_DATABASE_URL")
	admissiontest.VerifyBookingPostgres(t, bookingDSN)
	admissiontest.VerifyRefundComponentPostgres(t, paymentDSN, os.Getenv(dualDBOwnerManifestEnv))
	admissiontest.RequireDistinctPostgresEndpoints(t, bookingDSN, paymentDSN)
	bookingSchema := admissiontest.OpenMigrationSchema(t, bookingDSN, "../../migrations", 12)
	paymentSchema := admissiontest.OpenMigrationSchema(t, paymentDSN, "../../../payment/migrations", 12)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := seedDualDBBookingRows(t, bookingSchema.Pool)
	staff := []uuid.UUID{f.StaffID, uuid.New()}
	if _, err := bookingSchema.Pool.Exec(ctx, `INSERT INTO event_scanner_assignments(event_id,staff_user_id,active,granted_by,granted_at) VALUES($1,$2,true,$3,clock_timestamp())`, f.EventID, staff[1], f.StaffID); err != nil {
		t.Fatal(err)
	}

	ca := admissiontest.NewPKI(t, "Admission concurrent scanner isolated CA")
	dir := t.TempDir()
	caPath := admissiontest.WriteCA(t, dir, ca.CAPEM)
	bookingServer := ca.NewLeaf(t, dualDBBookingServer, grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageServerAuth)
	bookingClient := ca.NewLeaf(t, "", grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageClientAuth)
	paymentServer := ca.NewLeaf(t, dualDBPaymentServer, grpcsecurity.ServiceIdentityPayment, x509.ExtKeyUsageServerAuth)
	paymentClient := ca.NewLeaf(t, "", grpcsecurity.ServiceIdentityPayment, x509.ExtKeyUsageClientAuth)
	gatewayClient := ca.NewLeaf(t, "", grpcsecurity.ServiceIdentityGateway, x509.ExtKeyUsageClientAuth)
	serverCert, serverKey := admissiontest.WriteLeaf(t, dir, "payment-server", paymentServer)
	clientCert, clientKey := admissiontest.WriteLeaf(t, dir, "payment-client", paymentClient)
	creds, err := grpcsecurity.NewServerCredentials(bookingServer.CertificatePEM, bookingServer.PrivateKeyPEM, ca.CAPEM, []string{grpcsecurity.ServiceIdentityGateway, grpcsecurity.ServiceIdentityPayment})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	repo := repository.NewPostgresBookingRepository(bookingSchema.Pool)
	authority := &scannerConcurrentAuthority{observed: make(chan struct{}, 2), release: make(chan struct{}, 2)}
	manager := service.NewTicketManager(repo, authority)
	server := NewBookingServer(nil, zap.NewNop(), manager)
	secret := []byte(uuid.NewString())
	server.SetTicketPrincipalAuthenticator(NewTicketPrincipalAuthenticator(scannerConcurrentUsers{secret: secret}, scannerConcurrentRevocations{}))
	rpc := grpc.NewServer(grpc.Creds(creds))
	bookingv1.RegisterBookingServiceServer(rpc, server)
	admissionv1.RegisterBookingAdmissionAuthorityServer(rpc, NewAdmissionServer(repo))
	go func() { _ = rpc.Serve(listener) }()
	t.Cleanup(func() { rpc.Stop(); _ = listener.Close() })
	config := dualDBPaymentConfig{Version: 1, DatabaseURL: paymentSchema.DSN,
		BookingAddress: listener.Addr().String(), BookingServerName: dualDBBookingServer,
		BookingClientCert: clientCert, BookingClientKey: clientKey, PaymentServerCert: serverCert, PaymentServerKey: serverKey, CAFile: caPath,
		PaymentID: f.PaymentID.String(), BookingID: f.BookingID.String(), UserID: f.UserID.String(), AmountCents: 2175,
		PaymentIntentID: "pi_admission_integration_local", ProviderAccount: dualDBProviderAccount, SeedFinancial: true}
	payment := startDualDBPaymentProcess(t, os.Getenv(dualDBPaymentBinaryEnv), filepath.Join(dir, "payment.json"), config)
	t.Cleanup(func() { payment.stop(t) })
	conn := newDualDBClient(t, payment.ready.GRPCAddress, dualDBPaymentServer, bookingClient, ca.CAPEM, grpcsecurity.ServiceIdentityBooking)
	t.Cleanup(func() { _ = conn.Close() })
	coordinator := &scannerConcurrentCoordinator{Coordinator: admission.NewCoordinatorClient(conn), granted: make(chan struct{}, 2), release: make(chan struct{}, 2), ticketID: f.TicketIDs[0]}
	manager.SetAdmissionCoordinator(coordinator)
	if _, err := bookingSchema.Pool.Exec(ctx, `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	if !payment.post(t, "/v1/activate").OK {
		t.Fatal("Payment activation failed")
	}
	clients := make([]bookingv1.BookingServiceClient, 2)
	tokens := make([]string, 2)
	for i := range clients {
		clientConn := newDualDBClient(t, listener.Addr().String(), dualDBBookingServer, gatewayClient, ca.CAPEM, grpcsecurity.ServiceIdentityGateway)
		t.Cleanup(func() { _ = clientConn.Close() })
		clients[i] = bookingv1.NewBookingServiceClient(clientConn)
		tokens[i] = scannerConcurrentToken(staff[i], secret)
	}
	type outcome struct {
		index    int
		response *bookingv1.CheckInTicketResponse
		err      error
	}
	invoke := func(i, ticket int, key string) outcome {
		callCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tokens[i])
		response, err := clients[i].CheckInTicket(callCtx, &bookingv1.CheckInTicketRequest{EventId: f.EventID.String(), StaffUserId: staff[i].String(), Credential: f.Credentials[ticket], IdempotencyKey: key})
		return outcome{i, response, err}
	}
	awaitTwo := func(ch <-chan struct{}) {
		t.Helper()
		for i := 0; i < 2; i++ {
			select {
			case <-ch:
			case <-ctx.Done():
				t.Fatal("concurrent RPC barrier timeout")
			}
		}
	}
	keys := []string{"scanner-race-a", "scanner-race-b"}
	results := make(chan outcome, 2)
	for i := range clients {
		go func(i int) { results <- invoke(i, 0, keys[i]) }(i)
	}
	awaitTwo(authority.observed)
	authority.release <- struct{}{}
	authority.release <- struct{}{}
	awaitTwo(coordinator.granted)
	coordinator.release <- struct{}{}
	coordinator.release <- struct{}{}
	winner, conflicts := -1, 0
	var original *bookingv1.CheckInTicketResponse
	for i := 0; i < 2; i++ {
		got := <-results
		if status.Code(got.err) == codes.AlreadyExists {
			conflicts++
			continue
		}
		if got.err != nil {
			t.Fatalf("concurrent scanner %d: %v", got.index, got.err)
		}
		switch got.response.ResultCode {
		case "accepted":
			if winner != -1 || got.response.Replayed {
				t.Fatal("duplicate first acceptance")
			}
			winner = got.index
			original = got.response
		default:
			t.Fatalf("unexpected scanner outcome: %+v", got.response)
		}
	}
	if winner < 0 || conflicts != 1 {
		t.Fatalf("first accepts/conflicts: winner=%d conflicts=%d", winner, conflicts)
	}
	assertDualDBTicketState(t, bookingSchema.Pool, f.TicketIDs[0], "CHECKED_IN")
	assertDualDBScanFacts(t, bookingSchema.Pool, f.TicketIDs[0], 1, 1)
	var committed, pending int
	if err := paymentSchema.Pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state='COMMITTED'),count(*) FILTER(WHERE state='PENDING') FROM payment_admission_permits WHERE ticket_id=$1`, f.TicketIDs[0]).Scan(&committed, &pending); err != nil || committed != 1 || pending != 0 {
		t.Fatalf("Payment outcomes committed=%d pending=%d err=%v", committed, pending, err)
	}
	replay := invoke(winner, 0, keys[winner])
	if replay.err != nil || replay.response.ResultCode != "accepted" || !replay.response.Replayed {
		t.Fatalf("same-key replay: %+v %v", replay.response, replay.err)
	}
	expectedReplay := proto.Clone(original).(*bookingv1.CheckInTicketResponse)
	expectedReplay.Replayed = true
	if !proto.Equal(expectedReplay, replay.response) {
		t.Fatalf("same-key replay changed the persisted response: expected=%v actual=%v", expectedReplay, replay.response)
	}
	assertDualDBScanFacts(t, bookingSchema.Pool, f.TicketIDs[0], 1, 1)
	t.Log("SCANNER_CONCURRENCY first_accepts=1 conflicts=1 successful_audits=1 receipts=1 same_key_replay=true")

	// Both requests have obtained eligible evidence before PREPARED is committed.
	for i := range clients {
		go func(i int) { results <- invoke(i, 1, "refund-first-"+keys[i]) }(i)
	}
	awaitTwo(authority.observed)
	if !payment.post(t, "/v1/require-refund").OK {
		t.Fatal("require local refund")
	}
	refund := payment.post(t, "/v1/refund-once")
	if !refund.OK || refund.Processed != 1 || refund.RefundCalls != 1 {
		t.Fatalf("local refund preparation: %+v", refund)
	}
	assertPaymentPreparedCount(t, paymentSchema.Pool, f.PaymentID, 1)
	authority.release <- struct{}{}
	authority.release <- struct{}{}
	for i := 0; i < 2; i++ {
		got := <-results
		if status.Code(got.err) != codes.FailedPrecondition {
			t.Fatalf("stale eligible evidence crossed refund gate: response=%+v err=%v", got.response, got.err)
		}
	}
	assertDualDBTicketState(t, bookingSchema.Pool, f.TicketIDs[1], "ISSUED")
	assertDualDBScanFacts(t, bookingSchema.Pool, f.TicketIDs[1], 0, 0)
	var permits int
	if err := paymentSchema.Pool.QueryRow(ctx, `SELECT count(*) FROM payment_admission_permits WHERE ticket_id=$1`, f.TicketIDs[1]).Scan(&permits); err != nil || permits != 0 {
		t.Fatalf("refund-first permits=%d err=%v", permits, err)
	}
	t.Log("SCANNER_REFUND_ORDER eligible_observed_before_prepared=true prepared=1 admission_permits=0 checkins=0 both_failed_precondition=true")
}
