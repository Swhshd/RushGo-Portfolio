package repository

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/admission"
	"github.com/ticketbox/pkg/grpcsecurity"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc"
)

// This is a real Event mTLS server backed by its own migrated PostgreSQL schema.
// Payment evidence is a fixture; this test never contacts a payment provider.
func refundTimeEventFixture(t *testing.T) (eventv1.EventServiceClient, *pgxpool.Pool) {
	t.Helper()
	binary, cwd := os.Getenv("EVENT_REFUND_FIXTURE_BINARY"), os.Getenv("EVENT_REFUND_FIXTURE_CWD")
	if binary == "" {
		t.Skip("EVENT_REFUND_FIXTURE_BINARY is required for cloud cross-service verification")
	}
	if cwd == "" || os.Getenv("EVENT_REFUND_TEST_DATABASE_URL") == "" {
		t.Fatal("incomplete Event fixture environment")
	}
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "isolated-refund-test"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	leaf := func(serial int64, identity string, usage x509.ExtKeyUsage) ([]byte, []byte) {
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		uri, _ := url.Parse(identity)
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "event-refund-fixture"}, DNSNames: []string{"event-refund-fixture"}, URIs: []*url.URL{uri}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		der, e := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		if e != nil {
			t.Fatal(e)
		}
		kb, e := x509.MarshalPKCS8PrivateKey(key)
		if e != nil {
			t.Fatal(e)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb})
	}
	serverCert, serverKey := leaf(2, grpcsecurity.ServiceIdentityEvent, x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := leaf(3, grpcsecurity.ServiceIdentityBooking, x509.ExtKeyUsageClientAuth)
	ready, stop := filepath.Join(dir, "ready.json"), filepath.Join(dir, "stop")
	config, _ := json.Marshal(map[string]string{"server_cert": write("server.crt", serverCert), "server_key": write("server.key", serverKey), "client_ca": write("ca.crt", caPEM), "ready_file": ready, "stop_file": stop})
	configPath := write("config.json", config)
	output, err := os.Create(filepath.Join(dir, "event-fixture.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestRefundTimeFixtureServer$", "-test.v", "-test.timeout=130s")
	cmd.Dir = cwd
	cmd.Env = append(os.Environ(), "REFUND_TIME_FIXTURE_CONFIG="+configPath)
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		output.Close()
		t.Fatal("start owned Event fixture:", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	exited := false
	t.Cleanup(func() {
		if !exited {
			_ = os.WriteFile(stop, []byte("stop"), 0600)
			select {
			case e := <-done:
				if e != nil {
					t.Errorf("Event fixture exit: %v", e)
				}
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Error("Event fixture required forced shutdown")
			}
		}
		output.Close()
		if t.Failed() {
			b, _ := os.ReadFile(output.Name())
			t.Logf("Event fixture diagnostics: %s", b)
		}
	})
	var state struct {
		Address string `json:"address"`
		Schema  string `json:"schema"`
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		if b, e := os.ReadFile(ready); e == nil {
			if e = json.Unmarshal(b, &state); e != nil {
				t.Fatal(e)
			}
			break
		}
		select {
		case e := <-done:
			exited = true
			t.Fatalf("Event fixture exited before ready: %v", e)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Event fixture readiness timeout")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !regexp.MustCompile(`^event_refund_time_fixture_[0-9a-f]{32}$`).MatchString(state.Schema) {
		t.Fatal("invalid owned Event schema")
	}
	dsn, err := url.Parse(os.Getenv("EVENT_REFUND_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid Event fixture DSN")
	}
	q := dsn.Query()
	q.Set("search_path", state.Schema+",public")
	dsn.RawQuery = q.Encode()
	pool, err := pgxpool.New(context.Background(), dsn.String())
	if err != nil {
		t.Fatal("connect Event fixture schema")
	}
	t.Cleanup(pool.Close)
	credentials, err := grpcsecurity.NewClientCredentials(clientCert, clientKey, caPEM, "event-refund-fixture", grpcsecurity.ServiceIdentityBooking)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(state.Address, grpc.WithTransportCredentials(credentials))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return eventv1.NewEventServiceClient(conn), pool
}

func TestRefundTimeRealEventBookingCoordinationIntegration(t *testing.T) {
	client, eventDB := refundTimeEventFixture(t)
	for _, scenario := range []string{"qualified_before_start_finalized_after_start", "checkin_wins_after_qualification", "abort_before_acquire_tombstone"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			pool, repo := newBookingOutboxIntegrationFixture(t)
			f := seedUnusedRefund(t, pool, repo)
			activateUnusedRefund(t, pool)
			if _, err := eventDB.Exec(ctx, `INSERT INTO events(id,title,category,venue,location,date) VALUES($1,'coordination fixture','test','test','test',clock_timestamp()+interval '3 seconds')`, f.bookingEventID); err != nil {
				t.Fatal(err)
			}
			a, err := repo.BeginUnusedRefundApproval(ctx, domain.BeginUnusedRefundApprovalCommand{AttemptID: f.decision.DecisionID, RequestID: f.request.RequestID, EventID: f.bookingEventID, ApproverID: f.approver, Reason: f.decision.Reason, KeyDigest: f.decision.KeyDigest, Fingerprint: f.decision.Fingerprint})
			if err != nil {
				t.Fatal(err)
			}
			b := &eventv1.RefundTimeBinding{AttemptId: a.AttemptID.String(), RequestId: a.RequestID.String(), BookingId: a.BookingID.String(), PaymentId: a.PaymentID.String(), EventId: a.EventID.String(), ApproverId: a.ApproverID.String(), Generation: a.Generation, Fingerprint: hex.EncodeToString(a.Fingerprint[:])}
			release := func(outcome string) {
				t.Helper()
				p, e := client.ReleaseUnusedRefundTimePermit(ctx, &eventv1.ReleaseUnusedRefundTimePermitRequest{Binding: b, Outcome: outcome})
				if e != nil || p.GetState() != "RELEASED" {
					t.Fatalf("release %s: %v %v", outcome, p, e)
				}
				if e = repo.AcknowledgeUnusedRefundAttemptRelease(ctx, a.AttemptID, outcome); e != nil {
					t.Fatal(e)
				}
			}
			if scenario == "abort_before_acquire_tombstone" {
				aborted, e := repo.AbortUnusedRefundApprovalAttempt(ctx, a.AttemptID, "test_abort")
				if e != nil || aborted.State != domain.RefundApprovalAttemptAborted {
					t.Fatalf("abort: %+v %v", aborted, e)
				}
				release("ABORTED")
				p, e := client.AcquireUnusedRefundTimePermit(ctx, b)
				if e != nil || p.GetState() != "RELEASED" {
					t.Fatalf("late acquire resurrected abort: %v %v", p, e)
				}
				var count int
				if e = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE id=$1`, a.AttemptID).Scan(&count); e != nil || count != 0 {
					t.Fatalf("aborted attempt emitted financial command: %d %v", count, e)
				}
				return
			}
			permit, err := client.AcquireUnusedRefundTimePermit(ctx, b)
			if err != nil || permit.GetState() != "QUALIFIED" || permit.GetQualifiedAt() == nil || !permit.GetQualifiedAt().AsTime().Before(permit.GetStartsAt().AsTime()) {
				t.Fatalf("qualification: %v %v", permit, err)
			}
			if _, err = eventDB.Exec(ctx, `UPDATE events SET date=date+interval '1 hour' WHERE id=$1`, f.bookingEventID); err == nil {
				t.Fatal("pending permit allowed reschedule")
			}
			if scenario == "checkin_wins_after_qualification" {
				scan := acceptedTicketScanFixture(f.ticketRepositoryFixture, time.Now().UTC())
				identity := admissionIdentityForTicketFixture(f.ticketRepositoryFixture, scan.StaffUserID, scan.IdempotencyKey, scan.RequestFingerprint)
				if err = repo.EnsureAdmissionIntent(ctx, identity); err != nil {
					t.Fatalf("time permit froze ticket: %v", err)
				}
				ap := admission.Permit{Identity: identity, Token: uuid.New(), Fence: 1, State: admission.Pending}
				if err = repo.AttachAdmissionPermit(ctx, ap); err != nil {
					t.Fatal(err)
				}
				scan.Admission = &ap
				result, e := repo.CommitTicketCheckIn(ctx, scan)
				if e != nil || result.ResultCode != "accepted" {
					t.Fatalf("checkin did not win: %+v %v", result, e)
				}
			} else {
				for {
					var started bool
					if err = eventDB.QueryRow(ctx, `SELECT clock_timestamp()>=date FROM events WHERE id=$1`, f.bookingEventID).Scan(&started); err != nil {
						t.Fatal(err)
					}
					if started {
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			command := f.decision
			command.QualificationAttemptID = a.AttemptID
			command.Event = domain.RefundEventSnapshot{EventID: a.EventID, StartsAt: permit.StartsAt.AsTime(), EventQualifiedAt: permit.QualifiedAt.AsTime(), QualificationAttemptID: a.AttemptID, AuthorityVersion: strconv.FormatInt(permit.Version, 10)}
			result, err := repo.FinalizeUnusedRefundApproval(ctx, command)
			if err != nil {
				t.Fatal(err)
			}
			expected, outcome, count := domain.RefundRequestApproved, "APPROVED", 1
			if scenario == "checkin_wins_after_qualification" {
				expected, outcome, count = domain.RefundRequestRejected, "REJECTED", 0
			}
			if result.Status != expected {
				t.Fatalf("final state %s expected %s", result.Status, expected)
			}
			var actual int
			if err = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE id=$1`, a.AttemptID).Scan(&actual); err != nil || actual != count {
				t.Fatalf("financial commands %d expected %d: %v", actual, count, err)
			}
			release(outcome)
			if _, err = eventDB.Exec(ctx, `UPDATE events SET date=date+interval '1 hour' WHERE id=$1`, f.bookingEventID); err != nil {
				t.Fatalf("terminal release left Event blocked: %v", err)
			}
			late, e := client.AcquireUnusedRefundTimePermit(ctx, b)
			if e != nil || late.GetState() != "RELEASED" {
				t.Fatalf("late acquire resurrected terminal permit: %v %v", late, e)
			}
		})
	}
}
