//go:build linux && recoverycomponents

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
	"github.com/ticketbox/payment/internal/repository"
)

const (
	refundLeaseTakeoverChildEnv    = "REFUND_WORKER_LEASE_TAKEOVER_CHILD"
	refundLeaseTakeoverEventTag    = "REFUND_LEASE_EVENT "
	refundLeaseTakeoverDuration    = 4 * time.Second
	refundLeaseTakeoverWorkerLease = 30 * time.Second
)

// TestRefundWorkerLeaseTakeoverIntegration races two independent test-binary
// processes for one real Payment row, then pauses the winning old owner at the
// exact point after its no_refunds Provider snapshot and before CheckRefundPreSend.
// The local Provider double is in-process HTTP only; it does not model Stripe.
func TestRefundWorkerLeaseTakeoverIntegration(t *testing.T) {
	if worker := os.Getenv(refundLeaseTakeoverChildEnv); worker != "" {
		runRefundLeaseTakeoverChild(t, worker)
		return
	}

	pool := refundWorkerManifestedDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	repo := repository.NewPostgresPaymentRepository(pool)
	op := seedRefundWorkerPayment(t, ctx, repo)
	if err := repo.ActivatePaymentAdmission(ctx); err != nil {
		t.Fatal("cannot activate admission mode in the disposable Payment schema")
	}
	var admissionActive bool
	if err := pool.QueryRow(ctx, `SELECT active FROM payment_admission_mode WHERE singleton=TRUE`).Scan(&admissionActive); err != nil || !admissionActive {
		t.Fatalf("Payment admission mode active=%v err=%v; want active", admissionActive, err)
	}
	var schema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil || !refundProvenanceWorkerSchemaPattern.MatchString(schema) {
		t.Fatalf("cannot identify the disposable worker schema: err=%v", err)
	}

	stableKey := fmt.Sprintf("ticketbox:payment:%s:g1:refund", op.PaymentID)
	provider := newRefundLeaseTakeoverProvider(op, "pi_refund_worker_fixture", stableKey)
	providerServer := httptest.NewServer(provider)
	t.Cleanup(providerServer.Close)

	first := startRefundLeaseTakeoverChild(t, "worker-1", schema, op, providerServer.URL)
	second := startRefundLeaseTakeoverChild(t, "worker-2", schema, op, providerServer.URL)
	for _, child := range []*refundLeaseTakeoverChild{first, second} {
		if event := child.nextEvent(t, ctx); event.Kind != "ready" {
			t.Fatalf("child %s event=%q; want ready", child.worker, event.Kind)
		}
	}
	// Release both owned processes together. PostgreSQL row locking and the
	// persisted lease determine the winner; neither process gets a test token.
	var sendWG sync.WaitGroup
	var sendErr [2]error
	for i, child := range []*refundLeaseTakeoverChild{first, second} {
		sendWG.Add(1)
		go func(i int, child *refundLeaseTakeoverChild) {
			defer sendWG.Done()
			sendErr[i] = child.send("claim")
		}(i, child)
	}
	sendWG.Wait()
	if sendErr[0] != nil || sendErr[1] != nil {
		t.Fatalf("cannot release both worker claim attempts: %v / %v", sendErr[0], sendErr[1])
	}
	firstClaim := first.nextEvent(t, ctx)
	secondClaim := second.nextEvent(t, ctx)
	winner, loser := (*refundLeaseTakeoverChild)(nil), (*refundLeaseTakeoverChild)(nil)
	oldClaim, losingClaim := refundLeaseTakeoverEvent{}, refundLeaseTakeoverEvent{}
	switch {
	case firstClaim.Kind == "claimed" && secondClaim.Kind == "empty":
		winner, loser, oldClaim, losingClaim = first, second, firstClaim, secondClaim
	case secondClaim.Kind == "claimed" && firstClaim.Kind == "empty":
		winner, loser, oldClaim, losingClaim = second, first, secondClaim, firstClaim
	default:
		t.Fatalf("two live processes did not yield one claim owner: worker-1=%+v worker-2=%+v", firstClaim, secondClaim)
	}
	if oldClaim.Fence < 1 || len(oldClaim.TokenDigest) != 64 || losingClaim.Fence != 0 {
		t.Fatalf("invalid simultaneous claim observations: owner=%+v other=%+v", oldClaim, losingClaim)
	}
	if err := winner.send("execute_pause_inspect"); err != nil {
		t.Fatal("cannot start the winning worker's real refund execution")
	}
	inspectHeld := winner.nextEvent(t, ctx)
	if inspectHeld.Kind != "inspect_held" || inspectHeld.Status != "no_refunds" {
		t.Fatalf("old worker did not hold the actual no_refunds snapshot before the pre-send guard: %+v", inspectHeld)
	}

	// The losing process retries while the old claim is still valid. It must
	// remain empty before the OS pause and before natural lease expiry.
	active := refundWorkerSnapshot(t, ctx, pool, op.PaymentID)
	if active.token == nil || refundProvenanceClaimTokenDigest(*active.token) != oldClaim.TokenDigest || active.fence != oldClaim.Fence {
		gotDigest := ""
		if active.token != nil {
			gotDigest = refundProvenanceClaimTokenDigest(*active.token)
		}
		t.Fatalf("database claim digest=%q fence=%d; want digest=%q fence=%d", gotDigest, active.fence, oldClaim.TokenDigest, oldClaim.Fence)
	}
	var leaseValid bool
	if err := pool.QueryRow(ctx, `SELECT lease_until>clock_timestamp() FROM payments WHERE id=$1`, op.PaymentID).Scan(&leaseValid); err != nil || !leaseValid {
		t.Fatalf("old claim lease valid before loser retry=%v err=%v; want true", leaseValid, err)
	}
	if err := loser.send("claim"); err != nil {
		t.Fatal("cannot retry claim while the old lease is valid")
	}
	if event := loser.nextEvent(t, ctx); event.Kind != "empty" {
		t.Fatalf("worker B bypassed the active A lease: %+v", event)
	}
	prepared, responses := refundLeaseTakeoverAuditCounts(t, ctx, pool, op.PaymentID)
	if prepared != 0 || responses != 0 {
		t.Fatalf("old worker was paused before its pre-send guard but already wrote audit rows: PREPARED=%d RESPONSE=%d", prepared, responses)
	}

	// Signals are sent only through the exact child Process handle returned by
	// exec.Cmd.Start. The test never targets a numeric PID or another process.
	if err := winner.process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("cannot SIGSTOP the owned old worker process: %v", err)
	}
	winner.stopped = true
	waitRefundLeaseTakeoverProcessState(t, ctx, winner, true)
	waitRefundLeaseExpiry(t, ctx, pool, op.PaymentID)
	if err := loser.send("takeover"); err != nil {
		t.Fatal("cannot request takeover after natural lease expiry")
	}
	newClaim := loser.nextEvent(t, ctx)
	if newClaim.Kind != "claimed" || newClaim.Fence <= oldClaim.Fence || newClaim.TokenDigest == oldClaim.TokenDigest {
		t.Fatalf("worker B did not obtain a new fenced claim after natural expiry: old=%+v new=%+v", oldClaim, newClaim)
	}
	if err := loser.send("execute"); err != nil {
		t.Fatal("cannot execute the takeover worker's refund")
	}
	newGuard := loser.nextEvent(t, ctx)
	if newGuard.Kind != "presend" || newGuard.Decision != string(repository.RefundPreSendAllow) || newGuard.AttemptID == uuid.Nil {
		t.Fatalf("new owner did not pass its persisted pre-send guard: %+v", newGuard)
	}
	newFinish := loser.nextEvent(t, ctx)
	if newFinish.Kind != "finish" || !newFinish.Owned {
		t.Fatalf("takeover worker's financial finish was not committed by its owner: %+v", newFinish)
	}
	if event := loser.nextEvent(t, ctx); event.Kind != "done" {
		t.Fatalf("takeover worker did not finish: %+v", event)
	}
	t.Logf("natural lease expiry permitted worker %s to claim fence=%d after old fence=%d", loser.worker, newClaim.Fence, oldClaim.Fence)

	afterB := refundWorkerSnapshot(t, ctx, pool, op.PaymentID)
	assertRefundLeaseTakeoverSuccess(t, afterB, newClaim, op.PaymentID, provider)
	prepared, responses = refundLeaseTakeoverAuditCounts(t, ctx, pool, op.PaymentID)
	if prepared != 1 || responses != 1 {
		t.Fatalf("after B's successful execution: PREPARED=%d RESPONSE=%d; want 1/1", prepared, responses)
	}
	if err := refundLeaseTakeoverAssertPreparedOwner(ctx, pool, op.PaymentID, newClaim); err != nil {
		t.Fatalf("B's PREPARED audit does not bind its new claim fence/token digest: %v", err)
	}
	var finishOutcome string
	if err := pool.QueryRow(ctx, `SELECT payload->>'financial_finish_outcome' FROM payment_refund_audit
		WHERE payment_id=$1 AND record_kind='RESPONSE'`, op.PaymentID).Scan(&finishOutcome); err != nil || finishOutcome != domain.RefundAuditFinishPersisted {
		t.Fatalf("B response audit finish outcome=%q err=%v; want persisted", finishOutcome, err)
	}
	inspectCalls, createCalls, effectCount, createKeys := provider.counts()
	if inspectCalls != 3 || createCalls != 1 || effectCount != 1 || len(createKeys) != 1 || createKeys[0] != stableKey {
		t.Fatalf("local Provider double before A resumes: Inspect=%d Create attempts=%d synthetic refunds=%d stable keys=%v; want 3/1/1/[original key]", inspectCalls, createCalls, effectCount, createKeys)
	}
	t.Logf("two owned OS workers competed; old fence=%d, takeover fence=%d; short lease expired naturally; local Provider double observed Inspect=3, Create=1, synthetic refund objects=1", oldClaim.Fence, newClaim.Fence)

	if err := winner.process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("cannot SIGCONT the same owned old worker process: %v", err)
	}
	winner.stopped = false
	waitRefundLeaseTakeoverProcessState(t, ctx, winner, false)
	if err := winner.send("release_inspect"); err != nil {
		t.Fatal("cannot release A's held local Provider snapshot")
	}
	oldGuard := winner.nextEvent(t, ctx)
	if oldGuard.Kind != "presend" || oldGuard.Decision != string(repository.RefundPreSendStaleOwner) || oldGuard.AttemptID != uuid.Nil {
		t.Fatalf("resumed old claim was not rejected by the persisted pre-send fence: %+v", oldGuard)
	}
	t.Logf("resumed old worker's actual pre-send guard returned stale_owner; it received no PREPARED attempt and did not call Create")
	oldFinish := winner.nextEvent(t, ctx)
	if oldFinish.Kind != "finish" || oldFinish.Owned {
		t.Fatalf("resumed stale worker unexpectedly completed financial work: %+v", oldFinish)
	}
	if event := winner.nextEvent(t, ctx); event.Kind != "done" {
		t.Fatalf("resumed old worker did not complete its stale path: %+v", event)
	}

	afterA := refundWorkerSnapshot(t, ctx, pool, op.PaymentID)
	if !sameRefundWorkerClaim(afterB, afterA) || afterA.refundState != afterB.refundState ||
		!sameRefundWorkerString(afterA.refundID, afterB.refundID) || !sameRefundWorkerString(afterA.refundProviderStatus, afterB.refundProviderStatus) ||
		!sameRefundWorkerTime(afterA.completedAt, afterB.completedAt) || afterA.reconciliationState != afterB.reconciliationState {
		t.Fatalf("resumed stale owner changed B's persisted result: B(fence=%d,state=%s,reconciliation=%s) A(fence=%d,state=%s,reconciliation=%s)",
			afterB.fence, afterB.refundState, afterB.reconciliationState, afterA.fence, afterA.refundState, afterA.reconciliationState)
	}
	if afterA.fence != newClaim.Fence || afterA.refundState != "succeeded" || afterA.reconciliationState != "resolved" ||
		afterA.refundID == nil || *afterA.refundID != provider.refundID(stableKey) || afterA.token != nil || afterA.lease != nil || afterA.kind != nil {
		t.Fatalf("final Payment state is not B's terminal fenced result: %+v", afterA)
	}
	prepared, responses = refundLeaseTakeoverAuditCounts(t, ctx, pool, op.PaymentID)
	if prepared != 1 || responses != 1 {
		t.Fatalf("A's stale pre-send continuation created new audit decisions: PREPARED=%d RESPONSE=%d; want unchanged 1/1", prepared, responses)
	}
	inspectCalls, createCalls, effectCount, createKeys = provider.counts()
	if inspectCalls != 3 || createCalls != 1 || effectCount != 1 || len(createKeys) != 1 || createKeys[0] != stableKey {
		t.Fatalf("local Provider double after A resumes: Inspect=%d Create attempts=%d synthetic refunds=%d stable keys=%v; want unchanged 3/1/1", inspectCalls, createCalls, effectCount, createKeys)
	}

	finishRefundLeaseTakeoverChild(t, ctx, winner)
	finishRefundLeaseTakeoverChild(t, ctx, loser)
}

type refundLeaseTakeoverEvent struct {
	Worker      string    `json:"worker"`
	Kind        string    `json:"kind"`
	TokenDigest string    `json:"token_digest,omitempty"`
	Fence       int64     `json:"fence,omitempty"`
	LeaseUntil  time.Time `json:"lease_until,omitempty"`
	Decision    string    `json:"decision,omitempty"`
	AttemptID   uuid.UUID `json:"attempt_id,omitempty"`
	Status      string    `json:"status,omitempty"`
	Owned       bool      `json:"owned,omitempty"`
}

func emitRefundLeaseTakeoverEvent(event refundLeaseTakeoverEvent) {
	data, _ := json.Marshal(event)
	fmt.Fprintf(os.Stdout, "%s%s\n", refundLeaseTakeoverEventTag, data)
}

func runRefundLeaseTakeoverChild(t *testing.T, worker string) {
	t.Helper()
	if worker != "worker-1" && worker != "worker-2" {
		t.Fatal("unknown test worker role")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	parsed, err := url.Parse(os.Getenv("REFUND_COMPONENT_DATABASE_URL"))
	if err != nil {
		t.Fatal("manifested Payment database URL is invalid")
	}
	schema := os.Getenv("REFUND_WORKER_LEASE_TAKEOVER_SCHEMA")
	if !refundProvenanceWorkerSchemaPattern.MatchString(schema) {
		t.Fatal("child schema is outside the exact disposable worker schema pattern")
	}
	pool := refundProvenancePoolForProcessSchema(t, ctx, parsed, schema)
	defer pool.Close()
	var paymentID, bookingID uuid.UUID
	paymentID, err = uuid.Parse(os.Getenv("REFUND_WORKER_LEASE_TAKEOVER_PAYMENT"))
	if err != nil {
		t.Fatal("child Payment identity is invalid")
	}
	bookingID, err = uuid.Parse(os.Getenv("REFUND_WORKER_LEASE_TAKEOVER_BOOKING"))
	if err != nil {
		t.Fatal("child Booking identity is invalid")
	}
	provider := &refundLeaseTakeoverProviderClient{
		worker: worker, endpoint: os.Getenv("REFUND_WORKER_LEASE_TAKEOVER_PROVIDER"),
		paymentID: paymentID, bookingID: bookingID, account: "acct_refund_worker_fixture",
		intentID: "pi_refund_worker_fixture", amountCents: 725, currency: "usd", generation: 1,
		client: &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 15 * time.Second},
	}
	commands := bufio.NewScanner(os.Stdin)
	provider.commands = commands
	repo := repository.NewPostgresPaymentRepository(pool)
	observedRepo := &refundLeaseTakeoverGuardObserver{PostgresPaymentRepository: repo, worker: worker}
	financialWorker := &FinancialWorker{repo: observedRepo, provider: provider}
	var currentClaim *domain.FinancialClaim
	emitRefundLeaseTakeoverEvent(refundLeaseTakeoverEvent{Worker: worker, Kind: "ready"})
	for commands.Scan() {
		command := commands.Text()
		switch command {
		case "claim", "takeover":
			lease := refundLeaseTakeoverDuration
			if command == "takeover" {
				lease = refundLeaseTakeoverWorkerLease
			}
			claims, err := repo.ClaimFinancialWork(ctx, 1, lease)
			if err != nil {
				t.Fatal("worker claim failed")
			}
			if len(claims) == 0 {
				currentClaim = nil
				emitRefundLeaseTakeoverEvent(refundLeaseTakeoverEvent{Worker: worker, Kind: "empty"})
				continue
			}
			if len(claims) != 1 || claims[0].Kind != "refund" {
				t.Fatalf("worker received unexpected financial work count=%d", len(claims))
			}
			claim := claims[0]
			currentClaim = &claim
			emitRefundLeaseTakeoverEvent(refundLeaseTakeoverEvent{
				Worker: worker, Kind: "claimed", TokenDigest: refundProvenanceClaimTokenDigest(claim.Token),
				Fence: claim.Fence, LeaseUntil: claim.LeaseUntil,
			})
		case "execute", "execute_pause_inspect":
			if currentClaim == nil {
				t.Fatal("worker execution has no persisted claim")
			}
			provider.pauseNextInspect = command == "execute_pause_inspect"
			if err := financialWorker.executeRefund(ctx, *currentClaim); err != nil {
				t.Fatal("worker refund execution failed")
			}
			currentClaim = nil
			emitRefundLeaseTakeoverEvent(refundLeaseTakeoverEvent{Worker: worker, Kind: "done"})
		case "exit":
			emitRefundLeaseTakeoverEvent(refundLeaseTakeoverEvent{Worker: worker, Kind: "exiting"})
			return
		default:
			t.Fatal("unknown worker command")
		}
	}
	if err := commands.Err(); err != nil {
		t.Fatal("worker command channel failed")
	}
}

type refundLeaseTakeoverChild struct {
	worker     string
	cmd        *exec.Cmd
	process    *os.Process
	input      io.WriteCloser
	writer     *bufio.Writer
	events     chan refundLeaseTakeoverEvent
	diagnostic *refundProvenanceChildDiagnostic
	stopped    bool
	reaped     bool
}

func startRefundLeaseTakeoverChild(t *testing.T, worker, schema string, op domain.CreateOperation, providerURL string) *refundLeaseTakeoverChild {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRefundWorkerLeaseTakeoverIntegration$", "-test.v", "-test.timeout=60s")
	cmd.Env = replaceRefundProvenanceEnv(os.Environ(), refundLeaseTakeoverChildEnv, worker)
	cmd.Env = replaceRefundProvenanceEnv(cmd.Env, "REFUND_WORKER_LEASE_TAKEOVER_SCHEMA", schema)
	cmd.Env = replaceRefundProvenanceEnv(cmd.Env, "REFUND_WORKER_LEASE_TAKEOVER_PAYMENT", op.PaymentID.String())
	cmd.Env = replaceRefundProvenanceEnv(cmd.Env, "REFUND_WORKER_LEASE_TAKEOVER_BOOKING", op.Identity.BookingID.String())
	cmd.Env = replaceRefundProvenanceEnv(cmd.Env, "REFUND_WORKER_LEASE_TAKEOVER_PROVIDER", providerURL)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal("cannot create owned worker stdin")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal("cannot create owned worker stdout")
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal("cannot start owned Payment worker process")
	}
	child := &refundLeaseTakeoverChild{
		worker: worker, cmd: cmd, process: cmd.Process, input: input, writer: bufio.NewWriter(input),
		events: make(chan refundLeaseTakeoverEvent, 16), diagnostic: &refundProvenanceChildDiagnostic{},
	}
	if child.process == nil || child.process.Pid <= 0 {
		t.Fatal("started worker has no owned OS process handle")
	}
	go func() {
		defer close(child.events)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, refundLeaseTakeoverEventTag) {
				_, _ = child.diagnostic.Write([]byte(line + "\n"))
				continue
			}
			var event refundLeaseTakeoverEvent
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, refundLeaseTakeoverEventTag)), &event); err != nil {
				_, _ = child.diagnostic.Write([]byte("--- FAIL: invalid worker event\n"))
				continue
			}
			child.events <- event
		}
		if err := scanner.Err(); err != nil {
			_, _ = child.diagnostic.Write([]byte("--- FAIL: worker output read failed\n"))
		}
	}()
	t.Cleanup(func() { child.cleanup() })
	return child
}

func (c *refundLeaseTakeoverChild) send(command string) error {
	if _, err := c.writer.WriteString(command + "\n"); err != nil {
		return err
	}
	return c.writer.Flush()
}

func (c *refundLeaseTakeoverChild) nextEvent(t *testing.T, ctx context.Context) refundLeaseTakeoverEvent {
	t.Helper()
	select {
	case event, ok := <-c.events:
		if !ok {
			t.Fatalf("worker %s exited before its next event (%s)", c.worker, c.diagnostic.String())
		}
		if event.Worker != c.worker {
			t.Fatalf("worker event role=%q; expected %q", event.Worker, c.worker)
		}
		return event
	case <-ctx.Done():
		t.Fatalf("worker %s did not reach its next barrier: %s", c.worker, c.diagnostic.String())
		return refundLeaseTakeoverEvent{}
	}
}

func (c *refundLeaseTakeoverChild) cleanup() {
	if c.reaped || c.cmd == nil || c.process == nil {
		return
	}
	if c.stopped {
		_ = c.process.Signal(syscall.SIGCONT)
		c.stopped = false
	}
	_ = c.process.Kill()
	_ = c.input.Close()
	_ = c.cmd.Wait()
	c.reaped = true
}

func finishRefundLeaseTakeoverChild(t *testing.T, ctx context.Context, child *refundLeaseTakeoverChild) {
	t.Helper()
	if err := child.send("exit"); err != nil {
		t.Fatalf("cannot stop child %s cleanly: %v", child.worker, err)
	}
	if event := child.nextEvent(t, ctx); event.Kind != "exiting" {
		t.Fatalf("child %s final event=%q; want exiting", child.worker, event.Kind)
	}
	for {
		select {
		case _, ok := <-child.events:
			if !ok {
				goto outputClosed
			}
		case <-ctx.Done():
			t.Fatalf("child %s stdout did not close after exit", child.worker)
		}
	}
outputClosed:
	_ = child.input.Close()
	err := child.cmd.Wait()
	child.reaped = true
	if err != nil {
		t.Fatalf("child %s exited with an error: %s", child.worker, child.diagnostic.String())
	}
}

func waitRefundLeaseExpiry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var expired bool
		if err := pool.QueryRow(ctx, `SELECT lease_until<=clock_timestamp() FROM payments WHERE id=$1`, paymentID).Scan(&expired); err != nil {
			t.Fatal("cannot observe the persisted refund lease")
		}
		if expired {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("Payment lease did not expire naturally before the test deadline")
		}
	}
}

func waitRefundLeaseTakeoverProcessState(t *testing.T, ctx context.Context, child *refundLeaseTakeoverChild, stopped bool) {
	t.Helper()
	path := fmt.Sprintf("/proc/%d/status", child.process.Pid)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := os.ReadFile(path)
		if err != nil {
			t.Fatal("cannot inspect the owned child process state")
		}
		observedStopped := false
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "State:") {
				fields := strings.Fields(line)
				observedStopped = len(fields) > 1 && (fields[1] == "T" || fields[1] == "t")
				break
			}
		}
		if observedStopped == stopped {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("owned worker did not reach the requested OS process state")
		}
	}
}

func refundLeaseTakeoverAuditCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) (prepared, response int) {
	t.Helper()
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE record_kind='PREPARED'),
		count(*) FILTER (WHERE record_kind='RESPONSE') FROM payment_refund_audit WHERE payment_id=$1`, paymentID).Scan(&prepared, &response); err != nil {
		t.Fatal("cannot inspect immutable refund audit rows")
	}
	return prepared, response
}

func refundLeaseTakeoverAssertPreparedOwner(ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID, claim refundLeaseTakeoverEvent) error {
	var fence int64
	var tokenDigest string
	if err := pool.QueryRow(ctx, `SELECT claim_fence,encode(claim_token_digest,'hex') FROM payment_refund_audit
		WHERE payment_id=$1 AND record_kind='PREPARED'`, paymentID).Scan(&fence, &tokenDigest); err != nil {
		return err
	}
	if fence != claim.Fence || tokenDigest != claim.TokenDigest {
		return fmt.Errorf("PREPARED fence/token digest did not match the takeover claim")
	}
	return nil
}

func assertRefundLeaseTakeoverSuccess(t *testing.T, snapshot refundWorkerRowSnapshot, claim refundLeaseTakeoverEvent, paymentID uuid.UUID, provider *refundLeaseTakeoverProviderDouble) {
	t.Helper()
	if snapshot.fence != claim.Fence || snapshot.refundState != "succeeded" || snapshot.reconciliationState != "resolved" ||
		snapshot.refundID == nil || *snapshot.refundID != provider.refundID(fmt.Sprintf("ticketbox:payment:%s:g1:refund", paymentID)) ||
		snapshot.refundProviderStatus == nil || *snapshot.refundProviderStatus != "succeeded" || snapshot.completedAt == nil ||
		snapshot.token != nil || snapshot.lease != nil || snapshot.kind != nil {
		t.Fatalf("takeover worker did not persist terminal success: fence=%d state=%s reconciliation=%s provider_status=%v owner_token_present=%t", claim.Fence,
			snapshot.refundState, snapshot.reconciliationState, snapshot.refundProviderStatus != nil, snapshot.token != nil)
	}
}

// refundLeaseTakeoverProviderClient sends only fixture identities and stable
// command keys to a loopback Provider double. No external Provider is used.
type refundLeaseTakeoverProviderClient struct {
	paymentgateway.FinancialProvider
	worker           string
	endpoint         string
	client           *http.Client
	paymentID        uuid.UUID
	bookingID        uuid.UUID
	account          string
	intentID         string
	amountCents      int64
	currency         string
	generation       int64
	commands         *bufio.Scanner
	pauseNextInspect bool
}

func (p *refundLeaseTakeoverProviderClient) InspectSucceededRefunds(ctx context.Context, account string, live bool, intentID string) (*domain.ProviderObservation, error) {
	if account != p.account || intentID != p.intentID {
		return nil, errors.New("Provider inspection identity mismatch")
	}
	request := refundLeaseTakeoverProviderRequest{
		Worker: p.worker, PaymentID: p.paymentID, BookingID: p.bookingID, Account: account,
		LiveMode: live, IntentID: intentID, Generation: p.generation, AmountCents: p.amountCents, Currency: p.currency,
	}
	reply, err := p.post(ctx, "/inspect", request)
	if err != nil {
		return nil, err
	}
	observation := p.observation(reply, live)
	if p.pauseNextInspect {
		p.pauseNextInspect = false
		emitRefundLeaseTakeoverEvent(refundLeaseTakeoverEvent{Worker: p.worker, Kind: "inspect_held", Status: reply.Status})
		if p.commands == nil || !p.commands.Scan() || p.commands.Text() != "release_inspect" {
			return nil, errors.New("held local Provider snapshot was not released by the parent")
		}
	}
	return observation, nil
}

func (p *refundLeaseTakeoverProviderClient) CreateFullRefund(ctx context.Context, claim domain.FinancialClaim, intentID string) (*domain.ProviderObservation, error) {
	if intentID != p.intentID || claim.Refund == nil {
		return nil, errors.New("Provider create identity mismatch")
	}
	capture := domain.RefundResponseCaptureFromContext(ctx)
	if capture == nil || capture.AttemptID == uuid.Nil {
		return nil, errors.New("Provider create has no scoped attempt capture")
	}
	request := refundLeaseTakeoverProviderRequest{
		Worker: p.worker, PaymentID: claim.PaymentID, BookingID: claim.Operation.Identity.BookingID,
		Account: claim.Operation.Identity.ProviderAccount, LiveMode: claim.Operation.Identity.LiveMode,
		IntentID: intentID, Generation: claim.Operation.Identity.Generation, AmountCents: claim.Refund.AmountCents,
		Currency: claim.Refund.Currency, StableKey: claim.Refund.Key, ClaimKey: claim.Key,
		OperationKey: claim.Operation.Key, RefundKey: claim.Refund.Key, AttemptID: capture.AttemptID,
	}
	reply, err := p.post(ctx, "/create", request)
	if err != nil {
		return nil, err
	}
	return p.observation(reply, claim.Operation.Identity.LiveMode), nil
}

func (p *refundLeaseTakeoverProviderClient) observation(reply refundLeaseTakeoverProviderReply, live bool) *domain.ProviderObservation {
	return &domain.ProviderObservation{
		Account: p.account, LiveMode: live, Status: reply.Status, IntentID: p.intentID, RefundID: reply.RefundID,
		PaymentID: p.paymentID, BookingID: p.bookingID, Generation: p.generation,
		AmountCents: p.amountCents, SucceededRefundCents: reply.SucceededCents,
		Currency: p.currency, Collected: true, ObservedAt: time.Now().UTC(),
	}
}

func (p *refundLeaseTakeoverProviderClient) post(ctx context.Context, path string, value any) (refundLeaseTakeoverProviderReply, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return refundLeaseTakeoverProviderReply{}, errors.New("cannot encode local Provider fixture request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return refundLeaseTakeoverProviderReply{}, errors.New("cannot build local Provider fixture request")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := p.client.Do(request)
	if err != nil {
		return refundLeaseTakeoverProviderReply{}, errors.New("local Provider fixture request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return refundLeaseTakeoverProviderReply{}, errors.New("local Provider fixture rejected request")
	}
	var reply refundLeaseTakeoverProviderReply
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&reply); err != nil {
		return refundLeaseTakeoverProviderReply{}, errors.New("local Provider fixture response was invalid")
	}
	return reply, nil
}

var _ paymentgateway.FinancialProvider = (*refundLeaseTakeoverProviderClient)(nil)

type refundLeaseTakeoverGuardObserver struct {
	*repository.PostgresPaymentRepository
	worker string
}

func (r *refundLeaseTakeoverGuardObserver) CheckRefundPreSend(ctx context.Context, claim domain.FinancialClaim) (repository.RefundPreSendResult, error) {
	result, err := r.PostgresPaymentRepository.CheckRefundPreSend(ctx, claim)
	emitRefundLeaseTakeoverEvent(refundLeaseTakeoverEvent{Worker: r.worker, Kind: "presend", Decision: string(result.Decision), AttemptID: result.AttemptID})
	return result, err
}

func (r *refundLeaseTakeoverGuardObserver) FinishFinancialWork(ctx context.Context, claim domain.FinancialClaim, result domain.FinancialResult) (bool, error) {
	owned, err := r.PostgresPaymentRepository.FinishFinancialWork(ctx, claim, result)
	emitRefundLeaseTakeoverEvent(refundLeaseTakeoverEvent{Worker: r.worker, Kind: "finish", Owned: owned})
	return owned, err
}

type refundLeaseTakeoverProviderRequest struct {
	Worker       string    `json:"worker"`
	PaymentID    uuid.UUID `json:"payment_id"`
	BookingID    uuid.UUID `json:"booking_id"`
	Account      string    `json:"account"`
	LiveMode     bool      `json:"live_mode"`
	IntentID     string    `json:"intent_id"`
	Generation   int64     `json:"generation"`
	AmountCents  int64     `json:"amount_cents"`
	Currency     string    `json:"currency"`
	StableKey    string    `json:"stable_key,omitempty"`
	ClaimKey     string    `json:"claim_key,omitempty"`
	OperationKey string    `json:"operation_key,omitempty"`
	RefundKey    string    `json:"refund_key,omitempty"`
	AttemptID    uuid.UUID `json:"attempt_id,omitempty"`
}

type refundLeaseTakeoverProviderReply struct {
	Status         string `json:"status"`
	RefundID       string `json:"refund_id,omitempty"`
	SucceededCents int64  `json:"succeeded_cents,omitempty"`
}

type refundLeaseTakeoverProviderDouble struct {
	mu           sync.Mutex
	op           domain.CreateOperation
	intentID     string
	stableKey    string
	inspectCalls int
	createCalls  int
	createKeys   []string
	effects      map[string]string
}

func newRefundLeaseTakeoverProvider(op domain.CreateOperation, intentID, stableKey string) *refundLeaseTakeoverProviderDouble {
	return &refundLeaseTakeoverProviderDouble{op: op, intentID: intentID, stableKey: stableKey, effects: make(map[string]string)}
}

func (p *refundLeaseTakeoverProviderDouble) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request refundLeaseTakeoverProviderRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 8192)).Decode(&request); err != nil || !p.validIdentity(request) {
		http.Error(w, "invalid local Provider fixture request", http.StatusBadRequest)
		return
	}
	switch r.URL.Path {
	case "/inspect":
		p.mu.Lock()
		p.inspectCalls++
		refundID := p.effects[p.stableKey]
		p.mu.Unlock()
		reply := refundLeaseTakeoverProviderReply{Status: "no_refunds"}
		if refundID != "" {
			reply = refundLeaseTakeoverProviderReply{Status: "succeeded", RefundID: refundID, SucceededCents: p.op.Identity.AmountCents}
		}
		writeRefundLeaseTakeoverJSON(w, reply)
	case "/create":
		if request.StableKey != p.stableKey || request.ClaimKey != p.stableKey || request.OperationKey != p.stableKey ||
			request.RefundKey != p.stableKey || request.AttemptID == uuid.Nil {
			http.Error(w, "refund command key or attempt identity mismatch", http.StatusConflict)
			return
		}
		p.mu.Lock()
		p.createCalls++
		p.createKeys = append(p.createKeys, request.StableKey)
		refundID := p.effects[p.stableKey]
		if refundID == "" {
			refundID = "re_synthetic_" + p.op.PaymentID.String()
			p.effects[p.stableKey] = refundID
		}
		p.mu.Unlock()
		writeRefundLeaseTakeoverJSON(w, refundLeaseTakeoverProviderReply{Status: "pending", RefundID: refundID})
	default:
		http.NotFound(w, r)
	}
}

func (p *refundLeaseTakeoverProviderDouble) validIdentity(request refundLeaseTakeoverProviderRequest) bool {
	return request.PaymentID == p.op.PaymentID && request.BookingID == p.op.Identity.BookingID &&
		request.Account == p.op.Identity.ProviderAccount && request.LiveMode == p.op.Identity.LiveMode &&
		request.IntentID == p.intentID && request.Generation == p.op.Identity.Generation &&
		request.AmountCents == p.op.Identity.AmountCents && request.Currency == p.op.Identity.Currency
}

func (p *refundLeaseTakeoverProviderDouble) counts() (inspectCalls, createCalls, effectCount int, keys []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	keys = append([]string(nil), p.createKeys...)
	return p.inspectCalls, p.createCalls, len(p.effects), keys
}

func (p *refundLeaseTakeoverProviderDouble) refundID(key string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.effects[key]
}

func writeRefundLeaseTakeoverJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
