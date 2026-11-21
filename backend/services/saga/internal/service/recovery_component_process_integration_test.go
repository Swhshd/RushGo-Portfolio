//go:build reliability_component

package service

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/recoverycomponent"
	"github.com/ticketbox/saga/internal/repository"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	componentWorkerLease       = 9 * time.Second
	componentWorkerRemoteLimit = 2 * time.Second
	componentHookTimeout       = 60 * time.Second
)

func TestSagaComponentOwnedWorkerProcess(t *testing.T) {
	if os.Getenv("SAGA_COMPONENT_CHILD_MODE") != "worker" {
		return
	}
	startID := os.Getenv("SAGA_COMPONENT_START_ID")
	workerID := os.Getenv("SAGA_COMPONENT_WORKER_ID")
	observerAddress := os.Getenv("SAGA_COMPONENT_PARENT_OBSERVER_ADDR")
	if startID == "" || workerID == "" || observerAddress == "" {
		t.Fatal("component worker child identity or observer endpoint is missing")
	}
	startedAt := time.Now().UTC()
	sendComponentProcessMessage(t, observerAddress, componentProcessMessage{
		Kind: "process_started", PID: os.Getpid(), StartID: startID, WorkerID: workerID, StartedAt: startedAt,
	})

	pool := recoverycomponent.OpenComponentPool(t)
	repo := repository.NewSagaRepository(pool)
	sagaID, err := uuid.Parse(os.Getenv("SAGA_COMPONENT_SAGA_ID"))
	if err != nil || sagaID == uuid.Nil {
		t.Fatal("component worker Saga identity is invalid")
	}
	saga, err := repo.GetSagaById(context.Background(), sagaID)
	if err != nil || saga == nil {
		t.Fatal("component worker could not read its owned Saga")
	}
	var backendPID int
	if err := pool.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
		t.Fatal("component worker could not identify its PostgreSQL backend")
	}
	service := componentServiceForSaga(t, repo, saga)
	config := RecoveryConfig{
		ScanInterval: 200 * time.Millisecond, LeaseTTL: componentWorkerLease,
		RemoteTimeout: componentWorkerRemoteLimit, BatchSize: 1, Concurrency: 1, MaxAttempts: 3,
	}
	var workerRepo repository.RecoveryRepository = repo
	if address := os.Getenv("SAGA_COMPONENT_RENEWAL_OBSERVER_ADDR"); address != "" {
		workerRepo = &componentRenewalObservedRepository{
			SagaRepository: repo,
			address:        address,
			workerID:       workerID,
			startID:        startID,
			runID:          os.Getenv("SAGA_COMPONENT_RUN_ID"),
		}
	}
	worker, err := NewRecoveryWorker(workerRepo, service, zap.NewNop(), config)
	if err != nil {
		t.Fatal("component worker configuration rejected")
	}
	handled, err := worker.RunOnce(context.Background(), 1)
	reportRunError := os.Getenv("SAGA_COMPONENT_REPORT_RUN_ERROR") == "true"
	if err != nil && !reportRunError {
		t.Fatal("component worker execution failed")
	}
	runError := ""
	if err != nil {
		runError = err.Error()
	}
	sendComponentProcessMessage(t, observerAddress, componentProcessMessage{
		Kind: "worker_complete", PID: os.Getpid(), StartID: startID, WorkerID: workerID,
		Handled: handled, CompletedAt: time.Now().UTC(), RunID: os.Getenv("SAGA_COMPONENT_RUN_ID"), BackendPID: backendPID,
		RunError: runError, RecoveryConflict: errors.Is(err, repository.ErrRecoveryConflict),
		ContextCanceled: errors.Is(err, context.Canceled),
	})
}

func TestSagaComponentRealProcessCrashLeaseRecoveryAndReleasedClaimTakeoverIntegration(t *testing.T) {
	fixture := recoverycomponent.CreateDatabaseFixture(t)
	if fixture.Schema == "" || fixture.URL == "" {
		t.Fatal("owned Saga component schema was not created")
	}
	pool := recoverycomponent.OpenComponentPool(t)
	reader := recoverycomponent.OpenIndependentPool(t, "saga_component_process_reader")
	repo := repository.NewSagaRepository(pool)
	var appPID, readerPID int
	if err := pool.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&appPID); err != nil {
		t.Fatal(err)
	}
	if err := reader.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&readerPID); err != nil {
		t.Fatal(err)
	}
	if appPID == readerPID {
		t.Fatal("Saga repository and independent reader share a PostgreSQL backend")
	}
	observer := newComponentProcessObserver(t)
	runID := os.Getenv("SAGA_COMPONENT_RUN_ID")
	if runID == "" {
		t.Fatal("SAGA_COMPONENT_RUN_ID is required")
	}
	t.Logf("component_database_identity run_id=%s schema=%s repository_backend_pid=%d independent_reader_backend_pid=%d distinct_backend_connections=true", runID, fixture.Schema, appPID, readerPID)

	t.Run("retained_claim_requires_expiry_then_successor_process_recovers", func(t *testing.T) {
		ctx := context.Background()
		sagaID := createComponentProcessSaga(t, repo)
		target := spawnComponentWorker(t, observer, sagaID, "retained-target", true, runID)
		started := observer.awaitStarted(t)
		assertComponentProcessStarted(t, target, started)

		result, resultConn := observer.awaitPostCommit(t)
		assertProcessPostCommit(t, target, result, "retained-target", 0, 1, "retained")
		first := readProcessSagaSnapshot(t, reader, sagaID)
		assertProcessSnapshot(t, first, 1, "PROCESSING", true, result, 0, "pending", 0, 0, 0)
		if first.Token == "" {
			t.Fatal("retained target token missing from independent database snapshot")
		}
		oldToken, err := uuid.Parse(first.Token)
		if err != nil || oldToken == uuid.Nil {
			t.Fatal("retained target token is invalid")
		}
		oldFence := first.Fence
		t.Logf("retained_target_commit saga=%s worker=%s pid=%d fence=%d token_sha256=%s index=%d next_state=%s lease_live=%t", sagaID, result.WorkerID, result.PID, result.ClaimFence, result.ClaimTokenSHA256, first.Index, first.ExecuteStates[1], first.LeaseLive)
		if err := target.killAndWait(t); err != nil {
			t.Fatal("could not terminate owned retained-claim worker process")
		}
		_ = resultConn.Close()
		if !target.killed {
			t.Fatal("target OS process did not exit by the owned process handle")
		}
		contender := spawnComponentWorker(t, observer, sagaID, "retained-pre-expiry-contender", false, runID)
		assertComponentProcessStarted(t, contender, observer.awaitStarted(t))
		completion := observer.awaitCompletion(t)
		if completion.WorkerID != "retained-pre-expiry-contender" || completion.Handled != 0 {
			t.Fatalf("another worker claimed before the retained lease became eligible: worker=%s handled=%d", completion.WorkerID, completion.Handled)
		}
		t.Logf("pre_expiry_contender worker=%s pid=%d handled=%d", completion.WorkerID, completion.PID, completion.Handled)
		if err := contender.waitClean(t); err != nil {
			t.Fatal("pre-expiry contender did not exit cleanly")
		}
		beforeExpiry := readProcessSagaSnapshot(t, reader, sagaID)
		if !beforeExpiry.LeaseLive || beforeExpiry.Token == "" || beforeExpiry.Fence != oldFence {
			t.Fatal("contender ran after the target lease had already expired")
		}
		waitForComponentLeaseExpiry(t, reader, sagaID, 15*time.Second)

		successor := spawnComponentWorker(t, observer, sagaID, "retained-recovery-successor", true, runID)
		assertComponentProcessStarted(t, successor, observer.awaitStarted(t))
		recovered, recoveredConn := observer.awaitPostCommit(t)
		assertComponentPostCommit(t, successor, recovered, "retained-recovery-successor", 1, 2, "released")
		second := readProcessSagaSnapshot(t, reader, sagaID)
		assertProcessSnapshot(t, second, 2, "PROCESSING", false, recovered, 1, "pending", 1, 1, 0)
		if recovered.ClaimFence <= oldFence || recovered.ClaimTokenSHA256 == result.ClaimTokenSHA256 {
			t.Fatal("successor did not receive a new token and higher fence")
		}
		stillOwned, err := repo.RenewClaim(ctx, domain.SagaClaim{SagaID: sagaID, Token: oldToken, Fence: oldFence, Direction: "forward"}, componentWorkerLease)
		if err != nil || stillOwned {
			t.Fatal("expired old process token regained ownership after successor recovery")
		}
		if err := acknowledgeComponentEvent(recoveredConn); err != nil {
			t.Fatal("could not release the recovered successor after independent read")
		}
		if completion := observer.awaitCompletion(t); completion.WorkerID != "retained-recovery-successor" || completion.Handled != 1 {
			t.Fatal("recovered successor did not complete one normal worker dispatch")
		}
		if err := successor.waitClean(t); err != nil {
			t.Fatal("recovered successor did not exit cleanly")
		}
		if _, err := pool.Exec(ctx, `UPDATE sagas SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE id=$1`, sagaID); err != nil {
			t.Fatal("could not isolate the completed recovery fixture from the next process scenario")
		}
	})

	t.Run("released_S1_claim_is_not_revived_and_successor_dispatches_S2", func(t *testing.T) {
		ctx := context.Background()
		sagaID := createComponentProcessSaga(t, repo)
		target := spawnComponentWorker(t, observer, sagaID, "released-target", true, runID)
		assertComponentProcessStarted(t, target, observer.awaitStarted(t))

		s0, s0Conn := observer.awaitPostCommit(t)
		assertComponentPostCommit(t, target, s0, "released-target", 0, 1, "retained")
		first := readProcessSagaSnapshot(t, reader, sagaID)
		assertProcessSnapshot(t, first, 1, "PROCESSING", true, s0, 0, "pending", 0, 0, 0)
		oldToken, err := uuid.Parse(first.Token)
		if err != nil || oldToken == uuid.Nil {
			t.Fatal("target claim token is invalid")
		}
		oldFence := first.Fence
		t.Logf("released_target_S0 saga=%s worker=%s pid=%d fence=%d token_sha256=%s next_state=%s lease_live=%t", sagaID, s0.WorkerID, s0.PID, s0.ClaimFence, s0.ClaimTokenSHA256, first.ExecuteStates[1], first.LeaseLive)
		if err := acknowledgeComponentEvent(s0Conn); err != nil {
			t.Fatal("could not release target after independent S0 read")
		}

		s1, s1Conn := observer.awaitPostCommit(t)
		assertComponentPostCommit(t, target, s1, "released-target", 1, 2, "released")
		second := readProcessSagaSnapshot(t, reader, sagaID)
		assertProcessSnapshot(t, second, 2, "PROCESSING", false, s1, 1, "pending", 1, 1, 0)
		if s1.ClaimFence != oldFence || s1.ClaimTokenSHA256 != s0.ClaimTokenSHA256 {
			t.Fatal("S1 result did not release the same target claim transaction")
		}
		t.Logf("released_target_S1 saga=%s worker=%s pid=%d fence=%d token_sha256=%s index=%d next_state=%s lease_live=%t wake=%d/%d", sagaID, s1.WorkerID, s1.PID, s1.ClaimFence, s1.ClaimTokenSHA256, second.Index, second.ExecuteStates[2], second.LeaseLive, second.WakeVersion, second.ConsumedWakeVersion)
		if err := target.killAndWait(t); err != nil {
			t.Fatal("could not terminate owned released-claim worker process")
		}
		_ = s1Conn.Close()

		successor := spawnComponentWorker(t, observer, sagaID, "released-successor", true, runID)
		assertComponentProcessStarted(t, successor, observer.awaitStarted(t))
		s2, s2Conn := observer.awaitPostCommit(t)
		assertComponentPostCommit(t, successor, s2, "released-successor", 2, 3, "retained")
		third := readProcessSagaSnapshot(t, reader, sagaID)
		assertProcessSnapshot(t, third, 3, "PROCESSING", true, s2, 2, "pending", 1, 1, 0)
		if sameComponentProcessIdentity(successor, target) || successor.startID == target.startID ||
			s2.PID != successor.pid || s2.ClaimFence <= oldFence || s2.ClaimTokenSHA256 == s0.ClaimTokenSHA256 {
			t.Fatal("released claim was not replaced by a successor with a distinct process identity, token, and fence")
		}
		t.Logf("released_claim_process_identity target_pid=%d target_start_id=%s successor_pid=%d successor_start_id=%s distinct_original_handles=true distinct_start_ids=true", target.pid, target.startID, successor.pid, successor.startID)
		stillOwned, err := repo.RenewClaim(ctx, domain.SagaClaim{SagaID: sagaID, Token: oldToken, Fence: oldFence, Direction: "forward"}, componentWorkerLease)
		if err != nil || stillOwned {
			t.Fatal("released S1 token regained ownership after S2 successor dispatch")
		}
		if err := successor.killAndWait(t); err != nil {
			t.Fatal("could not terminate owned released-claim successor process")
		}
		_ = s2Conn.Close()
	})
}

type componentProcessMessage struct {
	Kind             string    `json:"kind"`
	PID              int       `json:"pid"`
	BackendPID       int       `json:"backend_pid,omitempty"`
	StartID          string    `json:"start_id"`
	WorkerID         string    `json:"worker_id"`
	StartedAt        time.Time `json:"started_at,omitempty"`
	CompletedAt      time.Time `json:"completed_at,omitempty"`
	Handled          int       `json:"handled,omitempty"`
	RunError         string    `json:"run_error,omitempty"`
	RecoveryConflict bool      `json:"recovery_conflict,omitempty"`
	ContextCanceled  bool      `json:"context_canceled,omitempty"`
	RunID            string    `json:"run_id,omitempty"`
	ClaimFence       int64     `json:"claim_fence,omitempty"`
	TokenSHA256      string    `json:"token_sha256,omitempty"`
	Owned            *bool     `json:"owned,omitempty"`
	RenewalError     string    `json:"renewal_error,omitempty"`
}

type observedComponentEvent struct {
	Event recoverycomponent.PostCommitEvent
	Conn  net.Conn
}

type componentProcessObserver struct {
	listener net.Listener
	started  chan componentProcessMessage
	complete chan componentProcessMessage
	renewals chan componentProcessMessage
	events   chan observedComponentEvent
	close    sync.Once
}

func newComponentProcessObserver(t *testing.T) *componentProcessObserver {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observer := &componentProcessObserver{
		listener: listener,
		started:  make(chan componentProcessMessage, 8),
		complete: make(chan componentProcessMessage, 8),
		renewals: make(chan componentProcessMessage, 16),
		events:   make(chan observedComponentEvent, 8),
	}
	go observer.acceptLoop()
	t.Cleanup(func() { observer.closeListener() })
	return observer
}

func (o *componentProcessObserver) acceptLoop() {
	for {
		conn, err := o.listener.Accept()
		if err != nil {
			return
		}
		go o.handle(conn)
	}
}

func (o *componentProcessObserver) handle(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	var raw json.RawMessage
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&raw); err != nil {
		_ = conn.Close()
		return
	}
	var header struct {
		Kind string `json:"kind"`
	}
	_ = json.Unmarshal(raw, &header)
	if header.Kind != "" {
		var message componentProcessMessage
		if err := json.Unmarshal(raw, &message); err != nil {
			_ = conn.Close()
			return
		}
		switch message.Kind {
		case "process_started":
			o.started <- message
		case "worker_complete":
			o.complete <- message
		case "lease_renewal":
			o.renewals <- message
		default:
			_ = conn.Close()
			return
		}
		_ = conn.Close()
		return
	}
	var event recoverycomponent.PostCommitEvent
	if err := json.Unmarshal(raw, &event); err != nil || event.ContractVersion == "" {
		_ = conn.Close()
		return
	}
	o.events <- observedComponentEvent{Event: event, Conn: conn}
}

func (o *componentProcessObserver) awaitStarted(t *testing.T) componentProcessMessage {
	t.Helper()
	select {
	case message := <-o.started:
		return message
	case <-time.After(15 * time.Second):
		t.Fatal("owned child process did not publish its start identity")
		return componentProcessMessage{}
	}
}

func (o *componentProcessObserver) awaitCompletion(t *testing.T) componentProcessMessage {
	t.Helper()
	select {
	case message := <-o.complete:
		return message
	case <-time.After(15 * time.Second):
		t.Fatal("component worker did not publish its completion count")
		return componentProcessMessage{}
	}
}

func (o *componentProcessObserver) awaitPostCommit(t *testing.T) (recoverycomponent.PostCommitEvent, net.Conn) {
	t.Helper()
	select {
	case observed := <-o.events:
		return observed.Event, observed.Conn
	case <-time.After(20 * time.Second):
		t.Fatal("post-COMMIT observer event was not received")
		return recoverycomponent.PostCommitEvent{}, nil
	}
}

func (o *componentProcessObserver) closeListener() {
	o.close.Do(func() { _ = o.listener.Close() })
}

type ownedComponentProcess struct {
	cmd       *exec.Cmd
	workerID  string
	startID   string
	startedAt time.Time
	pid       int
	waited    bool
	killed    bool
}

func spawnComponentWorker(t *testing.T, observer *componentProcessObserver, sagaID uuid.UUID, workerID string, observe bool, runID string) *ownedComponentProcess {
	return spawnComponentWorkerWithControls(t, observer, sagaID, workerID, observe, runID, "", "", false)
}

func spawnComponentWorkerWithControls(t *testing.T, observer *componentProcessObserver, sagaID uuid.UUID, workerID string, observe bool, runID, preCommitAddress, effectLedgerAddress string, confirmedBooking bool) *ownedComponentProcess {
	t.Helper()
	startID := uuid.NewString()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSagaComponentOwnedWorkerProcess$")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	values := map[string]string{
		"SAGA_COMPONENT_CHILD_MODE":           "worker",
		"SAGA_COMPONENT_PARENT_OBSERVER_ADDR": observer.listener.Addr().String(),
		"SAGA_COMPONENT_START_ID":             startID,
		"SAGA_COMPONENT_WORKER_ID":            workerID,
		"SAGA_COMPONENT_SAGA_ID":              sagaID.String(),
		"SAGA_COMPONENT_PROVIDER_OUTCOME":     "collected",
		"SAGA_COMPONENT_RUN_ID":               runID,
	}
	if effectLedgerAddress != "" {
		values["SAGA_COMPONENT_EFFECT_LEDGER_ADDR"] = effectLedgerAddress
	}
	if confirmedBooking {
		values["SAGA_COMPONENT_FIXTURE_CONFIRMED"] = "true"
	}
	if observe {
		values["RUSHGO_SAGA_COMPONENT_OBSERVER_ADDR"] = observer.listener.Addr().String()
		values["RUSHGO_SAGA_COMPONENT_RUN_ID"] = runID
		values["RUSHGO_SAGA_COMPONENT_WORKER_ID"] = workerID
		values["RUSHGO_SAGA_COMPONENT_HOOK_TIMEOUT"] = componentHookTimeout.String()
	}
	if preCommitAddress != "" {
		values["RUSHGO_SAGA_COMPONENT_PRECOMMIT_CONTROL_ADDR"] = preCommitAddress
		values["RUSHGO_SAGA_COMPONENT_PRECOMMIT_RUN_ID"] = runID
		values["RUSHGO_SAGA_COMPONENT_PRECOMMIT_WORKER_ID"] = workerID
		values["RUSHGO_SAGA_COMPONENT_PRECOMMIT_START_ID"] = startID
		values["RUSHGO_SAGA_COMPONENT_PRECOMMIT_TIMEOUT"] = componentHookTimeout.String()
	}
	cmd.Env = componentProcessEnvironment(os.Environ(), values)
	if err := cmd.Start(); err != nil {
		t.Fatal("could not start the owned Saga component worker process")
	}
	process := &ownedComponentProcess{cmd: cmd, workerID: workerID, startID: startID, startedAt: time.Now().UTC(), pid: cmd.Process.Pid}
	t.Cleanup(func() { process.stopForCleanup() })
	return process
}

func componentProcessEnvironment(base []string, overrides map[string]string) []string {
	filtered := make([]string, 0, len(base)+len(overrides))
	for _, item := range base {
		key, _, _ := strings.Cut(item, "=")
		_, overridden := overrides[key]
		componentOnly := strings.HasPrefix(key, "RUSHGO_SAGA_COMPONENT_") || strings.HasPrefix(key, "SAGA_COMPONENT_FIXTURE_") ||
			key == "SAGA_COMPONENT_EFFECT_LEDGER_ADDR" || key == "SAGA_COMPONENT_FIXTURE_CONFIRMED" ||
			key == "SAGA_COMPONENT_RENEWAL_OBSERVER_ADDR" || key == "SAGA_COMPONENT_REPORT_RUN_ERROR"
		if !overridden && !componentOnly {
			filtered = append(filtered, item)
		}
	}
	for key, value := range overrides {
		filtered = append(filtered, key+"="+value)
	}
	return filtered
}

func (p *ownedComponentProcess) assertIdentity(t *testing.T, message componentProcessMessage) {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil || p.pid != p.cmd.Process.Pid || message.PID != p.pid ||
		message.StartID != p.startID || message.WorkerID != p.workerID || message.StartedAt.IsZero() || !message.StartedAt.After(p.startedAt.Add(-time.Second)) {
		t.Fatal("child process identity did not match its original process handle and start marker")
	}
	p.startedAt = message.StartedAt
	t.Logf("component_process_started worker=%s pid=%d start_id=%s started_at=%s original_process_handle=owned", p.workerID, p.pid, p.startID, message.StartedAt.UTC().Format(time.RFC3339Nano))
}

func assertComponentProcessStarted(t *testing.T, p *ownedComponentProcess, message componentProcessMessage) {
	t.Helper()
	p.assertIdentity(t, message)
}

func (p *ownedComponentProcess) killAndWait(t *testing.T) error {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil || p.startID == "" || p.pid != p.cmd.Process.Pid || p.waited {
		return os.ErrProcessDone
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return err
	}
	waitErr := p.cmd.Wait()
	p.waited = true
	p.killed = true
	if p.cmd.ProcessState == nil || p.cmd.ProcessState.Success() || waitErr == nil {
		return os.ErrInvalid
	}
	t.Logf("component_process_killed worker=%s pid=%d start_id=%s exit_code=%d original_process_handle=used", p.workerID, p.pid, p.startID, p.cmd.ProcessState.ExitCode())
	return nil
}

func (p *ownedComponentProcess) waitClean(t *testing.T) error {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil || p.pid != p.cmd.Process.Pid || p.waited {
		return os.ErrProcessDone
	}
	err := p.cmd.Wait()
	p.waited = true
	if err != nil || p.cmd.ProcessState == nil || !p.cmd.ProcessState.Success() {
		return os.ErrInvalid
	}
	t.Logf("component_process_exited worker=%s pid=%d start_id=%s exit_code=%d", p.workerID, p.pid, p.startID, p.cmd.ProcessState.ExitCode())
	return nil
}

func (p *ownedComponentProcess) stopForCleanup() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil || p.waited || p.pid != p.cmd.Process.Pid || p.startID == "" {
		return
	}
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
	p.waited = true
	p.killed = true
}

func createComponentProcessSaga(t *testing.T, repo *repository.SagaRepository) uuid.UUID {
	t.Helper()
	_, saga, _, _, _ := replayLiteralFixture()
	created, _, err := repo.CreateOrGetSaga(context.Background(), *saga.Input, saga.Steps)
	if err != nil || created == nil {
		t.Fatal("could not create isolated Saga process fixture")
	}
	return created.ID
}

func componentServiceForSaga(t *testing.T, repo *repository.SagaRepository, saga *domain.Saga) *SagaService {
	t.Helper()
	service, _, booking, payment, event := replayLiteralFixture()
	if saga == nil || saga.Input == nil || len(saga.Input.SeatIDs) == 0 {
		t.Fatal("stored component Saga fixture is incomplete")
	}
	input := saga.Input
	booking.view.Booking.Id = input.BookingID.String()
	booking.view.Booking.UserId = input.UserID.String()
	booking.view.Booking.EventId = input.EventID.String()
	booking.view.Booking.Status = "PENDING"
	booking.view.Booking.TotalAmountCents = input.AmountCents
	booking.view.Booking.Items = nil
	seats := make([]string, len(input.SeatIDs))
	for i, seat := range input.SeatIDs {
		seats[i] = seat.String()
	}
	booking.view.Booking.Items = []*bookingv1.BookingItem{{TicketTierId: uuid.NewString(), Quantity: int32(len(seats)), SeatIds: seats, UnitPriceCents: input.AmountCents}}
	booking.view.Generation = input.Generation
	booking.view.Phase = "OPEN"
	booking.view.Currency = input.Currency
	booking.view.BookingMode = input.BookingMode
	booking.view.ReservationDeadline = timestamppb.New(input.ReservationDeadline)
	booking.view.ConfirmationDecisionId = decisionID(input.BookingID.String(), "claim_confirmation")
	if os.Getenv("SAGA_COMPONENT_FIXTURE_CONFIRMED") == "true" {
		booking.view.Booking.Status = "CONFIRMED"
		booking.view.Phase = "TERMINAL"
		booking.view.DecisionId = decisionID(input.BookingID.String(), "confirm")
	}
	if os.Getenv("SAGA_COMPONENT_FIXTURE_TERMINAL") == "true" {
		booking.view.Booking.Status = "FAILED"
		booking.view.Phase = "TERMINAL"
		booking.view.DecisionId = decisionID(input.BookingID.String(), "fail")
	}
	// Payment identity is stable per booking/generation in the product. A fresh
	// random ID per worker process would make S1's durable wake and S2's binding
	// disagree even though both read the same synthetic Payment.
	payment.entry.Id = uuid.NewSHA1(uuid.NameSpaceOID,
		[]byte(input.BookingID.String()+"|payment|1")).String()
	if configuredPaymentID := os.Getenv("SAGA_COMPONENT_FIXTURE_PAYMENT_ID"); configuredPaymentID != "" {
		paymentID, err := uuid.Parse(configuredPaymentID)
		if err != nil || paymentID == uuid.Nil {
			t.Fatal("component payment fixture identity is invalid")
		}
		payment.entry.Id = paymentID.String()
	}
	payment.entry.UserId = input.UserID.String()
	payment.entry.BookingId = input.BookingID.String()
	payment.entry.Price = int32(input.AmountCents)
	payment.entry.Currency = input.Currency
	payment.entry.PaymentGeneration = input.Generation
	payment.entry.PaymentIntentId = "pi_component_fixture"
	payment.entry.ProviderOutcome = os.Getenv("SAGA_COMPONENT_PROVIDER_OUTCOME")
	if payment.entry.ProviderOutcome == "" {
		payment.entry.ProviderOutcome = "collected"
	}
	payment.entry.ProviderStatus = os.Getenv("SAGA_COMPONENT_FIXTURE_PROVIDER_STATUS")
	payment.entry.ReconciliationState = os.Getenv("SAGA_COMPONENT_FIXTURE_RECONCILIATION_STATE")
	payment.entry.RefundState = os.Getenv("SAGA_COMPONENT_FIXTURE_REFUND_STATE")
	if payment.entry.RefundState == "succeeded" {
		payment.entry.RefundAmountCents = input.AmountCents
	}
	payment.entry.Status = "success"
	service.repo = repo
	service.bookingClient = booking
	service.paymentClient = payment
	if effectAddress := os.Getenv("SAGA_COMPONENT_EFFECT_LEDGER_ADDR"); effectAddress != "" {
		service.eventClient = &componentEventEffectDouble{
			EventServiceClient: event,
			ledgerAddress:      effectAddress,
			workerID:           os.Getenv("SAGA_COMPONENT_WORKER_ID"),
			startID:            os.Getenv("SAGA_COMPONENT_START_ID"),
		}
		service.paymentClient = &componentPaymentResolutionEffectDouble{
			PaymentServiceClient: payment,
			ledgerAddress:        effectAddress,
			workerID:             os.Getenv("SAGA_COMPONENT_WORKER_ID"),
			startID:              os.Getenv("SAGA_COMPONENT_START_ID"),
		}
	} else {
		service.eventClient = event
	}
	return service
}

func (o *componentProcessObserver) awaitPostCommitForWorker(t *testing.T, workerID string) (recoverycomponent.PostCommitEvent, net.Conn) {
	t.Helper()
	event, conn := o.awaitPostCommit(t)
	if event.WorkerID != workerID {
		_ = conn.Close()
		t.Fatalf("post-COMMIT came from unexpected worker %q", event.WorkerID)
	}
	return event, conn
}

func assertProcessPostCommit(t *testing.T, child *ownedComponentProcess, event recoverycomponent.PostCommitEvent, workerID string, prior, next int, action string) {
	t.Helper()
	assertComponentPostCommit(t, child, event, workerID, prior, next, action)
}

func assertComponentPostCommit(t *testing.T, child *ownedComponentProcess, event recoverycomponent.PostCommitEvent, workerID string, prior, next int, action string) {
	t.Helper()
	if event.ContractVersion != recoverycomponent.PostCommitContractVersion || event.CommitOutcome != "COMMIT_ACKED" ||
		event.WorkerID != workerID || event.PID != child.pid || event.RunID == "" || event.EmittedAt.IsZero() ||
		event.Direction != "forward" || event.ResultState != "completed" || event.PriorIndex != prior ||
		event.NextIndex != next || event.ClaimAction != action || event.ClaimTokenSHA256 == "" || event.ClaimFence < 1 ||
		event.RunID != os.Getenv("SAGA_COMPONENT_RUN_ID") || event.TicketOutboxExpected {
		t.Fatalf("post-COMMIT event does not match the owned worker/process boundary: %+v", event)
	}
	t.Logf("component_post_commit version=%s saga=%s step=%s worker=%s pid=%d commit=%s index=%d->%d status=%s claim=%s fence=%d token_sha256=%s", event.ContractVersion, event.SagaID, event.StepID, event.WorkerID, event.PID, event.CommitOutcome, event.PriorIndex, event.NextIndex, event.NextStatus, event.ClaimAction, event.ClaimFence, event.ClaimTokenSHA256)
}

type componentProcessSagaSnapshot struct {
	Index               int
	Status              string
	Direction           string
	Recovery            string
	Token               string
	Fence               int64
	LeaseLive           bool
	WakeVersion         int64
	ConsumedWakeVersion int64
	ExecuteStates       [5]string
	CompensateStates    [5]string
	OutboxCount         int
}

func readProcessSagaSnapshot(t *testing.T, pool *pgxpool.Pool, sagaID uuid.UUID) componentProcessSagaSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var snapshot componentProcessSagaSnapshot
	if err := pool.QueryRow(ctx, `SELECT current_step_index,status::text,direction,recovery_state,COALESCE(claim_token::text,''),claim_fence,COALESCE(lease_until>clock_timestamp(),false),wake_version,consumed_wake_version,(SELECT count(*)::int FROM saga_ticket_issuance_outbox WHERE saga_id=$1) FROM sagas WHERE id=$1`, sagaID).Scan(&snapshot.Index, &snapshot.Status, &snapshot.Direction, &snapshot.Recovery, &snapshot.Token, &snapshot.Fence, &snapshot.LeaseLive, &snapshot.WakeVersion, &snapshot.ConsumedWakeVersion, &snapshot.OutboxCount); err != nil {
		t.Fatal("independent observer could not read Saga row")
	}
	for i := range snapshot.ExecuteStates {
		if err := pool.QueryRow(ctx, `SELECT execute_state FROM saga_steps WHERE saga_id=$1 AND "order"=$2`, sagaID, i).Scan(&snapshot.ExecuteStates[i]); err != nil {
			t.Fatal("independent observer could not read Saga step state")
		}
		if err := pool.QueryRow(ctx, `SELECT compensate_state FROM saga_steps WHERE saga_id=$1 AND "order"=$2`, sagaID, i).Scan(&snapshot.CompensateStates[i]); err != nil {
			t.Fatal("independent observer could not read Saga compensation state")
		}
	}
	return snapshot
}

func assertProcessSnapshot(t *testing.T, got componentProcessSagaSnapshot, index int, status string, leaseLive bool, event recoverycomponent.PostCommitEvent, resultIndex int, nextState string, wake, consumed, outbox int) {
	t.Helper()
	if got.Index != index || got.Status != status || got.Direction != "forward" || got.Recovery != "ready" ||
		got.LeaseLive != leaseLive || got.Fence != event.ClaimFence || got.WakeVersion != int64(wake) ||
		got.ConsumedWakeVersion != int64(consumed) || got.OutboxCount != outbox || got.ExecuteStates[resultIndex] != "completed" {
		t.Fatal("independent PostgreSQL snapshot disagreed with committed worker event")
	}
	if index < len(got.ExecuteStates) && got.ExecuteStates[index] != nextState {
		t.Fatal("next business step was dispatched before its preceding result was acknowledged")
	}
	if event.ClaimAction == "retained" {
		digest := sha256.Sum256([]byte(got.Token))
		if got.Token == "" || !got.LeaseLive || hex.EncodeToString(digest[:]) != event.ClaimTokenSHA256 {
			t.Fatal("retained owner did not match the independently read token/fence/lease")
		}
	} else if event.ClaimAction == "released" {
		if got.Token != "" || got.LeaseLive {
			t.Fatal("released claim still has a live token or lease")
		}
	}
	t.Logf("independent_postgres_snapshot saga=%s worker=%s pid=%d index=%d status=%s claim=%s fence=%d token_sha256=%s lease_live=%t wake=%d/%d next_step_state=%s outbox_rows=%d", event.SagaID, event.WorkerID, event.PID, got.Index, got.Status, event.ClaimAction, got.Fence, event.ClaimTokenSHA256, got.LeaseLive, got.WakeVersion, got.ConsumedWakeVersion, got.ExecuteStates[index], got.OutboxCount)
}

func waitForComponentLeaseExpiry(t *testing.T, reader *pgxpool.Pool, sagaID uuid.UUID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var expired bool
		if err := reader.QueryRow(context.Background(), `SELECT claim_token IS NULL OR lease_until<=clock_timestamp() FROM sagas WHERE id=$1`, sagaID).Scan(&expired); err != nil {
			t.Fatal("could not check retained Saga lease after worker death")
		}
		if expired {
			t.Logf("retained_claim_expired saga=%s after_owned_worker_exit=true", sagaID)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("retained Saga lease did not expire after its owned worker process was killed")
}

func sendComponentProcessMessage(t *testing.T, address string, message componentProcessMessage) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal("component worker could not report process identity")
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if err := json.NewEncoder(conn).Encode(message); err != nil {
		t.Fatal("component worker could not send process identity")
	}
}

func acknowledgeComponentEvent(conn net.Conn) error {
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	_, err := conn.Write([]byte("1"))
	return err
}
