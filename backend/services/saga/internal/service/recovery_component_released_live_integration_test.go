//go:build reliability_component

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/recoverycomponent"
	"github.com/ticketbox/saga/internal/repository"
)

// TestSagaComponentReleasedLiveTargetConcurrentAdvance holds A alive at the
// existing post-COMMIT observation for the released S1 claim while B executes
// the ordinary RunOnce/ClaimDueSagas path through S2-S4. A resumes only after
// B's committed terminal state and outbox are independently visible.
func TestSagaComponentReleasedLiveTargetConcurrentAdvanceIntegration(t *testing.T) {
	fixture := recoverycomponent.CreateDatabaseFixture(t)
	if fixture.Schema == "" || fixture.URL == "" {
		t.Fatal("owned Saga component schema was not created")
	}
	pool := recoverycomponent.OpenComponentPool(t)
	reader := recoverycomponent.OpenIndependentPool(t, "saga_component_released_live_reader")
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
	runID := os.Getenv("SAGA_COMPONENT_RUN_ID")
	if runID == "" {
		t.Fatal("SAGA_COMPONENT_RUN_ID is required")
	}
	t.Logf("component_database_identity run_id=%s schema=%s repository_backend_pid=%d independent_reader_backend_pid=%d distinct_backend_connections=true", runID, fixture.Schema, appPID, readerPID)

	observer := newComponentProcessObserver(t)
	effects := newComponentEffectLedger(t)
	sagaID := createComponentProcessSaga(t, repo)
	saga, err := repo.GetSagaById(context.Background(), sagaID)
	if err != nil || saga == nil || len(saga.Steps) != 5 {
		t.Fatal("could not read the newly created normal Saga graph")
	}
	stepIDs := [5]uuid.UUID{}
	for i, step := range saga.Steps {
		stepIDs[i] = step.ID
	}
	t.Logf("component_released_live_fixture saga=%s direction=%s index=%d steps=%v created_through_repository=true", sagaID, saga.Direction, saga.CurrentStepIndex, stepIDs)

	target := spawnComponentWorkerWithRenewalReport(t, observer, effects.Address(), sagaID, "released-live-target", runID)
	assertComponentProcessStarted(t, target, observer.awaitStarted(t))

	s0Event, s0Conn := observer.awaitPostCommitForWorker(t, target.workerID)
	assertComponentPostCommit(t, target, s0Event, target.workerID, 0, 1, "retained")
	if s0Event.StepID != stepIDs[0].String() {
		t.Fatal("A did not complete the fixture's normal S0 step")
	}
	s0Snapshot := readProcessSagaSnapshot(t, reader, sagaID)
	assertReleasedLiveSnapshot(t, s0Snapshot, 1, "PROCESSING", true, s0Event, 0, "pending", 0, 0, 0)
	if componentTokenDigest(s0Snapshot.Token) != s0Event.ClaimTokenSHA256 {
		t.Fatal("independent S0 snapshot did not match A's claim token digest")
	}
	initialRenewal := awaitComponentLeaseRenewal(t, observer, target.workerID, 15*time.Second)
	if initialRenewal.PID != target.pid || initialRenewal.StartID != target.startID || initialRenewal.Owned == nil || !*initialRenewal.Owned || initialRenewal.ClaimFence != s0Event.ClaimFence ||
		initialRenewal.TokenSHA256 != s0Event.ClaimTokenSHA256 || initialRenewal.RenewalError != "" || initialRenewal.RunID != runID {
		t.Fatalf("A's actual initial renewal did not match its active S0 claim: owned=%v fence=%d token_sha256=%s error=%q", renewalOwned(initialRenewal), initialRenewal.ClaimFence, initialRenewal.TokenSHA256, initialRenewal.RenewalError)
	}
	t.Logf("component_lease_renewal worker=%s pid=%d start_id=%s run_id=%s claim_fence=%d token_sha256=%s owned=true stage=initial", initialRenewal.WorkerID, initialRenewal.PID, initialRenewal.StartID, initialRenewal.RunID, initialRenewal.ClaimFence, initialRenewal.TokenSHA256)
	if err := acknowledgeComponentEvent(s0Conn); err != nil {
		t.Fatal("could not resume A after the independent S0 read")
	}

	s1Event, s1Conn := observer.awaitPostCommitForWorker(t, target.workerID)
	assertComponentPostCommit(t, target, s1Event, target.workerID, 1, 2, "released")
	if s1Event.StepID != stepIDs[1].String() || s1Event.NextStatus != "PROCESSING" ||
		s1Event.ClaimFence != s0Event.ClaimFence || s1Event.ClaimTokenSHA256 != s0Event.ClaimTokenSHA256 {
		t.Fatal("A's S1 commit did not release the same normal claim")
	}
	s1Snapshot := readProcessSagaSnapshot(t, reader, sagaID)
	assertReleasedLiveSnapshot(t, s1Snapshot, 2, "PROCESSING", false, s1Event, 1, "pending", 1, 1, 0)
	if s1Snapshot.Token != "" {
		t.Fatal("S1's committed release left a current claim token")
	}
	targetBackendPID := waitForReleasedLiveBackendPID(t, reader, target.pid)
	t.Logf("component_worker_database_identity worker=%s os_pid=%d start_id=%s postgres_backend_pid=%d", target.workerID, target.pid, target.startID, targetBackendPID)
	t.Logf("component_released_live_commit saga=%s worker=%s pid=%d start_id=%s step=%s commit=COMMIT_ACKED index=%d status=%s claim=released fence=%d token_sha256=%s lease_live=%t wake=%d/%d next_step_state=%s observer_gate_open=true", sagaID, target.workerID, target.pid, target.startID, s1Event.StepID, s1Snapshot.Index, s1Snapshot.Status, s1Event.ClaimFence, s1Event.ClaimTokenSHA256, s1Snapshot.LeaseLive, s1Snapshot.WakeVersion, s1Snapshot.ConsumedWakeVersion, s1Snapshot.ExecuteStates[2])

	rejectedRenewal := awaitRejectedComponentRenewal(t, observer, target.workerID, target.pid, target.startID, s1Event.ClaimFence, s1Event.ClaimTokenSHA256, runID, 15*time.Second)
	if rejectedRenewal.RenewalError != "" {
		t.Fatalf("A's normal stale renewal errored instead of being rejected by ownership: %s", rejectedRenewal.RenewalError)
	}
	releasedAfterRenewal := readProcessSagaSnapshot(t, reader, sagaID)
	assertReleasedLiveSnapshot(t, releasedAfterRenewal, 2, "PROCESSING", false, s1Event, 1, "pending", 1, 1, 0)
	assertComponentStillAtPostCommitGate(t, s1Conn, target.workerID)
	t.Logf("component_stale_renewal worker=%s pid=%d start_id=%s run_id=%s claim_fence=%d token_sha256=%s owned=false claim_token_empty=true lease_live=false process_alive_at_gate=true", rejectedRenewal.WorkerID, rejectedRenewal.PID, rejectedRenewal.StartID, rejectedRenewal.RunID, rejectedRenewal.ClaimFence, rejectedRenewal.TokenSHA256)

	successor := spawnComponentWorkerWithControls(t, observer, sagaID, "released-live-successor", true, runID, "", effects.Address(), false)
	assertComponentProcessStarted(t, successor, observer.awaitStarted(t))
	if sameComponentProcessIdentity(target, successor) || target.cmd.Process == successor.cmd.Process || target.startID == successor.startID {
		t.Fatal("B did not start as a distinct owned OS process while A remained at its gate")
	}
	t.Logf("component_released_live_processes saga=%s worker_a=%s pid_a=%d start_id_a=%s original_handle_a=owned worker_b=%s pid_b=%d start_id_b=%s original_handle_b=owned distinct_handles=true distinct_start_ids=true", sagaID, target.workerID, target.pid, target.startID, successor.workerID, successor.pid, successor.startID)

	s2Event, s2Conn := observer.awaitPostCommitForWorker(t, successor.workerID)
	assertComponentPostCommit(t, successor, s2Event, successor.workerID, 2, 3, "retained")
	if s2Event.StepID != stepIDs[2].String() || s2Event.ClaimFence <= s1Event.ClaimFence || s2Event.ClaimTokenSHA256 == s1Event.ClaimTokenSHA256 {
		t.Fatal("B did not acquire a new fenced claim and commit the normal S2 step")
	}
	s2Snapshot := readProcessSagaSnapshot(t, reader, sagaID)
	assertReleasedLiveSnapshot(t, s2Snapshot, 3, "PROCESSING", true, s2Event, 2, "pending", 1, 1, 0)
	successorBackendPID := waitForReleasedLiveBackendPID(t, reader, successor.pid)
	t.Logf("component_worker_database_identity worker=%s os_pid=%d start_id=%s postgres_backend_pid=%d", successor.workerID, successor.pid, successor.startID, successorBackendPID)
	t.Logf("component_worker_dispatch saga=%s worker=%s pid=%d start_id=%s source=normal_RunOnce_ClaimDueSagas handled_pending=true claim_fence=%d token_sha256=%s index=%d step=%s claim=retained lease_live=true", sagaID, successor.workerID, successor.pid, successor.startID, s2Event.ClaimFence, s2Event.ClaimTokenSHA256, s2Snapshot.Index, s2Event.StepID)
	if err := acknowledgeComponentEvent(s2Conn); err != nil {
		t.Fatal("could not resume B after the independent S2 read")
	}

	s3Event, s3Conn := observer.awaitPostCommitForWorker(t, successor.workerID)
	assertComponentPostCommit(t, successor, s3Event, successor.workerID, 3, 4, "retained")
	if s3Event.StepID != stepIDs[3].String() || s3Event.ClaimFence != s2Event.ClaimFence || s3Event.ClaimTokenSHA256 != s2Event.ClaimTokenSHA256 {
		t.Fatal("B's S3 result did not continue the same claim")
	}
	s3Snapshot := readProcessSagaSnapshot(t, reader, sagaID)
	assertReleasedLiveSnapshot(t, s3Snapshot, 4, "PROCESSING", true, s3Event, 3, "pending", 1, 1, 0)
	if err := acknowledgeComponentEvent(s3Conn); err != nil {
		t.Fatal("could not resume B after the independent S3 read")
	}

	s4Event, s4Conn := observer.awaitPostCommitForWorker(t, successor.workerID)
	assertComponentS4PostCommit(t, s4Event, successor, sagaID, stepIDs[4], runID)
	if s4Event.ClaimFence != s2Event.ClaimFence || s4Event.ClaimTokenSHA256 != s2Event.ClaimTokenSHA256 {
		t.Fatal("B's S4 result did not release its own claim")
	}
	s4Snapshot := readProcessSagaSnapshot(t, reader, sagaID)
	assertS4OutboxSnapshot(t, s4Snapshot, s4Event)
	if err := acknowledgeComponentEvent(s4Conn); err != nil {
		t.Fatal("could not resume B after the independent S4/outbox read")
	}
	bCompletion := observer.awaitCompletion(t)
	if bCompletion.WorkerID != successor.workerID || bCompletion.PID != successor.pid || bCompletion.StartID != successor.startID ||
		bCompletion.RunID != runID || bCompletion.Handled != 1 || bCompletion.RunError != "" {
		t.Fatalf("B did not complete one normal recovery dispatch: worker=%s pid=%d start_id=%s handled=%d run_error=%q", bCompletion.WorkerID, bCompletion.PID, bCompletion.StartID, bCompletion.Handled, bCompletion.RunError)
	}
	if err := successor.waitClean(t); err != nil {
		t.Fatal("B's normal S2-S4 worker process did not exit cleanly")
	}
	bFinal := readProcessSagaSnapshot(t, reader, sagaID)
	assertSameReleasedLiveSnapshot(t, bFinal, s4Snapshot, "B completion")
	assertComponentStillAtPostCommitGate(t, s1Conn, target.workerID)
	t.Logf("component_worker_completed saga=%s worker=%s pid=%d start_id=%s handled=%d index=%d status=%s claim=released fence=%d outbox_rows=%d target_a_still_alive_at_gate=true", sagaID, successor.workerID, successor.pid, successor.startID, bCompletion.Handled, bFinal.Index, bFinal.Status, bFinal.Fence, bFinal.OutboxCount)

	if err := acknowledgeComponentEvent(s1Conn); err != nil {
		t.Fatal("could not resume A after B's committed terminal state was independently verified")
	}
	aCompletion := awaitCompletionOrUnexpectedCommit(t, observer, target.workerID)
	// RunOnce intentionally suppresses ErrRecoveryConflict from a raced claim;
	// the delegated RenewClaim(false) above is the ownership rejection evidence.
	if aCompletion.PID != target.pid || aCompletion.StartID != target.startID || aCompletion.RunID != runID ||
		aCompletion.Handled != 1 || aCompletion.RunError != "" || aCompletion.RecoveryConflict {
		t.Fatalf("A's normal RunOnce did not finish after the stale renewal was rejected: pid=%d start_id=%s handled=%d run_error=%q recovery_conflict=%t", aCompletion.PID, aCompletion.StartID, aCompletion.Handled, aCompletion.RunError, aCompletion.RecoveryConflict)
	}
	if err := target.waitClean(t); err != nil {
		t.Fatal("A did not exit cleanly after resuming from its rejected stale renewal")
	}
	t.Logf("component_released_live_target_resumed saga=%s worker=%s pid=%d start_id=%s handled=%d run_error_empty=true stale_renewal_rejected=true recovery_conflict_suppressed_by_normal_RunOnce=true", sagaID, target.workerID, target.pid, target.startID, aCompletion.Handled)

	final := readProcessSagaSnapshot(t, reader, sagaID)
	assertSameReleasedLiveSnapshot(t, final, bFinal, "A resume")
	if final.Index != 5 || final.Status != "COMPLETED" || final.Token != "" || final.LeaseLive || final.OutboxCount != 1 {
		t.Fatal("A's resume changed B's completed Saga or single Ticket outbox result")
	}
	assertComponentEffectOwnedBy(t, effects.Snapshot("reserve", stepIDs[0].String()), "reserve", stepIDs[0].String(), target)
	assertComponentEffectOwnedBy(t, effects.Snapshot("book", stepIDs[2].String()), "book", stepIDs[2].String(), successor)
	assertComponentEffectOwnedBy(t, effects.Snapshot("account", stepIDs[4].String()), "account", stepIDs[4].String(), successor)
	select {
	case unexpected := <-observer.events:
		_ = unexpected.Conn.Close()
		t.Fatalf("stale A emitted an additional persisted-step commit: worker=%s pid=%d index=%d->%d", unexpected.Event.WorkerID, unexpected.Event.PID, unexpected.Event.PriorIndex, unexpected.Event.NextIndex)
	default:
	}
	t.Logf("component_released_live_final saga=%s index=%d status=%s fence=%d token_empty=true lease_live=false steps=S0-S4_completed outbox_rows=%d reserve_attempts=1 book_attempts=1 account_attempts=1 a_effects_after_release=0 b_effects_preserved=true provider=local_test_double_only", sagaID, final.Index, final.Status, final.Fence, final.OutboxCount)
}

// TestSagaComponentTerminalCompensationReleaseNoSuccessorIntegration holds
// A alive at the first natural terminal S0 compensation release. A normal B
// scan sees no successor work, then A resumes without changing terminal state.
func TestSagaComponentTerminalCompensationReleaseNoSuccessorIntegration(t *testing.T) {
	fixture := recoverycomponent.CreateDatabaseFixture(t)
	if fixture.Schema == "" || fixture.URL == "" {
		t.Fatal("owned Saga component schema was not created")
	}
	pool := recoverycomponent.OpenComponentPool(t)
	reader := recoverycomponent.OpenIndependentPool(t, "saga_component_comp_terminal_reader")
	repo := repository.NewSagaRepository(pool)
	var repositoryPID, readerPID int
	if err := pool.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&repositoryPID); err != nil {
		t.Fatal(err)
	}
	if err := reader.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&readerPID); err != nil {
		t.Fatal(err)
	}
	if repositoryPID == readerPID {
		t.Fatal("Saga repository and compensation observer share a PostgreSQL backend")
	}
	runID := os.Getenv("SAGA_COMPONENT_RUN_ID")
	if runID == "" {
		t.Fatal("SAGA_COMPONENT_RUN_ID is required")
	}
	for _, name := range []string{
		"RUSHGO_SAGA_COMPONENT_OBSERVER_ADDR", "RUSHGO_SAGA_COMPONENT_HOOK_TIMEOUT",
		"RUSHGO_SAGA_COMPONENT_PRECOMMIT_CONTROL_ADDR", "RUSHGO_SAGA_COMPONENT_PRECOMMIT_TIMEOUT",
	} {
		t.Setenv(name, "")
	}
	t.Logf("component_database_identity run_id=%s schema=%s repository_backend_pid=%d independent_reader_backend_pid=%d distinct_backend_connections=true", runID, fixture.Schema, repositoryPID, readerPID)

	ctx := context.Background()
	sagaID := createComponentProcessSaga(t, repo)
	saga, err := repo.GetSagaById(ctx, sagaID)
	if err != nil || saga == nil || saga.Input == nil || len(saga.Steps) != 5 {
		t.Fatal("could not read the normally created Saga for terminal compensation setup")
	}
	paymentID := uuid.New()
	t.Setenv("SAGA_COMPONENT_PROVIDER_OUTCOME", "not_collected")
	t.Setenv("SAGA_COMPONENT_FIXTURE_PAYMENT_ID", paymentID.String())
	t.Setenv("SAGA_COMPONENT_FIXTURE_PROVIDER_STATUS", "requires_payment_method")
	t.Setenv("SAGA_COMPONENT_FIXTURE_RECONCILIATION_STATE", "resolved")
	bootstrapService := componentServiceForSaga(t, repo, saga)
	booking, ok := bootstrapService.bookingClient.(*replayBooking)
	if !ok {
		t.Fatal("terminal-compensation setup lacks its named Booking test double")
	}
	bootstrapPayment, ok := bootstrapService.paymentClient.(*replayPayment)
	if !ok {
		t.Fatal("terminal-compensation setup lacks its named Payment test double")
	}
	if booking.view.Phase != "OPEN" || booking.view.Booking.Status != "PENDING" || bootstrapPayment.entry.Id != paymentID.String() {
		t.Fatal("normal bootstrap test doubles do not match the new forward Saga identity")
	}
	bootstrapConfig := RecoveryConfig{
		ScanInterval: 200 * time.Millisecond, LeaseTTL: componentWorkerLease,
		RemoteTimeout: componentWorkerRemoteLimit, BatchSize: 1, Concurrency: 1, MaxAttempts: 3,
	}
	bootstrapWorker, err := NewRecoveryWorker(repo, bootstrapService, bootstrapService.logger, bootstrapConfig)
	if err != nil {
		t.Fatal("normal forward bootstrap worker configuration was rejected")
	}
	if handled, err := bootstrapWorker.RunOnce(ctx, 1); err != nil || handled != 1 {
		t.Fatalf("normal forward bootstrap did not reach payment wait: handled=%d err=%v", handled, err)
	}
	forwardWait, err := repo.GetSagaById(ctx, sagaID)
	if err != nil || forwardWait.Direction != "forward" || forwardWait.Status != domain.SAGA_WAIT_FOR_PAYMENT || forwardWait.CurrentStepIndex != 2 ||
		forwardWait.Steps[0].ExecuteState != "completed" || forwardWait.Steps[1].ExecuteState != "completed" {
		t.Fatal("normal forward bootstrap did not durably reach the S2 payment wait")
	}
	if _, err := bootstrapService.SettleOrderSaga(ctx, forwardWait.BookingID, forwardWait.Generation, "", paymentID.String(), "component_terminal_compensation_release_seed"); err != nil {
		t.Fatalf("normal settlement did not wake the pending S2 step: %v", err)
	}
	awakeSnapshot := readProcessSagaSnapshot(t, reader, sagaID)
	if awakeSnapshot.Index != 2 || awakeSnapshot.Status != "PROCESSING" || awakeSnapshot.Direction != "forward" ||
		awakeSnapshot.Token != "" || awakeSnapshot.LeaseLive || awakeSnapshot.Fence != forwardWait.ClaimFence || awakeSnapshot.OutboxCount != 0 {
		t.Fatalf("normal settlement did not make the forward S2 step due without a claim: %s", releasedLiveSnapshotSummary(awakeSnapshot))
	}
	awakeSaga, err := repo.GetSagaById(ctx, sagaID)
	if err != nil || awakeSaga.PaymentID != paymentID {
		t.Fatal("normal settlement did not persist the synthetic payment identity used by the worker fixture")
	}
	decisionIDValue := decisionID(saga.BookingID.String(), "fail")
	t.Logf("component_compensation_setup name=TERMINAL_COMPENSATION_RELEASE_NO_SUCCESSOR source=CreateOrGetSaga_RunOnce_S0_S1_SettleOrderSaga saga=%s index=%d status=%s direction=%s fence=%d wake=%d/%d payment_outcome=not_collected booking_phase=OPEN provider=local_test_double_only", sagaID, awakeSnapshot.Index, awakeSnapshot.Status, awakeSnapshot.Direction, awakeSnapshot.Fence, awakeSnapshot.WakeVersion, awakeSnapshot.ConsumedWakeVersion)

	observer := newComponentProcessObserver(t)
	effects := newComponentEffectLedger(t)
	target := spawnComponentCompensationTargetWorker(t, observer, effects.Address(), sagaID, "terminal-compensation-target", runID, paymentID.String())
	assertComponentProcessStarted(t, target, observer.awaitStarted(t))
	initialRenewal := awaitComponentLeaseRenewal(t, observer, target.workerID, 15*time.Second)
	if initialRenewal.PID != target.pid || initialRenewal.StartID != target.startID || initialRenewal.Owned == nil || !*initialRenewal.Owned ||
		initialRenewal.ClaimFence != awakeSnapshot.Fence+1 || initialRenewal.RunID != runID || initialRenewal.RenewalError != "" {
		t.Fatalf("target A did not acquire and renew the normal terminal compensation claim: pid=%d start_id=%s fence=%d owned=%v error=%q", initialRenewal.PID, initialRenewal.StartID, initialRenewal.ClaimFence, renewalOwned(initialRenewal), initialRenewal.RenewalError)
	}
	if initialRenewal.ClaimFence == 0 {
		t.Fatal("target A acquired no fenced claim")
	}

	compS2Event, compS2Conn := observer.awaitPostCommitForWorker(t, target.workerID)
	assertComponentCompensationStepPostCommit(t, target, compS2Event, runID, sagaID, saga.Steps[2].ID, 2, 1, "ROLLING_BACK", "retained", initialRenewal.ClaimFence)
	compS2Snapshot := readProcessSagaSnapshot(t, reader, sagaID)
	assertCompensationProgressSnapshot(t, compS2Snapshot, compS2Event, 1, [5]string{"pending", "pending", "completed", "pending", "pending"}, "S2 compensation COMMIT")
	if err := acknowledgeComponentEvent(compS2Conn); err != nil {
		t.Fatal("could not resume A after the independent S2 compensation read")
	}

	compS1Event, compS1Conn := observer.awaitPostCommitForWorker(t, target.workerID)
	assertComponentCompensationStepPostCommit(t, target, compS1Event, runID, sagaID, saga.Steps[1].ID, 1, 0, "ROLLING_BACK", "retained", initialRenewal.ClaimFence)
	compS1Snapshot := readProcessSagaSnapshot(t, reader, sagaID)
	assertCompensationProgressSnapshot(t, compS1Snapshot, compS1Event, 0, [5]string{"pending", "completed", "completed", "pending", "pending"}, "S1 compensation COMMIT")
	if err := acknowledgeComponentEvent(compS1Conn); err != nil {
		t.Fatal("could not resume A after the independent S1 compensation read")
	}

	event, gateConn := observer.awaitPostCommitForWorker(t, target.workerID)
	assertComponentCompensationStepPostCommit(t, target, event, runID, sagaID, saga.Steps[0].ID, 0, -1, "ROLLED_BACK", "released", initialRenewal.ClaimFence)
	committed := readProcessSagaSnapshot(t, reader, sagaID)
	assertTerminalCompensationSnapshot(t, committed, event, "A's natural S0 compensation COMMIT")
	t.Logf("component_terminal_compensation_commit version=%s saga=%s worker=%s pid=%d start_id=%s commit=%s direction=%s step=%s index=%d->%d status=%s claim=%s fence=%d token_sha256=%s lease_live=%t outbox_rows=%d comp_states=%v observer_gate_open=true", event.ContractVersion, sagaID, target.workerID, target.pid, target.startID, event.CommitOutcome, event.Direction, event.StepID, event.PriorIndex, event.NextIndex, event.NextStatus, event.ClaimAction, event.ClaimFence, event.ClaimTokenSHA256, committed.LeaseLive, committed.OutboxCount, committed.CompensateStates)

	staleRenewal := awaitRejectedComponentRenewal(t, observer, target.workerID, target.pid, target.startID, event.ClaimFence, event.ClaimTokenSHA256, runID, 15*time.Second)
	if staleRenewal.RenewalError != "" {
		t.Fatalf("terminal compensation stale renewal errored rather than returning unowned: %s", staleRenewal.RenewalError)
	}
	assertSameReleasedLiveSnapshot(t, readProcessSagaSnapshot(t, reader, sagaID), committed, "stale compensation renewal")
	assertComponentStillAtPostCommitGate(t, gateConn, target.workerID)
	t.Logf("component_terminal_compensation_stale_renewal worker=%s pid=%d start_id=%s run_id=%s fence=%d token_sha256=%s owned=false token_empty=true lease_live=false process_alive_at_gate=true", target.workerID, target.pid, target.startID, runID, staleRenewal.ClaimFence, staleRenewal.TokenSHA256)

	successor := spawnComponentWorkerWithControls(t, observer, sagaID, "terminal-compensation-no-successor", false, runID, "", "", false)
	assertComponentProcessStarted(t, successor, observer.awaitStarted(t))
	if sameComponentProcessIdentity(target, successor) || target.cmd.Process == successor.cmd.Process || target.startID == successor.startID {
		t.Fatal("terminal-compensation successor attempt did not use a distinct owned OS process")
	}
	bCompletion := observer.awaitCompletion(t)
	if bCompletion.WorkerID != successor.workerID || bCompletion.PID != successor.pid || bCompletion.StartID != successor.startID ||
		bCompletion.RunID != runID || bCompletion.Handled != 0 || bCompletion.RunError != "" || bCompletion.BackendPID < 1 {
		t.Fatalf("B's normal claim scan did not complete without dispatching terminal work: worker=%s pid=%d start_id=%s handled=%d backend_pid=%d error=%q", bCompletion.WorkerID, bCompletion.PID, bCompletion.StartID, bCompletion.Handled, bCompletion.BackendPID, bCompletion.RunError)
	}
	if err := successor.waitClean(t); err != nil {
		t.Fatal("B's normal no-successor claim scan did not exit cleanly")
	}
	assertSameReleasedLiveSnapshot(t, readProcessSagaSnapshot(t, reader, sagaID), committed, "B normal terminal claim scan")
	assertComponentStillAtPostCommitGate(t, gateConn, target.workerID)
	t.Logf("component_terminal_compensation_dispatch saga=%s worker_b=%s pid_b=%d start_id_b=%s backend_pid_b=%d original_handle_b=owned source=normal_RunOnce_ClaimDueSagas handled=0 dispatch=none status=ROLLED_BACK fence=%d token_empty=true lease_live=false comp_states=%v outbox_rows=%d claimable=false worker_a_alive_at_gate=true", sagaID, successor.workerID, successor.pid, successor.startID, bCompletion.BackendPID, committed.Fence, committed.CompensateStates, committed.OutboxCount)

	if err := acknowledgeComponentEvent(gateConn); err != nil {
		t.Fatal("could not resume A after B's normal no-successor scan")
	}
	aCompletion := awaitCompletionOrUnexpectedCommit(t, observer, target.workerID)
	if aCompletion.PID != target.pid || aCompletion.StartID != target.startID || aCompletion.RunID != runID || aCompletion.Handled != 1 || aCompletion.RunError != "" {
		t.Fatalf("A did not return cleanly after its terminal compensation commit: pid=%d start_id=%s handled=%d error=%q", aCompletion.PID, aCompletion.StartID, aCompletion.Handled, aCompletion.RunError)
	}
	if err := target.waitClean(t); err != nil {
		t.Fatal("A's terminal compensation worker did not exit cleanly after resuming")
	}
	final := readProcessSagaSnapshot(t, reader, sagaID)
	assertSameReleasedLiveSnapshot(t, final, committed, "A compensation resume")
	if final.Index != -1 || final.Status != "ROLLED_BACK" || final.Token != "" || final.LeaseLive || final.OutboxCount != 0 {
		t.Fatal("A's resume changed the terminal compensation result")
	}
	operationID := paymentID.String() + ":" + decisionIDValue
	closeS2OperationID := compensationID(saga.Steps[2].ID).String()
	closeS0OperationID := compensationID(saga.Steps[0].ID).String()
	assertComponentEffectOwnedBy(t, effects.Snapshot("payment_resolution", operationID), "payment_resolution", operationID, target)
	assertComponentEffectOwnedBy(t, effects.Snapshot("close", closeS2OperationID), "close", closeS2OperationID, target)
	assertComponentEffectOwnedBy(t, effects.Snapshot("close", closeS0OperationID), "close", closeS0OperationID, target)
	select {
	case unexpected := <-observer.events:
		_ = unexpected.Conn.Close()
		t.Fatalf("terminal compensation A emitted another post-COMMIT event: worker=%s pid=%d index=%d->%d", unexpected.Event.WorkerID, unexpected.Event.PID, unexpected.Event.PriorIndex, unexpected.Event.NextIndex)
	default:
	}
	t.Logf("component_terminal_compensation_final saga=%s index=%d status=%s direction=%s fence=%d token_empty=true lease_live=false comp_states=%v outbox_rows=%d close_s2_operation=%s close_s2_attempts=1 payment_resolution_attempts=1 close_s0_operation=%s close_s0_attempts=1 a_effects_after_release=0 provider=local_test_double_only", sagaID, final.Index, final.Status, final.Direction, final.Fence, final.CompensateStates, final.OutboxCount, closeS2OperationID, closeS0OperationID)
}

func assertComponentCompensationStepPostCommit(t *testing.T, child *ownedComponentProcess, event recoverycomponent.PostCommitEvent, runID string, sagaID, stepID uuid.UUID, prior, next int, nextStatus, claimAction string, wantFence int64) {
	t.Helper()
	if event.ContractVersion != recoverycomponent.PostCommitContractVersion || event.CommitOutcome != "COMMIT_ACKED" ||
		event.SagaID != sagaID.String() || event.StepID != stepID.String() || event.Direction != "compensate" ||
		event.ResultState != "completed" || event.PriorIndex != prior || event.NextIndex != next || event.NextStatus != nextStatus ||
		event.ClaimAction != claimAction || event.ClaimFence != wantFence || event.ClaimTokenSHA256 == "" ||
		event.WorkerID != child.workerID || event.PID != child.pid || event.RunID != runID || event.EmittedAt.IsZero() || event.TicketOutboxExpected {
		t.Fatalf("compensation post-COMMIT event did not match the owned process and boundary: %+v", event)
	}
	t.Logf("component_post_commit version=%s saga=%s step=%s worker=%s pid=%d commit=%s direction=%s index=%d->%d status=%s claim=%s fence=%d token_sha256=%s outbox_expected=false", event.ContractVersion, event.SagaID, event.StepID, event.WorkerID, event.PID, event.CommitOutcome, event.Direction, event.PriorIndex, event.NextIndex, event.NextStatus, event.ClaimAction, event.ClaimFence, event.ClaimTokenSHA256)
}

func assertCompensationProgressSnapshot(t *testing.T, got componentProcessSagaSnapshot, event recoverycomponent.PostCommitEvent, nextIndex int, compensateStates [5]string, stage string) {
	t.Helper()
	if got.Index != nextIndex || got.Status != "ROLLING_BACK" || got.Direction != "compensate" || got.Recovery != "ready" ||
		got.Token == "" || componentTokenDigest(got.Token) != event.ClaimTokenSHA256 || !got.LeaseLive || got.Fence != event.ClaimFence ||
		got.OutboxCount != 0 || got.CompensateStates != compensateStates {
		t.Fatalf("independent compensation progress snapshot mismatch after %s: got=%s", stage, releasedLiveSnapshotSummary(got))
	}
}

func assertTerminalCompensationSnapshot(t *testing.T, got componentProcessSagaSnapshot, event recoverycomponent.PostCommitEvent, stage string) {
	t.Helper()
	wantExecute := [5]string{"completed", "completed", "intended", "pending", "pending"}
	wantCompensate := [5]string{"completed", "completed", "completed", "pending", "pending"}
	if got.Index != -1 || got.Status != "ROLLED_BACK" || got.Direction != "compensate" || got.Recovery != "ready" ||
		got.Token != "" || got.LeaseLive || got.Fence != event.ClaimFence || got.OutboxCount != 0 ||
		got.ExecuteStates != wantExecute || got.CompensateStates != wantCompensate {
		t.Fatalf("independent terminal compensation snapshot mismatch after %s: got=%s", stage, releasedLiveSnapshotSummary(got))
	}
}

type componentRenewalObservedRepository struct {
	*repository.SagaRepository
	address  string
	workerID string
	startID  string
	runID    string
}

func (r *componentRenewalObservedRepository) RenewClaim(ctx context.Context, claim domain.SagaClaim, lease time.Duration) (bool, error) {
	owned, err := r.SagaRepository.RenewClaim(ctx, claim, lease)
	message := componentProcessMessage{
		Kind: "lease_renewal", PID: os.Getpid(), StartID: r.startID, WorkerID: r.workerID,
		RunID: r.runID, ClaimFence: claim.Fence, TokenSHA256: componentTokenDigest(claim.Token.String()),
	}
	ownedValue := owned
	message.Owned = &ownedValue
	if err != nil {
		message.RenewalError = err.Error()
	}
	if sendErr := sendComponentRenewalMessage(r.address, message); sendErr != nil {
		return owned, errors.Join(err, sendErr)
	}
	return owned, err
}

func spawnComponentWorkerWithRenewalReport(t *testing.T, observer *componentProcessObserver, effectLedgerAddress string, sagaID uuid.UUID, workerID, runID string) *ownedComponentProcess {
	t.Helper()
	startID := uuid.NewString()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSagaComponentOwnedWorkerProcess$")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	values := map[string]string{
		"SAGA_COMPONENT_CHILD_MODE":            "worker",
		"SAGA_COMPONENT_PARENT_OBSERVER_ADDR":  observer.listener.Addr().String(),
		"SAGA_COMPONENT_START_ID":              startID,
		"SAGA_COMPONENT_WORKER_ID":             workerID,
		"SAGA_COMPONENT_SAGA_ID":               sagaID.String(),
		"SAGA_COMPONENT_PROVIDER_OUTCOME":      "collected",
		"SAGA_COMPONENT_RUN_ID":                runID,
		"SAGA_COMPONENT_EFFECT_LEDGER_ADDR":    effectLedgerAddress,
		"SAGA_COMPONENT_RENEWAL_OBSERVER_ADDR": observer.listener.Addr().String(),
		"SAGA_COMPONENT_REPORT_RUN_ERROR":      "true",
		"RUSHGO_SAGA_COMPONENT_OBSERVER_ADDR":  observer.listener.Addr().String(),
		"RUSHGO_SAGA_COMPONENT_RUN_ID":         runID,
		"RUSHGO_SAGA_COMPONENT_WORKER_ID":      workerID,
		"RUSHGO_SAGA_COMPONENT_HOOK_TIMEOUT":   componentHookTimeout.String(),
	}
	cmd.Env = componentProcessEnvironment(os.Environ(), values)
	if err := cmd.Start(); err != nil {
		t.Fatal("could not start the owned released-live A worker process")
	}
	process := &ownedComponentProcess{cmd: cmd, workerID: workerID, startID: startID, startedAt: time.Now().UTC(), pid: cmd.Process.Pid}
	t.Cleanup(func() { process.stopForCleanup() })
	return process
}

func spawnComponentCompensationTargetWorker(t *testing.T, observer *componentProcessObserver, effectLedgerAddress string, sagaID uuid.UUID, workerID, runID, paymentID string) *ownedComponentProcess {
	t.Helper()
	startID := uuid.NewString()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSagaComponentOwnedWorkerProcess$")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	values := map[string]string{
		"SAGA_COMPONENT_CHILD_MODE":                   "worker",
		"SAGA_COMPONENT_PARENT_OBSERVER_ADDR":         observer.listener.Addr().String(),
		"SAGA_COMPONENT_START_ID":                     startID,
		"SAGA_COMPONENT_WORKER_ID":                    workerID,
		"SAGA_COMPONENT_SAGA_ID":                      sagaID.String(),
		"SAGA_COMPONENT_PROVIDER_OUTCOME":             "not_collected",
		"SAGA_COMPONENT_RUN_ID":                       runID,
		"SAGA_COMPONENT_EFFECT_LEDGER_ADDR":           effectLedgerAddress,
		"SAGA_COMPONENT_RENEWAL_OBSERVER_ADDR":        observer.listener.Addr().String(),
		"SAGA_COMPONENT_REPORT_RUN_ERROR":             "true",
		"SAGA_COMPONENT_FIXTURE_PAYMENT_ID":           paymentID,
		"SAGA_COMPONENT_FIXTURE_PROVIDER_STATUS":      "requires_payment_method",
		"SAGA_COMPONENT_FIXTURE_RECONCILIATION_STATE": "resolved",
		"RUSHGO_SAGA_COMPONENT_OBSERVER_ADDR":         observer.listener.Addr().String(),
		"RUSHGO_SAGA_COMPONENT_RUN_ID":                runID,
		"RUSHGO_SAGA_COMPONENT_WORKER_ID":             workerID,
		"RUSHGO_SAGA_COMPONENT_HOOK_TIMEOUT":          componentHookTimeout.String(),
	}
	cmd.Env = componentProcessEnvironment(os.Environ(), values)
	if err := cmd.Start(); err != nil {
		t.Fatal("could not start the owned compensation target A worker process")
	}
	process := &ownedComponentProcess{cmd: cmd, workerID: workerID, startID: startID, startedAt: time.Now().UTC(), pid: cmd.Process.Pid}
	t.Cleanup(func() { process.stopForCleanup() })
	return process
}

func awaitComponentLeaseRenewal(t *testing.T, observer *componentProcessObserver, workerID string, timeout time.Duration) componentProcessMessage {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case message := <-observer.renewals:
		if message.Kind != "lease_renewal" || message.WorkerID != workerID || message.PID < 1 || message.StartID == "" || message.Owned == nil {
			t.Fatalf("renewal observation did not identify its process and result: worker=%s pid=%d start_id=%s kind=%s", message.WorkerID, message.PID, message.StartID, message.Kind)
		}
		return message
	case <-timer.C:
		t.Fatalf("worker %s did not report an actual RenewClaim call within %s", workerID, timeout)
		return componentProcessMessage{}
	}
}

func awaitRejectedComponentRenewal(t *testing.T, observer *componentProcessObserver, workerID string, pid int, startID string, fence int64, tokenSHA256, runID string, timeout time.Duration) componentProcessMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		message := awaitComponentLeaseRenewal(t, observer, workerID, time.Until(deadline))
		if message.PID != pid || message.StartID != startID || message.ClaimFence != fence || message.TokenSHA256 != tokenSHA256 || message.RunID != runID {
			t.Fatalf("A's renewal no longer matched the released S1 identity: fence=%d token_sha256=%s run_id=%s", message.ClaimFence, message.TokenSHA256, message.RunID)
		}
		if *message.Owned {
			t.Logf("component_lease_renewal worker=%s pid=%d start_id=%s run_id=%s claim_fence=%d token_sha256=%s owned=true stage=before_release_observation", message.WorkerID, message.PID, message.StartID, message.RunID, message.ClaimFence, message.TokenSHA256)
			continue
		}
		return message
	}
	t.Fatalf("worker %s never reported a rejected stale-claim renewal", workerID)
	return componentProcessMessage{}
}

func awaitCompletionOrUnexpectedCommit(t *testing.T, observer *componentProcessObserver, workerID string) componentProcessMessage {
	t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case message := <-observer.complete:
		if message.WorkerID != workerID {
			t.Fatalf("worker completion came from %q while awaiting %q", message.WorkerID, workerID)
		}
		return message
	case event := <-observer.events:
		_ = event.Conn.Close()
		t.Fatalf("resumed stale worker emitted a new post-COMMIT event before completing: worker=%s pid=%d index=%d->%d", event.Event.WorkerID, event.Event.PID, event.Event.PriorIndex, event.Event.NextIndex)
		return componentProcessMessage{}
	case <-timer.C:
		t.Fatal("resumed A did not return from its released-claim boundary")
		return componentProcessMessage{}
	}
}

func assertComponentStillAtPostCommitGate(t *testing.T, conn net.Conn, workerID string) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatal("could not set a bounded liveness probe on the open post-COMMIT observation")
	}
	var probe [1]byte
	n, err := conn.Read(probe[:])
	var networkError net.Error
	if n != 0 || !errors.As(err, &networkError) || !networkError.Timeout() {
		t.Fatalf("worker %s did not remain alive awaiting its explicit post-COMMIT ACK: read_bytes=%d error=%v", workerID, n, err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal("could not clear the bounded liveness probe deadline")
	}
}

func assertReleasedLiveSnapshot(t *testing.T, got componentProcessSagaSnapshot, index int, status string, leaseLive bool, event recoverycomponent.PostCommitEvent, resultIndex int, nextState string, wake, consumed, outbox int) {
	t.Helper()
	identityOK := got.Token == ""
	if leaseLive {
		identityOK = got.Token != "" && componentTokenDigest(got.Token) == event.ClaimTokenSHA256
	}
	if got.Index != index || got.Status != status || got.Direction != "forward" || got.Recovery != "ready" ||
		got.LeaseLive != leaseLive || got.Fence != event.ClaimFence || got.WakeVersion != int64(wake) ||
		got.ConsumedWakeVersion != int64(consumed) || got.OutboxCount != outbox ||
		got.ExecuteStates[resultIndex] != "completed" || got.ExecuteStates[index] != nextState || !identityOK {
		t.Fatalf("independent Saga snapshot mismatch: %s expected_index=%d expected_status=%s expected_lease_live=%t expected_step=%d:%s", releasedLiveSnapshotSummary(got), index, status, leaseLive, resultIndex, "completed")
	}
}

func assertSameReleasedLiveSnapshot(t *testing.T, got, want componentProcessSagaSnapshot, stage string) {
	t.Helper()
	if got != want {
		t.Fatalf("Saga state changed after %s: got=%s want=%s", stage, releasedLiveSnapshotSummary(got), releasedLiveSnapshotSummary(want))
	}
}

func releasedLiveSnapshotSummary(snapshot componentProcessSagaSnapshot) string {
	tokenDigest := "empty"
	if snapshot.Token != "" {
		tokenDigest = componentTokenDigest(snapshot.Token)
	}
	return fmt.Sprintf("index=%d status=%s direction=%s recovery=%s token_sha256=%s fence=%d lease_live=%t wake=%d/%d execute=%v compensate=%v outbox=%d", snapshot.Index, snapshot.Status, snapshot.Direction, snapshot.Recovery, tokenDigest, snapshot.Fence, snapshot.LeaseLive, snapshot.WakeVersion, snapshot.ConsumedWakeVersion, snapshot.ExecuteStates, snapshot.CompensateStates, snapshot.OutboxCount)
}

func assertComponentEffectOwnedBy(t *testing.T, got componentEffectSnapshot, action, operationID string, owner *ownedComponentProcess) {
	t.Helper()
	if got.Attempts != 1 || got.Applications != 1 || len(got.ProcessStartIDs) != 1 || !got.ProcessStartIDs[owner.startID] ||
		len(got.ProcessPIDs) != 1 || got.ProcessPIDs[owner.pid] != 1 {
		t.Fatalf("local effect double recorded an unexpected application: action=%s operation_id=%s attempts=%d applications=%d start_ids=%v pids=%v expected_owner=%s/%d", action, operationID, got.Attempts, got.Applications, got.ProcessStartIDs, got.ProcessPIDs, owner.startID, owner.pid)
	}
	t.Logf("component_effect_double action=%s operation_id=%s attempts=%d applications=%d owner_worker=%s owner_pid=%d owner_start_id=%s", action, operationID, got.Attempts, got.Applications, owner.workerID, owner.pid, owner.startID)
}

func componentTokenDigest(token string) string {
	if token == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

func renewalOwned(message componentProcessMessage) bool {
	return message.Owned != nil && *message.Owned
}

func sendComponentRenewalMessage(address string, message componentProcessMessage) error {
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return fmt.Errorf("dialing local component renewal observer: %w", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return fmt.Errorf("setting local component renewal observer deadline: %w", err)
	}
	if err := json.NewEncoder(conn).Encode(message); err != nil {
		return fmt.Errorf("sending local component renewal observation: %w", err)
	}
	return nil
}

func waitForReleasedLiveBackendPID(t *testing.T, reader *pgxpool.Pool, processPID int) int {
	t.Helper()
	applicationName := fmt.Sprintf("saga_component_%d", processPID)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var backendPID int
		err := reader.QueryRow(ctx, `SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND datname=current_database() ORDER BY backend_start DESC LIMIT 1`, applicationName).Scan(&backendPID)
		cancel()
		if err == nil {
			return backendPID
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("independent reader could not correlate worker process %d with its PostgreSQL backend", processPID)
	return 0
}
