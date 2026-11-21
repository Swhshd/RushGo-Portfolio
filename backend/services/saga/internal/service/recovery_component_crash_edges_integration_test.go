//go:build reliability_component

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func TestComponentProcessIdentityUsesHandleAndStartIDInsteadOfPIDUniqueness(t *testing.T) {
	pid := &os.Process{Pid: 4321}
	left := &ownedComponentProcess{cmd: &exec.Cmd{Process: pid}, pid: 4321, startID: "start-a"}
	reusedPID := &ownedComponentProcess{cmd: &exec.Cmd{Process: &os.Process{Pid: 4321}}, pid: 4321, startID: "start-b"}
	if sameComponentProcessIdentity(left, reusedPID) {
		t.Fatal("same PID with a distinct original process handle/start ID was treated as the same process")
	}
	sameInstance := &ownedComponentProcess{cmd: &exec.Cmd{Process: pid}, pid: 4321, startID: "start-a"}
	if !sameComponentProcessIdentity(left, sameInstance) {
		t.Fatal("same original process handle and start ID were not recognized")
	}
}

func TestSagaComponentPreCommitCrashAndS4OutboxCrashRecoveryIntegration(t *testing.T) {
	fixture := recoverycomponent.CreateDatabaseFixture(t)
	if fixture.Schema == "" || fixture.URL == "" {
		t.Fatal("owned Saga component schema was not created")
	}
	pool := recoverycomponent.OpenComponentPool(t)
	reader := recoverycomponent.OpenIndependentPool(t, "saga_component_crash_edge_reader")
	repo := repository.NewSagaRepository(pool)
	observer := newComponentProcessObserver(t)
	effects := newComponentEffectLedger(t)
	runID := os.Getenv("SAGA_COMPONENT_RUN_ID")
	if runID == "" {
		t.Fatal("SAGA_COMPONENT_RUN_ID is required")
	}
	var repositoryPID, readerPID int
	if err := pool.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&repositoryPID); err != nil {
		t.Fatal(err)
	}
	if err := reader.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&readerPID); err != nil {
		t.Fatal(err)
	}
	if repositoryPID == readerPID {
		t.Fatal("Saga repository and independent observer share a PostgreSQL backend")
	}
	t.Logf("component_database_identity run_id=%s schema=%s repository_backend_pid=%d independent_reader_backend_pid=%d distinct_backend_connections=true", runID, fixture.Schema, repositoryPID, readerPID)

	t.Run("uncommitted_result_is_invisible_rolls_back_and_replays_one_business_effect", func(t *testing.T) {
		sagaID := createComponentProcessSaga(t, repo)
		saga, err := repo.GetSagaById(context.Background(), sagaID)
		if err != nil || saga == nil || len(saga.Steps) != 5 {
			t.Fatal("could not read initial Saga fixture")
		}
		stepID := saga.Steps[0].ID
		precommit := newComponentPreCommitObserver(t)
		target := spawnComponentWorkerWithControls(t, observer, sagaID, "precommit-target", false, runID, precommit.Address(), effects.Address(), false)
		assertComponentProcessStarted(t, target, observer.awaitStarted(t))
		pending, conn := precommit.await(t)
		assertComponentPreCommit(t, pending, target, sagaID, stepID, "precommit-target", runID, 0, 1)
		beforeKill := readProcessSagaSnapshot(t, reader, sagaID)
		assertUncommittedResultInvisible(t, beforeKill, pending)
		reserveBefore := effects.Snapshot("reserve", stepID.String())
		if reserveBefore.Attempts != 1 || reserveBefore.Applications != 1 || !reserveBefore.ProcessStartIDs[target.startID] {
			t.Fatalf("first local business-command effect was not recorded: %+v", reserveBefore)
		}
		t.Logf("component_precommit_visible_state saga=%s step=%s target_pid=%d start_id=%s persisted_index=%d persisted_step_state=%s claim_fence=%d lease_live=%t outbox_rows=%d", sagaID, stepID, target.pid, target.startID, beforeKill.Index, beforeKill.ExecuteStates[0], beforeKill.Fence, beforeKill.LeaseLive, beforeKill.OutboxCount)
		if err := target.killAndWait(t); err != nil {
			t.Fatal("could not terminate the owned worker during its uncommitted result transaction")
		}
		_ = conn.Close()
		waitForComponentBackendExit(t, reader, target.pid, 3*time.Second)
		afterKill := readProcessSagaSnapshot(t, reader, sagaID)
		assertUncommittedResultInvisible(t, afterKill, pending)
		select {
		case event := <-observer.events:
			_ = event.Conn.Close()
			t.Fatal("a killed pre-COMMIT result emitted a COMMIT_ACKED post-COMMIT event")
		default:
		}
		t.Logf("component_precommit_rollback saga=%s step=%s killed_target_pid=%d start_id=%s persisted_index=%d persisted_step_state=%s outbox_rows=%d postgres_backend_gone=true", sagaID, stepID, target.pid, target.startID, afterKill.Index, afterKill.ExecuteStates[0], afterKill.OutboxCount)

		waitForComponentLeaseExpiry(t, reader, sagaID, 15*time.Second)
		successor := spawnComponentWorkerWithControls(t, observer, sagaID, "precommit-recovery-successor", false, runID, "", effects.Address(), false)
		assertComponentProcessStarted(t, successor, observer.awaitStarted(t))
		completion := observer.awaitCompletion(t)
		if completion.WorkerID != "precommit-recovery-successor" || completion.Handled != 1 {
			t.Fatalf("eligible successor did not execute the recovered Saga: worker=%s handled=%d", completion.WorkerID, completion.Handled)
		}
		if err := successor.waitClean(t); err != nil {
			t.Fatal("pre-COMMIT recovery successor did not exit cleanly")
		}
		recovered := readProcessSagaSnapshot(t, reader, sagaID)
		if recovered.Index != 2 || recovered.Status != "PROCESSING" || recovered.ExecuteStates[0] != "completed" ||
			recovered.ExecuteStates[1] != "completed" || recovered.WakeVersion < 1 || recovered.WakeVersion != recovered.ConsumedWakeVersion ||
			recovered.OutboxCount != 0 || recovered.Fence <= pending.ClaimFence || recovered.Token != "" || recovered.LeaseLive {
			t.Fatalf("successor did not persist S0 under a new released claim: %+v", recovered)
		}
		reserveAfter := effects.Snapshot("reserve", stepID.String())
		if reserveAfter.Attempts != 2 || reserveAfter.Applications != 1 ||
			!reserveAfter.ProcessStartIDs[target.startID] || !reserveAfter.ProcessStartIDs[successor.startID] ||
			target.startID == successor.startID {
			t.Fatalf("replay did not deduplicate the same business command across process identities: %+v", reserveAfter)
		}
		oldToken, err := uuid.Parse(beforeKill.Token)
		if err != nil || oldToken == uuid.Nil {
			t.Fatal("pre-COMMIT target token was not independently readable")
		}
		stillOwned, err := repo.RenewClaim(context.Background(), domain.SagaClaim{SagaID: sagaID, Token: oldToken, Fence: pending.ClaimFence, Direction: "forward"}, componentWorkerLease)
		if err != nil || stillOwned {
			t.Fatal("pre-COMMIT crashed target token regained ownership after successor recovery")
		}
		t.Logf("component_effect_dedup action=reserve operation_id=%s attempts=%d unique_applications=%d process_start_ids=%d target_pid=%d target_start_id=%s successor_pid=%d successor_start_id=%s successor_index=%d successor_status=%s", stepID, reserveAfter.Attempts, reserveAfter.Applications, len(reserveAfter.ProcessStartIDs), target.pid, target.startID, successor.pid, successor.startID, recovered.Index, recovered.Status)
		if _, err := pool.Exec(context.Background(), `UPDATE sagas SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE id=$1`, sagaID); err != nil {
			t.Fatal("could not isolate the recovered pre-COMMIT fixture from the S4 process scenario")
		}
		t.Logf("component_fixture_deferred_after_recovery saga=%s step_index=%d", sagaID, recovered.Index)
	})

	t.Run("S4_commit_then_process_death_keeps_single_ticket_outbox_and_effect", func(t *testing.T) {
		sagaID, stepID := createComponentProcessSagaAtIndex(t, pool, repo, 4)
		target := spawnComponentWorkerWithControls(t, observer, sagaID, "s4-postcommit-target", true, runID, "", effects.Address(), true)
		assertComponentProcessStarted(t, target, observer.awaitStarted(t))
		event, conn := observer.awaitPostCommit(t)
		assertComponentS4PostCommit(t, event, target, sagaID, stepID, runID)
		beforeKill := readProcessSagaSnapshot(t, reader, sagaID)
		assertS4OutboxSnapshot(t, beforeKill, event)
		accountBefore := effects.Snapshot("account", stepID.String())
		if accountBefore.Attempts != 1 || accountBefore.Applications != 1 || !accountBefore.ProcessStartIDs[target.startID] {
			t.Fatalf("S4 local business-command effect was not recorded once: %+v", accountBefore)
		}
		if err := target.killAndWait(t); err != nil {
			t.Fatal("could not terminate the owned process after the S4 commit boundary")
		}
		_ = conn.Close()
		waitForComponentBackendExit(t, reader, target.pid, 3*time.Second)

		successor := spawnComponentWorkerWithControls(t, observer, sagaID, "s4-outbox-recovery-successor", false, runID, "", effects.Address(), true)
		assertComponentProcessStarted(t, successor, observer.awaitStarted(t))
		completion := observer.awaitCompletion(t)
		if completion.WorkerID != "s4-outbox-recovery-successor" || completion.Handled != 0 {
			t.Fatalf("completed Saga was dispatched again after S4 crash: worker=%s handled=%d", completion.WorkerID, completion.Handled)
		}
		if err := successor.waitClean(t); err != nil {
			t.Fatal("S4 outbox recovery successor did not exit cleanly")
		}
		afterReplay := readProcessSagaSnapshot(t, reader, sagaID)
		assertS4OutboxSnapshot(t, afterReplay, event)
		accountAfter := effects.Snapshot("account", stepID.String())
		if accountAfter.Attempts != 1 || accountAfter.Applications != 1 || len(accountAfter.ProcessStartIDs) != 1 || !accountAfter.ProcessStartIDs[target.startID] {
			t.Fatalf("S4 accounting command or effect duplicated across recovery process: %+v", accountAfter)
		}
		t.Logf("component_s4_after_commit_recovery saga=%s step=%s target_pid=%d target_start_id=%s successor_pid=%d successor_start_id=%s successor_handled=%d outbox_rows=%d account_attempts=%d unique_applications=%d", sagaID, stepID, target.pid, target.startID, successor.pid, successor.startID, completion.Handled, afterReplay.OutboxCount, accountAfter.Attempts, accountAfter.Applications)
	})
}

type componentPreCommitObserver struct {
	listener net.Listener
}

func newComponentPreCommitObserver(t *testing.T) *componentPreCommitObserver {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observer := &componentPreCommitObserver{listener: listener}
	t.Cleanup(func() { _ = listener.Close() })
	return observer
}

func (o *componentPreCommitObserver) Address() string { return o.listener.Addr().String() }

func (o *componentPreCommitObserver) await(t *testing.T) (recoverycomponent.PreCommitEvent, net.Conn) {
	t.Helper()
	if tcp, ok := o.listener.(*net.TCPListener); ok {
		if err := tcp.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	conn, err := o.listener.Accept()
	if err != nil {
		t.Fatal("owned worker did not reach its uncommitted result transaction")
	}
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	var event recoverycomponent.PreCommitEvent
	if err := json.NewDecoder(conn).Decode(&event); err != nil {
		_ = conn.Close()
		t.Fatal("could not decode the versioned pre-COMMIT event")
	}
	return event, conn
}

func assertComponentPreCommit(t *testing.T, event recoverycomponent.PreCommitEvent, child *ownedComponentProcess, sagaID, stepID uuid.UUID, workerID, runID string, prior, next int) {
	t.Helper()
	if event.ContractVersion != recoverycomponent.PreCommitContractVersion || event.TransactionState != "OPEN_UNCOMMITTED" ||
		event.SagaID != sagaID.String() || event.StepID != stepID.String() || event.Direction != "forward" || event.ResultState != "completed" ||
		event.PriorIndex != prior || event.NextIndex != next || event.ClaimFence < 1 || event.ClaimTokenSHA256 == "" ||
		event.RunID != runID || event.WorkerID != workerID || event.StartID != child.startID || event.PID != child.pid || event.EmittedAt.IsZero() ||
		event.ContractVersion == recoverycomponent.PostCommitContractVersion {
		t.Fatalf("pre-COMMIT result-control identity mismatch: %+v", event)
	}
	t.Logf("component_pre_commit version=%s saga=%s step=%s worker=%s pid=%d transaction=%s index=%d->%d claim_fence=%d token_sha256=%s", event.ContractVersion, event.SagaID, event.StepID, event.WorkerID, event.PID, event.TransactionState, event.PriorIndex, event.NextIndex, event.ClaimFence, event.ClaimTokenSHA256)
}

func assertUncommittedResultInvisible(t *testing.T, got componentProcessSagaSnapshot, event recoverycomponent.PreCommitEvent) {
	t.Helper()
	digest := sha256.Sum256([]byte(got.Token))
	if got.Index != event.PriorIndex || got.Status != "PROCESSING" || got.Direction != "forward" || got.Recovery != "ready" ||
		got.ExecuteStates[0] != "intended" || got.Fence != event.ClaimFence || !got.LeaseLive || got.Token == "" ||
		hex.EncodeToString(digest[:]) != event.ClaimTokenSHA256 || got.OutboxCount != 0 {
		t.Fatalf("independent PostgreSQL reader observed a result before COMMIT or lost the live owner: %+v", got)
	}
}

func assertComponentS4PostCommit(t *testing.T, event recoverycomponent.PostCommitEvent, child *ownedComponentProcess, sagaID, stepID uuid.UUID, runID string) {
	t.Helper()
	if event.ContractVersion != recoverycomponent.PostCommitContractVersion || event.CommitOutcome != "COMMIT_ACKED" ||
		event.SagaID != sagaID.String() || event.StepID != stepID.String() || event.Direction != "forward" || event.ResultState != "completed" ||
		event.PriorIndex != 4 || event.NextIndex != 5 || event.NextStatus != "COMPLETED" || event.ClaimAction != "released" ||
		event.ClaimFence < 1 || event.ClaimTokenSHA256 == "" || !event.TicketOutboxExpected || event.RunID != runID ||
		event.WorkerID != child.workerID || event.PID != child.pid || event.EmittedAt.IsZero() {
		t.Fatalf("S4 post-COMMIT event did not identify its committed outbox boundary: %+v", event)
	}
	t.Logf("component_s4_post_commit version=%s saga=%s step=%s worker=%s pid=%d index=%d->%d claim=%s fence=%d outbox_expected=%t", event.ContractVersion, event.SagaID, event.StepID, event.WorkerID, event.PID, event.PriorIndex, event.NextIndex, event.ClaimAction, event.ClaimFence, event.TicketOutboxExpected)
}

func assertS4OutboxSnapshot(t *testing.T, got componentProcessSagaSnapshot, event recoverycomponent.PostCommitEvent) {
	t.Helper()
	if got.Index != 5 || got.Status != "COMPLETED" || got.Direction != "forward" || got.Recovery != "ready" ||
		got.ExecuteStates[4] != "completed" || got.Token != "" || got.LeaseLive || got.Fence != event.ClaimFence || got.OutboxCount != 1 {
		t.Fatalf("independent PostgreSQL reader did not verify one committed S4 result/outbox: %+v", got)
	}
}

func waitForComponentBackendExit(t *testing.T, reader *pgxpool.Pool, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	applicationName := fmt.Sprintf("saga_component_%d", pid)
	for time.Now().Before(deadline) {
		var count int
		if err := reader.QueryRow(context.Background(), `SELECT count(*)::int FROM pg_stat_activity WHERE application_name=$1`, applicationName).Scan(&count); err != nil {
			t.Fatal("could not check for the killed worker's PostgreSQL backend")
		}
		if count == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("PostgreSQL backend for killed owned worker PID %d did not exit", pid)
}

func createComponentProcessSagaAtIndex(t *testing.T, pool *pgxpool.Pool, repo *repository.SagaRepository, index int) (uuid.UUID, uuid.UUID) {
	t.Helper()
	if index != 4 {
		t.Fatalf("unsupported pre-positioned Saga step %d", index)
	}
	sagaID := createComponentProcessSaga(t, repo)
	saga, err := repo.GetSagaById(context.Background(), sagaID)
	if err != nil || saga == nil || len(saga.Steps) != 5 {
		t.Fatal("could not read S4 fixture Saga")
	}
	if _, err := pool.Exec(context.Background(), `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<$2`, sagaID, index); err != nil {
		t.Fatal("could not prepare completed S0-S3 component fixture")
	}
	if _, err := pool.Exec(context.Background(), `UPDATE sagas SET current_step_index=$2,status='PROCESSING',direction='forward',recovery_state='ready',next_attempt_at=clock_timestamp(),payment_id=$3 WHERE id=$1`, sagaID, index, uuid.New()); err != nil {
		t.Fatal("could not position the owned component Saga at S4")
	}
	return sagaID, saga.Steps[index].ID
}

func sameComponentProcessIdentity(left, right *ownedComponentProcess) bool {
	if left == nil || right == nil || left.cmd == nil || right.cmd == nil || left.cmd.Process == nil || right.cmd.Process == nil {
		return false
	}
	return left.cmd.Process == right.cmd.Process && left.pid == right.pid && left.startID != "" && left.startID == right.startID
}
