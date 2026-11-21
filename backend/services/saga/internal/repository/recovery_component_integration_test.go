//go:build reliability_component

package repository

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/recoverycomponent"
)

type componentBoundaryCase struct {
	name        string
	direction   string
	index       int
	wake        bool
	skipEarlier bool
	wantNext    int
	wantStatus  string
	wantAction  string
	wantOutbox  int
}

func TestSagaPostCommitBoundariesAgainstIndependentPostgresReaderIntegration(t *testing.T) {
	fixture := recoverycomponent.CreateDatabaseFixture(t)
	if fixture.Schema == "" || fixture.URL == "" {
		t.Fatal("owned Saga component schema was not created")
	}
	pool := recoverycomponent.OpenComponentPool(t)
	reader := recoverycomponent.OpenIndependentPool(t, "saga_component_matrix_reader")
	repo := NewSagaRepository(pool)
	var repositoryBackendPID, readerBackendPID int
	if err := pool.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&repositoryBackendPID); err != nil {
		t.Fatal(err)
	}
	if err := reader.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&readerBackendPID); err != nil {
		t.Fatal(err)
	}
	if repositoryBackendPID == readerBackendPID {
		t.Fatal("repository and observer unexpectedly share a PostgreSQL backend connection")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	runID := os.Getenv("SAGA_COMPONENT_RUN_ID")
	if runID == "" {
		t.Fatal("SAGA_COMPONENT_RUN_ID is required")
	}
	t.Logf("component_database_identity run_id=%s schema=%s repository_backend_pid=%d independent_reader_backend_pid=%d distinct_backend_connections=true", runID, fixture.Schema, repositoryBackendPID, readerBackendPID)
	t.Setenv("RUSHGO_SAGA_COMPONENT_OBSERVER_ADDR", listener.Addr().String())
	t.Setenv("RUSHGO_SAGA_COMPONENT_RUN_ID", runID)
	t.Setenv("RUSHGO_SAGA_COMPONENT_WORKER_ID", "matrix-repository-worker")
	t.Setenv("RUSHGO_SAGA_COMPONENT_HOOK_TIMEOUT", "10s")

	cases := []componentBoundaryCase{
		{name: "forward_S0", direction: "forward", index: 0, wantNext: 1, wantStatus: "PROCESSING", wantAction: "retained"},
		{name: "forward_S1_wake", direction: "forward", index: 1, wake: true, wantNext: 2, wantStatus: "PROCESSING", wantAction: "released"},
		{name: "forward_S1_no_wake", direction: "forward", index: 1, wantNext: 2, wantStatus: "WAIT_FOR_PAYMENT", wantAction: "released"},
		{name: "forward_S2", direction: "forward", index: 2, wantNext: 3, wantStatus: "PROCESSING", wantAction: "retained"},
		{name: "forward_S3", direction: "forward", index: 3, wantNext: 4, wantStatus: "PROCESSING", wantAction: "retained"},
		{name: "forward_S4_with_atomic_outbox", direction: "forward", index: 4, wantNext: 5, wantStatus: "COMPLETED", wantAction: "released", wantOutbox: 1},
		{name: "compensation_S0_terminal", direction: "compensate", index: 0, wantNext: -1, wantStatus: "ROLLED_BACK", wantAction: "released"},
		{name: "compensation_S1", direction: "compensate", index: 1, wantNext: 0, wantStatus: "ROLLING_BACK", wantAction: "retained"},
		{name: "compensation_S2", direction: "compensate", index: 2, wantNext: 1, wantStatus: "ROLLING_BACK", wantAction: "retained"},
		{name: "compensation_S3", direction: "compensate", index: 3, wantNext: 2, wantStatus: "ROLLING_BACK", wantAction: "retained"},
		{name: "compensation_S3_skips_completed_earlier_steps", direction: "compensate", index: 3, skipEarlier: true, wantNext: -1, wantStatus: "ROLLED_BACK", wantAction: "released"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			input := recoveryInputForCase(tc.name)
			saga, _, err := repo.CreateOrGetSaga(ctx, input, recoverySteps())
			if err != nil {
				t.Fatal(err)
			}
			if tc.direction == "forward" && tc.index > 0 {
				if _, err := pool.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',status='COMPLETED',executed_at=clock_timestamp() WHERE saga_id=$1 AND "order"<$2`, saga.ID, tc.index); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx, `UPDATE sagas SET current_step_index=$2,status='PROCESSING',next_attempt_at=clock_timestamp() WHERE id=$1`, saga.ID, tc.index); err != nil {
					t.Fatal(err)
				}
			}
			if tc.direction == "compensate" {
				if _, err := pool.Exec(ctx, `UPDATE sagas SET direction='compensate',status='ROLLING_BACK',current_step_index=$2,next_attempt_at=clock_timestamp() WHERE id=$1`, saga.ID, tc.index); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx, `UPDATE saga_steps SET compensate_state='completed',status='COMPENSATED',compensated_at=clock_timestamp() WHERE saga_id=$1 AND "order">$2`, saga.ID, tc.index); err != nil {
					t.Fatal(err)
				}
				if tc.skipEarlier {
					if _, err := pool.Exec(ctx, `UPDATE saga_steps SET compensate_state='completed',status='COMPENSATED',compensated_at=clock_timestamp() WHERE saga_id=$1 AND "order"<$2`, saga.ID, tc.index); err != nil {
						t.Fatal(err)
					}
				}
			}
			if tc.wake {
				if _, err := pool.Exec(ctx, `UPDATE sagas SET wake_version=1,consumed_wake_version=0 WHERE id=$1`, saga.ID); err != nil {
					t.Fatal(err)
				}
			}
			if tc.index == 4 && tc.direction == "forward" {
				if _, err := pool.Exec(ctx, `UPDATE sagas SET payment_id=$2 WHERE id=$1`, saga.ID, uuid.New()); err != nil {
					t.Fatal(err)
				}
			}
			claim, err := repo.ClaimSaga(ctx, saga.ID, 30*time.Second)
			if err != nil || claim == nil {
				t.Fatalf("claim fixture: present=%v err=%v", claim != nil, err)
			}
			stepID := claim.Saga.Steps[tc.index].ID
			if owned, err := repo.PersistStepIntent(ctx, *claim, stepID, tc.direction); err != nil || !owned {
				t.Fatalf("persist intent: owned=%v err=%v", owned, err)
			}

			result := make(chan error, 1)
			go func() {
				owned, err := repo.PersistStepResult(ctx, *claim, domain.StepResult{StepID: stepID, Direction: tc.direction, State: "completed"})
				if err != nil {
					result <- err
					return
				}
				if !owned {
					result <- ErrRecoveryConflict
					return
				}
				result <- nil
			}()

			conn, err := acceptComponentPostCommit(t, listener)
			if err != nil {
				t.Fatal(err)
			}
			var event recoverycomponent.PostCommitEvent
			if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&event); err != nil {
				conn.Close()
				t.Fatal(err)
			}
			snapshot := readComponentSagaSnapshot(t, reader, saga.ID, stepID, tc.direction, tc.wantNext)
			assertComponentBoundary(t, event, snapshot, tc, *claim)
			t.Logf("component_boundary window=%s version=%s saga=%s step=%s pid=%d worker=%s commit=%s direction=%s index=%d->%d status=%s claim=%s fence=%d token_sha256=%s next_step_state=%s outbox_rows=%d reader_backend_pid=%d", tc.name, event.ContractVersion, event.SagaID, event.StepID, event.PID, event.WorkerID, event.CommitOutcome, event.Direction, event.PriorIndex, event.NextIndex, event.NextStatus, event.ClaimAction, event.ClaimFence, event.ClaimTokenSHA256, snapshot.NextState, snapshot.OutboxCount, readerBackendPID)
			if evidenceDir := os.Getenv("SAGA_COMPONENT_EVIDENCE_DIR"); evidenceDir != "" {
				emitSagaCandidateEvidence(t, evidenceDir, tc, event, snapshot, *claim,
					repositoryBackendPID, readerBackendPID, reader)
			}
			if _, err := conn.Write([]byte("1")); err != nil {
				conn.Close()
				t.Fatal(err)
			}
			conn.Close()
			select {
			case err := <-result:
				if err != nil {
					t.Fatalf("persist committed result: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("repository did not return after post-commit ACK")
			}
		})
	}
}

type componentSagaSnapshot struct {
	Index               int
	Status              string
	Recovery            string
	Token               string
	Fence               int64
	LeaseLive           bool
	ResultState         string
	NextState           string
	OutboxCount         int
	WakeVersion         int64
	ConsumedWakeVersion int64
}

func acceptComponentPostCommit(t *testing.T, listener net.Listener) (net.Conn, error) {
	t.Helper()
	if tcp, ok := listener.(*net.TCPListener); ok {
		if err := tcp.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return nil, err
		}
	}
	return listener.Accept()
}

func readComponentSagaSnapshot(t *testing.T, pool *pgxpool.Pool, sagaID, stepID uuid.UUID, direction string, nextIndex int) componentSagaSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got componentSagaSnapshot
	var resultState, nextState string
	query := `SELECT s.current_step_index,s.status::text,s.recovery_state,COALESCE(s.claim_token::text,''),s.claim_fence,COALESCE(s.lease_until>clock_timestamp(),false),s.wake_version,s.consumed_wake_version,st.`
	if direction == "forward" {
		query += `execute_state`
	} else {
		query += `compensate_state`
	}
	query += `,(SELECT count(*)::int FROM saga_ticket_issuance_outbox WHERE saga_id=s.id) FROM sagas s JOIN saga_steps st ON st.id=$2 WHERE s.id=$1`
	if err := pool.QueryRow(ctx, query, sagaID, stepID).Scan(&got.Index, &got.Status, &got.Recovery, &got.Token, &got.Fence, &got.LeaseLive, &got.WakeVersion, &got.ConsumedWakeVersion, &resultState, &got.OutboxCount); err != nil {
		t.Fatal(err)
	}
	got.ResultState = resultState
	if nextIndex >= 0 && nextIndex < 5 {
		column := "execute_state"
		if direction == "compensate" {
			column = "compensate_state"
		}
		if err := pool.QueryRow(ctx, `SELECT `+column+` FROM saga_steps WHERE saga_id=$1 AND "order"=$2`, sagaID, nextIndex).Scan(&nextState); err != nil {
			t.Fatal(err)
		}
	}
	got.NextState = nextState
	return got
}

func sagaCandidateWindow(caseName string) string {
	switch {
	case strings.HasPrefix(caseName, "forward_S0"):
		return "saga.forward.S0.result_before_next"
	case strings.HasPrefix(caseName, "forward_S1"):
		return "saga.forward.S1.result_before_next"
	case strings.HasPrefix(caseName, "forward_S2"):
		return "saga.forward.S2.result_before_next"
	case strings.HasPrefix(caseName, "forward_S3"):
		return "saga.forward.S3.result_before_next"
	case strings.HasPrefix(caseName, "forward_S4"):
		return "saga.forward.S4.result_before_next"
	case strings.HasPrefix(caseName, "compensation_S0"):
		return "saga.compensation.S0.result_before_next"
	case strings.HasPrefix(caseName, "compensation_S1"):
		return "saga.compensation.S1.result_before_next"
	case strings.HasPrefix(caseName, "compensation_S2"):
		return "saga.compensation.S2.result_before_next"
	case strings.HasPrefix(caseName, "compensation_S3"):
		return "saga.compensation.S3.result_before_next"
	default:
		return ""
	}
}

func emitSagaCandidateEvidence(t *testing.T, directory string, tc componentBoundaryCase,
	event recoverycomponent.PostCommitEvent, snapshot componentSagaSnapshot,
	claim domain.SagaClaim, repositoryPID, readerPID int, reader *pgxpool.Pool) {
	t.Helper()
	window := sagaCandidateWindow(tc.name)
	if window == "" {
		t.Fatalf("candidate evidence window mapping missing for %s", tc.name)
	}
	sha := os.Getenv("SAGA_COMPONENT_SOURCE_SHA256")
	commit := os.Getenv("SAGA_COMPONENT_SOURCE_COMMIT")
	tree := os.Getenv("SAGA_COMPONENT_SOURCE_TREE")
	if len(sha) != 64 || len(commit) != 40 || len(tree) != 40 {
		t.Fatalf("gated Saga evidence requires SAGA_COMPONENT_SOURCE_SHA256/COMMIT/TREE")
	}
	runID := os.Getenv("SAGA_COMPONENT_RUN_ID")
	if runID == "" {
		t.Fatal("gated Saga evidence requires SAGA_COMPONENT_RUN_ID")
	}
	attempt := os.Getenv("SAGA_COMPONENT_ATTEMPT")
	if attempt == "" {
		attempt = "1"
	}
	project := os.Getenv("SAGA_COMPONENT_PROJECT")
	if project == "" {
		project = "rushgo"
	}
	campaign := os.Getenv("SAGA_COMPONENT_CAMPAIGN")
	if campaign == "" {
		campaign = "candidate-saga-component"
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("create Saga candidate evidence directory: %v", err)
	}
	ctx := context.Background()
	var readerClock float64
	if err := reader.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM clock_timestamp())`).Scan(&readerClock); err != nil {
		t.Fatalf("read independent Saga clock: %v", err)
	}
	var leaseUntil *float64
	if err := reader.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM lease_until) FROM sagas WHERE id=$1`, claim.SagaID).Scan(&leaseUntil); err != nil {
		t.Fatalf("read Saga lease timestamp: %v", err)
	}
	prior := []int{}
	if tc.direction == "compensate" {
		rows, err := reader.Query(ctx, `SELECT "order" FROM saga_steps WHERE saga_id=$1 AND "order"<$2 AND compensate_state='completed' ORDER BY "order"`, claim.SagaID, tc.index)
		if err != nil {
			t.Fatalf("read prior compensation orders: %v", err)
		}
		for rows.Next() {
			var order int
			if err := rows.Scan(&order); err != nil {
				rows.Close()
				t.Fatalf("scan prior compensation order: %v", err)
			}
			prior = append(prior, order)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("read prior compensation orders: %v", err)
		}
		rows.Close()
	}
	var outboxCount int
	if err := reader.QueryRow(ctx, `SELECT count(*)::int FROM saga_ticket_issuance_outbox WHERE saga_id=$1`, claim.SagaID).Scan(&outboxCount); err != nil {
		t.Fatalf("read outbox count: %v", err)
	}
	outbox := map[string]any{
		"ticketOutboxExpected": event.TicketOutboxExpected,
		"rows":                 outboxCount,
	}
	if outboxCount == 1 {
		var eventID string
		var visibleAt float64
		var published bool
		var publishedAt float64
		if err := reader.QueryRow(ctx, `SELECT id::text, EXTRACT(EPOCH FROM created_at), published_at IS NOT NULL, COALESCE(EXTRACT(EPOCH FROM published_at),0) FROM saga_ticket_issuance_outbox WHERE saga_id=$1`, claim.SagaID).Scan(&eventID, &visibleAt, &published, &publishedAt); err != nil {
			t.Fatalf("read outbox evidence: %v", err)
		}
		outbox["eventId"] = eventID
		outbox["visibleAt"] = visibleAt
		if published {
			outbox["publishedAt"] = publishedAt
		} else {
			outbox["publishedAt"] = nil
		}
	}
	commitTime := float64(event.EmittedAt.UnixNano()) / 1e9
	visibleAt := readerClock
	if visibleAt < commitTime {
		visibleAt = commitTime
	}
	claimAction := strings.ToUpper(event.ClaimAction)
	commitStatus := event.CommitOutcome
	if commitStatus == "COMMIT_ACKED" {
		commitStatus = "COMMITTED"
	}
	windowDirection := event.Direction
	if windowDirection == "compensate" {
		windowDirection = "compensation"
	}
	productDirection := "forward"
	if event.Direction == "compensate" {
		productDirection = "compensate"
	}
	claimEvidence := map[string]any{
		"ownerStatus": claimAction,
		"observedAt":  visibleAt,
		"fence":       event.ClaimFence,
		"leaseLive":   snapshot.LeaseLive,
		"renewal":     nil,
		"revived":     false,
	}
	if claimAction == "RETAINED" {
		claimEvidence["tokenSha256"] = event.ClaimTokenSHA256
		if leaseUntil == nil {
			t.Fatal("retained candidate evidence has no persisted lease_until")
		}
		claimEvidence["leaseUntil"] = *leaseUntil
	} else {
		claimEvidence["tokenSha256"] = nil
		claimEvidence["leaseUntil"] = nil
		claimEvidence["releasedAt"] = visibleAt
	}
	var nextState any
	if snapshot.NextState != "" {
		nextState = snapshot.NextState
	}
	observation := map[string]any{
		"schemaVersion": 1,
		"kind":          "saga_result_before_next",
		"window":        window,
		"source":        map[string]any{"sha256": sha, "commit": commit, "tree": tree},
		"scope": map[string]any{
			"project": project, "campaign": campaign, "runId": runID,
			"attemptId": attempt, "window": window,
			"nonce": runID + "-" + tc.name,
		},
		"frozenIdentity":                   map[string]string{"sagaId": claim.SagaID.String(), "bookingId": claim.Saga.BookingID.String()},
		"observedIdentity":                 map[string]string{"sagaId": claim.SagaID.String(), "bookingId": claim.Saga.BookingID.String()},
		"expectedStep":                     claim.Saga.Steps[tc.index].Name,
		"completedStep":                    claim.Saga.Steps[tc.index].Name,
		"expectedStepOrder":                tc.index,
		"completedStepOrder":               tc.index,
		"expectedCurrentIndex":             event.NextIndex,
		"committedCurrentIndex":            event.NextIndex,
		"direction":                        windowDirection,
		"productDirection":                 productDirection,
		"sagaStatus":                       event.NextStatus,
		"wakeSignal":                       nil,
		"commitStatus":                     commitStatus,
		"commitTime":                       commitTime,
		"visibleAt":                        visibleAt,
		"observedAt":                       visibleAt,
		"nextDispatched":                   false,
		"independentRead":                  true,
		"nextStepState":                    nextState,
		"nextDispatchAt":                   nil,
		"priorCompletedCompensationOrders": prior,
		"observer": map[string]any{
			"writerBackendPid": repositoryPID, "readerBackendPid": readerPID,
			"readerApplicationName": "saga_component_matrix_reader", "readAt": visibleAt,
		},
		"claim":  claimEvidence,
		"outbox": outbox,
		"evidenceMeta": map[string]any{
			"emitter": "saga_component_matrix", "contract": event.ContractVersion,
			"case": tc.name, "componentOnly": true, "formalEvidence": false,
			"timeBinding":        "POST_COMMIT_EVENT_UPPER_BOUND",
			"releaseTimeBinding": "FIRST_READER_VISIBLE_UPPER_BOUND",
			"source":             "REAL_PG16_COMPONENT", "provider": "synthetic",
			"sourceBinding": "EMITTER_TEST_FILE_SHA256",
		},
	}
	if tc.direction == "forward" && tc.index == 1 {
		observation["wakeSignal"] = tc.wake
	}
	if window == "saga.compensation.S3.result_before_next" {
		caseBookingID := strings.TrimSpace(os.Getenv("SAGA_COMPONENT_BOOKING_ID_" + strings.ToUpper(tc.name)))
		if bookingPath := strings.TrimSpace(os.Getenv("SAGA_COMPONENT_BOOKING_OBSERVATION")); bookingPath != "" && caseBookingID != "" {
			expectedSHA := strings.TrimSpace(os.Getenv("SAGA_COMPONENT_BOOKING_SOURCE_SHA256"))
			expectedRun := strings.TrimSpace(os.Getenv("SAGA_COMPONENT_BOOKING_RUN_ID_" + strings.ToUpper(tc.name)))
			if expectedRun == "" {
				expectedRun = strings.TrimSpace(os.Getenv("SAGA_COMPONENT_BOOKING_RUN_ID"))
			}
			rawBooking, booking := loadBookingCompensationObservation(t, bookingPath, claim.Saga.BookingID.String(), expectedRun)
			bookingSource, _ := booking["source"].(map[string]any)
			bookingSHA, _ := bookingSource["sha256"].(string)
			bookingCommit, _ := bookingSource["commit"].(string)
			bookingTree, _ := bookingSource["tree"].(string)
			if len(expectedSHA) != 64 || expectedRun == "" || bookingSHA != expectedSHA ||
				bookingCommit != commit || bookingTree != tree ||
				booking["runId"] != expectedRun {
				t.Fatalf("Booking compensation observation is not bound to this saga source/run")
			}
			booking["sagaId"] = claim.SagaID.String()
			booking["stepOrder"] = tc.index
			booking["direction"] = "compensation"
			observation["compensationS3Booking"] = booking
			digest := sha256.Sum256(rawBooking)
			observation["evidenceMeta"].(map[string]any)["bookingObservationSha256"] = hex.EncodeToString(digest[:])
			observation["evidenceMeta"].(map[string]any)["bookingSourceSha256"] = expectedSHA
			observation["evidenceMeta"].(map[string]any)["bookingSourceRunId"] = expectedRun
		}
		// Absent a separately produced Booking observation, the window stays
		// UNKNOWN; the saga component never fabricates the booking fact.
	}
	path := filepath.Join(directory, "saga-"+strings.ReplaceAll(window, ".", "_")+"-"+tc.name+".json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create Saga candidate evidence %s: %v", path, err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(observation); err != nil {
		file.Close()
		t.Fatalf("encode Saga candidate evidence: %v", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatalf("sync Saga candidate evidence: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close Saga candidate evidence: %v", err)
	}
}

func loadBookingCompensationObservation(t *testing.T, path, bookingID, expectedRun string) ([]byte, map[string]any) {
	t.Helper()
	candidates := []string{path}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("inspect Booking compensation observation path: %v", err)
	}
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatalf("read Booking compensation observation directory: %v", err)
		}
		candidates = candidates[:0]
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
				candidates = append(candidates, filepath.Join(path, entry.Name()))
			}
		}
	}
	var matched []byte
	var matchedBooking map[string]any
	for _, candidate := range candidates {
		raw, err := os.ReadFile(candidate)
		if err != nil {
			t.Fatalf("read Booking compensation observation %s: %v", candidate, err)
		}
		var booking map[string]any
		if err := json.Unmarshal(raw, &booking); err != nil {
			t.Fatalf("decode Booking compensation observation %s: %v", candidate, err)
		}
		if booking["bookingId"] != bookingID || booking["runId"] != expectedRun {
			continue
		}
		if matched != nil {
			t.Fatalf("multiple Booking compensation observations name %s run %s", bookingID, expectedRun)
		}
		matched, matchedBooking = raw, booking
	}
	if matched == nil {
		t.Fatalf("no Booking compensation observation names %s", bookingID)
	}
	return matched, matchedBooking
}

func assertComponentBoundary(t *testing.T, event recoverycomponent.PostCommitEvent, got componentSagaSnapshot, tc componentBoundaryCase, claim domain.SagaClaim) {
	t.Helper()
	if event.ContractVersion != recoverycomponent.PostCommitContractVersion || event.CommitOutcome != "COMMIT_ACKED" ||
		event.SagaID != claim.SagaID.String() || event.StepID != claim.Saga.Steps[tc.index].ID.String() ||
		event.Direction != tc.direction || event.ResultState != "completed" || event.PriorIndex != tc.index ||
		event.NextIndex != tc.wantNext || event.NextStatus != tc.wantStatus || event.ClaimAction != tc.wantAction ||
		event.ClaimFence != claim.Fence || event.WorkerID != "matrix-repository-worker" || event.PID != os.Getpid() || event.EmittedAt.IsZero() ||
		event.TicketOutboxExpected != (tc.wantOutbox == 1) {
		t.Fatalf("unexpected versioned result boundary: %+v", event)
	}
	if got.Index != tc.wantNext || got.Status != tc.wantStatus || got.Recovery != "ready" || got.ResultState != "completed" || got.OutboxCount != tc.wantOutbox {
		t.Fatalf("independent DB read disagreed with committed boundary: event=%+v db=%+v", event, got)
	}
	if tc.wake && (got.WakeVersion != 1 || got.ConsumedWakeVersion != 1) {
		t.Fatalf("wake version was not consumed with the S1 result: %+v", got)
	}
	if tc.name == "forward_S1_no_wake" && (got.WakeVersion != 0 || got.ConsumedWakeVersion != 0) {
		t.Fatalf("no-wake S1 branch changed wake state: %+v", got)
	}
	if event.ClaimAction == "retained" {
		digest := sha256.Sum256([]byte(got.Token))
		if got.Token == "" || !got.LeaseLive || got.Fence != event.ClaimFence || hex.EncodeToString(digest[:]) != event.ClaimTokenSHA256 {
			t.Fatalf("retained claim did not match the independent token/fence/lease read: %+v", got)
		}
	} else if event.ClaimAction == "released" {
		if got.Token != "" || got.LeaseLive || got.Fence != event.ClaimFence {
			t.Fatalf("release did not clear the live token/lease: %+v", got)
		}
	} else {
		t.Fatalf("unknown claim transaction action %q", event.ClaimAction)
	}
	if got.NextState != "" && got.NextState != "pending" {
		t.Fatalf("next business step was already dispatched before ACK: state=%s", got.NextState)
	}
}
