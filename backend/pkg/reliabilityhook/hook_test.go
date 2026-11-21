package reliabilityhook

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testToken = "t23-private-token-for-unit-tests-only"

func enabledHook(t *testing.T) (*Controller, string) {
	t.Helper()
	dir := t.TempDir()
	h, err := New(Config{Enabled: true, Token: testToken, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	return h, dir
}

func TestW28RefundReadsTheKeyedCommandAlongsideTheReceiptCommand(t *testing.T) {
	hook, dir := enabledHook(t)
	window := "saga.late_collection.compensation_verified_full_refund"
	payment := "11111111-1111-4111-8111-111111111111"
	boundary := Boundary{WindowID: window, OperationID: payment,
		Checkpoint: "payment_w28_verified_refund_before_marker", Action: ActionPause}
	key := boundaryKey(window, payment, boundary.Checkpoint)
	nonce := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	writeCommand(t, dir, Command{WindowID: window, OperationID: payment,
		Checkpoint: "payment_w28_initial_receipt", Action: ActionPause, Token: testToken,
		Nonce: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})
	if err := writeJSONAtomic(filepath.Join(dir, "w28-refund-command-"+key+".json"),
		Command{WindowID: window, OperationID: payment, Checkpoint: boundary.Checkpoint,
			Action: ActionPause, Token: testToken, Nonce: nonce}); err != nil {
		t.Fatal(err)
	}
	writeDecision(t, dir, key, nonce, DecisionContinue, testToken, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	decision, matched, err := hook.AwaitW28Refund(ctx, boundary)
	if err != nil || !matched || decision != DecisionContinue {
		t.Fatalf("W28 keyed refund command was not accepted, matched=%v decision=%v err=%v", matched, decision, err)
	}
	receipt, err := readJSONFile[Command](filepath.Join(dir, commandFile))
	if err != nil || receipt.Checkpoint != "payment_w28_initial_receipt" {
		t.Fatalf("initial receipt command changed, err=%v", err)
	}
}

func TestHasMatchingCommandRequiresExactAuthenticatedCommand(t *testing.T) {
	boundary := Boundary{WindowID: "saga.forward.S1.result_before_next", OperationID: "booking-t23",
		Checkpoint: "saga.forward.S1.result_before_next", Action: ActionPause}
	tests := []struct {
		name      string
		command   *Command
		wantMatch bool
	}{
		{name: "exact command", command: &Command{WindowID: boundary.WindowID,
			OperationID: boundary.OperationID, Checkpoint: boundary.Checkpoint, Action: boundary.Action,
			Token: testToken, Nonce: "nonce-exact"}, wantMatch: true},
		{name: "different window", command: &Command{WindowID: "saga.forward.S1.before_effect",
			OperationID: boundary.OperationID, Checkpoint: boundary.Checkpoint, Action: boundary.Action,
			Token: testToken, Nonce: "nonce-other-window"}},
		{name: "wrong token", command: &Command{WindowID: boundary.WindowID,
			OperationID: boundary.OperationID, Checkpoint: boundary.Checkpoint, Action: boundary.Action,
			Token: "another-private-token", Nonce: "nonce-wrong-token"}},
		{name: "missing command"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hook, dir := enabledHook(t)
			if test.command != nil {
				writeCommand(t, dir, *test.command)
			}
			if got := hook.HasMatchingCommand(boundary); got != test.wantMatch {
				t.Fatalf("HasMatchingCommand() = %v, want %v", got, test.wantMatch)
			}
			if test.wantMatch {
				if _, err := os.Stat(filepath.Join(dir, commandFile)); err != nil {
					t.Fatalf("preflight consumed the pending command: %v", err)
				}
				if _, err := os.Stat(filepath.Join(dir, barrierFile(boundaryKey(boundary.WindowID,
					boundary.OperationID, boundary.Checkpoint)))); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("preflight published a barrier: %v", err)
				}
			}
		})
	}
}

func TestTerminalAckServerInterceptorWithholdsOnlyAfterSuccessfulCancelHandler(t *testing.T) {
	h, dir := enabledHook(t)
	bookingID := "bfb6bbd9-f98d-4ed3-9523-f2f32ca4c3a7"
	method := bookingv1.BookingService_CancelBooking_FullMethodName
	key := boundaryKey("booking.terminal_ack_lost", bookingID, method)
	writeCommand(t, dir, Command{WindowID: "booking.terminal_ack_lost", OperationID: bookingID,
		Checkpoint: method, Action: ActionAckGate, Token: testToken, Nonce: "nonce-terminal"})
	type outcome struct {
		response any
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		response, err := h.UnaryServerInterceptor()(context.Background(),
			&bookingv1.CancelBookingRequest{BookingId: bookingID},
			&grpc.UnaryServerInfo{FullMethod: method},
			func(context.Context, any) (any, error) {
				return &bookingv1.BookingDetail{Id: bookingID, Status: "CANCELLED"}, nil
			})
		done <- outcome{response: response, err: err}
	}()
	readBarrier(t, dir, key)
	select {
	case got := <-done:
		t.Fatalf("successful CancelBooking passed the external durability gate: %+v", got)
	default:
	}
	writeDecision(t, dir, key, "nonce-terminal", DecisionWithhold, testToken, &AckProof{
		Kind: "saga_terminal", BookingID: bookingID,
		DecisionID:    "a4fc4f85-a7a3-4ca6-93f0-a36d6f86dfcf",
		OutboxEventID: "628bf45a-f159-4a0c-b1aa-a04e596c66af", Durable: true,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	got := <-done
	if got.response != nil || status.Code(got.err) != codes.Unavailable {
		t.Fatalf("terminal success ACK was not withheld: %+v", got)
	}
}

func TestTerminalAckServerInterceptorPassesHandlerErrors(t *testing.T) {
	h, dir := enabledHook(t)
	bookingID := "bfb6bbd9-f98d-4ed3-9523-f2f32ca4c3a7"
	method := bookingv1.BookingService_CancelBooking_FullMethodName
	writeCommand(t, dir, Command{WindowID: "booking.terminal_ack_lost", OperationID: bookingID,
		Checkpoint: method, Action: ActionAckGate, Token: testToken, Nonce: "nonce-terminal-error"})
	expected := status.Error(codes.FailedPrecondition, "not cancellable")
	_, err := h.UnaryServerInterceptor()(context.Background(),
		&bookingv1.CancelBookingRequest{BookingId: bookingID},
		&grpc.UnaryServerInfo{FullMethod: method},
		func(context.Context, any) (any, error) { return nil, expected })
	if err != expected {
		t.Fatalf("Booking handler error was changed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, barrierFile(boundaryKey("booking.terminal_ack_lost", bookingID, method)))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed Booking handler falsely emitted terminal ACK barrier: %v", err)
	}
}

func TestConcurrentCallersJoinOneClaimedReliabilityBarrier(t *testing.T) {
	h, dir := enabledHook(t)
	boundary := Boundary{WindowID: "booking.saga_ack_lost", OperationID: "request-t23",
		Checkpoint: "booking_dispatch_claimed_before_saga_start", Action: ActionPause}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	writeCommand(t, dir, Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-shared"})
	type result struct {
		decision Decision
		matched  bool
		err      error
	}
	first := make(chan result, 1)
	second := make(chan result, 1)
	go func() {
		decision, matched, err := h.Await(context.Background(), boundary)
		first <- result{decision: decision, matched: matched, err: err}
	}()
	readBarrier(t, dir, key)
	go func() {
		decision, matched, err := h.Await(context.Background(), boundary)
		second <- result{decision: decision, matched: matched, err: err}
	}()
	time.Sleep(10 * time.Millisecond)
	select {
	case got := <-second:
		t.Fatalf("duplicate caller escaped a still-claimed barrier: %+v", got)
	default:
	}
	writeDecision(t, dir, key, "nonce-shared", DecisionContinue, testToken, nil)
	for i, ch := range []chan result{first, second} {
		got := <-ch
		if got.err != nil || !got.matched || got.decision != DecisionContinue {
			t.Fatalf("caller %d did not join the same barrier: %+v", i, got)
		}
	}
}

func TestSchedulerRelayBarrierRequiresExactDurableClaim(t *testing.T) {
	h, dir := enabledHook(t)
	eventID := "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	configID := "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	checkpoint := "scheduler_claimed_before_publish"
	facts := map[string]string{
		"scheduler_config_id": configID, "outbound_event_id": eventID,
		"version": "2", "claim_token": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"claim_fence": "7", "lease_until": time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		"topic":      "scheduler.config.events.t23-aaaaaaaaaaaaaaaaaaaaaaaa",
		"event_type": "SchedulerConfigChanged",
	}
	key := boundaryKey("scheduler.outbound_claim_before_effect", eventID, checkpoint)
	writeCommand(t, dir, Command{WindowID: "scheduler.outbound_claim_before_effect",
		OperationID: eventID, Checkpoint: checkpoint, Action: ActionPause,
		Token: testToken, Nonce: "scheduler-claim"})
	done := make(chan error, 1)
	go func() {
		_, matched, err := h.AwaitSchedulerBoundary(context.Background(),
			"scheduler.outbound_claim_before_effect", eventID, checkpoint, facts)
		if err == nil && !matched {
			err = errors.New("Scheduler claim barrier did not match")
		}
		done <- err
	}()
	readBarrier(t, dir, key)
	select {
	case err := <-done:
		t.Fatalf("claim barrier returned before the exact decision: %v", err)
	default:
	}
	writeDecision(t, dir, key, "scheduler-claim", DecisionContinue, testToken, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	bad := map[string]string{}
	for key, value := range facts {
		bad[key] = value
	}
	bad["claim_fence"] = "0"
	if _, _, err := h.AwaitSchedulerBoundary(context.Background(),
		"scheduler.outbound_claim_before_effect", eventID, checkpoint, bad); err == nil {
		t.Fatal("invalid durable claim fence accepted")
	}
}

func TestSchedulerConfigBarrierRequiresAdjacentVersions(t *testing.T) {
	h, _ := enabledHook(t)
	eventID := "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	facts := map[string]string{
		"scheduler_config_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		"outbound_event_id":   eventID, "version": "3", "prior_version": "1",
	}
	if _, _, err := h.AwaitSchedulerBoundary(context.Background(),
		"scheduler.config_replacement_reload", eventID,
		"scheduler_config_before_apply", facts); err == nil {
		t.Fatal("nonadjacent version replacement accepted")
	}
}

func TestAdmissionRepairBoundaryBindsCapturedOwnerAndSweeper(t *testing.T) {
	h, dir := enabledHook(t)
	ownerID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	eventID := "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	seatID := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	key := "rushgo:seat-admission:" + eventID + ":" + seatID
	windowID := "redis.stale_owner_exact_repair"
	sweeperID := "sweeper-1"
	operationID := ownerID + "." + sweeperID
	checkpoint := "redis_before_compare_delete"
	boundaryKey := boundaryKey(windowID, operationID, checkpoint)
	writeCommand(t, dir, Command{WindowID: windowID, OperationID: operationID,
		Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "redis-repair"})
	if err := os.Rename(filepath.Join(dir, commandFile),
		filepath.Join(dir, admissionRepairCommandFile(sweeperID))); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		decision, matched, err := h.AwaitAdmissionRepair(context.Background(),
			ownerID, eventID, seatID, key, sweeperID)
		if err == nil && (!matched || decision != DecisionContinue) {
			err = errors.New("Event repair barrier did not continue")
		}
		done <- err
	}()
	barrier := readBarrier(t, dir, boundaryKey)
	if barrier.Facts["owner_id"] != ownerID || barrier.Facts["sweeper_id"] != sweeperID ||
		barrier.Facts["redis_key"] != key {
		t.Fatalf("repair barrier lost its captured identity: %+v", barrier.Facts)
	}
	writeDecision(t, dir, boundaryKey, "redis-repair", DecisionContinue, testToken, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := h.RecordAdmissionRepairOutcome(ownerID, eventID, seatID, key,
		sweeperID, false, nil); err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(dir, repairResultFile(boundaryKey, "redis-repair"))
	result, err := readJSONFile[AdmissionRepairResult](resultPath)
	if err != nil || result.WindowID != windowID || result.Nonce != "redis-repair" || result.Deleted {
		t.Fatalf("atomic repair outcome was not recorded accurately: %+v %v", result, err)
	}
	if _, _, err := h.AwaitAdmissionRepair(context.Background(),
		ownerID, eventID, seatID, key, "sweeper-3"); err == nil {
		t.Fatal("unscoped sweeper identity accepted")
	}
}

func TestAdmissionRepairWrapperFailsClosedAfterClaimCancellation(t *testing.T) {
	h, dir := enabledHook(t)
	ownerID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	eventID := "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	seatID := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	redisKey := "rushgo:seat-admission:" + eventID + ":" + seatID
	sweeperID := "sweeper-1"
	operationID := ownerID + "." + sweeperID
	checkpoint := "redis_before_compare_delete"
	key := boundaryKey("redis.stale_owner_exact_repair", operationID, checkpoint)
	commandPath := filepath.Join(dir, admissionRepairCommandFile(sweeperID))
	writeCommand(t, dir, Command{WindowID: "redis.stale_owner_exact_repair",
		OperationID: operationID, Checkpoint: checkpoint, Action: ActionPause,
		Token: testToken, Nonce: "redis-repair-cancel"})
	if err := os.Rename(filepath.Join(dir, commandFile), commandPath); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := h.AwaitAdmissionRepair(ctx, ownerID, eventID, seatID, redisKey, sweeperID)
		done <- err
	}()
	readBarrier(t, dir, key)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Event repair barrier returned %v", err)
	}
	decision, matched, err := h.AwaitAdmissionRepair(context.Background(),
		ownerID, eventID, seatID, redisKey, sweeperID)
	if !errors.Is(err, ErrUnresolvedClaim) || !matched || decision != DecisionContinue {
		t.Fatalf("retry bypassed unresolved Event repair claim: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
}

func TestRestartedControllerFailsClosedOnUnresolvedClaim(t *testing.T) {
	h, dir := enabledHook(t)
	boundary := Boundary{WindowID: "booking.commit_before_saga_dispatch", OperationID: "request-restart",
		Checkpoint: "booking_dispatch_claimed_before_saga_start", Action: ActionPause,
		Facts: map[string]string{"campaign_id": "campaign-a", "booking_id": "booking-a",
			"saga_id": "saga-a", "step_id": "step-a", "phase": "before_effect", "claim_fence": "1"}}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	writeCommand(t, dir, Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-restart"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := h.Await(ctx, boundary)
		done <- err
	}()
	readBarrier(t, dir, key)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("original process did not leave its claimed barrier: %v", err)
	}

	restarted, err := New(Config{Enabled: true, Token: testToken, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	decision, matched, err := restarted.Await(context.Background(), boundary)
	effectCalls := 0
	if err == nil && decision == DecisionContinue {
		effectCalls++
	}
	if err == nil || !matched || decision != DecisionContinue || effectCalls != 0 {
		t.Fatalf("restarted process bypassed an unresolved claim: decision=%q matched=%v effect_calls=%d err=%v",
			decision, matched, effectCalls, err)
	}
}

func TestSagaRecoveryPermitAllowsOneHigherFenceAfterObservedSIGKILL(t *testing.T) {
	h, dir := enabledHook(t)
	const bookingID = "11111111-1111-4111-8111-111111111111"
	const sagaID = "22222222-2222-4222-8222-222222222222"
	const stepID = "33333333-3333-4333-8333-333333333333"
	boundary := Boundary{WindowID: "saga.forward.S0.before_effect", OperationID: bookingID,
		Checkpoint: "saga.forward.S0.before_effect", Action: ActionPause,
		Facts: map[string]string{"booking_id": bookingID, "saga_id": sagaID,
			"step_id": stepID, "step_name": "RESERVED_SEAT_STEP", "direction": "forward",
			"phase": "before_effect", "claim_fence": "1", "step_status": "PENDING"}}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	writeCommand(t, dir, Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-saga-kill"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := h.Await(ctx, boundary)
		done <- err
	}()
	barrier := readBarrier(t, dir, key)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("original Saga barrier did not stop at cancellation: %v", err)
	}

	proof := map[string]any{
		"kind": "T23_SAGA_SIGKILL_RECOVERY", "window_id": boundary.WindowID,
		"operation_id": bookingID,
		"booking_id":   bookingID, "saga_id": sagaID, "step_id": stepID,
		"step_name": "RESERVED_SEAT_STEP", "direction": "forward", "phase": "before_effect",
		"claim_fence": "1", "fault_kind": "SIGKILL", "container_id": strings.Repeat("a", 64),
		"pid": 42, "started_at": "2026-10-18T08:00:00Z", "process_stopped": true,
	}
	decisionRaw, err := json.Marshal(map[string]any{
		"token": testToken, "nonce": "nonce-saga-kill", "decision": "continue",
		"saga_recovery_proof": proof,
	})
	if err != nil {
		t.Fatal(err)
	}
	var writtenDecision ControlDecision
	if err := json.Unmarshal(decisionRaw, &writtenDecision); err != nil {
		t.Fatal(err)
	}
	recovered := boundary
	recovered.Facts = cloneFacts(boundary.Facts)
	recovered.Facts["claim_fence"] = "2"
	recovered.Facts["step_status"] = "EXECUTING"
	if !sameSagaFactsForRecoveryAdvance(barrier.Facts, recovered.Facts) {
		t.Fatal("test fixture did not isolate the permitted PENDING-to-EXECUTING recovery transition")
	}
	if !validSagaRecoveryProof(recovered, barrier, writtenDecision.SagaRecoveryProof) {
		t.Fatal("test fixture did not construct a valid exact Saga SIGKILL proof")
	}
	if err := os.WriteFile(filepath.Join(dir, decisionFile(key)), decisionRaw, 0o600); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(Config{Enabled: true, Token: testToken, Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	decision, matched, err := restarted.Await(context.Background(), recovered)
	if err != nil || !matched || decision != DecisionContinue {
		t.Fatalf("exact higher-fence Saga recovery was not released: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".claimed-"+key+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered one-shot Saga boundary remained claimed: err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, barrierFile(key))); err != nil {
		t.Fatalf("recovered Saga barrier receipt was not preserved: err=%v", err)
	}
	laterRetry := recovered
	laterRetry.Facts = cloneFacts(recovered.Facts)
	laterRetry.Facts["claim_fence"] = "3"
	decision, matched, err = restarted.Await(context.Background(), laterRetry)
	if err != nil || matched || decision != DecisionContinue {
		t.Fatalf("later retry was blocked by the consumed Saga boundary: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
}

func TestSagaRecoveryPermitRejectsChangedIdentityAndUnprovenFence(t *testing.T) {
	tests := []struct {
		name        string
		fence       string
		change      func(map[string]string)
		permit      bool
		changeProof func(map[string]any)
	}{
		{name: "missing permit", fence: "2"},
		{name: "same fence", fence: "1", permit: true},
		{name: "lower fence", fence: "0", permit: true},
		{name: "changed Saga", fence: "2", permit: true,
			change: func(facts map[string]string) { facts["saga_id"] = "44444444-4444-4444-8444-444444444444" }},
		{name: "changed step phase", fence: "2", permit: true,
			change: func(facts map[string]string) { facts["phase"] = "effect_before_result" }},
		{name: "unsupported step status transition", fence: "2", permit: true,
			change: func(facts map[string]string) { facts["step_status"] = "PENDING" }},
		{name: "wrong old fence proof", fence: "2", permit: true,
			changeProof: func(proof map[string]any) { proof["claim_fence"] = "2" }},
		{name: "wrong fault proof", fence: "2", permit: true,
			changeProof: func(proof map[string]any) { proof["fault_kind"] = "SIGTERM" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, dir := enabledHook(t)
			const bookingID = "11111111-1111-4111-8111-111111111111"
			const sagaID = "22222222-2222-4222-8222-222222222222"
			const stepID = "33333333-3333-4333-8333-333333333333"
			boundary := Boundary{WindowID: "saga.forward.S0.before_effect", OperationID: bookingID,
				Checkpoint: "saga.forward.S0.before_effect", Action: ActionPause,
				Facts: map[string]string{"booking_id": bookingID, "saga_id": sagaID,
					"step_id": stepID, "step_name": "RESERVED_SEAT_STEP", "direction": "forward",
					"phase": "before_effect", "claim_fence": "1", "step_status": "EXECUTING"}}
			key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
			writeCommand(t, dir, Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
				Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-saga-negative"})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { _, _, err := h.Await(ctx, boundary); done <- err }()
			readBarrier(t, dir, key)
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("original Saga barrier did not cancel: %v", err)
			}
			if tc.permit {
				proof := map[string]any{
					"kind": "T23_SAGA_SIGKILL_RECOVERY", "window_id": boundary.WindowID,
					"operation_id": bookingID, "booking_id": bookingID, "saga_id": sagaID,
					"step_id": stepID, "step_name": "RESERVED_SEAT_STEP", "direction": "forward",
					"phase": "before_effect", "claim_fence": "1", "fault_kind": "SIGKILL",
					"container_id": strings.Repeat("a", 64), "pid": 42,
					"started_at": "2026-10-18T08:00:00Z", "process_stopped": true,
				}
				if tc.changeProof != nil {
					tc.changeProof(proof)
				}
				raw, err := json.Marshal(map[string]any{
					"token": testToken, "nonce": "nonce-saga-negative", "decision": "continue",
					"saga_recovery_proof": proof,
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, decisionFile(key)), raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			retry := boundary
			retry.Facts = cloneFacts(boundary.Facts)
			retry.Facts["claim_fence"] = tc.fence
			if tc.change != nil {
				tc.change(retry.Facts)
			}
			restarted, err := New(Config{Enabled: true, Token: testToken, Directory: dir})
			if err != nil {
				t.Fatal(err)
			}
			decision, matched, err := restarted.Await(context.Background(), retry)
			if err == nil || !matched || decision != DecisionContinue {
				t.Fatalf("unproven Saga retry reached the effect: decision=%q matched=%v err=%v",
					decision, matched, err)
			}
		})
	}
}

func TestCancelledClaimSameIdentityRetryCannotReachEffect(t *testing.T) {
	h, dir := enabledHook(t)
	boundary := Boundary{WindowID: "saga.forward.S0.before_effect", OperationID: "booking-retry",
		Checkpoint: "saga.forward.S0.before_effect", Action: ActionPause,
		Facts: map[string]string{"campaign_id": "campaign-a", "booking_id": "booking-a",
			"saga_id": "saga-a", "step_id": "step-a", "phase": "before_effect", "claim_fence": "1"}}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	writeCommand(t, dir, Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-retry"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := h.Await(ctx, boundary)
		done <- err
	}()
	readBarrier(t, dir, key)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("initial wait did not return context cancellation: %v", err)
	}

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer retryCancel()
	decision, matched, err := h.Await(retryCtx, boundary)
	effectCalls := 0
	if err == nil && decision == DecisionContinue {
		effectCalls++
	}
	if err == nil || !matched || effectCalls != 0 {
		t.Fatalf("same-identity retry escaped its unresolved Saga barrier: decision=%q matched=%v effect_calls=%d err=%v",
			decision, matched, effectCalls, err)
	}
}

func TestReplacementCommandCannotOverwriteUnresolvedSameIdentityClaim(t *testing.T) {
	h, dir := enabledHook(t)
	boundary := Boundary{WindowID: "saga.forward.S0.before_effect", OperationID: "booking-replace",
		Checkpoint: "saga.forward.S0.before_effect", Action: ActionPause,
		Facts: map[string]string{"campaign_id": "campaign-a", "booking_id": "booking-a",
			"saga_id": "saga-a", "step_id": "step-a", "phase": "before_effect", "claim_fence": "1"}}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	writeCommand(t, dir, Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-original"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := h.Await(ctx, boundary)
		done <- err
	}()
	readBarrier(t, dir, key)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("initial wait did not return context cancellation: %v", err)
	}
	writeCommand(t, dir, Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-replacement"})

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer retryCancel()
	decision, matched, err := h.Await(retryCtx, boundary)
	effectCalls := 0
	if err == nil && decision == DecisionContinue {
		effectCalls++
	}
	if !errors.Is(err, ErrUnresolvedClaim) || !matched || effectCalls != 0 {
		t.Fatalf("replacement command escaped the original unresolved claim: decision=%q matched=%v effect_calls=%d err=%v",
			decision, matched, effectCalls, err)
	}
	original, err := readJSONFile[Command](filepath.Join(dir, ".claimed-"+key+".json"))
	if err != nil || original.Nonce != "nonce-original" {
		t.Fatalf("replacement overwrote the durable original claim: command=%+v err=%v", original, err)
	}
	queued, err := readJSONFile[Command](filepath.Join(dir, commandFile))
	if err != nil || queued.Nonce != "nonce-replacement" {
		t.Fatalf("replacement command was not left queued: command=%+v err=%v", queued, err)
	}
}

func TestSameProcessActiveClaimRejectsChangedBoundaryFacts(t *testing.T) {
	h, dir := enabledHook(t)
	boundary := Boundary{WindowID: "saga.forward.S0.before_effect", OperationID: "booking-active-facts",
		Checkpoint: "saga.forward.S0.before_effect", Action: ActionPause,
		Facts: map[string]string{"campaign_id": "campaign-a", "booking_id": "booking-a",
			"saga_id": "saga-a", "step_id": "step-a", "phase": "before_effect", "claim_fence": "1"}}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	command := Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-active-facts"}
	barrier := Barrier{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Nonce: command.Nonce,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Facts: cloneFacts(boundary.Facts)}
	if err := writeJSONAtomic(filepath.Join(dir, barrierFile(key)), barrier); err != nil {
		t.Fatal(err)
	}
	writeDecision(t, dir, key, command.Nonce, DecisionContinue, testToken, nil)
	h.active[key] = command

	retry := boundary
	retry.Facts = cloneFacts(boundary.Facts)
	retry.Facts["claim_fence"] = "2"
	decision, matched, err := h.Await(context.Background(), retry)
	effectCalls := 0
	if err == nil && decision == DecisionContinue {
		effectCalls++
	}
	if err == nil || !matched || decision != DecisionContinue || effectCalls != 0 {
		t.Fatalf("same-process join accepted changed boundary facts: decision=%q matched=%v effect_calls=%d err=%v",
			decision, matched, effectCalls, err)
	}
}

func TestUnresolvedClaimDoesNotBlockUnrelatedIdentity(t *testing.T) {
	h, dir := enabledHook(t)
	boundary := Boundary{WindowID: "saga.forward.S0.before_effect", OperationID: "booking-one",
		Checkpoint: "saga.forward.S0.before_effect", Action: ActionPause}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	writeCommand(t, dir, Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-one"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := h.Await(ctx, boundary)
		done <- err
	}()
	readBarrier(t, dir, key)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("initial wait did not return context cancellation: %v", err)
	}

	unrelated := boundary
	unrelated.OperationID = "booking-two"
	decision, matched, err := h.Await(context.Background(), unrelated)
	if err != nil || matched || decision != DecisionContinue {
		t.Fatalf("unrelated operation was affected by another claim: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
}

func TestExplicitContinueResolvesClaimForSameIdentityRetry(t *testing.T) {
	h, dir := enabledHook(t)
	boundary := Boundary{WindowID: "saga.forward.S0.before_effect", OperationID: "booking-continue",
		Checkpoint: "saga.forward.S0.before_effect", Action: ActionPause}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	writeCommand(t, dir, Command{WindowID: boundary.WindowID, OperationID: boundary.OperationID,
		Checkpoint: boundary.Checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-continue"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := h.Await(ctx, boundary)
		done <- err
	}()
	readBarrier(t, dir, key)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("initial wait did not return context cancellation: %v", err)
	}
	writeDecision(t, dir, key, "nonce-continue", DecisionContinue, testToken, nil)
	decision, matched, err := h.Await(context.Background(), boundary)
	if err != nil || !matched || decision != DecisionContinue {
		t.Fatalf("explicit continue did not resolve the exact claim: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
}

func writeCommand(t *testing.T, dir string, c Command) {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, commandFile), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readBarrier(t *testing.T, dir, key string) Barrier {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(filepath.Join(dir, barrierFile(key)))
		if err == nil {
			var got Barrier
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			return got
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("barrier was not written")
	return Barrier{}
}

func writeDecision(t *testing.T, dir, key, nonce string, decision Decision, token string, proof *AckProof) {
	t.Helper()
	if err := writeJSONAtomic(filepath.Join(dir, decisionFile(key)),
		ControlDecision{Token: token, Nonce: nonce, Decision: decision, Proof: proof}); err != nil {
		t.Fatal(err)
	}
}

func TestDisabledControllerPreservesNormalBehavior(t *testing.T) {
	h, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	decision, matched, err := h.Await(context.Background(), Boundary{
		WindowID: "booking.commit_before_saga_dispatch", OperationID: "request-1",
		Checkpoint: "booking_committed_before_dispatch", Action: ActionPause,
	})
	if err != nil || matched || decision != DecisionContinue {
		t.Fatalf("disabled hook changed the path: decision=%q matched=%v err=%v", decision, matched, err)
	}
}

func TestEnabledControllerRequiresExplicitPrivateConfiguration(t *testing.T) {
	for _, cfg := range []Config{
		{Enabled: true, Directory: t.TempDir()},
		{Enabled: true, Token: testToken},
	} {
		if _, err := New(cfg); err == nil {
			t.Fatalf("accepted incomplete reliability-hook config: %+v", cfg)
		}
	}
}

func TestPaymentRelayGateRequiresFreshExactBrokerEvidence(t *testing.T) {
	boundary := Boundary{WindowID: "payment.relay_before_marker",
		OperationID: "11111111-1111-4111-8111-111111111111",
		Checkpoint:  "payment_broker_ack_dropped_before_relay_marker", Action: ActionAckGate,
		Facts: map[string]string{
			"payment_id":       "11111111-1111-4111-8111-111111111111",
			"booking_id":       "22222222-2222-4222-8222-222222222222",
			"receipt_event_id": "evt_t23receipt", "envelope_id": "33333333-3333-4333-8333-333333333333",
			"topic": "payment.events", "claim_token": "44444444-4444-4444-8444-444444444444",
			"claim_fence": "7", "lease_until": time.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339Nano),
		},
	}
	proof := &PaymentRelayProof{Kind: "payment_receipt_relay",
		PaymentID: boundary.OperationID, BookingID: boundary.Facts["booking_id"],
		ReceiptEventID: boundary.Facts["receipt_event_id"], EnvelopeID: boundary.Facts["envelope_id"],
		ClaimToken: boundary.Facts["claim_token"], ClaimFence: 7, LeaseUntil: boundary.Facts["lease_until"],
		Topic: "payment.events", Partition: 0, Offset: 41, RecordAtBroker: true,
		MarkerUncommitted: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if !validPaymentRelayProof(boundary, proof) {
		t.Fatal("accepted Payment relay evidence was rejected")
	}
	wrong := *proof
	wrong.PaymentID = "44444444-4444-4444-8444-444444444444"
	if validPaymentRelayProof(boundary, &wrong) {
		t.Fatal("accepted proof for a different Payment operation")
	}
	wrong = *proof
	wrong.ReceiptEventID = "evt_another_receipt"
	if validPaymentRelayProof(boundary, &wrong) {
		t.Fatal("accepted proof for a different durable receipt")
	}
	wrong = *proof
	wrong.ObservedAt = time.Now().Add(-3 * time.Minute).UTC().Format(time.RFC3339Nano)
	if validPaymentRelayProof(boundary, &wrong) {
		t.Fatal("accepted stale broker evidence")
	}
	wrong = *proof
	wrong.MarkerUncommitted = false
	if validPaymentRelayProof(boundary, &wrong) {
		t.Fatal("accepted already committed relay marker")
	}
	wrongBoundary := boundary
	wrongBoundary.Facts = cloneFacts(boundary.Facts)
	wrongBoundary.Facts["claim_fence"] = "8"
	if validPaymentRelayProof(wrongBoundary, proof) {
		t.Fatal("accepted broker proof for a different receipt claim fence")
	}
	wrongBoundary = boundary
	wrongBoundary.Facts = cloneFacts(boundary.Facts)
	wrongBoundary.Facts["claim_token"] = "55555555-5555-4555-8555-555555555555"
	if validPaymentRelayProof(wrongBoundary, proof) {
		t.Fatal("accepted broker proof for a different receipt claim token")
	}
	wrongBoundary = boundary
	wrongBoundary.Facts = cloneFacts(boundary.Facts)
	wrongBoundary.Facts["lease_until"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	if validPaymentRelayProof(wrongBoundary, proof) {
		t.Fatal("accepted broker proof after the receipt claim lease expired")
	}
}

func TestMatchingBoundaryWaitsForTokenBoundDecision(t *testing.T) {
	h, dir := enabledHook(t)
	writeCommand(t, dir, Command{WindowID: "booking.commit_before_saga_dispatch",
		OperationID: "request-1", Checkpoint: "booking_committed_before_dispatch",
		Action: ActionPause, Token: testToken, Nonce: "nonce-1"})
	result := make(chan struct {
		decision Decision
		matched  bool
		err      error
	}, 1)
	go func() {
		decision, matched, err := h.Await(context.Background(), Boundary{
			WindowID: "booking.commit_before_saga_dispatch", OperationID: "request-1",
			Checkpoint: "booking_committed_before_dispatch", Action: ActionPause,
			Facts: map[string]string{"booking_id": "booking-1", "dispatch_state": "pending"},
		})
		result <- struct {
			decision Decision
			matched  bool
			err      error
		}{decision, matched, err}
	}()
	key := boundaryKey("booking.commit_before_saga_dispatch", "request-1", "booking_committed_before_dispatch")
	barrier := readBarrier(t, dir, key)
	if barrier.OperationID != "request-1" || barrier.Facts["dispatch_state"] != "pending" {
		t.Fatalf("barrier lost durable boundary facts: %+v", barrier)
	}
	select {
	case <-result:
		t.Fatal("matching hook returned before the control decision")
	default:
	}
	writeDecision(t, dir, key, "nonce-1", DecisionContinue, testToken, nil)
	select {
	case got := <-result:
		if got.err != nil || !got.matched || got.decision != DecisionContinue {
			t.Fatalf("unexpected gate result: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("matching hook did not resume after control decision")
	}
}

func TestKafkaFailOnceDecisionIsScopedAndConsumed(t *testing.T) {
	h, dir := enabledHook(t)
	boundary := Boundary{WindowID: "kafka.partition.A_failure_offset_frontier",
		OperationID: "11111111-1111-4111-8111-111111111111",
		Checkpoint:  "kafka_before_handler", Action: ActionPause,
		Facts: map[string]string{"group_id": "notification-booking-group",
			"topic": "booking.events.t23-0123456789abcdef01234567", "partition": "0", "offset": "0",
			"generation": "4", "event_id": "11111111-1111-4111-8111-111111111111",
			"event_type": "BookingConfirmed"}}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	writeCommand(t, dir, Command{WindowID: boundary.WindowID,
		OperationID: boundary.OperationID, Checkpoint: boundary.Checkpoint,
		Action: ActionPause, Token: testToken, Nonce: "nonce-kafka-fail-once"})
	result := make(chan struct {
		decision Decision
		matched  bool
		err      error
	}, 1)
	go func() {
		decision, matched, err := h.Await(context.Background(), boundary)
		result <- struct {
			decision Decision
			matched  bool
			err      error
		}{decision, matched, err}
	}()
	barrier := readBarrier(t, dir, key)
	if barrier.Facts["generation"] != "4" || barrier.Facts["offset"] != "0" {
		t.Fatalf("Kafka boundary lost broker-consumer facts: %+v", barrier.Facts)
	}
	writeDecision(t, dir, key, barrier.Nonce, Decision("fail_once"), testToken, nil)
	select {
	case got := <-result:
		if got.err != nil || !got.matched || string(got.decision) != "fail_once" {
			t.Fatalf("Kafka one-shot failure was not returned: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Kafka one-shot failure did not release the boundary")
	}
	decision, matched, err := h.Await(context.Background(), boundary)
	if !errors.Is(err, ErrUnresolvedClaim) || !matched || decision != DecisionContinue {
		t.Fatalf("Kafka retry bypassed before injected-failure proof: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
	if err := h.RecordKafkaInjectedFailure(KafkaInjectedFailure{
		WindowID: boundary.WindowID, GroupID: boundary.Facts["group_id"],
		Generation: 4, Topic: boundary.Facts["topic"], Partition: 0, Offset: 0,
		EventID: boundary.Facts["event_id"], EventType: boundary.Facts["event_type"],
	}); err != nil {
		t.Fatalf("record exact injected Kafka failure: %v", err)
	}
	decision, matched, err = h.Await(context.Background(), boundary)
	if err != nil || matched || decision != DecisionContinue {
		t.Fatalf("consumed Kafka fault command replayed: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
}

func TestFailOnceDecisionIsRejectedForNonKafkaBoundary(t *testing.T) {
	h, dir := enabledHook(t)
	boundary := Boundary{WindowID: "booking.commit_before_saga_dispatch",
		OperationID: "booking-1", Checkpoint: "booking_committed_before_dispatch",
		Action: ActionPause}
	key := boundaryKey(boundary.WindowID, boundary.OperationID, boundary.Checkpoint)
	writeCommand(t, dir, Command{WindowID: boundary.WindowID,
		OperationID: boundary.OperationID, Checkpoint: boundary.Checkpoint,
		Action: ActionPause, Token: testToken, Nonce: "nonce-booking-fail-once"})
	result := make(chan error, 1)
	go func() {
		_, _, err := h.Await(context.Background(), boundary)
		result <- err
	}()
	barrier := readBarrier(t, dir, key)
	writeDecision(t, dir, key, barrier.Nonce, Decision("fail_once"), testToken, nil)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("non-Kafka boundary accepted a Kafka failure injection")
		}
	case <-time.After(time.Second):
		t.Fatal("invalid fail-once decision did not release the boundary")
	}
}

func TestKafkaRecordBoundaryIsBoundToExactGroupTopicAndBrokerPosition(t *testing.T) {
	h, dir := enabledHook(t)
	eventID := "11111111-1111-4111-8111-111111111111"
	windowID := "kafka.group.notification-booking-group.restart_fence"
	checkpoint := "kafka_before_handler"
	key := boundaryKey(windowID, eventID, checkpoint)
	writeCommand(t, dir, Command{WindowID: windowID, OperationID: eventID,
		Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-kafka-group"})
	decision, matched, err := h.AwaitKafkaRecord(context.Background(),
		"saga-payment-group", "booking.events.t23-0123456789abcdef01234567",
		5, 0, 9, eventID, "booking.confirmed")
	if err != nil || matched || decision != DecisionContinue {
		t.Fatalf("wrong group consumed Kafka command: decision=%q matched=%v err=%v", decision, matched, err)
	}
	result := make(chan struct {
		decision Decision
		matched  bool
		err      error
	}, 1)
	go func() {
		decision, matched, err := h.AwaitKafkaRecord(context.Background(),
			"notification-booking-group", "booking.events.t23-0123456789abcdef01234567",
			5, 0, 9, eventID, "booking.confirmed")
		result <- struct {
			decision Decision
			matched  bool
			err      error
		}{decision, matched, err}
	}()
	barrier := readBarrier(t, dir, key)
	if barrier.Facts["group_id"] != "notification-booking-group" ||
		barrier.Facts["generation"] != "5" || barrier.Facts["topic"] != "booking.events.t23-0123456789abcdef01234567" ||
		barrier.Facts["partition"] != "0" || barrier.Facts["offset"] != "9" || barrier.Facts["event_id"] != eventID {
		t.Fatalf("Kafka boundary did not preserve exact broker/consumer facts: %+v", barrier.Facts)
	}
	writeDecision(t, dir, key, barrier.Nonce, DecisionContinue, testToken, nil)
	select {
	case got := <-result:
		if got.err != nil || !got.matched || got.decision != DecisionContinue {
			t.Fatalf("Kafka boundary did not resume on its exact decision: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Kafka boundary did not resume")
	}
}

func TestKafkaRecordBoundaryRejectsWrongTopicAndOperation(t *testing.T) {
	h, dir := enabledHook(t)
	eventID := "11111111-1111-4111-8111-111111111111"
	windowID := "kafka.partition.A_failure_offset_frontier"
	checkpoint := "kafka_before_handler"
	writeCommand(t, dir, Command{WindowID: windowID, OperationID: eventID,
		Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-kafka-partition"})
	for _, input := range []struct {
		group, topic, operation string
	}{
		{"saga-payment-group", "booking.events.t23-0123456789abcdef01234567", eventID},
		{"notification-booking-group", "booking.events", eventID},
		{"notification-booking-group", "booking.events.t23-0123456789abcdef01234567", "22222222-2222-4222-8222-222222222222"},
	} {
		decision, matched, err := h.AwaitKafkaRecord(context.Background(), input.group,
			input.topic, 3, 0, 4, input.operation, "booking.confirmed")
		if err != nil || matched || decision != DecisionContinue {
			t.Fatalf("mismatched Kafka record consumed command: %+v decision=%q matched=%v err=%v",
				input, decision, matched, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, commandFile)); err != nil {
		t.Fatalf("mismatched Kafka input consumed the private command: %v", err)
	}
}

func TestKafkaRetryBoundaryCanHoldSameRecordAfterInjectedFailure(t *testing.T) {
	h, dir := enabledHook(t)
	eventID := "11111111-1111-4111-8111-111111111111"
	windowID := "kafka.partition.A_failure_offset_frontier"
	checkpoint := "kafka_retry_before_handler"
	key := boundaryKey(windowID, eventID, checkpoint)
	writeCommand(t, dir, Command{WindowID: windowID, OperationID: eventID,
		Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-kafka-retry"})
	result := make(chan struct {
		decision Decision
		matched  bool
		err      error
	}, 1)
	go func() {
		decision, matched, err := h.AwaitKafkaRecordAtCheckpoint(context.Background(),
			"notification-booking-group", "booking.events.t23-0123456789abcdef01234567",
			5, 0, 9, eventID, "BookingCancelled", checkpoint)
		result <- struct {
			decision Decision
			matched  bool
			err      error
		}{decision, matched, err}
	}()
	barrier := readBarrier(t, dir, key)
	if barrier.Checkpoint != checkpoint || barrier.Facts["offset"] != "9" {
		t.Fatalf("Kafka retry barrier lost its exact record identity: %+v", barrier)
	}
	writeDecision(t, dir, key, barrier.Nonce, DecisionContinue, testToken, nil)
	select {
	case got := <-result:
		if got.err != nil || !got.matched || got.decision != DecisionContinue {
			t.Fatalf("Kafka retry did not resume after exact release: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Kafka retry barrier did not resume")
	}
}

func TestKafkaWrapperFailsClosedAfterClaimCancellation(t *testing.T) {
	h, dir := enabledHook(t)
	eventID := "abcdefab-cdef-4abc-8def-abcdefabcdef"
	windowID := "kafka.partition.A_failure_offset_frontier"
	checkpoint := "kafka_retry_before_handler"
	key := boundaryKey(windowID, eventID, checkpoint)
	groupID := "notification-booking-group"
	topic := "booking.events.t23-0123456789abcdef01234567"
	writeCommand(t, dir, Command{WindowID: windowID, OperationID: eventID,
		Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "kafka-cancel"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := h.AwaitKafkaRecordAtCheckpoint(ctx, groupID, topic, 5, 0, 9,
			eventID, "BookingCancelled", checkpoint)
		done <- err
	}()
	readBarrier(t, dir, key)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Kafka barrier returned %v", err)
	}
	decision, matched, err := h.AwaitKafkaRecordAtCheckpoint(context.Background(),
		groupID, topic, 5, 0, 9, eventID, "BookingCancelled", checkpoint)
	if !errors.Is(err, ErrUnresolvedClaim) || !matched || decision != DecisionContinue {
		t.Fatalf("Kafka retry bypassed unresolved claim: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
	decision, matched, err = h.AwaitKafkaRecordAtCheckpoint(context.Background(),
		groupID, topic, 0, 0, 9, eventID, "BookingCancelled", checkpoint)
	if !errors.Is(err, ErrUnresolvedClaim) || !matched || decision != DecisionContinue {
		t.Fatalf("Kafka retry with invalid metadata bypassed unresolved claim: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
	decision, matched, err = h.AwaitKafkaRecordAtCheckpoint(context.Background(),
		groupID, topic, 5, 0, 9, "ABCDEFAB-CDEF-4ABC-8DEF-ABCDEFABCDEF",
		"BookingCancelled", checkpoint)
	if !errors.Is(err, ErrUnresolvedClaim) || !matched || decision != DecisionContinue {
		t.Fatalf("Kafka retry with UUID case alias bypassed unresolved claim: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
}

func TestKafkaRebalanceCanPauseNewGenerationAtTheSameRecord(t *testing.T) {
	h, dir := enabledHook(t)
	eventID := "11111111-1111-4111-8111-111111111111"
	windowID := "kafka.partition.rebalance_old_generation_fenced"
	checkpoint := "kafka_rebalance_new_generation_before_handler"
	key := boundaryKey(windowID, eventID, checkpoint)
	writeCommand(t, dir, Command{WindowID: windowID, OperationID: eventID,
		Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-kafka-rebalance-new"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := h.AwaitKafkaRecordAtCheckpoint(ctx,
			"notification-booking-group", "booking.events.t23-0123456789abcdef01234567",
			8, 0, 9, eventID, "BookingCancelled", checkpoint)
		result <- err
	}()
	barrier := readBarrier(t, dir, key)
	if barrier.Facts["generation"] != "8" || barrier.Facts["offset"] != "9" {
		t.Fatalf("rebalance barrier lost its new-generation position: %+v", barrier.Facts)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled new-generation barrier returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("new-generation barrier did not honor cancellation")
	}
}

func TestKafkaRebalanceCommitReceiptRequiresEveryClaimedBarrierFactAndNonce(t *testing.T) {
	mutations := []struct {
		name        string
		mutate      func(*KafkaRebalanceCommitAttempt)
		changeNonce bool
	}{
		{name: "group", mutate: func(a *KafkaRebalanceCommitAttempt) { a.GroupID = "notification-user-group" }},
		{name: "generation", mutate: func(a *KafkaRebalanceCommitAttempt) { a.Generation++ }},
		{name: "topic", mutate: func(a *KafkaRebalanceCommitAttempt) { a.Topic = "booking.events.t23-1123456789abcdef01234567" }},
		{name: "partition", mutate: func(a *KafkaRebalanceCommitAttempt) { a.Partition++ }},
		{name: "offset", mutate: func(a *KafkaRebalanceCommitAttempt) { a.RecordOffset++ }},
		{name: "event_id", mutate: func(a *KafkaRebalanceCommitAttempt) { a.EventID = "22222222-2222-4222-8222-222222222222" }},
		{name: "event_type", mutate: func(a *KafkaRebalanceCommitAttempt) { a.EventType = "BookingCreated" }},
		{name: "nonce", changeNonce: true},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			h, dir := enabledHook(t)
			eventID := "11111111-1111-4111-8111-111111111111"
			windowID := "kafka.partition.rebalance_old_generation_fenced"
			checkpoint := "kafka_before_handler"
			topic := "booking.events.t23-0123456789abcdef01234567"
			key := boundaryKey(windowID, eventID, checkpoint)
			command := Command{WindowID: windowID, OperationID: eventID,
				Checkpoint: checkpoint, Action: ActionPause, Token: testToken,
				Nonce: "nonce-kafka-rebalance-old"}
			writeCommand(t, dir, command)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, _, err := h.AwaitKafkaRecord(ctx, "notification-booking-group",
					topic, 7, 0, 9, eventID, "BookingCancelled")
				result <- err
			}()
			barrier := readBarrier(t, dir, key)
			if barrier.Nonce != command.Nonce || len(barrier.Facts) != 7 {
				t.Fatalf("stored Kafka barrier identity is incomplete: %+v", barrier)
			}
			attempt := KafkaRebalanceCommitAttempt{
				WindowID: windowID, GroupID: "notification-booking-group", Generation: 7,
				Topic: topic, Partition: 0, RecordOffset: 9, CommitOffset: 10,
				EventID: eventID, EventType: "BookingCancelled", ErrorCode: "ILLEGAL_GENERATION",
			}
			wrong := attempt
			if test.changeNonce {
				claimedPath := filepath.Join(dir, ".claimed-"+key+".json")
				claimed, err := readJSONFile[Command](claimedPath)
				if err != nil {
					t.Fatal(err)
				}
				claimed.Nonce = "nonce-not-bound-to-barrier"
				claimedRaw, err := json.Marshal(claimed)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(claimedPath, claimedRaw, 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				test.mutate(&wrong)
			}
			if h.KafkaRebalanceBoundaryClaimed(wrong.GroupID, wrong.Topic,
				wrong.Generation, wrong.Partition, wrong.RecordOffset,
				wrong.EventID, wrong.EventType) {
				t.Fatal("wrong Kafka identity was accepted as the claimed barrier")
			}
			if err := h.RecordKafkaRebalanceCommitAttempt(wrong); err == nil {
				t.Fatal("commit receipt for wrong Kafka identity or nonce was accepted")
			}
			if paths, err := filepath.Glob(filepath.Join(dir, "kafka-rebalance-*.json")); err != nil || len(paths) != 0 {
				t.Fatalf("wrong Kafka identity wrote commit receipt paths=%v err=%v", paths, err)
			}
			if test.changeNonce {
				claimedRaw, err := json.Marshal(command)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, ".claimed-"+key+".json"), claimedRaw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := h.RecordKafkaRebalanceCommitAttempt(attempt); err != nil {
				t.Fatalf("exact claimed Kafka identity was rejected after negative check: %v", err)
			}
			path := filepath.Join(dir, kafkaRebalanceFile(key))
			got, err := readJSONFile[KafkaRebalanceCommitAttempt](path)
			if err != nil || got.ErrorCode != "ILLEGAL_GENERATION" || got.Generation != 7 ||
				got.RecordOffset != 9 || got.CommitOffset != 10 || got.ObservedAt == "" {
				t.Fatalf("stale commit receipt is incomplete: %+v err=%v", got, err)
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled old-generation barrier returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("old-generation barrier did not stop after cancellation")
			}
		})
	}
}

func TestNotificationDeliveryBarrierBindsTheClaimAndContainsNoRecipientData(t *testing.T) {
	h, dir := enabledHook(t)
	notificationID := "11111111-1111-4111-8111-111111111111"
	windowID := "notification.intent_before_sender"
	checkpoint := "notification_before_sender"
	key := boundaryKey(windowID, notificationID, checkpoint)
	facts := map[string]string{
		"notification_id": notificationID, "delivery_id": notificationID,
		"notification_type": "booking_cancellation",
		"entity_id":         "22222222-2222-4222-8222-222222222222",
		"prior_status":      "PENDING", "delivery_token": "33333333-3333-4333-8333-333333333333",
		"delivery_fence": "4", "delivery_attempts": "2",
		"delivery_lease_until": "2026-10-16T12:00:30Z",
	}
	writeCommand(t, dir, Command{WindowID: windowID, OperationID: notificationID,
		Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-notification"})
	result := make(chan struct {
		decision Decision
		matched  bool
		err      error
	}, 1)
	go func() {
		decision, matched, err := h.AwaitNotificationDelivery(
			context.Background(), windowID, notificationID, checkpoint, facts)
		result <- struct {
			decision Decision
			matched  bool
			err      error
		}{decision, matched, err}
	}()
	barrier := readBarrier(t, dir, key)
	if barrier.Facts["notification_id"] != notificationID ||
		barrier.Facts["delivery_fence"] != "4" ||
		barrier.Facts["delivery_lease_until"] != facts["delivery_lease_until"] {
		t.Fatalf("Notification barrier lost its exact claimed delivery: %+v", barrier.Facts)
	}
	if _, exists := barrier.Facts["recipient"]; exists {
		t.Fatal("Notification barrier exposed recipient data")
	}
	writeDecision(t, dir, key, barrier.Nonce, DecisionContinue, testToken, nil)
	got := <-result
	if got.err != nil || !got.matched || got.decision != DecisionContinue {
		t.Fatalf("Notification worker did not resume after exact release: decision=%q matched=%v err=%v", got.decision, got.matched, got.err)
	}
}

func TestNotificationWrapperFailsClosedAfterClaimCancellation(t *testing.T) {
	h, dir := enabledHook(t)
	notificationID := "abcdefab-cdef-4abc-8def-abcdefabcdef"
	windowID := "notification.intent_before_sender"
	checkpoint := "notification_before_sender"
	key := boundaryKey(windowID, notificationID, checkpoint)
	facts := map[string]string{
		"notification_id": notificationID, "delivery_id": notificationID,
		"notification_type": "booking_cancellation",
		"entity_id":         "22222222-2222-4222-8222-222222222222",
		"prior_status":      "PENDING", "delivery_token": "33333333-3333-4333-8333-333333333333",
		"delivery_fence": "4", "delivery_attempts": "2",
		"delivery_lease_until": "2026-10-16T12:00:30Z",
	}
	writeCommand(t, dir, Command{WindowID: windowID, OperationID: notificationID,
		Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "notification-cancel"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := h.AwaitNotificationDelivery(ctx, windowID, notificationID, checkpoint, facts)
		done <- err
	}()
	readBarrier(t, dir, key)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Notification barrier returned %v", err)
	}
	for _, retryFacts := range []map[string]string{
		facts,
		func() map[string]string {
			changed := cloneFacts(facts)
			changed["delivery_fence"] = "5"
			return changed
		}(),
	} {
		decision, matched, err := h.AwaitNotificationDelivery(
			context.Background(), windowID, notificationID, checkpoint, retryFacts)
		if !errors.Is(err, ErrUnresolvedClaim) || !matched || decision != DecisionContinue {
			t.Fatalf("Notification retry bypassed unresolved claim: decision=%q matched=%v err=%v",
				decision, matched, err)
		}
	}
	invalidFacts := cloneFacts(facts)
	delete(invalidFacts, "delivery_fence")
	decision, matched, err := h.AwaitNotificationDelivery(
		context.Background(), windowID, notificationID, checkpoint, invalidFacts)
	if !errors.Is(err, ErrUnresolvedClaim) || !matched || decision != DecisionContinue {
		t.Fatalf("Notification retry with invalid facts bypassed unresolved claim: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
	decision, matched, err = h.AwaitNotificationDelivery(
		context.Background(), windowID, "ABCDEFAB-CDEF-4ABC-8DEF-ABCDEFABCDEF",
		checkpoint, facts)
	if !errors.Is(err, ErrUnresolvedClaim) || !matched || decision != DecisionContinue {
		t.Fatalf("Notification retry with UUID case alias bypassed unresolved claim: decision=%q matched=%v err=%v",
			decision, matched, err)
	}
}

func TestSameFactsRequiresIdenticalKeySet(t *testing.T) {
	if sameFacts(map[string]string{"a": ""}, map[string]string{"b": ""}) {
		t.Fatal("facts with different key sets were considered identical")
	}
	if !sameFacts(map[string]string{"a": "", "b": "2"},
		map[string]string{"b": "2", "a": ""}) {
		t.Fatal("facts with identical keys and values were considered different")
	}
}

func TestNotificationDeliveryBarrierRejectsWrongIdentityAndWindow(t *testing.T) {
	h, dir := enabledHook(t)
	id := "11111111-1111-4111-8111-111111111111"
	facts := map[string]string{
		"notification_id": id, "delivery_id": id,
		"notification_type": "booking_cancellation",
		"entity_id":         "22222222-2222-4222-8222-222222222222",
		"prior_status":      "PENDING", "delivery_token": "33333333-3333-4333-8333-333333333333",
		"delivery_fence": "4", "delivery_attempts": "2",
		"delivery_lease_until": "2026-10-16T12:00:30Z",
	}
	windowID, checkpoint := "notification.intent_before_sender", "notification_before_sender"
	key := boundaryKey(windowID, id, checkpoint)
	writeCommand(t, dir, Command{WindowID: windowID, OperationID: id,
		Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-notification-invalid"})
	for _, input := range []struct {
		windowID, operationID string
		facts                 map[string]string
	}{
		{windowID: "notification.sender_effect_before_marker", operationID: id, facts: facts},
		{windowID: windowID, operationID: "44444444-4444-4444-8444-444444444444", facts: facts},
		{windowID: windowID, operationID: id, facts: map[string]string{
			"notification_id": id, "delivery_id": id, "notification_type": "booking_cancellation",
			"entity_id": facts["entity_id"], "prior_status": "SENT",
			"delivery_token": facts["delivery_token"], "delivery_fence": "4",
			"delivery_attempts": "2", "delivery_lease_until": facts["delivery_lease_until"],
		}},
	} {
		_, matched, err := h.AwaitNotificationDelivery(
			context.Background(), input.windowID, input.operationID, checkpoint, input.facts)
		if err != nil || matched {
			t.Fatalf("unrelated Notification command matched: matched=%v err=%v", matched, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, barrierFile(key))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected Notification command left a barrier: %v", err)
	}
}

func TestNotificationUnknownRetryFailOnceRequiresDurableExactProof(t *testing.T) {
	for _, tc := range []struct {
		name   string
		proof  *NotificationFaultProof
		wantOK bool
	}{
		{name: "complete", wantOK: true,
			proof: &NotificationFaultProof{Kind: "UNKNOWN_EXTERNAL_OUTCOME",
				OperationID: "11111111-1111-4111-8111-111111111111", Observed: true,
				SenderAttemptObserved: true, ResultUnknown: true, UnknownStatePersisted: true}},
		{name: "missing durable state", wantOK: false,
			proof: &NotificationFaultProof{Kind: "UNKNOWN_EXTERNAL_OUTCOME",
				OperationID: "11111111-1111-4111-8111-111111111111", Observed: true,
				SenderAttemptObserved: true, ResultUnknown: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, dir := enabledHook(t)
			id := "11111111-1111-4111-8111-111111111111"
			windowID, checkpoint := "notification.unknown_retry_attention",
				"notification_unknown_before_retry"
			facts := map[string]string{
				"notification_id": id, "delivery_id": id,
				"notification_type": "booking_cancellation",
				"entity_id":         "22222222-2222-4222-8222-222222222222",
				"prior_status":      "UNKNOWN",
				"delivery_token":    "33333333-3333-4333-8333-333333333333",
				"delivery_fence":    "4", "delivery_attempts": "2",
				"delivery_lease_until": time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
			}
			key := boundaryKey(windowID, id, checkpoint)
			writeCommand(t, dir, Command{WindowID: windowID, OperationID: id,
				Checkpoint: checkpoint, Action: ActionPause, Token: testToken, Nonce: "nonce-unknown-proof"})
			result := make(chan struct {
				decision Decision
				matched  bool
				err      error
			}, 1)
			go func() {
				decision, matched, err := h.AwaitNotificationDelivery(
					context.Background(), windowID, id, checkpoint, facts)
				result <- struct {
					decision Decision
					matched  bool
					err      error
				}{decision, matched, err}
			}()
			barrier := readBarrier(t, dir, key)
			raw, err := json.Marshal(ControlDecision{Token: testToken, Nonce: barrier.Nonce,
				Decision: DecisionFailOnce, NotificationProof: tc.proof})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, decisionFile(key)), raw, 0o600); err != nil {
				t.Fatal(err)
			}
			got := <-result
			if tc.wantOK {
				if got.err != nil || !got.matched || got.decision != DecisionFailOnce {
					t.Fatalf("complete Notification proof was rejected: %+v", got)
				}
			} else if got.err == nil || !got.matched || got.decision != DecisionContinue {
				t.Fatalf("incomplete Notification proof was accepted: %+v", got)
			}
		})
	}
}

func TestKafkaInjectedFailureWritesExactSingleUseEvidence(t *testing.T) {
	h, dir := enabledHook(t)
	input := KafkaInjectedFailure{
		WindowID:   "kafka.partition.A_failure_offset_frontier",
		GroupID:    "notification-booking-group",
		Generation: 5,
		Topic:      "booking.events.t23-0123456789abcdef01234567",
		Partition:  0,
		Offset:     9,
		EventID:    "11111111-1111-4111-8111-111111111111",
		EventType:  "BookingCancelled",
	}
	if err := h.RecordKafkaInjectedFailure(input); err != nil {
		t.Fatalf("record injected failure: %v", err)
	}
	key := boundaryKey(input.WindowID, input.EventID, "kafka_before_handler")
	path := filepath.Join(dir, kafkaFailureFile(key))
	proof, err := readJSONFile[KafkaInjectedFailure](path)
	if err != nil {
		t.Fatal(err)
	}
	input.ObservedAt = proof.ObservedAt
	if proof != input || proof.ObservedAt == "" {
		t.Fatalf("failure evidence differs from the exact record boundary: %+v", proof)
	}
	if err := h.RecordKafkaInjectedFailure(input); err == nil {
		t.Fatal("duplicate injected-failure evidence overwrote the first proof")
	}
}

func TestKafkaInjectedFailureRejectsWrongDomainIdentity(t *testing.T) {
	h, dir := enabledHook(t)
	input := KafkaInjectedFailure{
		WindowID:   "kafka.partition.A_failure_offset_frontier",
		GroupID:    "saga-payment-group",
		Generation: 5,
		Topic:      "booking.events.t23-0123456789abcdef01234567",
		Partition:  0,
		Offset:     9,
		EventID:    "11111111-1111-4111-8111-111111111111",
		EventType:  "BookingCancelled",
	}
	if err := h.RecordKafkaInjectedFailure(input); err == nil {
		t.Fatal("wrong Kafka group was accepted for the A-frontier failure proof")
	}
	if matches, err := filepath.Glob(filepath.Join(dir, "kafka-failure-*.json")); err != nil || len(matches) != 0 {
		t.Fatalf("invalid failure proof wrote evidence: files=%v err=%v", matches, err)
	}
}

func TestWrongTokenAndUnrelatedBoundaryPassThrough(t *testing.T) {
	h, dir := enabledHook(t)
	writeCommand(t, dir, Command{WindowID: "booking.saga_ack_lost",
		OperationID: "booking-1", Checkpoint: sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName,
		Action: ActionAckGate, Token: "wrong-token", Nonce: "nonce-2"})
	for _, boundary := range []Boundary{
		{WindowID: "booking.saga_ack_lost", OperationID: "booking-1",
			Checkpoint: sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName, Action: ActionAckGate},
		{WindowID: "booking.saga_ack_lost", OperationID: "booking-2",
			Checkpoint: sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName, Action: ActionAckGate},
	} {
		decision, matched, err := h.Await(context.Background(), boundary)
		if err != nil || matched || decision != DecisionContinue {
			t.Fatalf("unmatched boundary was not passed through: %+v %v", boundary, err)
		}
	}
}

func TestAckInterceptorOnlyWithholdsSuccessfulExactTargetRPC(t *testing.T) {
	h, dir := enabledHook(t)
	bookingID := "bfb6bbd9-f98d-4ed3-9523-f2f32ca4c3a7"
	method := sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName
	key := boundaryKey("booking.saga_ack_lost", bookingID, method)
	writeCommand(t, dir, Command{WindowID: "booking.saga_ack_lost", OperationID: bookingID,
		Checkpoint: method, Action: ActionAckGate, Token: testToken, Nonce: "nonce-3"})
	decision := make(chan error, 1)
	go func() {
		err := h.UnaryClientInterceptor()(context.Background(), sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName, &sagav1.StartOrderSagaRequest{
			BookingId: bookingID,
		}, new(sagav1.OrderSagaResponse), nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return nil
		})
		decision <- err
	}()
	readBarrier(t, dir, key)
	select {
	case got := <-decision:
		t.Fatalf("interceptor passed the external durable-observer gate: %v", got)
	default:
	}
	writeDecision(t, dir, key, "nonce-3", DecisionWithhold, testToken, &AckProof{
		Kind: "saga_start", BookingID: bookingID,
		SagaID: "4de95e80-7817-4a85-9ab2-cfd73ea616a1", Durable: true,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err := <-decision; status.Code(err) != codes.Unavailable {
		t.Fatalf("successful upstream ACK was not withheld: %v", err)
	}
}

func TestAckInterceptorPassesThroughServerErrorsAndOtherMethods(t *testing.T) {
	h, dir := enabledHook(t)
	bookingID := "bfb6bbd9-f98d-4ed3-9523-f2f32ca4c3a7"
	method := sagav1.SagaOrchestratorService_StartOrderSaga_FullMethodName
	writeCommand(t, dir, Command{WindowID: "booking.saga_ack_lost", OperationID: bookingID,
		Checkpoint: method, Action: ActionAckGate, Token: testToken, Nonce: "nonce-4"})
	serverErr := status.Error(codes.Unavailable, "upstream unavailable")
	err := h.UnaryClientInterceptor()(context.Background(), method, &sagav1.StartOrderSagaRequest{
		BookingId: bookingID,
	}, new(sagav1.OrderSagaResponse), nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return serverErr
	})
	if err != serverErr {
		t.Fatalf("server error was not preserved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, barrierFile(boundaryKey("booking.saga_ack_lost", bookingID, method)))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("server error falsely emitted an ACK-loss barrier: %v", err)
	}
	otherMethod := sagav1.SagaOrchestratorService_ExpireOrderSaga_FullMethodName
	err = h.UnaryClientInterceptor()(context.Background(), otherMethod, &sagav1.ExpireOrderSagaRequest{
		BookingId: bookingID,
	}, new(sagav1.ExpireOrderSagaResponse), nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return nil
	})
	if err != nil || otherMethod == method {
		t.Fatalf("unrelated RPC was altered: %v", err)
	}
}
