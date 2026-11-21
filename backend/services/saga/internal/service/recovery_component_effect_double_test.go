//go:build reliability_component

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc"
)

// componentEffectLedger is an in-memory idempotent test double shared by the
// parent and owned OS worker processes. It is keyed by the production business
// operation id and action; it never contacts an external Provider.
type componentEffectLedger struct {
	listener net.Listener
	mu       sync.Mutex
	entries  map[string]*componentEffectLedgerEntry
}

type componentEffectLedgerEntry struct {
	Attempts        int
	Applications    int
	ProcessStartIDs map[string]bool
	ProcessPIDs     map[int]int
}

type componentEffectSnapshot struct {
	Attempts        int
	Applications    int
	ProcessStartIDs map[string]bool
	ProcessPIDs     map[int]int
}

type componentEffectRequest struct {
	Action      string `json:"action"`
	OperationID string `json:"operation_id"`
	WorkerID    string `json:"worker_id"`
	StartID     string `json:"start_id"`
	PID         int    `json:"pid"`
}

type componentEffectAcknowledgement struct {
	Applied bool `json:"applied"`
	Valid   bool `json:"valid"`
}

func newComponentEffectLedger(t *testing.T) *componentEffectLedger {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ledger := &componentEffectLedger{listener: listener, entries: make(map[string]*componentEffectLedgerEntry)}
	go ledger.acceptLoop()
	t.Cleanup(func() { _ = listener.Close() })
	return ledger
}

func (l *componentEffectLedger) Address() string { return l.listener.Addr().String() }

func (l *componentEffectLedger) acceptLoop() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			return
		}
		go l.handle(conn)
	}
}

func (l *componentEffectLedger) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	var request componentEffectRequest
	if err := json.NewDecoder(conn).Decode(&request); err != nil || request.Action == "" || request.OperationID == "" || request.WorkerID == "" || request.StartID == "" || request.PID < 1 {
		_ = json.NewEncoder(conn).Encode(componentEffectAcknowledgement{})
		return
	}
	key := componentEffectCommandKey(request.Action, request.OperationID)
	l.mu.Lock()
	entry := l.entries[key]
	if entry == nil {
		entry = &componentEffectLedgerEntry{ProcessStartIDs: make(map[string]bool), ProcessPIDs: make(map[int]int)}
		l.entries[key] = entry
	}
	entry.Attempts++
	firstApplication := entry.Applications == 0
	if firstApplication {
		entry.Applications++
	}
	entry.ProcessStartIDs[request.StartID] = true
	entry.ProcessPIDs[request.PID]++
	l.mu.Unlock()
	_ = json.NewEncoder(conn).Encode(componentEffectAcknowledgement{Applied: firstApplication, Valid: true})
}

func (l *componentEffectLedger) Snapshot(action, operationID string) componentEffectSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := l.entries[componentEffectCommandKey(action, operationID)]
	if entry == nil {
		return componentEffectSnapshot{ProcessStartIDs: map[string]bool{}, ProcessPIDs: map[int]int{}}
	}
	result := componentEffectSnapshot{
		Attempts: entry.Attempts, Applications: entry.Applications,
		ProcessStartIDs: make(map[string]bool, len(entry.ProcessStartIDs)),
		ProcessPIDs:     make(map[int]int, len(entry.ProcessPIDs)),
	}
	for startID, seen := range entry.ProcessStartIDs {
		result.ProcessStartIDs[startID] = seen
	}
	for pid, count := range entry.ProcessPIDs {
		result.ProcessPIDs[pid] = count
	}
	return result
}

func componentEffectCommandKey(action, operationID string) string {
	return action + ":" + operationID
}

func recordComponentEffect(ctx context.Context, address string, request componentEffectRequest) (bool, error) {
	dialer := net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return false, fmt.Errorf("component effect ledger unavailable: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(time.Second))
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return false, fmt.Errorf("component effect ledger request failed: %w", err)
	}
	var acknowledgement componentEffectAcknowledgement
	if err := json.NewDecoder(conn).Decode(&acknowledgement); err != nil {
		return false, fmt.Errorf("component effect ledger acknowledgement failed: %w", err)
	}
	if !acknowledgement.Valid {
		return false, fmt.Errorf("component effect ledger rejected its local test command")
	}
	return acknowledgement.Applied, nil
}

type componentEventEffectDouble struct {
	eventv1.EventServiceClient
	ledgerAddress string
	workerID      string
	startID       string
}

func (e *componentEventEffectDouble) ApplySeatLifecycle(ctx context.Context, request *eventv1.SeatLifecycleRequest, options ...grpc.CallOption) (*eventv1.SeatLifecycleResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("component effect request is nil")
	}
	applied, err := recordComponentEffect(ctx, e.ledgerAddress, componentEffectRequest{
		Action: request.Action, OperationID: request.OperationId, WorkerID: e.workerID, StartID: e.startID, PID: os.Getpid(),
	})
	if err != nil {
		return nil, err
	}
	if !applied {
		return componentSeatLifecycleSuccess(request.Action)
	}
	return e.EventServiceClient.ApplySeatLifecycle(ctx, request, options...)
}

func (e *componentEventEffectDouble) ApplySaleAccounting(ctx context.Context, request *eventv1.SeatLifecycleRequest, options ...grpc.CallOption) (*eventv1.SeatLifecycleResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("component accounting effect request is nil")
	}
	applied, err := recordComponentEffect(ctx, e.ledgerAddress, componentEffectRequest{
		Action: request.Action, OperationID: request.OperationId, WorkerID: e.workerID, StartID: e.startID, PID: os.Getpid(),
	})
	if err != nil {
		return nil, err
	}
	if !applied {
		return &eventv1.SeatLifecycleResponse{State: "booked", AccountingState: "applied"}, nil
	}
	return e.EventServiceClient.ApplySaleAccounting(ctx, request, options...)
}

type componentPaymentResolutionEffectDouble struct {
	paymentv1.PaymentServiceClient
	ledgerAddress string
	workerID      string
	startID       string
}

func (p *componentPaymentResolutionEffectDouble) RequestPaymentResolution(ctx context.Context, request *paymentv1.PaymentResolutionRequest, options ...grpc.CallOption) (*paymentv1.PaymentEntry, error) {
	if request == nil || request.PaymentId == "" || request.DecisionId == "" {
		return nil, fmt.Errorf("component payment-resolution effect request is incomplete")
	}
	if _, err := recordComponentEffect(ctx, p.ledgerAddress, componentEffectRequest{
		Action: "payment_resolution", OperationID: request.PaymentId + ":" + request.DecisionId,
		WorkerID: p.workerID, StartID: p.startID, PID: os.Getpid(),
	}); err != nil {
		return nil, err
	}
	return p.PaymentServiceClient.RequestPaymentResolution(ctx, request, options...)
}

func componentSeatLifecycleSuccess(action string) (*eventv1.SeatLifecycleResponse, error) {
	switch action {
	case "reserve":
		return &eventv1.SeatLifecycleResponse{State: "held"}, nil
	case "book":
		return &eventv1.SeatLifecycleResponse{State: "booked"}, nil
	case "close":
		return &eventv1.SeatLifecycleResponse{State: "closed"}, nil
	default:
		return nil, fmt.Errorf("unexpected component seat action %q", action)
	}
}
