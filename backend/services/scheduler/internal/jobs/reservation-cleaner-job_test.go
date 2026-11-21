package cronjob

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	sagav1 "github.com/ticketbox/pkg/proto/saga/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// A shared Booking authority barrier models two RPC callers with the same stale
// snapshot. Only its winning CAS can authorize a terminal settlement wake.
type expiryAuthority struct {
	mu        sync.Mutex
	bookingID string
	confirmed bool
	decisions int
	entered   chan struct{}
	release   chan struct{}
	decision  string
}

func (a *expiryAuthority) ListExpiryCandidates(context.Context, *bookingv1.ListExpiryCandidatesRequest, ...grpc.CallOption) (*bookingv1.ListExpiryCandidatesResponse, error) {
	return &bookingv1.ListExpiryCandidatesResponse{Bookings: []*bookingv1.LifecycleDisposition{{Booking: &bookingv1.BookingDetail{Id: a.bookingID, Status: "PENDING"}, Generation: 1, Version: 1, Phase: "OPEN"}}}, nil
}
func (a *expiryAuthority) DecideLifecycle(ctx context.Context, req *bookingv1.LifecycleDecisionRequest, _ ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	if req.Kind != "expire" || req.Generation != 1 || req.ExpectedVersion != 1 {
		return nil, errors.New("invalid expiry decision")
	}
	if a.entered != nil {
		a.entered <- struct{}{}
		select {
		case <-a.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.confirmed {
		return nil, errors.New("confirmation won")
	}
	if a.decision == "" {
		a.decision = req.DecisionId
		a.decisions++
	}
	return &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: a.bookingID, Status: "EXPIRED"}, Generation: 1, Phase: "TERMINAL", Version: 2, DecisionId: a.decision}, nil
}
func (a *expiryAuthority) GetLifecycleDisposition(context.Context, *bookingv1.LifecycleRequest, ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.confirmed {
		return &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: a.bookingID, Status: "CONFIRMED"}, Generation: 1, Phase: "TERMINAL"}, nil
	}
	return nil, errors.New("no terminal evidence")
}

type settlementRecorder struct {
	mu        sync.Mutex
	decisions []string
}

func (s *settlementRecorder) SettleOrderSaga(_ context.Context, r *sagav1.SettleOrderSagaRequest, _ ...grpc.CallOption) (*sagav1.OrderSagaResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decisions = append(s.decisions, r.DecisionId)
	return &sagav1.OrderSagaResponse{}, nil
}

func TestReconcileBookingRequiresAuthoritativeExpiryAndRetainsSettlement(t *testing.T) {
	a := &expiryAuthority{bookingID: uuid.NewString(), entered: make(chan struct{}, 2), release: make(chan struct{})}
	s := &settlementRecorder{}
	done := make(chan error, 2)
	for range 2 {
		job := NewReservationCleanerJob(nil, a, nil, s, nil, zap.NewNop())
		go func() { done <- job.Run(context.Background()) }()
	}
	for range 2 {
		select {
		case <-a.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("expiry RPC barrier not reached")
		}
	}
	close(a.release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if a.decisions != 1 || len(s.decisions) != 2 || s.decisions[0] != s.decisions[1] {
		t.Fatal("duplicate workers created different terminal responsibility", a.decisions, s.decisions)
	}
}
func TestReconcileBookingDoesNotReleaseWhenConfirmationWinsRace(t *testing.T) {
	a := &expiryAuthority{bookingID: uuid.NewString(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	s := &settlementRecorder{}
	done := make(chan error, 1)
	go func() { done <- NewReservationCleanerJob(nil, a, nil, s, nil, zap.NewNop()).Run(context.Background()) }()
	select {
	case <-a.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("expiry after-discovery barrier missing")
	}
	a.mu.Lock()
	a.confirmed = true
	a.mu.Unlock()
	close(a.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if a.decisions != 0 || len(s.decisions) != 0 {
		t.Fatal("losing expiry changed confirmed lifecycle")
	}
}

type fairAuthority struct {
	ids       []string
	attempted []string
}

func (a *fairAuthority) ListExpiryCandidates(_ context.Context, r *bookingv1.ListExpiryCandidatesRequest, _ ...grpc.CallOption) (*bookingv1.ListExpiryCandidatesResponse, error) {
	result := &bookingv1.ListExpiryCandidatesResponse{}
	for _, id := range a.ids {
		if id > r.AfterId {
			result.Bookings = append(result.Bookings, &bookingv1.LifecycleDisposition{Booking: &bookingv1.BookingDetail{Id: id}, Generation: 1, Version: 1, Phase: "OPEN"})
			if len(result.Bookings) == int(r.Limit) {
				break
			}
		}
	}
	return result, nil
}
func (a *fairAuthority) DecideLifecycle(_ context.Context, r *bookingv1.LifecycleDecisionRequest, _ ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	a.attempted = append(a.attempted, r.BookingId)
	return nil, errors.New("persistent candidate failure")
}
func (a *fairAuthority) GetLifecycleDisposition(context.Context, *bookingv1.LifecycleRequest, ...grpc.CallOption) (*bookingv1.LifecycleDisposition, error) {
	return nil, errors.New("still pending")
}
func TestExpiryCursorReachesCandidatesBehindPersistentFailures(t *testing.T) {
	a := &fairAuthority{}
	for i := 1; i <= 70; i++ {
		a.ids = append(a.ids, fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
	}
	job := NewReservationCleanerJob(nil, a, nil, nil, nil, zap.NewNop())
	for range 2 {
		_ = job.Run(context.Background())
	}
	found := false
	for _, id := range a.attempted {
		if id == a.ids[69] {
			found = true
		}
	}
	if !found {
		t.Fatal("persistent first pages starved later expiry candidates")
	}
}
