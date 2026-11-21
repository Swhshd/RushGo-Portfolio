package repository

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagementFlagsAndMetricsIntegration(t *testing.T) {
	pool := operationsFixture(t)
	ctx := context.Background()
	repo := NewPostgresBookingRepository(pool)
	flags, audit, err := repo.ListFeatureFlags(ctx)
	if err != nil || len(flags) != 1 || flags[0].Enabled || flags[0].Default || len(audit) != 0 {
		t.Fatalf("unsafe defaults %+v %v", flags, err)
	}
	if _, err = repo.ChangeFeatureFlag(ctx, "disable_payment_checks", true, 0, uuid.New()); !errors.Is(err, domain.ErrUnknownFeature) {
		t.Fatal("financial flag accepted")
	}
	var successes, conflicts atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := repo.ChangeFeatureFlag(ctx, "discovery_advanced_filters", true, 0, uuid.New())
			if err == nil {
				successes.Add(1)
			} else if errors.Is(err, domain.ErrFeatureConflict) {
				conflicts.Add(1)
			} else {
				t.Errorf("feature mutation %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if successes.Load() != 1 || conflicts.Load() != 1 {
		t.Fatal("stale concurrent change accepted")
	}
	if _, err = repo.ChangeFeatureFlag(ctx, "discovery_advanced_filters", true, 1, uuid.New()); err != nil {
		t.Fatal(err)
	}
	flags, audit, err = NewPostgresBookingRepository(pool).ListFeatureFlags(ctx)
	if err != nil || len(flags) != 1 || flags[0].Version != 1 || !flags[0].Enabled || len(audit) != 1 || audit[0].Before || !audit[0].After || audit[0].ActorID == uuid.Nil {
		t.Fatalf("durable flag/audit mismatch %+v %+v %v", flags, audit, err)
	}
	seedAdminBookingDetail(t, pool, true)
	v, err := repo.BookingMetrics(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil || v.Orders != 1 || v.IssuedTickets != 1 || v.CheckedInTickets != 0 || v.RefundRequests != 1 || v.ConfirmedRefunds != 0 {
		t.Fatalf("metrics %+v %v", v, err)
	}
	if _, err = repo.BookingMetrics(ctx, time.Now().Add(-32*24*time.Hour), time.Now()); !errors.Is(err, domain.ErrManagementBounds) {
		t.Fatal("unbounded window accepted")
	}
}
