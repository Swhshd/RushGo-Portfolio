package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/repository"
)

type adminEventTicketOperationsTestReader struct {
	snapshot repository.AdminEventTicketOperationsSnapshot
	err      error
}

func (r *adminEventTicketOperationsTestReader) ReadAdminEventTicketOperations(context.Context, uuid.UUID) (repository.AdminEventTicketOperationsSnapshot, error) {
	return r.snapshot, r.err
}

func TestAdminEventTicketOperationsCountsHistoricalAuditsSeparatelyAndLimitsRecentTimes(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	snapshot := validAdminEventTicketOperationsSnapshot(eventID)
	service := NewAdminEventTicketOperationsService(&adminEventTicketOperationsTestReader{snapshot: snapshot})
	result, err := service.Get(context.Background(), eventID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if result.EventID != eventID.String() || result.TotalRecordedCount != 8 || result.IssuedCount != 1 || result.CheckedInCount != 2 || result.RevokedCount != 5 || result.SuccessfulCheckinCount != 6 {
		t.Fatalf("recorded and successful-audit counts = %#v", result)
	}
	if result.SourceRecordLimit != 5000 || len(result.RecentCheckins) != 5 {
		t.Fatalf("record limit/recent summary count = %d/%d, want 5000/5", result.SourceRecordLimit, len(result.RecentCheckins))
	}
	if !result.RecentCheckins[0].Equal(snapshot.Audits[0].CheckedInAt) || !result.RecentCheckins[4].Equal(snapshot.Audits[4].CheckedInAt) {
		t.Fatalf("recent successful audit times are not newest-first: %#v", result.RecentCheckins)
	}
}

func TestAdminEventTicketOperationsObservationCanFollowVisibleConcurrentCheckIn(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	snapshot := validAdminEventTicketOperationsSnapshot(eventID)
	transactionStartedAt := snapshot.ObservedAt
	snapshot.ObservedAt = transactionStartedAt.Add(2 * time.Second)
	checkInCommittedAfterBegin := transactionStartedAt.Add(time.Second)
	snapshot.Audits[0].CheckedInAt = checkInCommittedAfterBegin
	for index := range snapshot.Tickets {
		if snapshot.Tickets[index].TicketID == snapshot.Audits[0].TicketID {
			snapshot.Tickets[index].CheckedInAt = ptrTime(checkInCommittedAfterBegin)
		}
	}
	service := NewAdminEventTicketOperationsService(&adminEventTicketOperationsTestReader{snapshot: snapshot})
	if _, err := service.Get(context.Background(), eventID); err != nil {
		t.Fatalf("snapshot observation after visible check-in commit returned error: %v", err)
	}
}

func TestAdminEventTicketOperationsFailsClosedOnUnknownOrInconsistentSnapshot(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	cases := []struct {
		name   string
		mutate func(*repository.AdminEventTicketOperationsSnapshot)
	}{
		{name: "unknown status", mutate: func(s *repository.AdminEventTicketOperationsSnapshot) { s.Tickets[0].Status = "CHECK_IN_PENDING" }},
		{name: "checked in without audit", mutate: func(s *repository.AdminEventTicketOperationsSnapshot) { s.Audits = s.Audits[1:] }},
		{name: "issued ticket with audit", mutate: func(s *repository.AdminEventTicketOperationsSnapshot) {
			s.Audits = append(s.Audits, repository.AdminEventTicketOperationsAudit{TicketID: s.Tickets[0].TicketID, EventID: eventID, ResultCode: "accepted", CheckedInAt: s.ObservedAt.Add(-time.Minute)})
		}},
		{name: "audit belongs to another event", mutate: func(s *repository.AdminEventTicketOperationsSnapshot) {
			s.Audits[0].EventID = uuid.MustParse("60000000-0000-4000-8000-000000000002")
		}},
		{name: "audit timestamp conflicts with ticket", mutate: func(s *repository.AdminEventTicketOperationsSnapshot) {
			s.Audits[0].CheckedInAt = s.Audits[0].CheckedInAt.Add(time.Second)
		}},
		{name: "revoked historical timestamp without audit", mutate: func(s *repository.AdminEventTicketOperationsSnapshot) { s.Audits = s.Audits[:5] }},
		{name: "missing revoked timestamp", mutate: func(s *repository.AdminEventTicketOperationsSnapshot) { s.Tickets[3].RevokedAt = nil }},
		{name: "nil ticket identity", mutate: func(s *repository.AdminEventTicketOperationsSnapshot) { s.Tickets[0].TicketID = uuid.Nil }},
		{name: "audit newer than observation", mutate: func(s *repository.AdminEventTicketOperationsSnapshot) {
			s.Audits[0].CheckedInAt = s.ObservedAt.Add(time.Second)
			s.Tickets[1].CheckedInAt = ptrTime(s.ObservedAt.Add(time.Second))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := validAdminEventTicketOperationsSnapshot(eventID)
			tc.mutate(&snapshot)
			service := NewAdminEventTicketOperationsService(&adminEventTicketOperationsTestReader{snapshot: snapshot})
			if _, err := service.Get(context.Background(), eventID); !errors.Is(err, ErrAdminEventTicketOperationsInvalidSnapshot) {
				t.Fatalf("Get() error = %v, want invalid snapshot", err)
			}
		})
	}
}

func TestAdminEventTicketOperationsProjectsConfirmedTicketDeliveryExceptionsWithoutPaymentClaims(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	snapshot := validAdminEventTicketOperationsSnapshot(eventID)
	bookingID := uuid.MustParse("62000000-0000-4000-8000-000000000001")
	snapshot.DeliveryExceptions = []repository.AdminEventAfterSalesException{{
		BookingID: bookingID, State: AdminEventAfterSalesDeliveryPending, BookingStatus: "CONFIRMED",
		ExpectedTicketCount: 2, RecordedTicketCount: 0, IssuanceReceiptOutcome: "NOT_RECORDED",
		BookingCreatedAt: snapshot.ObservedAt.Add(-time.Hour),
	}}
	service := NewAdminEventTicketOperationsService(&adminEventTicketOperationsTestReader{snapshot: snapshot})
	result, err := service.Get(context.Background(), eventID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if len(result.DeliveryExceptions) != 1 {
		t.Fatalf("delivery exceptions = %d, want 1", len(result.DeliveryExceptions))
	}
	exception := result.DeliveryExceptions[0]
	if exception.BookingID != bookingID.String() || exception.State != AdminEventAfterSalesDeliveryPending || exception.BookingStatus != "CONFIRMED" ||
		exception.ExpectedTicketCount != 2 || exception.RecordedTicketCount != 0 || exception.IssuanceReceiptOutcome != "NOT_RECORDED" {
		t.Fatalf("delivery exception = %+v", exception)
	}
}

func TestAdminEventTicketOperationsRejectsInconsistentDeliveryException(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	base := repository.AdminEventAfterSalesException{
		BookingID: uuid.MustParse("62000000-0000-4000-8000-000000000001"), State: AdminEventAfterSalesDeliveryPending,
		BookingStatus: "CONFIRMED", ExpectedTicketCount: 2, RecordedTicketCount: 0,
		IssuanceReceiptOutcome: "NOT_RECORDED", BookingCreatedAt: time.Date(2026, 10, 31, 11, 0, 0, 0, time.UTC),
	}
	for _, test := range []struct {
		name   string
		mutate func(*repository.AdminEventAfterSalesException)
	}{
		{name: "wrong booking status", mutate: func(v *repository.AdminEventAfterSalesException) { v.BookingStatus = "PENDING" }},
		{name: "matching counts without conflict", mutate: func(v *repository.AdminEventAfterSalesException) { v.RecordedTicketCount = v.ExpectedTicketCount }},
		{name: "unknown receipt state", mutate: func(v *repository.AdminEventAfterSalesException) { v.IssuanceReceiptOutcome = "unknown" }},
		{name: "conflict without reason", mutate: func(v *repository.AdminEventAfterSalesException) { v.IssuanceReceiptOutcome = "CONFLICT" }},
		{name: "created after observation", mutate: func(v *repository.AdminEventAfterSalesException) {
			v.BookingCreatedAt = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := validAdminEventTicketOperationsSnapshot(eventID)
			exception := base
			test.mutate(&exception)
			snapshot.DeliveryExceptions = []repository.AdminEventAfterSalesException{exception}
			service := NewAdminEventTicketOperationsService(&adminEventTicketOperationsTestReader{snapshot: snapshot})
			if _, err := service.Get(context.Background(), eventID); !errors.Is(err, ErrAdminEventTicketOperationsInvalidSnapshot) {
				t.Fatalf("Get() error = %v, want invalid snapshot", err)
			}
		})
	}
}

func TestAdminEventTicketOperationsHidesReaderErrorsAndPreservesCapacitySignal(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	secretFailure := errors.New("private PostgreSQL detail")
	service := NewAdminEventTicketOperationsService(&adminEventTicketOperationsTestReader{err: secretFailure})
	if _, err := service.Get(context.Background(), eventID); !errors.Is(err, ErrAdminEventTicketOperationsReadFailed) || errors.Is(err, secretFailure) {
		t.Fatalf("Get() error = %v, want fixed read failure without wrapping the source error", err)
	}
	service = NewAdminEventTicketOperationsService(&adminEventTicketOperationsTestReader{err: repository.ErrAdminEventTicketOperationsCapacityExceeded})
	if _, err := service.Get(context.Background(), eventID); !errors.Is(err, ErrAdminEventTicketOperationsCapacityExceeded) {
		t.Fatalf("capacity Get() error = %v", err)
	}
}

func validAdminEventTicketOperationsSnapshot(eventID uuid.UUID) repository.AdminEventTicketOperationsSnapshot {
	observed := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	snapshot := repository.AdminEventTicketOperationsSnapshot{EventID: eventID, ObservedAt: observed}
	addTicket := func(id int, status string, checkedInAt, revokedAt *time.Time) uuid.UUID {
		ticketID := uuid.MustParse(fmt.Sprintf("61000000-0000-4000-8000-%012d", id))
		snapshot.Tickets = append(snapshot.Tickets, repository.AdminEventTicketOperationsTicket{TicketID: ticketID, EventID: eventID, Status: status, CheckedInAt: checkedInAt, RevokedAt: revokedAt})
		return ticketID
	}
	addAudit := func(ticketID uuid.UUID, at time.Time) {
		snapshot.Audits = append(snapshot.Audits, repository.AdminEventTicketOperationsAudit{TicketID: ticketID, EventID: eventID, ResultCode: "accepted", CheckedInAt: at})
	}
	addTicket(1, "ISSUED", nil, nil)
	for i := 2; i <= 3; i++ {
		at := observed.Add(-time.Duration(i-1) * time.Minute)
		id := addTicket(i, "CHECKED_IN", &at, nil)
		addAudit(id, at)
	}
	for i := 4; i <= 7; i++ {
		at := observed.Add(-time.Duration(i-1) * time.Minute)
		revokedAt := observed.Add(-time.Duration(i-1) * time.Second)
		id := addTicket(i, "REVOKED", &at, &revokedAt)
		addAudit(id, at)
	}
	addTicket(8, "REVOKED", nil, ptrTime(observed.Add(-time.Second)))
	return snapshot
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
