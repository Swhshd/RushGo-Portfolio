package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/adminticketlist"
)

type adminEventTicketsListTestReader struct {
	snapshot repository.AdminEventTicketListSnapshot
	err      error
	calls    int
	eventID  uuid.UUID
	query    repository.AdminEventTicketListQuery
}

func (r *adminEventTicketsListTestReader) ReadAdminEventTickets(_ context.Context, eventID uuid.UUID, query repository.AdminEventTicketListQuery) (repository.AdminEventTicketListSnapshot, error) {
	r.calls++
	r.eventID = eventID
	r.query = query
	return r.snapshot, r.err
}

func TestAdminEventTicketListDefaultsToAllAndTwenty(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	reader := &adminEventTicketsListTestReader{snapshot: repository.AdminEventTicketListSnapshot{
		EventID: eventID, ObservedAt: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC),
	}}
	page, err := NewAdminEventTicketListService(reader).List(context.Background(), eventID, "", 0, "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if page.StatusFilter != adminticketlist.StatusAll || page.PageSize != adminticketlist.DefaultPageSize || reader.query.StatusFilter != adminticketlist.StatusAll || reader.query.PageSize != adminticketlist.DefaultPageSize {
		t.Fatalf("default list filter/size = response:%s/%d query:%s/%d", page.StatusFilter, page.PageSize, reader.query.StatusFilter, reader.query.PageSize)
	}
}

func TestAdminEventTicketsListUsesDefaultAndBuildsTieSafeNextCursor(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	observedAt := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	issuedAt := observedAt.Add(-time.Hour)
	checkedInAt := observedAt.Add(-30 * time.Minute)
	revokedAt := observedAt.Add(-15 * time.Minute)
	checkedInTicketID := uuid.MustParse("61000000-0000-4000-8000-000000000003")
	revokedTicketID := uuid.MustParse("61000000-0000-4000-8000-000000000004")
	statusReader := &adminEventTicketsListTestReader{snapshot: repository.AdminEventTicketListSnapshot{
		EventID: eventID, ObservedAt: observedAt,
		Tickets: []repository.AdminEventTicketListRecord{
			{TicketID: revokedTicketID, EventID: eventID, Status: "REVOKED", IssuedAt: revokedAt.Add(-time.Hour), CheckedInAt: &checkedInAt, RevokedAt: &revokedAt},
			{TicketID: checkedInTicketID, EventID: eventID, Status: "CHECKED_IN", IssuedAt: checkedInAt.Add(-time.Hour), CheckedInAt: &checkedInAt},
			{TicketID: uuid.MustParse("61000000-0000-4000-8000-000000000005"), EventID: eventID, Status: "ISSUED", IssuedAt: observedAt.Add(-4 * time.Hour)},
		},
	}}
	statusPage, err := NewAdminEventTicketListService(statusReader).List(context.Background(), eventID, "ALL", 20, "")
	if err != nil {
		t.Fatalf("List(ALL) status rows error = %v", err)
	}
	if len(statusPage.Tickets) != 3 || statusPage.Tickets[0].Status != "REVOKED" || statusPage.Tickets[0].CheckedInAt == nil || statusPage.Tickets[0].RevokedAt == nil || statusPage.Tickets[1].Status != "CHECKED_IN" || statusPage.Tickets[1].CheckedInAt == nil || statusPage.Tickets[1].RevokedAt != nil || statusPage.Tickets[2].Status != "ISSUED" || statusPage.Tickets[2].CheckedInAt != nil || statusPage.Tickets[2].RevokedAt != nil {
		t.Fatalf("safe status rows = %#v", statusPage.Tickets)
	}

	reader := &adminEventTicketsListTestReader{snapshot: repository.AdminEventTicketListSnapshot{
		EventID: eventID, ObservedAt: observedAt,
		Tickets: []repository.AdminEventTicketListRecord{
			{TicketID: uuid.MustParse("61000000-0000-4000-8000-000000000002"), EventID: eventID, Status: "ISSUED", IssuedAt: issuedAt},
			{TicketID: uuid.MustParse("61000000-0000-4000-8000-000000000001"), EventID: eventID, Status: "ISSUED", IssuedAt: issuedAt},
		},
	}}
	service := NewAdminEventTicketListService(reader)
	page, err := service.List(context.Background(), eventID, "", 1, "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if reader.calls != 1 || reader.query.StatusFilter != adminticketlist.StatusAll || reader.query.PageSize != 1 || reader.query.After != nil {
		t.Fatalf("reader query = calls:%d query:%#v", reader.calls, reader.query)
	}
	if page.EventID != eventID.String() || page.StatusFilter != adminticketlist.StatusAll || page.PageSize != 1 || !page.HasMore || page.NextCursor == "" || len(page.Tickets) != 1 {
		t.Fatalf("page envelope = %#v", page)
	}
	if page.Tickets[0].TicketID != "61000000-0000-4000-8000-000000000002" {
		t.Fatalf("first row in equal-time descending order = %s", page.Tickets[0].TicketID)
	}
	cursor, err := adminticketlist.DecodeCursor(page.NextCursor, eventID, adminticketlist.StatusAll)
	if err != nil {
		t.Fatalf("DecodeCursor(next_cursor) error = %v", err)
	}
	if cursor.TicketID != uuid.MustParse(page.Tickets[0].TicketID) || !cursor.IssuedAt.Equal(issuedAt) {
		t.Fatalf("next cursor = %#v, does not point to final emitted row", cursor)
	}
}

func TestAdminEventTicketsListRejectsInvalidQueryBeforeRepository(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	validCursor, err := adminticketlist.EncodeCursor(adminticketlist.Cursor{
		EventID: eventID, StatusFilter: adminticketlist.StatusCheckedIn,
		IssuedAt: time.Date(2026, 11, 1, 11, 0, 0, 0, time.UTC),
		TicketID: uuid.MustParse("61000000-0000-4000-8000-000000000001"),
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		status string
		page   int32
		cursor string
	}{
		{name: "unknown status", status: "PENDING", page: 20},
		{name: "oversized page", status: "ALL", page: 101},
		{name: "wrong event cursor", status: "CHECKED_IN", page: 20, cursor: validCursor},
		{name: "wrong filter cursor", status: "ISSUED", page: 20, cursor: validCursor},
		{name: "malformed cursor", status: "ALL", page: 20, cursor: "!"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &adminEventTicketsListTestReader{}
			service := NewAdminEventTicketListService(reader)
			requestedEventID := eventID
			if tc.name == "wrong event cursor" {
				requestedEventID = uuid.MustParse("60000000-0000-4000-8000-000000000002")
			}
			if _, err := service.List(context.Background(), requestedEventID, tc.status, tc.page, tc.cursor); !errors.Is(err, ErrAdminEventTicketListInvalidRequest) {
				t.Fatalf("List() error = %v, want invalid request", err)
			}
			if reader.calls != 0 {
				t.Fatalf("repository calls = %d, want 0", reader.calls)
			}
		})
	}
}

func TestAdminEventTicketsListFailsClosedOnInvalidRowsAndSourceFailure(t *testing.T) {
	eventID := uuid.MustParse("60000000-0000-4000-8000-000000000001")
	observedAt := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	issuedAt := observedAt.Add(-time.Hour)
	validRecord := repository.AdminEventTicketListRecord{TicketID: uuid.MustParse("61000000-0000-4000-8000-000000000001"), EventID: eventID, Status: "ISSUED", IssuedAt: issuedAt}
	cases := []struct {
		name   string
		mutate func(*repository.AdminEventTicketListSnapshot)
	}{
		{name: "unknown status", mutate: func(s *repository.AdminEventTicketListSnapshot) { s.Tickets[0].Status = "CHECK_IN_PENDING" }},
		{name: "other event row", mutate: func(s *repository.AdminEventTicketListSnapshot) {
			s.Tickets[0].EventID = uuid.MustParse("60000000-0000-4000-8000-000000000002")
		}},
		{name: "nil ticket ID", mutate: func(s *repository.AdminEventTicketListSnapshot) { s.Tickets[0].TicketID = uuid.Nil }},
		{name: "issued after observation", mutate: func(s *repository.AdminEventTicketListSnapshot) {
			s.Tickets[0].IssuedAt = s.ObservedAt.Add(time.Second)
		}},
		{name: "issued ticket with status times", mutate: func(s *repository.AdminEventTicketListSnapshot) { s.Tickets[0].RevokedAt = &issuedAt }},
		{name: "checked-in missing timestamp", mutate: func(s *repository.AdminEventTicketListSnapshot) { s.Tickets[0].Status = "CHECKED_IN" }},
		{name: "revoked missing timestamp", mutate: func(s *repository.AdminEventTicketListSnapshot) { s.Tickets[0].Status = "REVOKED" }},
		{name: "check-in before issued", mutate: func(s *repository.AdminEventTicketListSnapshot) {
			s.Tickets[0].Status = "CHECKED_IN"
			checkedInAt := s.Tickets[0].IssuedAt.Add(-time.Second)
			s.Tickets[0].CheckedInAt = &checkedInAt
		}},
		{name: "revocation before issued", mutate: func(s *repository.AdminEventTicketListSnapshot) {
			s.Tickets[0].Status = "REVOKED"
			revokedAt := s.Tickets[0].IssuedAt.Add(-time.Second)
			s.Tickets[0].RevokedAt = &revokedAt
		}},
		{name: "check-in before issued on revoked ticket", mutate: func(s *repository.AdminEventTicketListSnapshot) {
			s.Tickets[0].Status = "REVOKED"
			checkedInAt := s.Tickets[0].IssuedAt.Add(-time.Second)
			revokedAt := s.Tickets[0].IssuedAt.Add(time.Second)
			s.Tickets[0].CheckedInAt = &checkedInAt
			s.Tickets[0].RevokedAt = &revokedAt
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := repository.AdminEventTicketListSnapshot{EventID: eventID, ObservedAt: observedAt, Tickets: []repository.AdminEventTicketListRecord{validRecord}}
			tc.mutate(&snapshot)
			service := NewAdminEventTicketListService(&adminEventTicketsListTestReader{snapshot: snapshot})
			if _, err := service.List(context.Background(), eventID, "ALL", 20, ""); !errors.Is(err, ErrAdminEventTicketListInvalidSnapshot) {
				t.Fatalf("List() error = %v, want invalid snapshot", err)
			}
		})
	}

	privateFailure := fmt.Errorf("private SQL detail")
	service := NewAdminEventTicketListService(&adminEventTicketsListTestReader{err: privateFailure})
	if _, err := service.List(context.Background(), eventID, "ALL", 20, ""); !errors.Is(err, ErrAdminEventTicketListReadFailed) || errors.Is(err, privateFailure) {
		t.Fatalf("List() source error = %v, want fixed source error without private detail", err)
	}
	service = NewAdminEventTicketListService(&adminEventTicketsListTestReader{err: repository.ErrAdminEventTicketOperationsCapacityExceeded})
	if _, err := service.List(context.Background(), eventID, "ALL", 20, ""); !errors.Is(err, ErrAdminEventTicketListCapacityExceeded) {
		t.Fatalf("List() capacity error = %v", err)
	}
}
