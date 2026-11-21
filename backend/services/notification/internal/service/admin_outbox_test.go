package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/repository"
)

type adminOutboxProjectionReaderStub struct {
	rows  []repository.AdminOutboxProjection
	err   error
	calls int
}

func (r *adminOutboxProjectionReaderStub) ReadAdminOutboxProjection(context.Context) ([]repository.AdminOutboxProjection, error) {
	r.calls++
	return append([]repository.AdminOutboxProjection(nil), r.rows...), r.err
}

func notificationAdminOutboxQuery(now time.Time) AdminOutboxQuery {
	return AdminOutboxQuery{
		Source: "notification", Status: "all", From: now.Add(-24 * time.Hour).Format(time.RFC3339Nano),
		To: now.Format(time.RFC3339Nano), Limit: 20,
	}
}

func TestNotificationAdminOutboxProjectsOnlyManagedSafeFacts(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	createdAt := now.Add(-time.Hour)
	reader := &adminOutboxProjectionReaderStub{rows: []repository.AdminOutboxProjection{
		{ID: uuid.MustParse("80000000-0000-4000-8000-000000000003"), EventType: "booking_confirmation", Status: "PENDING", Managed: true, CreatedAt: createdAt},
		{ID: uuid.MustParse("80000000-0000-4000-8000-000000000002"), EventType: "unsafe_legacy_type", Status: "SENT", Managed: true, CreatedAt: createdAt},
		{ID: uuid.MustParse("80000000-0000-4000-8000-000000000001"), EventType: "booking_failure", Status: "ATTENTION", Managed: false, CreatedAt: createdAt},
	}}
	service := NewAdminOutboxService(reader)
	service.now = func() time.Time { return now }

	page, err := service.List(context.Background(), notificationAdminOutboxQuery(now))
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(page.Records) != 2 {
		t.Fatalf("records = %d, want only the 2 managed rows", len(page.Records))
	}
	if page.Records[0].EventID != "80000000-0000-4000-8000-000000000003" || page.Records[1].EventID != "80000000-0000-4000-8000-000000000002" {
		t.Fatalf("stable created_at/id order = [%s %s]", page.Records[0].EventID, page.Records[1].EventID)
	}
	if page.Records[0].SourceService != "notification" || page.Records[0].EventType != "booking_confirmation" || page.Records[0].RecordedStatus != "pending" {
		t.Fatalf("first safe projection = %+v", page.Records[0])
	}
	if page.Records[1].EventType != "Other" || page.Records[1].RecordedStatus != "sent" {
		t.Fatalf("unknown type/status projection = %+v", page.Records[1])
	}
	for _, record := range page.Records {
		if record.ConsumerOutcome != "not_verified" {
			t.Fatalf("consumer outcome = %q, want not_verified", record.ConsumerOutcome)
		}
	}
	if page.ConsumerOutcome != "not_verified" || page.SourceRecordLimit != 5000 {
		t.Fatalf("page disclosure metadata = outcome %q, cap %d", page.ConsumerOutcome, page.SourceRecordLimit)
	}
}

func TestNotificationAdminOutboxCursorBindsSourceFiltersWindowAndLimit(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	createdAt := now.Add(-time.Hour)
	reader := &adminOutboxProjectionReaderStub{rows: []repository.AdminOutboxProjection{
		{ID: uuid.MustParse("80000000-0000-4000-8000-000000000003"), EventType: "booking_confirmation", Status: "PENDING", Managed: true, CreatedAt: createdAt},
		{ID: uuid.MustParse("80000000-0000-4000-8000-000000000002"), EventType: "booking_failure", Status: "SENT", Managed: true, CreatedAt: createdAt},
	}}
	service := NewAdminOutboxService(reader)
	service.now = func() time.Time { return now }
	query := notificationAdminOutboxQuery(now)
	query.Limit = 1
	query.EventType = ""

	first, err := service.List(context.Background(), query)
	if err != nil {
		t.Fatalf("first page List() error = %v", err)
	}
	if !first.HasMore || first.NextCursor == "" || len(first.Records) != 1 {
		t.Fatalf("first page = records %d, hasMore %t, cursorPresent %t", len(first.Records), first.HasMore, first.NextCursor != "")
	}
	query.Cursor = first.NextCursor
	second, err := service.List(context.Background(), query)
	if err != nil || len(second.Records) != 1 || second.Records[0].EventID != "80000000-0000-4000-8000-000000000002" {
		t.Fatalf("second page = %+v, error %v", second, err)
	}

	mutations := []struct {
		name string
		edit func(*AdminOutboxQuery)
	}{
		{name: "source", edit: func(q *AdminOutboxQuery) { q.Source = "booking" }},
		{name: "status", edit: func(q *AdminOutboxQuery) { q.Status = "sent" }},
		{name: "type", edit: func(q *AdminOutboxQuery) { q.EventType = "booking_failure" }},
		{name: "from", edit: func(q *AdminOutboxQuery) { q.From = now.Add(-12 * time.Hour).Format(time.RFC3339Nano) }},
		{name: "to", edit: func(q *AdminOutboxQuery) { q.To = now.Add(-12 * time.Hour).Format(time.RFC3339Nano) }},
		{name: "limit", edit: func(q *AdminOutboxQuery) { q.Limit = 2 }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			changed := query
			mutation.edit(&changed)
			if _, err := service.List(context.Background(), changed); !errors.Is(err, ErrAdminOutboxInvalidQuery) {
				t.Fatalf("List() with changed %s filter error = %v, want invalid query", mutation.name, err)
			}
		})
	}
}

func TestNotificationAdminOutboxCapacityCountsLegacyRowsBeforeManagedFilter(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	rows := make([]repository.AdminOutboxProjection, AdminOutboxRecordLimit+1)
	for i := range rows {
		rows[i] = repository.AdminOutboxProjection{
			ID: uuid.New(), EventType: "legacy", Status: "UNKNOWN", Managed: false, CreatedAt: now,
		}
	}
	reader := &adminOutboxProjectionReaderStub{rows: rows}
	_, err := NewAdminOutboxService(reader).List(context.Background(), notificationAdminOutboxQuery(now))
	if !errors.Is(err, ErrAdminOutboxCapacityExceeded) {
		t.Fatalf("List() error = %v, want whole-source capacity error", err)
	}
}

func TestNotificationAdminOutboxRejectsUnsupportedStatusAndOtherFilterBeforeRead(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	reader := &adminOutboxProjectionReaderStub{}
	service := NewAdminOutboxService(reader)

	for _, tc := range []struct {
		name   string
		status string
		typ    string
	}{
		{name: "delivered is not authoritative", status: "delivered"},
		{name: "other cannot be queried as a stored type", status: "all", typ: "Other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := notificationAdminOutboxQuery(now)
			if tc.status != "" {
				query.Status = tc.status
			}
			query.EventType = tc.typ
			if _, err := service.List(context.Background(), query); !errors.Is(err, ErrAdminOutboxInvalidQuery) {
				t.Fatalf("List() error = %v, want invalid query", err)
			}
		})
	}
	if reader.calls != 0 {
		t.Fatalf("invalid requests reached repository %d times", reader.calls)
	}
}
