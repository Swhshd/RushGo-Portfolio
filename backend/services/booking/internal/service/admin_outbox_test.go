package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/repository"
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

func adminOutboxTestQuery(now time.Time) AdminOutboxQuery {
	return AdminOutboxQuery{
		Source: "booking",
		Status: "all",
		From:   now.Add(-24 * time.Hour).Format(time.RFC3339Nano),
		To:     now.Format(time.RFC3339Nano),
		Limit:  20,
	}
}

func TestAdminOutboxRejectsInvalidFiltersAndTimeBoundsBeforeReading(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 123456789, time.UTC)
	base := adminOutboxTestQuery(now)
	cases := []struct {
		name string
		edit func(*AdminOutboxQuery)
	}{
		{name: "unknown source", edit: func(q *AdminOutboxQuery) { q.Source = "saga" }},
		{name: "unknown status", edit: func(q *AdminOutboxQuery) { q.Status = "failed" }},
		{name: "unknown event type", edit: func(q *AdminOutboxQuery) { q.EventType = "ArbitraryType" }},
		{name: "malformed from", edit: func(q *AdminOutboxQuery) { q.From = "yesterday" }},
		{name: "missing upper bound", edit: func(q *AdminOutboxQuery) { q.To = "" }},
		{name: "reversed range", edit: func(q *AdminOutboxQuery) { q.From, q.To = q.To, q.From }},
		{name: "range longer than seven days", edit: func(q *AdminOutboxQuery) {
			q.From = now.Add(-7*24*time.Hour - time.Nanosecond).Format(time.RFC3339Nano)
		}},
		{name: "zero limit", edit: func(q *AdminOutboxQuery) { q.Limit = 0 }},
		{name: "limit over one hundred", edit: func(q *AdminOutboxQuery) { q.Limit = 101 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &adminOutboxProjectionReaderStub{}
			query := base
			tc.edit(&query)
			service := &AdminOutboxService{reader: reader, now: func() time.Time { return now }}
			if _, err := service.List(context.Background(), query); !errors.Is(err, ErrAdminOutboxInvalidQuery) {
				t.Fatalf("List() error = %v, want invalid query", err)
			}
			if reader.calls != 0 {
				t.Fatalf("invalid query reached projection reader %d times", reader.calls)
			}
		})
	}
}

func TestAdminOutboxFailsClosedWhenGlobalProjectionExceedsCapacity(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	rows := make([]repository.AdminOutboxProjection, AdminOutboxRecordLimit+1)
	for i := range rows {
		rows[i] = repository.AdminOutboxProjection{
			ID:        uuid.New(),
			EventType: "BookingConfirmed",
			Published: true,
			CreatedAt: now,
		}
	}
	reader := &adminOutboxProjectionReaderStub{rows: rows}
	query := adminOutboxTestQuery(now)
	query.Status = "pending"
	query.EventType = "BookingCancelled"
	service := &AdminOutboxService{reader: reader, now: func() time.Time { return now }}
	if _, err := service.List(context.Background(), query); !errors.Is(err, ErrAdminOutboxCapacityExceeded) {
		t.Fatalf("List() error = %v, want capacity exceeded before narrowing filters", err)
	}
	if reader.calls != 1 {
		t.Fatalf("projection reader calls = %d, want 1", reader.calls)
	}
}

func TestAdminOutboxOrdersByCreatedAtAndUUIDBytesAndContinuesWithKeyset(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 987654321, time.UTC)
	tied := now.Add(-time.Minute)
	rows := []repository.AdminOutboxProjection{
		{ID: uuid.MustParse("00000000-0000-4000-8000-000000000001"), EventType: "BookingConfirmed", Published: true, CreatedAt: tied},
		{ID: uuid.MustParse("00000000-0000-4000-8000-000000000003"), EventType: "BookingConfirmed", Published: true, CreatedAt: tied},
		{ID: uuid.MustParse("00000000-0000-4000-8000-000000000002"), EventType: "BookingCancelled", Published: false, CreatedAt: tied},
		{ID: uuid.MustParse("00000000-0000-4000-8000-000000000004"), EventType: "BookingFailed", Published: false, CreatedAt: tied.Add(-time.Nanosecond)},
	}
	reader := &adminOutboxProjectionReaderStub{rows: rows}
	service := &AdminOutboxService{reader: reader, now: func() time.Time { return now }}
	query := adminOutboxTestQuery(now)
	query.Limit = 2
	first, err := service.List(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != 2 || first.Records[0].EventID != "00000000-0000-4000-8000-000000000003" || first.Records[1].EventID != "00000000-0000-4000-8000-000000000002" {
		t.Fatalf("first page order = %#v, want equal-time UUID bytes descending", first.Records)
	}
	if !first.HasMore || first.NextCursor == "" {
		t.Fatalf("first page continuation = has_more %v, cursor %q", first.HasMore, first.NextCursor)
	}

	query.Cursor = first.NextCursor
	second, err := service.List(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 2 || second.Records[0].EventID != "00000000-0000-4000-8000-000000000001" || second.Records[1].EventID != "00000000-0000-4000-8000-000000000004" {
		t.Fatalf("second page order = %#v, want no duplicate or skipped key", second.Records)
	}
	if second.HasMore || second.NextCursor != "" {
		t.Fatalf("last page continuation = has_more %v, cursor %q", second.HasMore, second.NextCursor)
	}
}

func TestAdminOutboxCursorRejectsMalformedAndQueryMismatchedTokens(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	reader := &adminOutboxProjectionReaderStub{rows: []repository.AdminOutboxProjection{
		{ID: uuid.MustParse("10000000-0000-4000-8000-000000000002"), EventType: "BookingConfirmed", Published: true, CreatedAt: now.Add(-time.Minute)},
		{ID: uuid.MustParse("10000000-0000-4000-8000-000000000001"), EventType: "BookingCancelled", Published: false, CreatedAt: now.Add(-2 * time.Minute)},
	}}
	service := &AdminOutboxService{reader: reader, now: func() time.Time { return now }}
	query := adminOutboxTestQuery(now)
	query.Limit = 1
	first, err := service.List(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if first.NextCursor == "" {
		t.Fatal("single-row page omitted its continuation cursor")
	}

	for _, tc := range []struct {
		name string
		edit func(*AdminOutboxQuery)
	}{
		{name: "malformed encoding", edit: func(q *AdminOutboxQuery) { q.Cursor = "%%%" }},
		{name: "changed source", edit: func(q *AdminOutboxQuery) { q.Cursor = first.NextCursor; q.Source = "event" }},
		{name: "changed status", edit: func(q *AdminOutboxQuery) { q.Cursor = first.NextCursor; q.Status = "pending" }},
		{name: "changed type", edit: func(q *AdminOutboxQuery) { q.Cursor = first.NextCursor; q.EventType = "BookingConfirmed" }},
		{name: "changed lower time", edit: func(q *AdminOutboxQuery) {
			q.Cursor = first.NextCursor
			q.From = now.Add(-23 * time.Hour).Format(time.RFC3339Nano)
		}},
		{name: "changed upper time", edit: func(q *AdminOutboxQuery) {
			q.Cursor = first.NextCursor
			q.To = now.Add(-time.Second).Format(time.RFC3339Nano)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := query
			tc.edit(&candidate)
			if _, err := service.List(context.Background(), candidate); !errors.Is(err, ErrAdminOutboxInvalidQuery) {
				t.Fatalf("List() error = %v, want invalid cursor/query binding", err)
			}
		})
	}
}

func TestAdminOutboxMapsUnknownStoredTypesAndExposesOnlySafeFacts(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	reader := &adminOutboxProjectionReaderStub{rows: []repository.AdminOutboxProjection{
		{ID: uuid.MustParse("20000000-0000-4000-8000-000000000001"), EventType: "BookingConfirmed", Published: true, CreatedAt: now.Add(-time.Minute)},
		{ID: uuid.MustParse("20000000-0000-4000-8000-000000000002"), EventType: "SensitiveInternalEvent", Published: false, CreatedAt: now.Add(-2 * time.Minute)},
	}}
	service := &AdminOutboxService{reader: reader, now: func() time.Time { return now }}
	query := adminOutboxTestQuery(now)
	query.EventType = "Other"
	page, err := service.List(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.Records[0].EventType != "Other" || page.Records[0].RecordedStatus != "pending" || page.Records[0].ConsumerOutcome != "not_verified" {
		t.Fatalf("safe unknown-type projection = %#v", page.Records)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"SensitiveInternalEvent", "event_key", "payload", "error_summary", "attempt_count", "published_at"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Errorf("response leaked or invented field %q: %s", forbidden, encoded)
		}
	}
	if strings.Contains(string(encoded), "event_id") && !strings.Contains(string(encoded), page.Records[0].EventID) {
		t.Fatalf("response event identifier is inconsistent: %s", encoded)
	}
}

func TestAdminOutboxProjectionReadFailureIsNotReportedAsEmpty(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	reader := &adminOutboxProjectionReaderStub{err: fmt.Errorf("synthetic database detail: secret")}
	service := &AdminOutboxService{reader: reader, now: func() time.Time { return now }}
	if page, err := service.List(context.Background(), adminOutboxTestQuery(now)); err == nil || page != nil {
		t.Fatalf("failed source read returned page=%#v error=%v", page, err)
	}
}
