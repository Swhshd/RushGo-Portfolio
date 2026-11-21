package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/event/internal/domain"
)

// Only database reads are replaced. The repository's query, scan, iteration,
// Event/tier assembly and error wrapping execute unchanged.
type iterationReadPool struct {
	*pgxpool.Pool
	events    *iterationRows
	tiers     *iterationRows
	seats     *iterationRows
	event     []any
	tierReads int
}

func (p *iterationReadPool) Query(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
	switch {
	case strings.Contains(query, "FROM events"):
		return p.events, nil
	case strings.Contains(query, "FROM ticket_tiers"):
		p.tierReads++
		if p.tiers == nil {
			return &iterationRows{}, nil
		}
		return p.tiers, nil
	case strings.Contains(query, "FROM seats"):
		return p.seats, nil
	default:
		return nil, fmt.Errorf("unexpected test query")
	}
}

func (p *iterationReadPool) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	if strings.Contains(query, "COUNT(*)") {
		return iterationRow{values: []any{len(p.events.values)}}
	}
	return iterationRow{values: p.event}
}

type iterationRows struct {
	pgx.Rows
	values  [][]any
	next    int
	ended   bool
	closed  bool
	endErr  error
	scanErr error
	scans   int
}

func (r *iterationRows) Next() bool {
	if r.closed || r.next == len(r.values) {
		r.ended = true
		r.Close()
		return false
	}
	r.next++
	return true
}

func (r *iterationRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		r.Close()
		return r.scanErr
	}
	r.scans++
	return (iterationRow{values: r.values[r.next-1]}).Scan(dest...)
}

func (r *iterationRows) Close() { r.closed = true }

func (r *iterationRows) Err() error {
	if r.ended {
		return r.endErr
	}
	return nil
}

type iterationRow struct{ values []any }

func (r iterationRow) Scan(dest ...any) error {
	if len(dest) != len(r.values) {
		return fmt.Errorf("test row column count mismatch")
	}
	for i, value := range r.values {
		target := reflect.ValueOf(dest[i]).Elem()
		if value == nil {
			target.Set(reflect.Zero(target.Type()))
		} else if source := reflect.ValueOf(value); source.Type().AssignableTo(target.Type()) {
			target.Set(source)
		} else {
			return fmt.Errorf("test row column %d type mismatch", i)
		}
	}
	return nil
}

type iterationReadCase struct {
	name       string
	rowsKind   string
	read       func(*iterationReadPool) (int, error)
	normalSize func(int) int
}

func iterationReadCases() []iterationReadCase {
	ctx := context.Background()
	eventID, tierID, seatID := uuid.New(), uuid.New(), uuid.New()
	list := func(p *iterationReadPool) (int, error) {
		result, _, err := (&PostgresEventRepository{pool: p}).List(ctx, "", "", 1, 20)
		return len(result), err
	}
	return []iterationReadCase{
		{name: "event_list", rowsKind: "events", read: list},
		{name: "event_list_nested_tiers", rowsKind: "tiers", read: list, normalSize: func(int) int { return 1 }},
		{name: "event_detail_tiers", rowsKind: "tiers", read: func(p *iterationReadPool) (int, error) {
			result, err := (&PostgresEventRepository{pool: p}).GetByID(ctx, eventID)
			if result == nil {
				return 0, err
			}
			return 1 + len(result.Tiers), err
		}, normalSize: func(n int) int { return 1 + n }},
		{name: "ticket_tiers_by_event", rowsKind: "tiers", read: func(p *iterationReadPool) (int, error) {
			result, err := (&PostgresTicketTierRepository{pool: p}).GetByEventID(ctx, eventID)
			return len(result), err
		}},
		{name: "seats_by_event", rowsKind: "seats", read: func(p *iterationReadPool) (int, error) {
			result, err := (&PostgresSeatRepository{pool: p}).GetByEventID(ctx, eventID, nil)
			return len(result), err
		}},
		{name: "seats_by_event_tier", rowsKind: "seats", read: func(p *iterationReadPool) (int, error) {
			result, err := (&PostgresSeatRepository{pool: p}).GetByEventID(ctx, eventID, &tierID)
			return len(result), err
		}},
		{name: "seats_by_tier", rowsKind: "seats", read: func(p *iterationReadPool) (int, error) {
			result, err := (&PostgresSeatRepository{pool: p}).GetByTierID(ctx, tierID)
			return len(result), err
		}},
		{name: "available_seats", rowsKind: "seats", read: func(p *iterationReadPool) (int, error) {
			result, err := (&PostgresSeatRepository{pool: p}).GetAvailableSeats(ctx, eventID)
			return len(result), err
		}},
		{name: "available_seats_by_tier", rowsKind: "seats", read: func(p *iterationReadPool) (int, error) {
			result, err := (&PostgresSeatRepository{pool: p}).GetAvailableSeatsByTier(ctx, tierID)
			return len(result), err
		}},
		{name: "seats_by_ids", rowsKind: "seats", read: func(p *iterationReadPool) (int, error) {
			result, err := (&PostgresSeatRepository{pool: p}).GetSeatsBySeatIds(ctx, []uuid.UUID{seatID})
			return len(result), err
		}},
	}
}

func newIterationReadFixture(kind string, count int) (*iterationReadPool, *iterationRows) {
	now := time.Unix(10, 0).UTC()
	description, image := "fixture description", "https://example.test/event.jpg"
	eventID, tierID := uuid.New(), uuid.New()
	event := []any{eventID, "fixture event", &description, "concerts", "venue", "location", now, int64(1), &image, "active", "published", "selling", (*time.Time)(nil), (*time.Time)(nil), now, now}
	p := &iterationReadPool{event: event, events: &iterationRows{values: [][]any{event}}}
	rows := &iterationRows{}
	for range count {
		switch kind {
		case "events":
			rows.values = append(rows.values, event)
		case "tiers":
			rows.values = append(rows.values, []any{uuid.New(), eventID, "General", int64(1500), int32(10), int32(10), int32(1), now})
		case "seats":
			rows.values = append(rows.values, []any{uuid.New(), eventID, tierID, domain.SeatStatusAvailable, (*uuid.UUID)(nil), ptrUUID(uuid.New()), []byte(`{"sectionId":"Floor","row":"A","seat":1,"x":0,"y":0}`), now, now, int64(17)})
		}
	}
	switch kind {
	case "events":
		p.events = rows
	case "tiers":
		p.tiers = rows
	case "seats":
		p.seats = rows
	}
	return p, rows
}

func TestRepositoryReadIterationErrors(t *testing.T) {
	for _, tc := range iterationReadCases() {
		t.Run(tc.name, func(t *testing.T) {
			pool, rows := newIterationReadFixture(tc.rowsKind, 1)
			sentinel := errors.New("controlled iteration failure")
			rows.endErr = sentinel
			count, err := tc.read(pool)
			if !errors.Is(err, sentinel) {
				t.Errorf("one valid row followed by iterator failure returned count=%d err=%v; want propagated sentinel", count, err)
			}
			if count != 0 {
				t.Error("repository must not expose a partial result on iterator failure")
			}
			if rows.scans != 1 || !rows.closed {
				t.Error("fixture must scan one valid row and close at iteration termination")
			}
			if tc.rowsKind == "events" && pool.tierReads != 0 {
				t.Error("event iteration failure must stop before tier assembly")
			}
		})
	}
}

func TestRepositoryReadNormalIteration(t *testing.T) {
	for _, tc := range iterationReadCases() {
		for _, size := range []int{0, 1, 3} {
			t.Run(fmt.Sprintf("%s/%d_rows", tc.name, size), func(t *testing.T) {
				pool, rows := newIterationReadFixture(tc.rowsKind, size)
				count, err := tc.read(pool)
				want := size
				if tc.normalSize != nil {
					want = tc.normalSize(size)
				}
				if err != nil || count != want {
					t.Errorf("normal iteration count=%d err=%v, want %d and nil", count, err, want)
				}
				if rows.scans != size || !rows.closed {
					t.Error("normal iteration must consume all rows and close")
				}
			})
		}
	}
}

func TestRepositoryReadScanErrors(t *testing.T) {
	for _, tc := range iterationReadCases() {
		t.Run(tc.name, func(t *testing.T) {
			pool, rows := newIterationReadFixture(tc.rowsKind, 1)
			sentinel := errors.New("controlled scan failure")
			rows.scanErr = sentinel
			count, err := tc.read(pool)
			if !errors.Is(err, sentinel) || count != 0 || !rows.closed {
				t.Errorf("Scan failure count=%d err=%v closed=%t, want no partial result and propagated sentinel", count, err, rows.closed)
			}
		})
	}
}
