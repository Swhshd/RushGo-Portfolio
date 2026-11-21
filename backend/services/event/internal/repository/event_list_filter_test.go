package repository

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type eventListQueryCapture struct {
	postgresPool
	countQuery string
	countArgs  []any
	listQuery  string
	listArgs   []any
}

func (p *eventListQueryCapture) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	p.countQuery = query
	p.countArgs = append([]any(nil), args...)
	return iterationRow{values: []any{0}}
}

func (p *eventListQueryCapture) Query(_ context.Context, query string, args ...any) (pgx.Rows, error) {
	p.listQuery = query
	p.listArgs = append([]any(nil), args...)
	return &iterationRows{}, nil
}

func TestEventListQueryValidationAppliesBoundsAndDefaults(t *testing.T) {
	from := time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 0, 1)
	minPrice, maxPrice := int64(0), MaxEventFilterPriceCents
	filter, page, pageSize, err := NormalizeEventListQuery(EventListFilter{
		DateFrom: &from, DateToExclusive: &to, Location: "  Seattle  ", MinPriceCents: &minPrice, MaxPriceCents: &maxPrice,
	}, 0, 0)
	if err != nil || page != 1 || pageSize != DefaultEventListPageSize || filter.Sort != "date_asc" || filter.Location != "Seattle" {
		t.Fatalf("normalized query=%+v page=%d size=%d err=%v", filter, page, pageSize, err)
	}
	for _, tc := range []struct {
		name     string
		filter   EventListFilter
		page     int
		pageSize int
	}{
		{name: "page zero is default only when omitted", page: -1, pageSize: 20},
		{name: "page upper bound", page: MaxEventListPage + 1, pageSize: 20},
		{name: "page size upper bound", page: 1, pageSize: MaxEventListPageSize + 1},
		{name: "unknown sort", filter: EventListFilter{Sort: "price_desc; DROP TABLE events"}, page: 1, pageSize: 20},
		{name: "date range reversed", filter: EventListFilter{DateFrom: &to, DateToExclusive: &from}, page: 1, pageSize: 20},
		{name: "price range reversed", filter: EventListFilter{MinPriceCents: &maxPrice, MaxPriceCents: &minPrice}, page: 1, pageSize: 20},
		{name: "negative minimum price", filter: EventListFilter{MinPriceCents: ptrInt64(-1)}, page: 1, pageSize: 20},
		{name: "price over supported bound", filter: EventListFilter{MaxPriceCents: ptrInt64(MaxEventFilterPriceCents + 1)}, page: 1, pageSize: 20},
		{name: "location over database bound", filter: EventListFilter{Location: strings.Repeat("x", 501)}, page: 1, pageSize: 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := NormalizeEventListQuery(tc.filter, tc.page, tc.pageSize); err == nil {
				t.Fatal("invalid list query accepted")
			}
		})
	}
}

func TestListFilteredBuildsParameterizedBoundedQueriesWithAllowlistedSort(t *testing.T) {
	from := time.Date(2027, time.March, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 0, 4)
	minPrice, maxPrice := int64(1000), int64(2500)
	filter := EventListFilter{
		Category: "concert' OR TRUE --", Search: "jazz", DateFrom: &from, DateToExclusive: &to,
		Location: "Seattle", MinPriceCents: &minPrice, MaxPriceCents: &maxPrice, Sort: "price_desc",
	}
	pool := &eventListQueryCapture{}
	if _, _, err := (&PostgresEventRepository{pool: pool}).ListFiltered(context.Background(), filter, 3, 50); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"e.publication_status = 'published'", "e.category = $1", "e.title ILIKE $2", "e.date >= $3", "e.date < $4",
		"e.location ILIKE $5", "EXISTS (SELECT 1 FROM ticket_tiers AS price_tier", "price_tier.price_cents >= $6", "price_tier.price_cents <= $7",
	} {
		if !strings.Contains(pool.countQuery, fragment) {
			t.Errorf("count query missing %q: %s", fragment, pool.countQuery)
		}
	}
	if strings.Contains(pool.countQuery, filter.Category) {
		t.Fatal("user category was interpolated into SQL instead of bound as a parameter")
	}
	if len(pool.countArgs) != 7 || pool.countArgs[0] != filter.Category || pool.countArgs[1] != "%jazz%" || pool.countArgs[5] != minPrice || pool.countArgs[6] != maxPrice {
		t.Fatalf("parameterized count args=%#v", pool.countArgs)
	}
	for _, fragment := range []string{"ORDER BY (SELECT MIN(sort_tier.price_cents)", "DESC NULLS LAST", "LIMIT $8 OFFSET $9"} {
		if !strings.Contains(pool.listQuery, fragment) {
			t.Errorf("paged sort query missing %q: %s", fragment, pool.listQuery)
		}
	}
	if len(pool.listArgs) != 9 || pool.listArgs[7] != 50 || pool.listArgs[8] != 100 {
		t.Fatalf("bounded pagination args=%#v", pool.listArgs)
	}
	if !strings.Contains(pool.listQuery, "EXISTS (SELECT 1 FROM ticket_tiers AS price_tier") {
		t.Fatal("price range must filter in SQL by an actual matching tier")
	}
}

func ptrInt64(value int64) *int64 { return &value }
