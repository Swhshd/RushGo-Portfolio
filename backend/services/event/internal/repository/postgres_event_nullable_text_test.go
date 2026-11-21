package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A real PostgreSQL NULL must not make an Event disappear or break list/detail
// reads; populated values must survive the same repository boundary unchanged.
func TestEventNullableTextReadsIntegration(t *testing.T) {
	pool := newEventNullableTextFixture(t)
	repo := NewPostgresEventRepository(pool)
	ctx := context.Background()
	cases := []struct {
		name        string
		image       any
		description any
		wantImage   string
		wantDesc    string
	}{
		{name: "null_image", image: nil, description: "image omitted", wantImage: "", wantDesc: "image omitted"},
		{name: "non_null", image: "https://example.test/event.jpg", description: "populated description", wantImage: "https://example.test/event.jpg", wantDesc: "populated description"},
		{name: "null_description", image: "https://example.test/event.jpg", description: nil, wantImage: "https://example.test/event.jpg", wantDesc: ""},
		{name: "both_null", image: nil, description: nil, wantImage: "", wantDesc: ""},
		{name: "empty_strings", image: "", description: "", wantImage: "", wantDesc: ""},
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := uuid.New()
			title := "nullable fixture " + tc.name
			date := time.Date(2030, 1, index+1, 0, 0, 0, 0, time.UTC)
			_, err := pool.Exec(ctx, `INSERT INTO events (id,title,description,category,venue,location,date,image_url,publication_status)
				VALUES ($1,$2,$3,$4,'fixture venue','fixture location',$5,$6,'published')`, id, title, tc.description, tc.name, date, tc.image)
			if err != nil {
				t.Fatal(err)
			}
			// Include a tier so reading the Event also exercises its normal assembly.
			if _, err := pool.Exec(ctx, `INSERT INTO ticket_tiers (id,event_id,name,price_cents,total_quantity,available_quantity)
				VALUES ($1,$2,'General',1500,10,10)`, uuid.New(), id); err != nil {
				t.Fatal(err)
			}
			t.Run("GetByID", func(t *testing.T) {
				event, err := repo.GetByID(ctx, id)
				if err != nil {
					t.Fatalf("GetByID must accept nullable Event text: %v", err)
				}
				if event.ID != id || event.Title != title || event.ImageURL != tc.wantImage || event.Description != tc.wantDesc || len(event.Tiers) != 1 {
					t.Fatal("detail read did not preserve Event values/tier or map NULL to existing empty-string representation")
				}
			})
			for _, filter := range []struct {
				name     string
				category string
				search   string
			}{
				{name: "List_category", category: tc.name},
				{name: "List_search", search: title},
				{name: "List_category_search", category: tc.name, search: title},
			} {
				t.Run(filter.name, func(t *testing.T) {
					events, total, err := repo.List(ctx, filter.category, filter.search, 1, 10)
					if err != nil {
						t.Fatalf("List must accept nullable Event text: %v", err)
					}
					if total != 1 || len(events) != 1 || events[0].ID != id || events[0].ImageURL != tc.wantImage || events[0].Description != tc.wantDesc || len(events[0].Tiers) != 1 {
						t.Fatal("list read dropped or changed the nullable/non-null Event")
					}
				})
			}
		})
	}
	for _, page := range []int{1, 2, 3} {
		events, total, err := repo.List(ctx, "", "", page, 2)
		if err != nil {
			t.Errorf("unfiltered paginated mixed-null List page %d: %v", page, err)
			continue
		}
		wantCount := 2
		if page == 3 {
			wantCount = 1
		}
		if total != len(cases) || len(events) != wantCount {
			t.Errorf("mixed-null List page %d: total=%d count=%d, want %d/%d", page, total, len(events), len(cases), wantCount)
		}
	}
	if _, err := repo.GetByID(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing Event must retain ErrNotFound, got %v", err)
	}
}

func newEventNullableTextFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	// The NULL regression must exercise the currently shipped schema.
	return eventLifecycleFixture(t)
}
