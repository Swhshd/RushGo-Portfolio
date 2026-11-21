package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/event/internal/domain"
)

func TestEventListFiltersInPostgresBeforePaginationIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	repo := NewPostgresEventRepository(pool)
	ctx := context.Background()
	seattle := "Seattle Hall"
	concert := "concert"
	alpha := insertCatalogEvent(t, pool, "Concert Alpha", concert, seattle, time.Date(2027, time.March, 1, 12, 0, 0, 0, time.UTC), "published", 1000, 3000)
	beta := insertCatalogEvent(t, pool, "Concert Beta", concert, seattle, time.Date(2027, time.March, 2, 12, 0, 0, 0, time.UTC), "published", 5000)
	gamma := insertCatalogEvent(t, pool, "Concert Gamma", concert, seattle, time.Date(2027, time.February, 28, 12, 0, 0, 0, time.UTC), "published", 500)
	draft := insertCatalogEvent(t, pool, "Concert Draft", concert, seattle, time.Date(2027, time.March, 2, 12, 0, 0, 0, time.UTC), "draft", 2500)
	noTiers := insertCatalogEvent(t, pool, "Concert No Tiers", concert, seattle, time.Date(2027, time.March, 3, 12, 0, 0, 0, time.UTC), "published")
	delta := insertCatalogEvent(t, pool, "Concert Delta", concert, "Portland Hall", time.Date(2027, time.March, 1, 13, 0, 0, 0, time.UTC), "published", 2000)
	splitRange := insertCatalogEvent(t, pool, "Concert Split", concert, seattle, time.Date(2027, time.March, 2, 13, 0, 0, 0, time.UTC), "published", 750, 5000)

	from := time.Date(2027, time.March, 1, 0, 0, 0, 0, time.UTC)
	toExclusive := time.Date(2027, time.March, 4, 0, 0, 0, 0, time.UTC)
	minPrice, maxPrice := int64(2000), int64(4000)
	filtered, total, err := repo.ListFiltered(ctx, EventListFilter{
		Category: "concert", Search: "concert", DateFrom: &from, DateToExclusive: &toExclusive,
		Location: "sEaTtLe", MinPriceCents: &minPrice, MaxPriceCents: &maxPrice, Sort: "date_asc",
	}, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(filtered) != 1 || filtered[0].ID != alpha {
		t.Fatalf("combined filters returned total=%d events=%v, want only tier-matching published Alpha", total, catalogIDs(filtered))
	}

	priceAsc, total, err := repo.ListFiltered(ctx, EventListFilter{Category: "concert", Sort: "price_asc"}, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if total != 6 || len(priceAsc) != 2 || priceAsc[0].ID != gamma || priceAsc[1].ID != splitRange {
		t.Fatalf("price ascending first page total=%d events=%v", total, catalogIDs(priceAsc))
	}
	secondPage, secondTotal, err := repo.ListFiltered(ctx, EventListFilter{Category: "concert", Sort: "price_asc"}, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if secondTotal != 6 || len(secondPage) != 2 || secondPage[0].ID != alpha || secondPage[1].ID != delta {
		t.Fatalf("price ascending second page total=%d events=%v", secondTotal, catalogIDs(secondPage))
	}
	priceDesc, descTotal, err := repo.ListFiltered(ctx, EventListFilter{Category: "concert", Sort: "price_desc"}, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if descTotal != 6 || len(priceDesc) != 2 || priceDesc[0].ID != beta || priceDesc[1].ID != delta {
		t.Fatalf("price descending first page total=%d events=%v", descTotal, catalogIDs(priceDesc))
	}
	adminEvents, adminTotal, err := repo.ListAdminFiltered(ctx, EventListFilter{Category: "concert"}, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if adminTotal != 7 || !containsCatalogID(adminEvents, draft) {
		t.Fatalf("admin list must retain the private event: total=%d events=%v", adminTotal, catalogIDs(adminEvents))
	}
	if containsCatalogID(filtered, draft) || containsCatalogID(filtered, noTiers) {
		t.Fatal("public filtered list exposed a draft or a price range without a matching ticket tier")
	}
}

func insertCatalogEvent(t *testing.T, pool *pgxpool.Pool, title, category, location string, date time.Time, publicationStatus string, prices ...int64) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := pool.Exec(context.Background(), `INSERT INTO events(id,title,description,category,venue,location,date,status,publication_status,sales_status)
		VALUES($1,$2,'catalog test event',$3,'catalog venue',$4,$5,'active',$6,'selling')`, id, title, category, location, date, publicationStatus); err != nil {
		t.Fatal(err)
	}
	for index, price := range prices {
		if _, err := pool.Exec(context.Background(), `INSERT INTO ticket_tiers(id,event_id,name,price_cents,total_quantity,available_quantity)
			VALUES($1,$2,$3,$4,10,10)`, uuid.New(), id, "Tier "+string(rune('A'+index)), price); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func catalogIDs(events []*domain.Event) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.ID)
	}
	return ids
}

func containsCatalogID(events []*domain.Event, id uuid.UUID) bool {
	for _, event := range events {
		if event.ID == id {
			return true
		}
	}
	return false
}
