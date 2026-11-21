package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/event/internal/service"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type filteredEventRepositoryStub struct {
	repository.EventRepository
	publicFilter repository.EventListFilter
	adminFilter  repository.EventListFilter
	page         int
	pageSize     int
	publicCalls  int
	adminCalls   int
}

func (r *filteredEventRepositoryStub) ListFiltered(_ context.Context, filter repository.EventListFilter, page, pageSize int) ([]*domain.Event, int, error) {
	r.publicFilter, r.page, r.pageSize = filter, page, pageSize
	r.publicCalls++
	return nil, 0, nil
}

func (r *filteredEventRepositoryStub) ListAdminFiltered(_ context.Context, filter repository.EventListFilter, page, pageSize int) ([]*domain.Event, int, error) {
	r.adminFilter, r.page, r.pageSize = filter, page, pageSize
	r.adminCalls++
	return nil, 0, nil
}

func TestEventListRPCMapsAdvancedFiltersAndPaginationDefaults(t *testing.T) {
	repo := &filteredEventRepositoryStub{}
	server := NewEventServer(service.NewEventService(repo, nil, nil, nil), nil)
	from := time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 0, 7)
	minPrice, maxPrice := int64(0), int64(10000)
	response, err := server.ListEvents(context.Background(), &eventv1.ListEventsRequest{
		Category: "concert", Search: "live", DateFrom: timestamppb.New(from), DateToExclusive: timestamppb.New(to),
		Location: "Seattle", MinPriceCents: &minPrice, MaxPriceCents: &maxPrice, Sort: "price_asc",
	})
	if err != nil || response.GetPage() != 1 || response.GetPageSize() != repository.DefaultEventListPageSize || repo.publicCalls != 1 {
		t.Fatalf("RPC response=%+v calls=%d err=%v", response, repo.publicCalls, err)
	}
	if repo.publicFilter.Category != "concert" || repo.publicFilter.Search != "live" || repo.publicFilter.Location != "Seattle" || repo.publicFilter.Sort != "price_asc" ||
		repo.publicFilter.DateFrom == nil || !repo.publicFilter.DateFrom.Equal(from) || repo.publicFilter.DateToExclusive == nil || !repo.publicFilter.DateToExclusive.Equal(to) ||
		repo.publicFilter.MinPriceCents == nil || *repo.publicFilter.MinPriceCents != 0 || repo.publicFilter.MaxPriceCents == nil || *repo.publicFilter.MaxPriceCents != maxPrice {
		t.Fatalf("mapped public filter=%+v", repo.publicFilter)
	}
	adminResponse, err := server.ListAdminEvents(context.Background(), &eventv1.ListEventsRequest{Category: "concert", Sort: "date_desc", Page: 2, PageSize: 10})
	if err != nil || adminResponse.GetPage() != 2 || repo.adminCalls != 1 || repo.adminFilter.Sort != "date_desc" || repo.page != 2 || repo.pageSize != 10 {
		t.Fatalf("admin response=%+v filter=%+v calls=%d err=%v", adminResponse, repo.adminFilter, repo.adminCalls, err)
	}
}

func TestEventListRPCRejectsInvalidBoundariesBeforeRepository(t *testing.T) {
	repo := &filteredEventRepositoryStub{}
	server := NewEventServer(service.NewEventService(repo, nil, nil, nil), nil)
	invalidPrice := int64(-1)
	for _, req := range []*eventv1.ListEventsRequest{
		nil,
		{Page: 1001, PageSize: 20},
		{Page: 1, PageSize: 51},
		{Sort: "price_desc;DROP TABLE events"},
		{MinPriceCents: &invalidPrice},
		{DateFrom: &timestamppb.Timestamp{Seconds: 1 << 40}},
	} {
		if _, err := server.ListEvents(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid public list request status=%s err=%v req=%+v", status.Code(err), err, req)
		}
	}
	if repo.publicCalls != 0 {
		t.Fatalf("invalid query reached repository %d times", repo.publicCalls)
	}
}
