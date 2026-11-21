package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/event/internal/repository"
	"github.com/ticketbox/event/internal/service"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readinessTierRepo struct {
	repository.TicketTierRepository
	tier  *domain.TicketTier
	ready bool
	err   error
}

type readinessEventRepo struct {
	repository.EventRepository
	event *domain.Event
	err   error
}

func (r *readinessEventRepo) GetByID(context.Context, uuid.UUID) (*domain.Event, error) {
	return r.event, r.err
}

func (r *readinessTierRepo) GetByID(context.Context, uuid.UUID) (*domain.TicketTier, error) {
	return r.tier, nil
}

func (r *readinessTierRepo) AccountingReadiness(context.Context, uuid.UUID) (bool, error) {
	return r.ready, r.err
}

func TestCatalogTiersCarryAccountingReadinessProjection(t *testing.T) {
	readyTier := &domain.TicketTier{ID: uuid.New(), EventID: uuid.New(), Name: "Ready", TotalQuantity: 2, AvailableQuantity: 2}
	unreadyTier := &domain.TicketTier{ID: uuid.New(), EventID: readyTier.EventID, Name: "Unready", TotalQuantity: 2, AvailableQuantity: 2}
	event := &domain.Event{ID: readyTier.EventID, Title: "Catalog", Tiers: []domain.TicketTier{*readyTier, *unreadyTier}}

	detail := toEventDetail(event, map[uuid.UUID]bool{readyTier.ID: true})
	if len(detail.Tiers) != 2 {
		t.Fatalf("tiers = %d", len(detail.Tiers))
	}
	if !detail.Tiers[0].AccountingReady || detail.Tiers[1].AccountingReady {
		t.Fatalf("catalog readiness leaked: %+v", detail.Tiers)
	}
	if detail.Tiers[1].AvailableQuantity != 2 {
		t.Fatal("unready tier must not be rewritten into a sold-out tier")
	}

	missing := toEventDetail(event, map[uuid.UUID]bool{})
	for _, tier := range missing.Tiers {
		if tier.AccountingReady {
			t.Fatal("a tier missing from the readiness projection must not be ready")
		}
	}
}

func TestTicketAvailabilityCarriesAccountingReadiness(t *testing.T) {
	eventID := uuid.New()
	events := &readinessEventRepo{event: &domain.Event{ID: eventID, PublicationStatus: domain.PublicationPublished}}
	tier := &domain.TicketTier{ID: uuid.New(), EventID: eventID, PriceCents: 725, AvailableQuantity: 1, Version: 3}
	for _, ready := range []bool{true, false} {
		repo := &readinessTierRepo{tier: tier, ready: ready}
		server := NewEventServer(service.NewEventService(events, repo, nil, zap.NewNop()), zap.NewNop())
		res, err := server.GetTicketAvailability(context.Background(), &eventv1.GetTicketAvailabilityRequest{TierId: tier.ID.String()})
		if err != nil {
			t.Fatal(err)
		}
		if res.AccountingReady != ready || res.TierId != tier.ID.String() || res.PriceCents != 725 || res.AvailableQuantity != 1 || res.Version != 3 {
			t.Fatalf("readiness projection mismatch: %+v", res)
		}
	}
	repo := &readinessTierRepo{tier: tier, err: errors.New("database unavailable")}
	server := NewEventServer(service.NewEventService(events, repo, nil, zap.NewNop()), zap.NewNop())
	if _, err := server.GetTicketAvailability(context.Background(), &eventv1.GetTicketAvailabilityRequest{TierId: tier.ID.String()}); status.Code(err) != codes.Unavailable {
		t.Fatalf("readiness authority failure mapped to %v", status.Code(err))
	}
}
