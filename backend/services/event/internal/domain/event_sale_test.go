package domain

import (
	"testing"
	"time"
)

func TestSaleEligibilityRequiresPublishedOpenWindowAndFutureEvent(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	start := now.Add(time.Hour)
	for _, tc := range []struct {
		name   string
		event  Event
		wantOK bool
		want   string
	}{
		{name: "draft", event: Event{PublicationStatus: PublicationDraft, SalesStatus: SalesSelling, Date: now.Add(24 * time.Hour)}, want: "event_not_published"},
		{name: "paused", event: Event{PublicationStatus: PublicationPublished, SalesStatus: SalesPaused, Date: now.Add(24 * time.Hour)}, want: "sales_paused"},
		{name: "cancelled", event: Event{PublicationStatus: PublicationPublished, SalesStatus: SalesCancelled, Date: now.Add(24 * time.Hour)}, want: "event_cancelled"},
		{name: "before sales start", event: Event{PublicationStatus: PublicationPublished, SalesStatus: SalesScheduled, SalesStartAt: &start, Date: now.Add(24 * time.Hour)}, want: "sales_not_started"},
		{name: "scheduled without a start stays closed", event: Event{PublicationStatus: PublicationPublished, SalesStatus: SalesScheduled, Date: now.Add(24 * time.Hour)}, want: "sales_not_started"},
		{name: "at sales start", event: Event{PublicationStatus: PublicationPublished, SalesStatus: SalesScheduled, SalesStartAt: &now, Date: now.Add(time.Hour)}, wantOK: true},
		{name: "at sales end", event: Event{PublicationStatus: PublicationPublished, SalesStatus: SalesSelling, SalesEndAt: &now, Date: now.Add(time.Hour)}, want: "sales_ended"},
		{name: "at event start", event: Event{PublicationStatus: PublicationPublished, SalesStatus: SalesSelling, Date: now}, want: "event_ended"},
		{name: "unknown state fails closed", event: Event{PublicationStatus: "unknown", SalesStatus: SalesSelling, Date: now.Add(time.Hour)}, want: "sale_state_unknown"},
		{name: "missing event date fails closed", event: Event{PublicationStatus: PublicationPublished, SalesStatus: SalesSelling}, want: "event_date_unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotOK, got := EventSaleEligibility(&tc.event, now)
			if gotOK != tc.wantOK || got != tc.want {
				t.Fatalf("EventSaleEligibility() = (%v, %q), want (%v, %q)", gotOK, got, tc.wantOK, tc.want)
			}
		})
	}
}

func TestSaleEligibilityPreservesLegacyActiveEvent(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	event := Event{Status: "active", Date: now.Add(time.Hour)}
	if ok, reason := EventSaleEligibility(&event, now); !ok || reason != "" {
		t.Fatalf("legacy active event eligibility = (%v, %q), want (true, empty)", ok, reason)
	}
}
