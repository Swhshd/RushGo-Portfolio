package handler

import (
	"testing"
	"time"

	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAdminDeliveryExceptionsProjection(t *testing.T) {
	eventID := uuid.NewString()
	now := time.Now().UTC()
	meta := adminEventTicketOperationsEventMetadata{eventID: eventID, eventAt: now}
	fixture := func() *bookingv1.GetAdminEventTicketOperationsResponse {
		return &bookingv1.GetAdminEventTicketOperationsResponse{EventId: eventID, ObservedAt: timestamppb.New(now), SourceRecordLimit: 5000, DeliveryExceptions: []*bookingv1.AdminEventAfterSalesException{{BookingId: uuid.NewString(), State: "CONFIRMED_TICKET_DELIVERY_PENDING", BookingStatus: "CONFIRMED", ExpectedTicketCount: 2, RecordedTicketCount: 0, IssuanceReceiptOutcome: "NOT_RECORDED", BookingCreatedAt: timestamppb.New(now.Add(-time.Hour))}}}
	}
	got, ok := projectAdminEventTicketOperationsResponse(fixture(), meta)
	if !ok || len(got.DeliveryExceptions) != 1 || got.DeliveryExceptions[0].ExpectedTicketCount != 2 {
		t.Fatal("durable delivery exception was not projected")
	}
	for name, mutate := range map[string]func(*bookingv1.GetAdminEventTicketOperationsResponse){
		"duplicate": func(s *bookingv1.GetAdminEventTicketOperationsResponse) {
			s.DeliveryExceptions = append(s.DeliveryExceptions, s.DeliveryExceptions[0])
		},
		"unsafe count": func(s *bookingv1.GetAdminEventTicketOperationsResponse) {
			s.DeliveryExceptions[0].ExpectedTicketCount = 9007199254740992
		},
		"untrusted reason": func(s *bookingv1.GetAdminEventTicketOperationsResponse) {
			s.DeliveryExceptions[0].IssuanceReasonCode = "provider response: secret"
		},
		"future fact": func(s *bookingv1.GetAdminEventTicketOperationsResponse) {
			s.DeliveryExceptions[0].BookingCreatedAt = timestamppb.New(now.Add(time.Hour))
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := fixture()
			mutate(s)
			if _, ok := projectAdminEventTicketOperationsResponse(s, meta); ok {
				t.Fatal("invalid downstream projection accepted")
			}
		})
	}
}
