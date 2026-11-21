package handler

import (
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventJSONPreservesAccountingReadinessProjection(t *testing.T) {
	now := timestamppb.New(time.Date(2026, time.October, 22, 0, 0, 0, 0, time.UTC))
	got := toEventJSON(&eventv1.EventDetail{
		Id:        "event-1",
		Date:      now,
		CreatedAt: now,
		Tiers: []*eventv1.TicketTier{
			{Id: "ready", AvailableQuantity: 2, AccountingReady: true},
			{Id: "unready", AvailableQuantity: 2, AccountingReady: false},
		},
	})

	tiers, ok := got["tiers"].([]gin.H)
	if !ok || len(tiers) != 2 {
		t.Fatalf("tiers projection = %#v", got["tiers"])
	}
	if tiers[0]["accounting_ready"] != true {
		t.Fatalf("ready tier accounting_ready = %#v", tiers[0]["accounting_ready"])
	}
	if tiers[1]["accounting_ready"] != false || tiers[1]["available_quantity"] != int32(2) {
		t.Fatalf("unready tier projection = %#v", tiers[1])
	}
}
