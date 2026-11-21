package service

import (
	"context"
	"encoding/hex"
	"errors"
	"strconv"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/pkg/reliabilityhook"
)

// Observe the exact committed receipt in the isolated acceptance runtime.
func (p *PaymentService) observeW28Receipt(ctx context.Context, incoming domain.ProviderReceipt, stored *domain.ProviderReceipt, inserted bool) error {
	hook := p.receiptReliability
	if hook == nil || stored == nil || stored.ID == uuid.Nil ||
		stored.ID != incoming.ID || incoming.EventType != "payment_intent.succeeded" ||
		stored.EventType != incoming.EventType || stored.CorrelationState != "applied" {
		return nil
	}
	if configured, ok := hook.(interface{ Enabled() bool }); ok && !configured.Enabled() {
		return nil
	}
	checkpoint, relayState := "payment_w28_correlated_duplicate_receipt", "published"
	if inserted {
		checkpoint, relayState = "payment_w28_initial_receipt", "pending"
	}
	if stored.RelayState != relayState {
		return nil
	}
	o, accepted := stored.Observation, incoming.Observation
	if !o.Collected || o.LiveMode || accepted.LiveMode ||
		o.EvidenceHash == [32]byte{} || o.EvidenceHash != accepted.EvidenceHash ||
		o.Generation != 1 || o.Generation != accepted.Generation ||
		o.AmountCents != 100 || o.AmountCents != accepted.AmountCents ||
		o.Currency != "usd" || o.Currency != accepted.Currency ||
		o.PaymentID == uuid.Nil || o.PaymentID != accepted.PaymentID ||
		o.BookingID == uuid.Nil || o.BookingID != accepted.BookingID ||
		o.EventID == "" || o.EventID != accepted.EventID ||
		o.IntentID == "" || o.IntentID != accepted.IntentID ||
		o.Account == "" || o.Account != accepted.Account {
		return nil
	}
	decision, matched, err := hook.Await(ctx, reliabilityhook.Boundary{
		WindowID:    "saga.late_collection.compensation_verified_full_refund",
		OperationID: o.PaymentID.String(),
		Checkpoint:  checkpoint,
		Action:      reliabilityhook.ActionPause,
		Facts: map[string]string{
			"invocation_id":     uuid.NewString(),
			"receipt_id":        stored.ID.String(),
			"event_id":          o.EventID,
			"payment_intent_id": o.IntentID,
			"payment_id":        o.PaymentID.String(),
			"booking_id":        o.BookingID.String(),
			"account":           o.Account,
			"livemode":          "false",
			"correlation_state": stored.CorrelationState,
			"relay_state":       stored.RelayState,
			"inserted":          strconv.FormatBool(inserted),
			"evidence_sha256":   hex.EncodeToString(o.EvidenceHash[:]),
		},
	})
	if err != nil {
		return err
	}
	if matched && decision != reliabilityhook.DecisionContinue {
		return errors.New("invalid W28 receipt observation decision")
	}
	return nil
}
