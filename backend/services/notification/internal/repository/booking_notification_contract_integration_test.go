package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/notification/internal/domain"
)

func TestBookingIssuanceOutboxPayloadParsesAsBusinessNotificationV1Integration(t *testing.T) {
	pool := businessNotificationIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	schema := "booking_notification_contract_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := pool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	bookingURL, err := url.Parse(pool.Config().ConnString())
	if err != nil {
		t.Fatal("parse isolated local database URL")
	}
	query := bookingURL.Query()
	query.Set("search_path", schema+",public")
	bookingURL.RawQuery = query.Encode()
	output, err := exec.CommandContext(ctx, "migrate", "-path", "../../../booking/migrations", "-database", bookingURL.String(), "up").CombinedOutput()
	if err != nil {
		t.Fatalf("apply Booking migrations in test-owned schema: %s", strings.ReplaceAll(string(output), bookingURL.String(), "[isolated Booking schema]"))
	}
	bookingPool, err := pgxpool.New(ctx, bookingURL.String())
	if err != nil {
		t.Fatal("open Booking-scoped test connection")
	}
	t.Cleanup(bookingPool.Close)

	bookingID, userID, eventID := uuid.New(), uuid.New(), uuid.New()
	receiptEventID, sagaID, paymentID := uuid.New(), uuid.New(), uuid.New()
	if _, err := bookingPool.Exec(ctx, `INSERT INTO `+schema+`.bookings
		(id,user_id,event_id,status,total_amount_cents,managed,lifecycle_generation,lifecycle_phase,currency,booking_mode,reservation_deadline)
		VALUES($1,$2,$3,'PENDING',72500,true,1,'OPEN','usd','pessimistic',clock_timestamp()+interval '1 hour')`, bookingID, userID, eventID); err != nil {
		t.Fatalf("create managed booking fixture: %v", err)
	}
	if _, err := bookingPool.Exec(ctx, `INSERT INTO `+schema+`.ticket_issuance_receipts
		(event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation,outcome,reason_code,dispute_state,refund_state,evidence_source,observed_at,ticket_count)
		VALUES($1,$2,$3,$4,$5,1,'issued','issued','provider_observed_no_dispute','provider_observed_no_refund','stripe_test_api',clock_timestamp(),1)`,
		receiptEventID, sagaID, bookingID, paymentID, uuid.New()); err != nil {
		t.Fatalf("insert issued receipt and execute Booking outbox trigger: %v", err)
	}
	var raw []byte
	if err := bookingPool.QueryRow(ctx, `SELECT payload FROM `+schema+`.outbox WHERE event_type='BusinessNotificationV1' AND payload->>'booking_id'=$1`, bookingID.String()).Scan(&raw); err != nil {
		t.Fatal("read authoritative Booking outbox payload", err)
	}
	event, _, err := domain.ParseBusinessNotificationV1(raw)
	if err != nil {
		t.Fatalf("Notification rejected actual Booking outbox payload %s: %v", raw, err)
	}
	if event.BookingID != bookingID || event.UserID != userID || event.EventID != eventID || event.Currency != "USD" || event.Total != 72500 || event.Type != "tickets_issued" {
		t.Fatalf("parsed Booking contract = %+v", event)
	}
}

func TestManagedLegacyRecipientReplayPreservesFirstSnapshotAndOldHashIntegration(t *testing.T) {
	pool := businessNotificationIntegrationPool(t)
	repo := NewPostgresNotificationRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bookingID, userID, eventID := uuid.New(), uuid.New(), uuid.New()
	input := func(email string, total int) []byte {
		raw, err := json.Marshal(map[string]any{"booking_id": bookingID.String(), "user_id": userID.String(), "event_id": eventID.String(), "total": total, "email": email, "currency": "USD"})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	firstKey, first, firstHash, err := domain.PrepareIntent("booking_confirmation", input("old@example.test", 72500))
	if err != nil {
		t.Fatal(err)
	}
	if existing, created, err := repo.AcceptIntent(ctx, firstKey, first, firstHash); err != nil || !created || existing.Recipient != "old@example.test" {
		t.Fatalf("first recipient acceptance: created=%v existing=%+v err=%v", created, existing, err)
	}
	newKey, replay, replayHash, err := domain.PrepareIntent("booking_confirmation", input("new@example.test", 72500))
	if err != nil {
		t.Fatal(err)
	}
	if existing, created, err := repo.AcceptIntent(ctx, newKey, replay, replayHash); err != nil || created || existing.Recipient != "old@example.test" {
		t.Fatalf("recipient refresh changed a durable semantic intent: created=%v existing=%+v err=%v", created, existing, err)
	}
	changedKey, changed, changedHash, err := domain.PrepareIntent("booking_confirmation", input("new@example.test", 72501))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.AcceptIntent(ctx, changedKey, changed, changedHash); err != ErrSemanticConflict {
		t.Fatalf("changed persisted business total conflict=%v, want ErrSemanticConflict", err)
	}

	// Seed a row in the exact old format: payload contains email and the hash
	// covers both the recipient and payload. Replays remain compatible while the
	// new implementation computes comparisons from immutable business fields.
	legacyBookingID := uuid.New()
	legacyInput, _ := json.Marshal(map[string]any{"booking_id": legacyBookingID.String(), "user_id": userID.String(), "event_id": eventID.String(), "total": 9100, "currency": "USD", "email": "legacy@example.test"})
	legacyKey, canonical, _, err := domain.PrepareIntent("booking_confirmation", legacyInput)
	if err != nil {
		t.Fatal(err)
	}
	oldPayload := make(map[string]any, len(canonical.Payload)+1)
	for key, value := range canonical.Payload {
		oldPayload[key] = value
	}
	oldPayload["email"] = "legacy@example.test"
	oldHashJSON, err := json.Marshal(struct {
		Recipient string         `json:"recipient"`
		Payload   map[string]any `json:"payload"`
	}{"legacy@example.test", oldPayload})
	if err != nil {
		t.Fatal(err)
	}
	oldHash := sha256.Sum256(oldHashJSON)
	oldPayloadJSON, err := json.Marshal(oldPayload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO notifications(id,type,recipient,channel,locale,payload,status,created_at,managed,entity_id,transition_generation,payload_hash,delivery_id,delivery_fence,delivery_due_at,delivery_attempts)
		VALUES($1,$2,$3,'email','en',$4,'PENDING',clock_timestamp(),true,$5,1,$6,$1,0,clock_timestamp(),0)`,
		canonical.ID, legacyKey.Type, "legacy@example.test", oldPayloadJSON, legacyKey.EntityID, oldHash[:]); err != nil {
		t.Fatalf("seed historical semantic hash format: %v", err)
	}
	legacyReplayKey, legacyReplay, legacyReplayHash, err := domain.PrepareIntent("booking_confirmation", []byte(strings.ReplaceAll(string(legacyInput), "legacy@example.test", "changed@example.test")))
	if err != nil {
		t.Fatal(err)
	}
	if existing, created, err := repo.AcceptIntent(ctx, legacyReplayKey, legacyReplay, legacyReplayHash); err != nil || created || existing.ID != canonical.ID || existing.Recipient != "legacy@example.test" {
		t.Fatalf("old hash compatibility/frozen recipient replay: created=%v existing=%+v err=%v", created, existing, err)
	}
}
