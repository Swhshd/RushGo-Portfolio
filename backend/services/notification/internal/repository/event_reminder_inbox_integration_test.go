package repository

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/pkg/eventreminder"
)

func TestMixedBookingAndEventReminderInboxProjectionIntegration(t *testing.T) {
	pool := finalMVPNotificationIntegrationPool(t)
	repo := NewPostgresNotificationRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	userID, eventID := uuid.New(), uuid.New()

	booking, bookingHash, bookingTemplate := businessNotificationFixture(t, userID, eventID)
	if created, err := repo.AcceptBusinessNotification(ctx, booking, bookingHash, bookingTemplate, "", false); err != nil || !created {
		t.Fatalf("persist booking notification: created=%v err=%v", created, err)
	}

	envelope := &eventreminder.Envelope{
		Version: 1, NotificationID: uuid.New(), SubscriptionID: uuid.New(), SubscriptionVersion: 1,
		Action: eventreminder.ActionTrigger, Type: eventreminder.TypeOnSale, UserID: userID,
		EventID: eventID, EventTitle: "Synthetic event", EventDate: time.Now().UTC().Add(24 * time.Hour),
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	parsed, reminderHash, err := domain.ParseEventReminder(raw)
	if err != nil {
		t.Fatal(err)
	}
	reminderTemplate, err := domain.RenderEventReminder(parsed, "en")
	if err != nil {
		t.Fatal(err)
	}
	if created, err := repo.AcceptEventReminder(ctx, parsed, reminderHash, reminderTemplate, "", false); err != nil || !created {
		t.Fatalf("persist event reminder: created=%v err=%v", created, err)
	}
	conflicting := *envelope
	conflicting.NotificationID = uuid.New()
	conflicting.SubscriptionVersion++
	conflicting.UserID = uuid.New()
	conflictingRaw, err := json.Marshal(&conflicting)
	if err != nil {
		t.Fatal(err)
	}
	conflictingParsed, conflictingHash, err := domain.ParseEventReminder(conflictingRaw)
	if err != nil {
		t.Fatal(err)
	}
	conflictCreated, conflictErr := repo.AcceptEventReminder(ctx, conflictingParsed, conflictingHash, reminderTemplate, "", false)
	if conflictCreated || !errors.Is(conflictErr, ErrSemanticConflict) {
		t.Fatalf("newer reminder revision changed subscription owner: created=%v err=%v", conflictCreated, conflictErr)
	}

	page, err := repo.ListInbox(ctx, userID, 1, 20, false)
	if err != nil || page.Total != 2 || len(page.Items) != 2 {
		t.Fatalf("mixed inbox page=%+v err=%v", page, err)
	}
	var foundBooking, foundReminder bool
	for _, item := range page.Items {
		switch item.Type {
		case "tickets_issued":
			foundBooking = item.BookingID == booking.BookingID && item.EventID == eventID
		case eventreminder.TypeOnSale:
			foundReminder = item.BookingID == uuid.Nil && item.EventID == eventID
		}
	}
	if !foundBooking || !foundReminder {
		t.Fatalf("mixed inbox lost booking/event identity: %+v", page.Items)
	}
}

func TestOutOfOrderTriggerUsesAuthoritativeActivationFloorIntegration(t *testing.T) {
	pool := finalMVPNotificationIntegrationPool(t)
	repo := NewPostgresNotificationRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	userID, eventID, subscriptionID := uuid.New(), uuid.New(), uuid.New()
	base := &eventreminder.Envelope{
		Version: 1, SubscriptionID: subscriptionID, UserID: userID, EventID: eventID,
		Type: eventreminder.TypeOnSale, EventTitle: "Synthetic event", EventDate: time.Now().UTC().Add(24 * time.Hour),
	}

	// A trigger from the new activation arrives before its resubscribe control.
	// Its activation floor must fence a late pre-withdrawal trigger even though
	// Notification has not yet observed the resubscribe message.
	trigger := *base
	trigger.NotificationID = uuid.New()
	trigger.SubscriptionVersion = 5
	trigger.ActiveSinceVersion = 4
	trigger.Action = eventreminder.ActionTrigger
	acceptReminderFixture(t, ctx, repo, &trigger, true)

	withdrawn := *base
	withdrawn.NotificationID = uuid.New()
	withdrawn.SubscriptionVersion = 3
	withdrawn.ActiveSinceVersion = 1
	withdrawn.Action = eventreminder.ActionWithdrawn
	acceptReminderFixture(t, ctx, repo, &withdrawn, false)

	staleTrigger := *base
	staleTrigger.NotificationID = uuid.New()
	staleTrigger.SubscriptionVersion = 2
	staleTrigger.ActiveSinceVersion = 1
	staleTrigger.Action = eventreminder.ActionTrigger
	acceptReminderFixture(t, ctx, repo, &staleTrigger, false)

	page, err := repo.ListInbox(ctx, userID, 1, 20, false)
	if err != nil || page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != trigger.NotificationID {
		t.Fatalf("out-of-order trigger inbox page=%+v err=%v, want only version-5 trigger", page, err)
	}
	var version, activeSince int64
	if err := pool.QueryRow(ctx, `SELECT subscription_version,active_since_version FROM notification_event_reminder_state WHERE subscription_id=$1`, subscriptionID).Scan(&version, &activeSince); err != nil {
		t.Fatal(err)
	}
	if version != 5 || activeSince != 4 {
		t.Fatalf("version fence version=%d active_since=%d, want 5 and 4", version, activeSince)
	}
}

func acceptReminderFixture(t *testing.T, ctx context.Context, repo *PostgresNotificationRepository, envelope *eventreminder.Envelope, hasTrigger bool) {
	t.Helper()
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	parsed, hash, err := domain.ParseEventReminder(raw)
	if err != nil {
		t.Fatalf("parse %s version %d: %v", envelope.Action, envelope.SubscriptionVersion, err)
	}
	template := domain.InboxTemplate{Locale: "en"}
	if envelope.Action == eventreminder.ActionTrigger {
		template, err = domain.RenderEventReminder(parsed, "en")
		if err != nil {
			t.Fatal(err)
		}
	}
	created, err := repo.AcceptEventReminder(ctx, parsed, hash, template, "", false)
	if err != nil || created != hasTrigger {
		t.Fatalf("accept %s version %d: created=%v err=%v, want %v", envelope.Action, envelope.SubscriptionVersion, created, err, hasTrigger)
	}
}

func finalMVPNotificationIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("FINAL_MVP_NOTIFICATION_DATABASE_URL")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "postgres" || parsed.Host != "127.0.0.1:55483" ||
		!strings.HasPrefix(strings.Trim(parsed.Path, "/"), "rushgo_final_notification_") || parsed.User == nil {
		t.Fatal("FINAL_MVP_NOTIFICATION_DATABASE_URL must point to the authorized isolated local final-MVP Notification database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal("connect to authorized isolated local Notification database")
	}
	schema := "event_reminder_inbox_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal("create isolated Notification test schema")
	}
	t.Cleanup(func() {
		_, dropErr := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if dropErr != nil {
			t.Errorf("drop isolated Notification test schema: %v", dropErr)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	if output, err := exec.CommandContext(ctx, "migrate", "-path", "../../migrations", "-database", parsed.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("apply isolated Notification migrations: %s", strings.ReplaceAll(string(output), parsed.String(), "[isolated DSN]"))
	}
	config, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		t.Fatal("parse isolated Notification test URL")
	}
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("connect to isolated Notification test schema")
	}
	t.Cleanup(pool.Close)
	return pool
}
