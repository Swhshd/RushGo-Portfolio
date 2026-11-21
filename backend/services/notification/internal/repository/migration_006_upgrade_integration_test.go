package repository

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestNotificationMigration006PreservesBookingRowsAndAllowsReminderDelivery(t *testing.T) {
	pool, migrationURL := notificationMigration006UpgradePool(t)
	repo := NewPostgresNotificationRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	userID, eventID := uuid.New(), uuid.New()
	booking, bookingHash, bookingTemplate := businessNotificationFixture(t, userID, eventID)
	const legacyRecipient = "legacy-booking@example.test"
	created, err := repo.AcceptBusinessNotification(ctx, booking, bookingHash, bookingTemplate, legacyRecipient, true)
	if err != nil || !created {
		t.Fatalf("seed booking inbox and email delivery before migration 006: created=%v err=%v", created, err)
	}

	var version int64
	var dirty bool
	if err := pool.QueryRow(ctx, `SELECT version,dirty FROM schema_migrations`).Scan(&version, &dirty); err != nil || version != 5 || dirty {
		t.Fatalf("pre-upgrade migration state: version=%d dirty=%v err=%v, want version 5 clean", version, dirty, err)
	}

	var oldUserID, oldBookingID, oldEventID uuid.UUID
	var oldType, oldLocale, oldTitle, oldBody string
	var oldPayloadHash []byte
	var oldReadAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT user_id,booking_id,event_id,type,locale,title,body,payload_hash,read_at
		FROM notification_inbox WHERE notification_id=$1`, booking.NotificationID).
		Scan(&oldUserID, &oldBookingID, &oldEventID, &oldType, &oldLocale, &oldTitle, &oldBody, &oldPayloadHash, &oldReadAt); err != nil {
		t.Fatalf("read seeded booking inbox before upgrade: %v", err)
	}
	if oldUserID != userID || oldBookingID != booking.BookingID || oldEventID != eventID || oldType != booking.Type ||
		oldLocale != bookingTemplate.Locale || oldTitle != bookingTemplate.Title || oldBody != bookingTemplate.Body ||
		!bytes.Equal(oldPayloadHash, bookingHash[:]) || oldReadAt != nil {
		t.Fatalf("unexpected seeded booking inbox before upgrade: user=%s booking=%s event=%s type=%s locale=%s title=%q body=%q readAt=%v",
			oldUserID, oldBookingID, oldEventID, oldType, oldLocale, oldTitle, oldBody, oldReadAt)
	}

	var legacyEmailType, legacyRecipientValue, legacyChannel, legacyLocale, legacyStatus string
	var legacyManaged bool
	var legacyEntityID, legacyDeliveryID uuid.UUID
	var legacyGeneration int64
	var legacyHash []byte
	if err := pool.QueryRow(ctx, `SELECT type,recipient,channel,locale,status,managed,entity_id,transition_generation,payload_hash,delivery_id
		FROM notifications WHERE id=$1`, booking.NotificationID).
		Scan(&legacyEmailType, &legacyRecipientValue, &legacyChannel, &legacyLocale, &legacyStatus, &legacyManaged, &legacyEntityID, &legacyGeneration, &legacyHash, &legacyDeliveryID); err != nil {
		t.Fatalf("read seeded booking delivery before upgrade: %v", err)
	}
	if legacyEmailType != booking.Type || legacyRecipientValue != legacyRecipient || legacyChannel != "email" ||
		legacyLocale != bookingTemplate.Locale || legacyStatus != "PENDING" || !legacyManaged ||
		legacyEntityID != booking.NotificationID || legacyGeneration != 1 || legacyDeliveryID != booking.NotificationID ||
		!bytes.Equal(legacyHash, bookingHash[:]) {
		t.Fatalf("unexpected seeded booking delivery before upgrade: type=%s recipient=%s channel=%s locale=%s status=%s managed=%v entity=%s generation=%d delivery=%s hashBytes=%d",
			legacyEmailType, legacyRecipientValue, legacyChannel, legacyLocale, legacyStatus, legacyManaged, legacyEntityID, legacyGeneration, legacyDeliveryID, len(legacyHash))
	}

	if err := exec.CommandContext(ctx, "migrate", "-path", "../../migrations", "-database", migrationURL, "up", "1").Run(); err != nil {
		t.Fatalf("apply Notification migration 006: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT version,dirty FROM schema_migrations`).Scan(&version, &dirty); err != nil || version != 6 || dirty {
		t.Fatalf("post-upgrade migration state: version=%d dirty=%v err=%v, want version 6 clean", version, dirty, err)
	}

	var preservedInboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notification_inbox WHERE notification_id=$1 AND user_id=$2 AND booking_id=$3 AND event_id=$4 AND type=$5 AND payload_hash=$6`,
		booking.NotificationID, userID, booking.BookingID, eventID, booking.Type, bookingHash[:]).Scan(&preservedInboxCount); err != nil || preservedInboxCount != 1 {
		t.Fatalf("booking inbox after migration 006 count=%d err=%v, want 1 unchanged row", preservedInboxCount, err)
	}
	var preservedDeliveryCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE id=$1 AND type=$2 AND recipient=$3 AND status='PENDING' AND managed AND entity_id=$4 AND transition_generation=1 AND delivery_id=$1 AND payload_hash=$5`,
		booking.NotificationID, booking.Type, legacyRecipient, booking.NotificationID, bookingHash[:]).Scan(&preservedDeliveryCount); err != nil || preservedDeliveryCount != 1 {
		t.Fatalf("booking delivery after migration 006 count=%d err=%v, want 1 unchanged row", preservedDeliveryCount, err)
	}
	bookingPage, err := repo.ListInbox(ctx, userID, 1, 20, false)
	if err != nil || bookingPage.Total != 1 || len(bookingPage.Items) != 1 || bookingPage.Items[0].ID != booking.NotificationID ||
		bookingPage.Items[0].BookingID != booking.BookingID || bookingPage.Items[0].Type != booking.Type {
		t.Fatalf("booking inbox projection after migration 006: page=%+v err=%v", bookingPage, err)
	}

	consentID := uuid.New()
	reminder := &eventreminder.Envelope{
		Version: 1, NotificationID: uuid.New(), SubscriptionID: uuid.New(), SubscriptionVersion: 1, ActiveSinceVersion: 1,
		Action: eventreminder.ActionTrigger, Type: eventreminder.TypeOnSale, UserID: userID, EventID: eventID,
		EventTitle: "Migration upgrade reminder", EventDate: time.Now().UTC().Add(24 * time.Hour),
		EmailOptIn: true, EmailConsentVersion: eventreminder.ConsentVersion, EmailConsentID: &consentID,
	}
	reminderPayload, err := json.Marshal(reminder)
	if err != nil {
		t.Fatalf("encode reminder envelope: %v", err)
	}
	parsedReminder, reminderHash, err := domain.ParseEventReminder(reminderPayload)
	if err != nil {
		t.Fatalf("parse reminder envelope: %v", err)
	}
	reminderTemplate, err := domain.RenderEventReminder(parsedReminder, "en")
	if err != nil {
		t.Fatalf("render reminder inbox projection: %v", err)
	}
	const reminderRecipient = "consented-user@example.test"
	created, err = repo.AcceptEventReminder(ctx, parsedReminder, reminderHash, reminderTemplate, reminderRecipient, true)
	if err != nil || !created {
		t.Fatalf("insert event reminder after migration 006: created=%v err=%v", created, err)
	}

	var reminderStateCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notification_event_reminder_state WHERE subscription_id=$1 AND user_id=$2 AND event_id=$3 AND type=$4 AND subscription_version=1 AND active`,
		reminder.SubscriptionID, userID, eventID, eventreminder.TypeOnSale).Scan(&reminderStateCount); err != nil || reminderStateCount != 1 {
		t.Fatalf("reminder state count after migration 006=%d err=%v, want 1", reminderStateCount, err)
	}
	var reminderDeliveryCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE id=$1 AND type=$2 AND recipient=$3 AND channel='email' AND locale='en' AND status='PENDING' AND managed AND entity_id=$4 AND transition_generation=1 AND delivery_id=$1 AND payload_hash=$5`,
		reminder.NotificationID, reminder.Type, reminderRecipient, reminder.SubscriptionID, reminderHash[:]).Scan(&reminderDeliveryCount); err != nil || reminderDeliveryCount != 1 {
		t.Fatalf("reminder delivery count after migration 006=%d err=%v, want 1", reminderDeliveryCount, err)
	}
	combinedPage, err := repo.ListInbox(ctx, userID, 1, 20, false)
	if err != nil || combinedPage.Total != 2 || len(combinedPage.Items) != 2 {
		t.Fatalf("combined booking/reminder inbox projection: page=%+v err=%v", combinedPage, err)
	}
	var foundBooking, foundReminder bool
	for _, item := range combinedPage.Items {
		if item.ID == booking.NotificationID && item.BookingID == booking.BookingID && item.Type == booking.Type {
			foundBooking = true
		}
		if item.ID == reminder.NotificationID && item.BookingID == uuid.Nil && item.EventID == eventID && item.Type == eventreminder.TypeOnSale {
			foundReminder = true
		}
	}
	if !foundBooking || !foundReminder {
		t.Fatalf("combined inbox lost booking/reminder projection: %+v", combinedPage.Items)
	}
}

func notificationMigration006UpgradePool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	raw := os.Getenv("FINAL_MVP_NOTIFICATION_DATABASE_URL")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "postgres" || parsed.Host != "127.0.0.1:55483" ||
		strings.Trim(parsed.Path, "/") != "rushgo_final_notification_20261113" || parsed.User == nil {
		t.Fatal("FINAL_MVP_NOTIFICATION_DATABASE_URL must target the authorized isolated local Round 3 Notification database")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal("connect to authorized isolated Round 3 Notification database")
	}
	schema := "migration_006_upgrade_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoteIdentifier := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+quoteIdentifier); err != nil {
		admin.Close()
		t.Fatal("create test-owned Notification migration schema")
	}
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	migrationURL := parsed.String()
	if err := exec.CommandContext(ctx, "migrate", "-path", "../../migrations", "-database", migrationURL, "up", "5").Run(); err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+quoteIdentifier+` CASCADE`)
		admin.Close()
		t.Fatalf("apply Notification migrations 001-005 to the test-owned schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(migrationURL)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+quoteIdentifier+` CASCADE`)
		admin.Close()
		t.Fatal("parse test-owned Notification migration URL")
	}
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+quoteIdentifier+` CASCADE`)
		admin.Close()
		t.Fatal("connect to test-owned Notification migration schema")
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, `DROP SCHEMA `+quoteIdentifier+` CASCADE`); err != nil {
			t.Errorf("drop test-owned Notification migration schema: %v", err)
		}
		admin.Close()
	})
	return pool, migrationURL
}
