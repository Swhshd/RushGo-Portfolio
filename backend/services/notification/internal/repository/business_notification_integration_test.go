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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/notification/internal/domain"
)

func TestBusinessNotificationInboxAndIntentAtomicityIntegration(t *testing.T) {
	pool := businessNotificationIntegrationPool(t)
	repo := NewPostgresNotificationRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	owner1, owner2 := uuid.New(), uuid.New()
	first, firstHash, firstTemplate := businessNotificationFixture(t, owner1, uuid.New())
	created, err := repo.AcceptBusinessNotification(ctx, first, firstHash, firstTemplate, "one@example.test", true)
	if err != nil || !created {
		t.Fatalf("first acceptance: created=%v err=%v", created, err)
	}
	created, err = repo.AcceptBusinessNotification(ctx, first, firstHash, firstTemplate, "one@example.test", true)
	if err != nil || created {
		t.Fatalf("same identity replay: created=%v err=%v", created, err)
	}
	assertBusinessNotificationRows(t, ctx, pool, first.NotificationID, 1, 1)

	conflict := *first
	conflict.Total++
	_, conflictHash, conflictTemplate := businessNotificationFixtureFromEvent(t, &conflict)
	if _, err := repo.AcceptBusinessNotification(ctx, &conflict, conflictHash, conflictTemplate, "one@example.test", true); !errors.Is(err, ErrSemanticConflict) {
		t.Fatalf("same ID with different payload error=%v, want ErrSemanticConflict", err)
	}
	assertBusinessNotificationRows(t, ctx, pool, first.NotificationID, 1, 1)

	second, secondHash, secondTemplate := businessNotificationFixture(t, owner2, uuid.New())
	if created, err := repo.AcceptBusinessNotification(ctx, second, secondHash, secondTemplate, "", false); err != nil || !created {
		t.Fatalf("unverified-email inbox acceptance: created=%v err=%v", created, err)
	}
	assertBusinessNotificationRows(t, ctx, pool, second.NotificationID, 1, 0)

	page1, err := repo.ListInbox(ctx, owner1, 1, 20, false)
	if err != nil || page1.Total != 1 || len(page1.Items) != 1 || page1.Items[0].ID != first.NotificationID {
		t.Fatalf("owner1 inbox projection=%+v err=%v", page1, err)
	}
	page2, err := repo.ListInbox(ctx, owner2, 1, 20, false)
	if err != nil || page2.Total != 1 || len(page2.Items) != 1 || page2.Items[0].ID != second.NotificationID {
		t.Fatalf("owner2 inbox projection=%+v err=%v", page2, err)
	}
	if _, err := repo.MarkInboxRead(ctx, owner2, first.NotificationID); !errors.Is(err, ErrInboxNotFound) {
		t.Fatalf("cross-owner mark-read error=%v, want ErrInboxNotFound", err)
	}
	page1, err = repo.ListInbox(ctx, owner1, 1, 20, false)
	if err != nil || page1.Items[0].ReadAt != nil {
		t.Fatalf("cross-owner mark changed owner's read state: %+v err=%v", page1, err)
	}
	firstReadAt, err := repo.MarkInboxRead(ctx, owner1, first.NotificationID)
	if err != nil || firstReadAt.IsZero() {
		t.Fatalf("owner mark-read: at=%v err=%v", firstReadAt, err)
	}
	replayedReadAt, err := repo.MarkInboxRead(ctx, owner1, first.NotificationID)
	if err != nil || !replayedReadAt.Equal(firstReadAt) {
		t.Fatalf("mark-read replay changed timestamp: first=%v replay=%v err=%v", firstReadAt, replayedReadAt, err)
	}

	rollbackEvent, rollbackHash, rollbackTemplate := businessNotificationFixture(t, uuid.New(), uuid.New())
	installBusinessNotificationRejectTrigger(t, ctx, pool, rollbackEvent.NotificationID)
	if _, err := repo.AcceptBusinessNotification(ctx, rollbackEvent, rollbackHash, rollbackTemplate, "rollback@example.test", true); err == nil {
		t.Fatal("injected email intent failure was accepted")
	}
	assertBusinessNotificationRows(t, ctx, pool, rollbackEvent.NotificationID, 0, 0)
}

func businessNotificationIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("MVP_NOTIFICATION_DATABASE_URL")
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "postgres" || parsed.Host != "127.0.0.1:1972" || strings.Trim(parsed.Path, "/") != "rushgo_mvp_phase1" || parsed.User == nil || parsed.User.Username() != "postgres" {
		t.Fatal("MVP_NOTIFICATION_DATABASE_URL must point to the authorized isolated local PostgreSQL source database")
	}
	adminURL := *parsed
	adminURL.Path = "/postgres"
	adminURL.RawPath = ""
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL.String())
	if err != nil {
		t.Fatal("connect to authorized local PostgreSQL admin database")
	}
	cloneName := "rushgo_mvp_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoteIdentifier := func(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+quoteIdentifier(cloneName)+` WITH TEMPLATE `+quoteIdentifier(strings.Trim(parsed.Path, "/"))); err != nil {
		_ = admin.Close(context.Background())
		t.Fatal("clone authorized local PostgreSQL source into an isolated Notification database")
	}
	cloneURL := *parsed
	cloneURL.Path = "/" + cloneName
	cloneURL.RawPath = ""
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, terminateErr := admin.Exec(cleanupCtx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()`, cloneName)
		_, dropErr := admin.Exec(cleanupCtx, `DROP DATABASE IF EXISTS `+quoteIdentifier(cloneName))
		_ = admin.Close(cleanupCtx)
		if terminateErr != nil {
			t.Errorf("terminate connections to test-owned clone: %v", terminateErr)
		}
		if dropErr != nil {
			t.Errorf("drop test-owned Notification database clone: %v", dropErr)
		}
	})
	schemaName := "notification_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cloneAdmin, err := pgx.Connect(ctx, cloneURL.String())
	if err != nil {
		t.Fatal("connect to isolated Notification database clone")
	}
	if _, err := cloneAdmin.Exec(ctx, `CREATE SCHEMA `+quoteIdentifier(schemaName)); err != nil {
		_ = cloneAdmin.Close(context.Background())
		t.Fatal("create isolated Notification migration schema")
	}
	if err := cloneAdmin.Close(ctx); err != nil {
		t.Fatal("close isolated Notification database setup connection")
	}
	query := cloneURL.Query()
	query.Set("search_path", schemaName+",public")
	cloneURL.RawQuery = query.Encode()

	output, err := exec.Command("migrate", "-path", "../../migrations", "-database", cloneURL.String(), "up").CombinedOutput()
	if err != nil {
		t.Fatalf("apply Notification migrations to isolated database clone: %s", strings.ReplaceAll(string(output), cloneURL.String(), "[isolated Notification database]"))
	}
	pool, err := pgxpool.New(ctx, cloneURL.String())
	if err != nil {
		t.Fatal("open isolated Notification database clone")
	}
	t.Cleanup(pool.Close)
	return pool
}

func businessNotificationFixture(t *testing.T, ownerID, eventID uuid.UUID) (*domain.BusinessNotificationV1, [32]byte, domain.InboxTemplate) {
	t.Helper()
	event := &domain.BusinessNotificationV1{
		Version: 1, NotificationID: uuid.New(), Type: "tickets_issued", BookingID: uuid.New(), UserID: ownerID,
		EventID: eventID, Generation: 1, Currency: "CNY", Total: 72500, Status: "issued",
	}
	return businessNotificationFixtureFromEvent(t, event)
}

func businessNotificationFixtureFromEvent(t *testing.T, event *domain.BusinessNotificationV1) (*domain.BusinessNotificationV1, [32]byte, domain.InboxTemplate) {
	t.Helper()
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	parsed, hash, err := domain.ParseBusinessNotificationV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	template, err := domain.RenderBusinessNotification(parsed, "en")
	if err != nil {
		t.Fatal(err)
	}
	return parsed, hash, template
}

func assertBusinessNotificationRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, wantInbox, wantEmail int) {
	t.Helper()
	var inboxCount, emailCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notification_inbox WHERE notification_id=$1`, id).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE id=$1`, id).Scan(&emailCount); err != nil {
		t.Fatal(err)
	}
	if inboxCount != wantInbox || emailCount != wantEmail {
		t.Fatalf("notification rows for %s: inbox=%d email=%d, want %d/%d", id, inboxCount, emailCount, wantInbox, wantEmail)
	}
}

func installBusinessNotificationRejectTrigger(t *testing.T, ctx context.Context, pool *pgxpool.Pool, notificationID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(ctx, `CREATE FUNCTION reject_test_business_notification_email() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id = '`+notificationID.String()+`'::uuid THEN RAISE EXCEPTION 'injected Notification email persistence failure'; END IF;
 RETURN NEW;
END $$`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER reject_test_business_notification_email BEFORE INSERT ON notifications FOR EACH ROW EXECUTE FUNCTION reject_test_business_notification_email()`); err != nil {
		t.Fatal(err)
	}
}
