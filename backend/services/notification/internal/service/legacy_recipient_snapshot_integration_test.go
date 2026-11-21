package service

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
	"go.uber.org/zap"
)

type rotatingRecipientLookup struct {
	userID uuid.UUID
	first  NotificationRecipient
	second NotificationRecipient
	calls  atomic.Int32
}

func (r *rotatingRecipientLookup) LookupNotificationRecipient(_ context.Context, userID uuid.UUID) (NotificationRecipient, error) {
	if userID != r.userID {
		return NotificationRecipient{}, errors.New("unexpected recipient user")
	}
	if r.calls.Add(1) == 1 {
		return r.first, nil
	}
	return r.second, nil
}

func TestBookingEmailReplayFreezesFirstResolvedRecipientIntegration(t *testing.T) {
	pool := isolatedMVPNotificationPool(t)
	repo := repository.NewPostgresNotificationRepository(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bookingID, userID, eventID := uuid.New(), uuid.New(), uuid.New()
	lookup := &rotatingRecipientLookup{userID: userID,
		first:  NotificationRecipient{UserID: userID, Email: "first@example.test", Locale: "en", EmailVerified: true},
		second: NotificationRecipient{UserID: userID, Email: "updated@example.test", Locale: "zh", EmailVerified: true},
	}
	service := NewNotificationService(repo, nil, zap.NewNop())
	service.SetRecipientLookup(lookup)
	bookingEvent := func(total int64) json.RawMessage {
		raw, err := json.Marshal(map[string]any{"booking_id": bookingID, "user_id": userID, "event_id": eventID, "total": total})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if err := service.SendBookingConfirmation(ctx, bookingEvent(72500)); err != nil {
		t.Fatalf("first resolved-recipient acceptance: %v", err)
	}
	if err := service.SendBookingConfirmation(ctx, bookingEvent(72500)); err != nil {
		t.Fatalf("same business event after profile email change: %v", err)
	}
	var count int
	var recipient, locale, payloadEmail string
	if err := pool.QueryRow(ctx, `SELECT count(*),min(recipient),min(locale),COALESCE(min(payload->>'email'),'') FROM notifications WHERE id=$1`, domain.NotificationID(domain.SemanticKey{Type: "booking_confirmation", EntityID: bookingID, Generation: 1, Channel: "email"})).Scan(&count, &recipient, &locale, &payloadEmail); err != nil {
		t.Fatal(err)
	}
	if count != 1 || recipient != "first@example.test" || locale != "en" || payloadEmail != "" || lookup.calls.Load() != 2 {
		t.Fatalf("duplicate refreshed frozen delivery snapshot: count=%d recipient=%q locale=%q payload_email=%q lookup_calls=%d",
			count, recipient, locale, payloadEmail, lookup.calls.Load())
	}
	if err := service.SendBookingConfirmation(ctx, bookingEvent(72501)); !errors.Is(err, repository.ErrSemanticConflict) {
		t.Fatalf("changed amount after profile lookup error=%v, want semantic conflict", err)
	}
	if err := pool.QueryRow(ctx, `SELECT recipient,locale FROM notifications WHERE id=$1`, domain.NotificationID(domain.SemanticKey{Type: "booking_confirmation", EntityID: bookingID, Generation: 1, Channel: "email"})).Scan(&recipient, &locale); err != nil {
		t.Fatal(err)
	}
	if recipient != "first@example.test" || locale != "en" {
		t.Fatalf("semantic conflict changed durable first-recipient snapshot: recipient=%q locale=%q", recipient, locale)
	}
}

func isolatedMVPNotificationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("MVP_NOTIFICATION_DATABASE_URL")
	if strings.TrimSpace(raw) == "" {
		t.Skip("MVP_NOTIFICATION_DATABASE_URL is unset; isolated PostgreSQL integration test skipped")
	}
	parsed, err := url.Parse(raw)
	databaseName := ""
	if parsed != nil {
		databaseName = strings.Trim(parsed.Path, "/")
	}
	loopback := false
	if parsed != nil {
		host := parsed.Hostname()
		ip := net.ParseIP(host)
		loopback = strings.EqualFold(host, "localhost") || ip != nil && ip.IsLoopback()
	}
	if err != nil || parsed == nil || parsed.Scheme != "postgres" || !loopback ||
		!strings.HasPrefix(databaseName, "rushgo_mvp_") || parsed.User == nil || parsed.User.Username() == "" {
		t.Fatal("MVP_NOTIFICATION_DATABASE_URL must point to a loopback PostgreSQL database with the rushgo_mvp_ test prefix")
	}
	adminURL := *parsed
	adminURL.Path = "/postgres"
	adminURL.RawPath = ""
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL.String())
	if err != nil {
		t.Fatal("connect to authorized local PostgreSQL admin database")
	}
	dbName := "rushgo_mvp_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoteIdentifier := func(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+quoteIdentifier(dbName)+` WITH TEMPLATE `+quoteIdentifier(databaseName)); err != nil {
		_ = admin.Close(context.Background())
		t.Fatal("clone authorized local PostgreSQL source into an isolated Notification database")
	}
	cloneURL := *parsed
	cloneURL.Path = "/" + dbName
	cloneURL.RawPath = ""
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, terminateErr := admin.Exec(cleanupCtx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()`, dbName)
		_, dropErr := admin.Exec(cleanupCtx, `DROP DATABASE IF EXISTS `+quoteIdentifier(dbName))
		_ = admin.Close(cleanupCtx)
		if terminateErr != nil {
			t.Errorf("terminate connections to test-owned database clone: %v", terminateErr)
		}
		if dropErr != nil {
			t.Errorf("drop test-owned Notification database clone: %v", dropErr)
		}
	})
	schema := "notification_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	setup, err := pgx.Connect(ctx, cloneURL.String())
	if err != nil {
		t.Fatal("connect to isolated Notification database clone")
	}
	if _, err := setup.Exec(ctx, `CREATE SCHEMA `+quoteIdentifier(schema)); err != nil {
		_ = setup.Close(context.Background())
		t.Fatal("create isolated Notification migration schema")
	}
	if err := setup.Close(ctx); err != nil {
		t.Fatal("close isolated Notification database setup connection")
	}
	query := cloneURL.Query()
	query.Set("search_path", schema+",public")
	cloneURL.RawQuery = query.Encode()
	output, err := exec.CommandContext(ctx, "migrate", "-path", "../../migrations", "-database", cloneURL.String(), "up").CombinedOutput()
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
