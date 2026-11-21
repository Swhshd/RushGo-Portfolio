package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
	"go.uber.org/zap"
)

func semanticNotificationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	u, err := url.Parse(os.Getenv("NOTIFICATION_TEST_DATABASE_URL"))
	if err != nil || u.Host != "127.0.0.1:25436" || u.Path != "/ticketbox_notification_test" {
		t.Fatal("BLOCKED: isolated Notification PG required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	schema := "notification_semantic_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public; CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	if out, err := exec.Command("migrate", "-path", "../../migrations", "-database", u.String(), "up").CombinedOutput(); err != nil {
		t.Fatalf("actual migrations: %s", out)
	}
	p, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestEveryNotificationTransitionDeduplicatesIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var sends atomic.Int32
	s := NewNotificationService(repository.NewPostgresNotificationRepository(p), NotificationSenderFunc(func(context.Context, *domain.Notification) error { sends.Add(1); return nil }), zap.NewNop())
	for _, typ := range []string{"booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email"} {
		t.Run(typ, func(t *testing.T) {
			id := uuid.New()
			field, prefix := "booking_id", "ticketbox:"+typ+":"
			handler := s.SendBookingConfirmation
			switch typ {
			case "booking_failure":
				handler = s.SendBookingFailure
			case "booking_cancellation":
				handler = s.SendBookingCancellation
			case "welcome_email":
				field, prefix = "user_id", "ticketbox:user_welcome:"
				handler = s.SendWelcomeEmail
			}
			stable := uuid.NewSHA1(uuid.NameSpaceURL, []byte(prefix+id.String()))
			payload := map[string]any{field: id.String(), "email": "semantic@example.com"}
			if typ == "welcome_email" {
				payload["name"] = "Fixture"
			} else {
				payload["user_id"] = uuid.NewString()
				payload["event_id"] = uuid.NewString()
			}
			if typ == "booking_confirmation" {
				payload["total"] = 725
			}
			if typ == "booking_failure" {
				payload["reason"] = "failed"
			}
			for range 5 {
				payload["technical_id"] = uuid.NewString()
				raw, _ := json.Marshal(payload)
				if err := handler(ctx, raw); err != nil {
					t.Fatal(err)
				}
			}
			var count int
			var actual uuid.UUID
			if err := p.QueryRow(ctx, `SELECT count(*),min(id::text)::uuid FROM notifications WHERE type=$1 AND payload->>$2=$3`, typ, field, id.String()).Scan(&count, &actual); err != nil {
				t.Fatal(err)
			}
			if count != 1 || actual != stable {
				t.Fatalf("semantic replay created %d rows; identity=%s want=%s", count, actual, stable)
			}
			if sends.Load() != 0 {
				t.Fatal("event handler performed external send before durable delivery")
			}
			var wg sync.WaitGroup
			errs := make(chan error, 12)
			for range 12 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					data := make(map[string]any)
					for k, v := range payload {
						data[k] = v
					}
					data["technical_id"] = uuid.NewString()
					raw, _ := json.Marshal(data)
					errs <- handler(ctx, raw)
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			originalRecipient := payload["email"]
			payload["email"] = "different@example.com"
			raw, _ := json.Marshal(payload)
			if err := handler(ctx, raw); err != nil {
				t.Fatal("recipient snapshot change altered immutable business identity", err)
			}
			var storedRecipient string
			var payloadEmail string
			if err := p.QueryRow(ctx, `SELECT recipient,COALESCE(payload->>'email','') FROM notifications WHERE id=$1`, stable).Scan(&storedRecipient, &payloadEmail); err != nil {
				t.Fatal(err)
			}
			if storedRecipient != originalRecipient || payloadEmail != "" {
				t.Fatalf("duplicate changed the frozen recipient or stored an envelope copy: recipient=%q payload_email=%q", storedRecipient, payloadEmail)
			}
			switch typ {
			case "booking_confirmation":
				payload["total"] = 726
			case "booking_failure":
				payload["reason"] = "different failure"
			case "booking_cancellation":
				payload["user_id"] = uuid.NewString()
			case "welcome_email":
				payload["name"] = "Different"
			}
			raw, _ = json.Marshal(payload)
			if err := handler(ctx, raw); !errors.Is(err, repository.ErrSemanticConflict) {
				t.Fatalf("changed immutable business fact was not diagnosed: %v", err)
			}
			var diagnostic string
			var status string
			var managed bool
			if err := p.QueryRow(ctx, `SELECT status,managed,last_error FROM notifications WHERE id=$1`, stable).Scan(&status, &managed, &diagnostic); err != nil {
				t.Fatal(err)
			}
			if status != "PENDING" || !managed || diagnostic != "semantic_payload_conflict" || storedRecipient != originalRecipient {
				t.Fatal("intent or conflict diagnostic lost", status, managed, diagnostic)
			}
		})
	}
}

func TestNotificationLegacySentAndDuplicateHistoryIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r := repository.NewPostgresNotificationRepository(p)
	var sends atomic.Int32
	s := NewNotificationService(r, NotificationSenderFunc(func(context.Context, *domain.Notification) error { sends.Add(1); return nil }), zap.NewNop())
	id := uuid.New()
	stable := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking_confirmation:"+id.String()))
	raw, _ := json.Marshal(map[string]any{"booking_id": id.String(), "email": "legacy@example.com", "total": 725})
	if _, err := p.Exec(ctx, `INSERT INTO notifications(id,type,recipient,channel,payload,status,sent_at) VALUES($1,'booking_confirmation','legacy@example.com','email',$2,'SENT',now())`, stable, raw); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := s.SendBookingConfirmation(ctx, raw); err != nil {
			t.Fatal("legacy sent replay", err)
		}
	}
	var managed bool
	var status string
	if err := p.QueryRow(ctx, `SELECT managed,status FROM notifications WHERE id=$1`, stable).Scan(&managed, &status); err != nil {
		t.Fatal(err)
	}
	if managed || status != "SENT" || sends.Load() != 0 {
		t.Fatal("historical SENT requeued")
	}
	conflict, _ := json.Marshal(map[string]any{"booking_id": id.String(), "email": "legacy@example.com", "total": 726})
	if err := s.SendBookingConfirmation(ctx, conflict); !errors.Is(err, repository.ErrSemanticConflict) {
		t.Fatal("historical content mismatch not diagnosed", err)
	}
	id = uuid.New()
	raw, _ = json.Marshal(map[string]any{"booking_id": id.String(), "email": "legacy@example.com"})
	for range 2 {
		if _, err := p.Exec(ctx, `INSERT INTO notifications(id,type,recipient,channel,payload,status) VALUES($1,'booking_cancellation','legacy@example.com','email',$2,'SENT')`, uuid.New(), raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SendBookingCancellation(ctx, raw); !errors.Is(err, repository.ErrLegacyAttention) {
		t.Fatal("unproven random duplicate history automatically adopted", err)
	}
	var count, diagnosed int
	if err := p.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE NOT managed AND last_error='legacy_semantic_history_attention') FROM notifications WHERE type='booking_cancellation' AND payload->>'booking_id'=$1`, id.String()).Scan(&count, &diagnosed); err != nil {
		t.Fatal(err)
	}
	if count != 2 || diagnosed != 2 {
		t.Fatal("legacy history deleted or replay invented", count, diagnosed)
	}
	newID := uuid.New()
	newRaw, _ := json.Marshal(map[string]any{"booking_id": newID.String(), "email": "new@example.com"})
	if err := s.SendBookingConfirmation(ctx, newRaw); err != nil {
		t.Fatal(err)
	}
	key, n, hash, err := domain.PrepareIntent("booking_confirmation", newRaw)
	if err != nil {
		t.Fatal(err)
	}
	if created, err := r.ProcessOnce(ctx, n, func(context.Context, *domain.Notification) error { sends.Add(1); return nil }); err != nil || created {
		t.Fatal("deprecated adapter failed durable acceptance", created, err)
	}
	if err := r.MarkSent(ctx, n.ID); err == nil {
		t.Fatal("managed SENT bypass allowed")
	}
	if existing, created, err := r.AcceptIntent(ctx, key, n, hash); err != nil || created || existing.Status != "PENDING" {
		t.Fatal("managed acceptance falsely delivered", created, err)
	}
	if sends.Load() != 0 {
		t.Fatal("deprecated path sent managed work")
	}
}

func TestNotificationRejectsMalformedBeforePersistence(t *testing.T) {
	p := semanticNotificationPool(t)
	s := NewNotificationService(repository.NewPostgresNotificationRepository(p), nil, zap.NewNop())
	for _, raw := range []string{`null`, `{}`, `{"user_id":7}`, `{"user_id":"00000000-0000-0000-0000-000000000000"}`, `{"user_id":"` + uuid.NewString() + `","email":null}`, `{"user_id":"` + uuid.NewString() + `","name":8}`} {
		if err := s.SendWelcomeEmail(context.Background(), []byte(raw)); err == nil {
			t.Fatal("malformed accepted", raw)
		}
	}
	var count int
	if err := p.QueryRow(context.Background(), `SELECT count(*) FROM notifications`).Scan(&count); err != nil || count != 0 {
		t.Fatal("malformed input created responsibility", count, err)
	}
}

func TestNotificationCanonicalLegacyIdentityCannotReenqueueIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	ctx := context.Background()
	s := NewNotificationService(repository.NewPostgresNotificationRepository(p), nil, zap.NewNop())
	for _, format := range []func(string) string{strings.ToUpper, func(s string) string { return strings.ReplaceAll(s, "-", "") }, func(s string) string { return "{" + s + "}" }, func(s string) string { return "urn:uuid:" + s }} {
		id := uuid.NewString()
		historical, _ := json.Marshal(map[string]string{"booking_id": format(id), "email": "legacy@example.com"})
		if _, err := p.Exec(ctx, `INSERT INTO notifications(id,type,recipient,payload,status) VALUES($1,'booking_failure','legacy@example.com',$2,'SENT')`, uuid.New(), historical); err != nil {
			t.Fatal(err)
		}
		incoming, _ := json.Marshal(map[string]string{"booking_id": id, "email": "legacy@example.com"})
		if err := s.SendBookingFailure(ctx, incoming); !errors.Is(err, repository.ErrLegacyAttention) {
			t.Fatal("canonical legacy entity produced a new intent", err)
		}
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE managed`).Scan(&count); err != nil || count != 0 {
		t.Fatal("canonical legacy duplicate requeued", count, err)
	}
}

func TestNotificationConcurrentFirstAcceptanceIntegration(t *testing.T) {
	p := semanticNotificationPool(t)
	r := repository.NewPostgresNotificationRepository(p)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	id := uuid.NewString()
	raw, _ := json.Marshal(map[string]any{"booking_id": id, "email": "race@example.com", "reason": "failed"})
	key, n, hash, err := domain.PrepareIntent("booking_failure", raw)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var winners atomic.Int32
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			actual, created, err := r.AcceptIntent(ctx, key, n, hash)
			if err == nil && (actual.ID != n.ID || actual.Status != "PENDING") {
				err = errors.New("acceptance identity or state lost")
			}
			if created {
				winners.Add(1)
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE managed AND entity_id=$1`, id).Scan(&count); err != nil || count != 1 || winners.Load() != 1 {
		t.Fatal("concurrent first acceptance not atomic", count, winners.Load(), err)
	}
}
