package admission

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestKeyUsesPlainEventAndSeatIdentity(t *testing.T) {
	eventID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	seatID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	got := Key(eventID, seatID)
	want := "rushgo:seat-admission:11111111-1111-1111-1111-111111111111:22222222-2222-2222-2222-222222222222"
	if got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
	if strings.Contains(got, "{") || strings.Contains(got, "}") {
		t.Fatalf("standalone key must not use a Redis Cluster hash tag: %q", got)
	}
}

func TestReservationTTLUsesRequestDuration(t *testing.T) {
	got, err := ReservationTTL(7)
	if err != nil {
		t.Fatalf("ReservationTTL returned error: %v", err)
	}
	if got != 7*time.Minute {
		t.Fatalf("ttl = %s, want 7m", got)
	}
}

func TestReservationTTLUsesSharedPostgresDefaultForMissingDuration(t *testing.T) {
	got, err := ReservationTTL(0)
	if err != nil {
		t.Fatalf("ReservationTTL returned error: %v", err)
	}
	if got != 15*time.Minute {
		t.Fatalf("ttl = %s, want 15m", got)
	}
}

func TestRedisAdmissionAcquireConflictDoesNotWriteBatch(t *testing.T) {
	client := newRedisTestClient(t)
	ctx := context.Background()
	eventID := uuid.New()
	seatOne := uuid.New()
	seatTwo := uuid.New()
	ownerA := uuid.New()
	ownerB := uuid.New()
	keys := []string{Key(eventID, seatOne), Key(eventID, seatTwo)}
	t.Cleanup(func() { _ = client.Del(ctx, keys...).Err() })

	admission := NewRedisAdmission(client)
	if _, err := admission.Acquire(ctx, eventID, []uuid.UUID{seatOne}, ownerA, 1); err != nil {
		t.Fatalf("owner A acquire: %v", err)
	}

	result, err := admission.Acquire(ctx, eventID, []uuid.UUID{seatOne, seatTwo}, ownerB, 1)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("owner B acquire error = %v, want ErrConflict", err)
	}
	if result.Created != 0 {
		t.Fatalf("conflicting acquire created %d keys, want 0", result.Created)
	}
	if exists, err := client.Exists(ctx, keys[1]).Result(); err != nil {
		t.Fatalf("check untouched key: %v", err)
	} else if exists != 0 {
		t.Fatalf("conflicting acquire wrote second key")
	}
}

func TestRedisAdmissionSameOwnerRetryDoesNotRefreshTTL(t *testing.T) {
	client := newRedisTestClient(t)
	ctx := context.Background()
	eventID := uuid.New()
	seatID := uuid.New()
	owner := uuid.New()
	key := Key(eventID, seatID)
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	admission := NewRedisAdmission(client)
	if _, err := admission.Acquire(ctx, eventID, []uuid.UUID{seatID}, owner, 1); err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	initialTTL, err := client.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("initial ttl: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	result, err := admission.Acquire(ctx, eventID, []uuid.UUID{seatID}, owner, 1)
	if err != nil {
		t.Fatalf("same-owner retry: %v", err)
	}
	if result.Existing != 1 || result.Created != 0 {
		t.Fatalf("retry result = %#v, want one existing and zero created", result)
	}
	remainingTTL, err := client.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("retry ttl: %v", err)
	}
	if remainingTTL >= initialTTL {
		t.Fatalf("same-owner retry refreshed ttl from %s to %s", initialTTL, remainingTTL)
	}
}

func TestRedisAdmissionReleaseDeletesOwnedKeysIndependently(t *testing.T) {
	client := newRedisTestClient(t)
	ctx := context.Background()
	eventID := uuid.New()
	owned := uuid.New()
	missing := uuid.New()
	other := uuid.New()
	owner := uuid.New()
	otherOwner := uuid.New()
	ownedKey := Key(eventID, owned)
	otherKey := Key(eventID, other)
	t.Cleanup(func() { _ = client.Del(ctx, ownedKey, otherKey).Err() })
	if err := client.Set(ctx, ownedKey, owner.String(), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, otherKey, otherOwner.String(), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	result, err := NewRedisAdmission(client).Release(ctx, eventID, []uuid.UUID{owned, missing, other}, owner)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if result.Deleted != 1 || result.Missing != 1 || result.OwnerMismatch != 1 {
		t.Fatalf("release result = %#v, want deleted=1 missing=1 mismatch=1", result)
	}
	if exists, err := client.Exists(ctx, ownedKey).Result(); err != nil {
		t.Fatal(err)
	} else if exists != 0 {
		t.Fatal("owned key was not deleted")
	}
	if value, err := client.Get(ctx, otherKey).Result(); err != nil {
		t.Fatal(err)
	} else if value != otherOwner.String() {
		t.Fatalf("other owner's key changed to %q", value)
	}
}

func TestRedisAdmissionRecoversAfterScriptFlush(t *testing.T) {
	client := newRedisTestClient(t)
	ctx := context.Background()
	eventID := uuid.New()
	seatID := uuid.New()
	owner := uuid.New()
	key := Key(eventID, seatID)
	t.Cleanup(func() { _ = client.Del(ctx, key).Err() })

	admission := NewRedisAdmission(client)
	if _, err := admission.Acquire(ctx, eventID, []uuid.UUID{seatID}, owner, 1); err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("flush scripts: %v", err)
	}
	if _, err := admission.Release(ctx, eventID, []uuid.UUID{seatID}, owner); err != nil {
		t.Fatalf("release after script flush: %v", err)
	}
}

func newRedisTestClient(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("REDIS_TEST_URL is not set; Redis integration test skipped")
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse REDIS_TEST_URL: %v", err)
	}
	client := redis.NewClient(options)
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		t.Fatalf("ping Redis: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
