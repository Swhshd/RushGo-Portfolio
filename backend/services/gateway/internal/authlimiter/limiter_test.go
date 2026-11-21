package authlimiter

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fixture(t *testing.T) (*redis.Client, *Limiter, string) {
	t.Helper()
	dsn := os.Getenv("REDIS_TEST_URL")
	if dsn == "" {
		t.Skip("isolated REDIS_TEST_URL required")
	}
	if dsn != "redis://127.0.0.1:16379/0" {
		t.Fatal("refusing live Redis fixture")
	}
	opt, err := redis.ParseURL(dsn)
	if err != nil {
		t.Fatal("fixture URL invalid")
	}
	c := redis.NewClient(opt)
	if c.Ping(context.Background()).Err() != nil {
		t.Fatal("fixture unavailable")
	}
	// The URL above is restricted to the dedicated disposable M6 Redis only.
	if c.FlushDB(context.Background()).Err() != nil {
		t.Fatal("isolated fixture reset failed")
	}
	t.Cleanup(func() { c.Close() })
	return c, New(c), uuid.NewString() + "@example.com"
}
func TestAuthLimiterConcurrentAndTTL(t *testing.T) {
	c, l, email := fixture(t)
	ctx := context.Background()
	start := make(chan struct{})
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := l.Admit(ctx, Send, email, "192.0.2.1"); err == nil {
				admitted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatal("atomic cooldown race")
	}
	keys, _ := c.Keys(ctx, "rushgo:auth:rl:v1:registration_send:*").Result()
	if len(keys) == 0 {
		t.Fatal("no fixed-window state")
	}
	for _, key := range keys {
		if strings.Contains(key, email) || strings.Contains(key, "192.0.2.1") {
			t.Fatal("readable identity key")
		}
		before := c.PTTL(ctx, key).Val()
		_, err := l.Admit(ctx, Send, email, "192.0.2.1")
		if !errors.Is(err, ErrLimited) {
			t.Fatal("cooldown retry admitted")
		}
		after := c.PTTL(ctx, key).Val()
		if after > before || after <= 0 {
			t.Fatal("rejection extended TTL")
		}
	}
}
func TestAuthLimiterBudgetAndCorruptFailClosed(t *testing.T) {
	c, l, email := fixture(t)
	ctx := context.Background()
	ip := "192.0.2.222"
	for i := 0; i < 15; i++ {
		if _, err := l.Admit(ctx, Verify, email, ip); err != nil {
			t.Fatal("budget rejected too early")
		}
	}
	if _, err := l.Admit(ctx, Verify, email, ip); !errors.Is(err, ErrLimited) {
		t.Fatal("email verification budget bypass")
	}
	keys, _ := c.Keys(ctx, "rushgo:auth:rl:v1:verify:email:*").Result()
	for _, key := range keys {
		if c.Set(ctx, key, "corrupt", 0).Err() != nil {
			t.Fatal("corrupt fixture failed")
		}
	}
	if _, err := l.Admit(ctx, Verify, email, ip); !errors.Is(err, ErrUnavailable) {
		t.Fatal("malformed nonexpiring state allowed")
	}
}
func TestAuthLimiterOutageAndInvalidIP(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	defer c.Close()
	l := New(c)
	for _, ip := range []string{"192.0.2.1", ""} {
		if _, err := l.Admit(context.Background(), Send, "a@example.com", ip); !errors.Is(err, ErrUnavailable) {
			t.Fatal("outage/invalid identity failed open")
		}
	}
}

func TestAuthLimiterRejectedAdmissionWritesNothing(t *testing.T) {
	c, l, email := fixture(t)
	ctx := context.Background()
	ip := "192.0.2.199"
	for i := 0; i < 30; i++ {
		if _, err := l.Admit(ctx, Reset, "", ip); err != nil {
			t.Fatal("IP budget rejected too early")
		}
	}
	before, _ := c.Keys(ctx, "rushgo:auth:rl:v1:reset:*").Result()
	if _, err := l.Admit(ctx, Reset, "", ip); !errors.Is(err, ErrLimited) {
		t.Fatal("IP budget bypassed")
	}
	after, _ := c.Keys(ctx, "rushgo:auth:rl:v1:reset:*").Result()
	if len(before) != len(after) {
		t.Fatal("rejected admission wrote state")
	}
	_ = email
}
