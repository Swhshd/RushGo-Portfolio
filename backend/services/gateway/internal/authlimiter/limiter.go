package authlimiter

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/pkg/authpolicy"
	"net/netip"
	"strconv"
	"time"
)

type Policy string

const (
	Send   Policy = "registration_send"
	Forgot Policy = "forgot"
	Verify Policy = "verify"
	Reset  Policy = "reset"
)

var (
	ErrUnavailable = errors.New("TEMPORARY_UNAVAILABLE")
	ErrLimited     = errors.New("RATE_LIMITED")
)

type AuthLimiter interface {
	Admit(context.Context, Policy, string, string) (time.Duration, error)
}
type Limiter struct{ client *redis.Client }

func New(client *redis.Client) *Limiter { return &Limiter{client} }

type budget struct {
	dimension, digest, window string
	limit                     int
	ttl                       time.Duration
}

func (l *Limiter) Admit(ctx context.Context, policy Policy, email, ip string) (time.Duration, error) {
	addr, err := netip.ParseAddr(ip)
	if err != nil || l.client == nil {
		return 0, ErrUnavailable
	}
	ipDigest := authpolicy.DigestIP(addr.Unmap().String())
	emailDigest := authpolicy.DigestEmail(authpolicy.NormalizeEmail(email))
	var budgets []budget
	switch policy {
	case Send, Forgot:
		if email == "" {
			return 0, ErrUnavailable
		}
		budgets = []budget{{"email", emailDigest, "60s", 1, time.Minute}, {"email", emailDigest, "1h", 3, time.Hour}, {"email", emailDigest, "1d", 6, 24 * time.Hour}, {"ip", ipDigest, "10m", 10, 10 * time.Minute}, {"ip", ipDigest, "1h", 30, time.Hour}}
	case Verify:
		if email == "" {
			return 0, ErrUnavailable
		}
		budgets = []budget{{"email", emailDigest, "1h", 15, time.Hour}, {"ip", ipDigest, "10m", 30, 10 * time.Minute}}
	case Reset:
		budgets = []budget{{"ip", ipDigest, "10m", 30, 10 * time.Minute}}
	default:
		return 0, ErrUnavailable
	}
	keys := make([]string, 0, len(budgets))
	args := make([]interface{}, 0, len(budgets)*2)
	for _, b := range budgets {
		keys = append(keys, "rushgo:auth:rl:v1:"+string(policy)+":"+b.dimension+":"+b.digest+":"+b.window)
		args = append(args, strconv.Itoa(b.limit), b.ttl.Milliseconds())
	}
	result, err := admissionScript.Run(ctx, l.client, keys, args...).Int64()
	if err != nil || result < 0 {
		return 0, ErrUnavailable
	}
	if result > 0 {
		return time.Duration(result) * time.Millisecond, ErrLimited
	}
	return 0, nil
}
