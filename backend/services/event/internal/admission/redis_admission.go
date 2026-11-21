package admission

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type RedisAdmission struct {
	client        *redis.Client
	scanMu        sync.Mutex
	pendingKeys   []string
	pendingCursor uint64
}

func NewRedisAdmission(client *redis.Client) *RedisAdmission {
	return &RedisAdmission{client: client}
}

var acquireScript = redis.NewScript(`
local existing = 0
for i, key in ipairs(KEYS) do
  local current = redis.call("GET", key)
  if current then
    if current ~= ARGV[1] then
      return {0, 0, existing}
    end
    existing = existing + 1
  end
end

local created = 0
for i, key in ipairs(KEYS) do
  if redis.call("SET", key, ARGV[1], "PX", ARGV[2], "NX") then
    created = created + 1
  end
end
return {1, created, existing}
`)

var releaseScript = redis.NewScript(`
local deleted = 0
local missing = 0
local ownerMismatch = 0
for i, key in ipairs(KEYS) do
  local current = redis.call("GET", key)
  if not current then
    missing = missing + 1
  elseif current == ARGV[1] then
    deleted = deleted + redis.call("DEL", key)
  else
    ownerMismatch = ownerMismatch + 1
  end
end
return {deleted, missing, ownerMismatch}
`)

func (r *RedisAdmission) Acquire(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, owner uuid.UUID, reservationMinutes int32) (AcquireResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if r == nil || r.client == nil {
		return AcquireResult{}, fmt.Errorf("redis client is nil: %w", ErrInfrastructure)
	}
	if err := validateRequest(eventID, owner, seatIDs); err != nil {
		return AcquireResult{}, err
	}
	ttl, err := ReservationTTL(reservationMinutes)
	if err != nil {
		return AcquireResult{}, err
	}
	keys := makeKeys(eventID, seatIDs)
	values, err := acquireScript.Run(ctx, r.client, keys, owner.String(), strconv.FormatInt(ttl.Milliseconds(), 10)).Int64Slice()
	if err != nil {
		return AcquireResult{}, fmt.Errorf("run seat admission acquire script: %w: %v", ErrInfrastructure, err)
	}
	if len(values) != 3 {
		return AcquireResult{}, fmt.Errorf("unexpected acquire script result %v: %w", values, ErrInfrastructure)
	}
	if values[0] == 0 {
		return AcquireResult{Existing: int(values[2])}, ErrConflict
	}
	return AcquireResult{Created: int(values[1]), Existing: int(values[2])}, nil
}

func (r *RedisAdmission) Release(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, owner uuid.UUID) (ReleaseResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if r == nil || r.client == nil {
		return ReleaseResult{}, fmt.Errorf("redis client is nil: %w", ErrInfrastructure)
	}
	if err := validateRequest(eventID, owner, seatIDs); err != nil {
		return ReleaseResult{}, err
	}
	values, err := releaseScript.Run(ctx, r.client, makeKeys(eventID, seatIDs), owner.String()).Int64Slice()
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("run seat admission release script: %w: %v", ErrInfrastructure, err)
	}
	if len(values) != 3 {
		return ReleaseResult{}, fmt.Errorf("unexpected release script result %v: %w", values, ErrInfrastructure)
	}
	return ReleaseResult{
		Deleted:       int(values[0]),
		Missing:       int(values[1]),
		OwnerMismatch: int(values[2]),
	}, nil
}

func makeKeys(eventID uuid.UUID, seatIDs []uuid.UUID) []string {
	keys := make([]string, len(seatIDs))
	for i, seatID := range seatIDs {
		keys[i] = Key(eventID, seatID)
	}
	return keys
}

var _ Admission = (*RedisAdmission)(nil)
