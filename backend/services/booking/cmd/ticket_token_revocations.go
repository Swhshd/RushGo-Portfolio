package main

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

type redisTicketTokenRevocations struct {
	client *redis.Client
}

func (r redisTicketTokenRevocations) IsRevoked(ctx context.Context, token string) (bool, error) {
	if r.client == nil || token == "" {
		return false, errors.New("ticket token revocation store unavailable")
	}
	exists, err := r.client.Exists(ctx, "blacklist:"+token).Result()
	if err != nil {
		return false, errors.New("ticket token revocation check failed")
	}
	return exists > 0, nil
}
