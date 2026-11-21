package main

import (
	"errors"

	"github.com/redis/go-redis/v9"
)

func newSchedulerRedisClient(redisURL string) (*redis.Client, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		// Parser errors can contain URL credentials. Keep startup diagnostics safe.
		return nil, errors.New("invalid Redis configuration")
	}
	return redis.NewClient(options), nil
}
