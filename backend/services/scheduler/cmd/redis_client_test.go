package main

import (
	"strings"
	"testing"
)

func TestSchedulerRedisUsesConfiguredTLSACLAndDatabase(t *testing.T) {
	client, err := newSchedulerRedisClient("rediss://scheduler:fixture-password@locks.internal:6380/3")
	if err != nil {
		t.Fatal("valid private Redis configuration rejected")
	}
	defer client.Close()
	opts := client.Options()
	if opts.Addr != "locks.internal:6380" || opts.Username != "scheduler" || opts.Password != "fixture-password" || opts.DB != 3 {
		t.Fatal("configured Redis endpoint, ACL or database was discarded")
	}
	if opts.TLSConfig == nil || opts.TLSConfig.InsecureSkipVerify || opts.TLSConfig.ServerName != "locks.internal" {
		t.Fatal("verified Redis TLS hostname was discarded")
	}
}

func TestSchedulerRedisInvalidURLFailsWithoutSecret(t *testing.T) {
	client, err := newSchedulerRedisClient("rediss://scheduler:fixture-canary@locks.internal:bad/0")
	if client != nil {
		client.Close()
		t.Fatal("invalid Redis URL accepted")
	}
	if err == nil || strings.Contains(err.Error(), "fixture-canary") {
		t.Fatal("invalid Redis URL must return a redacted failure")
	}
}

func TestSchedulerRedisLocalURLStillSupported(t *testing.T) {
	client, err := newSchedulerRedisClient("redis://127.0.0.1:6379/0")
	if err != nil {
		t.Fatal("local Redis configuration rejected")
	}
	defer client.Close()
	if client.Options().Addr != "127.0.0.1:6379" || client.Options().TLSConfig != nil {
		t.Fatal("local URL not respected")
	}
}
