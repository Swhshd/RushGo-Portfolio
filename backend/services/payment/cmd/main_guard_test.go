package main

import (
	"bytes"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestMain lets the same test binary act as the real payment-service entry
// point. With production configuration, run() reaches config.Load first and
// exits before mTLS, database, Redis, Stripe or listener setup.
func TestMain(m *testing.M) {
	if os.Getenv("PAYMENT_MAIN_GUARD_HELPER") == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

func guardEnv(extra map[string]string) []string {
	blocked := map[string]bool{
		"RUSHGO_ENV": true, "RUSHGO_PROVIDER_MODE": true, "DATABASE_URL": true,
		"REDIS_URL": true, "HTTP_PROXY": true, "HTTPS_PROXY": true,
		"NO_PROXY": true, "PAYMENT_MAIN_GUARD_HELPER": true,
	}
	var env []string
	for _, item := range os.Environ() {
		key := strings.SplitN(item, "=", 2)[0]
		if !blocked[key] {
			env = append(env, item)
		}
	}
	for key, value := range extra {
		env = append(env, key+"="+value)
	}
	return env
}

func TestProductionGuardRejectsBeforeDependencySideEffects(t *testing.T) {
	proxyListener, proxyConnections := guardListener(t)
	defer proxyListener.Close()
	dbListener, dbConnections := guardListener(t)
	defer dbListener.Close()
	redisListener, redisConnections := guardListener(t)
	defer redisListener.Close()
	command := exec.Command(os.Args[0], "-test.run=^$")
	command.Env = guardEnv(map[string]string{
		"PAYMENT_MAIN_GUARD_HELPER": "1",
		"RUSHGO_ENV":                "production",
		"DATABASE_URL":              "postgres://127.0.0.1:" + portOf(dbListener) + "/never",
		"REDIS_URL":                 "redis://127.0.0.1:" + portOf(redisListener) + "/never",
		"HTTP_PROXY":                "http://" + proxyListener.Addr().String(),
		"HTTPS_PROXY":               "http://" + proxyListener.Addr().String(),
		"NO_PROXY":                  "",
	})
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("production startup unexpectedly succeeded: %s", output)
	}
	if !bytes.Contains(output, []byte("LIVE")) {
		t.Fatalf("guard did not fail on LIVE/production before dependencies: %s", output)
	}
	time.Sleep(100 * time.Millisecond)
	if proxyConnections.Load() != 0 || dbConnections.Load() != 0 || redisConnections.Load() != 0 {
		t.Fatalf("guard side effects before rejection: proxy=%d db=%d redis=%d",
			proxyConnections.Load(), dbConnections.Load(), redisConnections.Load())
	}
}

func guardListener(t *testing.T) (net.Listener, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connections atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			_ = conn.Close()
		}
	}()
	return listener, &connections
}

func portOf(listener net.Listener) string {
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return "1"
	}
	return port
}
