package main

import (
	"github.com/ticketbox/pkg/config"
	"testing"
)

func TestTrustedProxyStartup(t *testing.T) {
	if validateGatewayProxyConfig(&config.Config{TrustedProxies: "0.0.0.0/0"}) == nil {
		t.Fatal("startup trusts all")
	}
	if validateGatewayProxyConfig(&config.Config{}) != nil {
		t.Fatal("no-proxy startup rejected")
	}
}
