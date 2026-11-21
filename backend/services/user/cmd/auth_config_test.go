package main

import (
	"github.com/ticketbox/pkg/config"
	"strings"
	"testing"
)

func TestExternalUserRequiresStrongSecretAndCorrectServiceIdentity(t *testing.T) {
	for _, secret := range []string{"", "short", strings.Repeat("x", 31)} {
		c := &config.Config{Release: config.ReleaseIdentity{Environment: "preproduction"}, ServiceName: "user-service", JWTSecret: secret}
		if validateUserAuthConfig(c) == nil {
			t.Fatal("missing/short JWT accepted")
		}
	}
	c := &config.Config{Release: config.ReleaseIdentity{Environment: "preproduction"}, ServiceName: "gateway-service", JWTSecret: strings.Repeat("x", 32)}
	if validateUserAuthConfig(c) == nil {
		t.Fatal("wrong service identity accepted")
	}
	c.ServiceName = "user-service"
	if validateUserAuthConfig(c) != nil {
		t.Fatal("valid strong key refused")
	}
}

func TestSMTPPartialConfigRejected(t *testing.T) {
	if validateUserAuthConfig(&config.Config{SMTPHost: "smtp.fixture.test"}) == nil {
		t.Fatal("partial auth mail configuration accepted")
	}
	if validateUserAuthConfig(&config.Config{}) != nil {
		t.Fatal("absent mail blocked legacy startup")
	}
}
func TestOriginAndTransportConfig(t *testing.T) {
	base := config.Config{SMTPHost: "smtp.fixture.test", SMTPPort: 465, SMTPTLSMode: "implicit", SMTPUsername: "synthetic", SMTPPassword: "synthetic", SMTPFrom: "sender@example.com", AuthFrontendOrigin: "http://localhost:3002"}
	if validateUserAuthConfig(&base) != nil {
		t.Fatal("valid local config rejected")
	}
	base.AuthFrontendOrigin = "http://example.com"
	if validateUserAuthConfig(&base) == nil {
		t.Fatal("untrusted origin accepted")
	}
}
