package authpolicy

import (
	"strings"
	"testing"
)

func TestNormalizeEmail(t *testing.T) {
	if got := NormalizeEmail("\u2003User.Name+Tag@Example.COM\u2003"); got != "user.name+tag@example.com" {
		t.Fatal("canonical identity mismatch")
	}
}

func TestCanonicalEmail(t *testing.T) {
	for _, raw := range []string{"A.Name+Tag@example.com", " user@example.com ", "\u2003user@example.com\u2003"} {
		if _, err := CanonicalEmail(raw); err != nil {
			t.Fatal("valid bare mailbox rejected")
		}
	}
	for _, raw := range []string{"Name <user@example.com>", "user@example.com\r\n", "a\x00@example.com", "bad", string([]byte{0xff}) + "@example.com", strings.Repeat("a", 256) + "@example.com"} {
		if _, err := CanonicalEmail(raw); err == nil {
			t.Fatal("invalid mailbox accepted")
		}
	}
	if DigestEmail("user@example.com") == DigestIP("user@example.com") {
		t.Fatal("digest domains overlap")
	}
	if strings.Contains(DigestEmail("user@example.com"), "user") {
		t.Fatal("readable identity key")
	}
}
