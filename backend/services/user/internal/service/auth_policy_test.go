package service

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestNewPasswordUnicodeBytes(t *testing.T) {
	for _, p := range []string{strings.Repeat("x", 15), strings.Repeat("😀", 15), strings.Repeat("😀", 18), strings.Repeat(" ", 15), "e\u0301e\u0301e\u0301e\u0301e\u0301e\u0301e\u0301x"} {
		if ValidateNewPassword(p) != nil {
			t.Fatal("valid password boundary rejected")
		}
	}
	for _, p := range []string{strings.Repeat("x", 14), strings.Repeat("x", 73), strings.Repeat("😀", 19), strings.Repeat("x", 15) + string([]byte{0xff})} {
		if ValidateNewPassword(p) == nil {
			t.Fatal("invalid password boundary accepted")
		}
	}
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, errors.New("synthetic rng failure") }
func TestOTPLeadingZeroAndFailure(t *testing.T) {
	code, err := GenerateOTP(bytes.NewReader(make([]byte, 64)))
	if err != nil || code != "000000" {
		t.Fatal("leading-zero draw not preserved")
	}
	if _, err := GenerateOTP(failedReader{}); err == nil {
		t.Fatal("rng failure ignored")
	}
}
func TestResetTokenEncodingHash(t *testing.T) {
	token, hash, err := GenerateResetToken(bytes.NewReader(bytes.Repeat([]byte{1}, 32)))
	if err != nil {
		t.Fatal("token generation failed")
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 || strings.Contains(token, "=") {
		t.Fatal("reset transport invalid")
	}
	if hash != "56d5fa7333f6d747db42c239407e5da4c32f4c79f35d092b134fd35a402d9c5c" {
		t.Fatal("stored digest mismatch")
	}
	if _, _, err := GenerateResetToken(io.LimitReader(bytes.NewReader(make([]byte, 32)), 31)); err == nil {
		t.Fatal("short rng ignored")
	}
}
func TestFrontendOrigin(t *testing.T) {
	for _, origin := range []string{"http://localhost:3002", "http://127.0.0.1:3002", "http://[::1]:3002", "https://example.com"} {
		if _, err := ValidateFrontendOrigin(origin); err != nil {
			t.Fatal("valid origin rejected")
		}
	}
	for _, origin := range []string{"", "http://example.com", "https://user:pass@example.com", "https://example.com/path", "https://example.com?x=y", "https://example.com#x", "javascript:bad", "https://example.com/", "https://example.com:bad"} {
		if _, err := ValidateFrontendOrigin(origin); err == nil {
			t.Fatal("invalid origin accepted")
		}
	}
}
