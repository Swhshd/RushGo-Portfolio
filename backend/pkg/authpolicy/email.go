package authpolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/mail"
	"strings"
	"unicode"
	"unicode/utf8"
)

func NormalizeEmail(raw string) string { return strings.ToLower(strings.TrimSpace(raw)) }
func CanonicalEmail(raw string) (string, error) {
	invalid := errors.New("invalid email")
	if !utf8.ValidString(raw) || strings.ContainsAny(raw, "\r\n") {
		return "", invalid
	}
	canonical := NormalizeEmail(raw)
	if canonical == "" || utf8.RuneCountInString(canonical) > 255 {
		return "", invalid
	}
	for _, r := range canonical {
		if unicode.IsControl(r) {
			return "", invalid
		}
	}
	addr, err := mail.ParseAddress(canonical)
	if err != nil || addr.Name != "" || addr.Address != canonical {
		return "", invalid
	}
	return canonical, nil
}
func digest(domain, value string) string {
	sum := sha256.Sum256([]byte("rushgo:auth:" + domain + "\x00" + value))
	return hex.EncodeToString(sum[:])
}
func DigestEmail(canonical string) string { return digest("email", canonical) }
func DigestIP(normalizedIP string) string { return digest("ip", normalizedIP) }
