package service

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/ticketbox/user/internal/domain"
)

func ValidateNewPassword(password string) error {
	if !utf8.ValidString(password) || utf8.RuneCountInString(password) < 15 || len(password) > 72 {
		return domain.ErrValidation
	}
	return nil
}
func GenerateOTP(rng io.Reader) (string, error) {
	n, err := rand.Int(rng, big.NewInt(1000000))
	if err != nil {
		return "", domain.ErrTemporaryUnavailable
	}
	return fmt.Sprintf("%06d", n.Int64()), nil
}
func GenerateResetToken(rng io.Reader) (string, string, error) {
	raw := make([]byte, 32)
	if _, err := io.ReadFull(rng, raw); err != nil {
		return "", "", domain.ErrTemporaryUnavailable
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	return token, HashToken(token), nil
}
func ValidateFrontendOrigin(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(raw, "\r\n") {
		return nil, domain.ErrValidation
	}
	hostname := u.Hostname()
	local := hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return nil, domain.ErrValidation
	}
	if hostname == "" || strings.ContainsAny(hostname, " \t") {
		return nil, domain.ErrValidation
	}
	if u.Port() != "" {
		if _, err := net.LookupPort("tcp", u.Port()); err != nil {
			return nil, domain.ErrValidation
		}
	}
	return u, nil
}
