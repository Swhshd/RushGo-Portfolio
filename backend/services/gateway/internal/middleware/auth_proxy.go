package middleware

import (
	"errors"
	"github.com/gin-gonic/gin"
	"net/netip"
	"strings"
)

func ConfigureTrustedProxies(r *gin.Engine, raw string) error {
	if raw == "" {
		return r.SetTrustedProxies(nil)
	}
	var proxies []string
	for _, element := range strings.Split(raw, ",") {
		value := strings.TrimSpace(element)
		if value == "" {
			return errors.New("invalid trusted proxies")
		}
		if prefix, err := netip.ParsePrefix(value); err == nil {
			if prefix.Bits() == 0 {
				return errors.New("trust-all proxies forbidden")
			}
		} else if addr, err := netip.ParseAddr(value); err != nil || addr.IsUnspecified() {
			return errors.New("invalid trusted proxies")
		}
		proxies = append(proxies, value)
	}
	return r.SetTrustedProxies(proxies)
}
