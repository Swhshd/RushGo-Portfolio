package middleware

import (
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"testing"
)

func TestProxyUntrustedForwardingIgnored(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if ConfigureTrustedProxies(r, "") != nil {
		t.Fatal("no-proxy rejected")
	}
	var ip string
	r.GET("/probe", func(c *gin.Context) { ip = c.ClientIP() })
	req := httptest.NewRequest("GET", "/probe", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	req.Header.Set("X-Forwarded-For", "203.0.113.77")
	r.ServeHTTP(httptest.NewRecorder(), req)
	if ip != "192.0.2.10" {
		t.Fatal("spoofed identity selected")
	}
}
func TestTrustedProxyChainAndValidation(t *testing.T) {
	for _, raw := range []string{"bad", "0.0.0.0/0", "::/0", "192.0.2.1,,192.0.2.2"} {
		if ConfigureTrustedProxies(gin.New(), raw) == nil {
			t.Fatal("unsafe proxy config accepted")
		}
	}
	r := gin.New()
	if ConfigureTrustedProxies(r, "192.0.2.10") != nil {
		t.Fatal("explicit peer rejected")
	}
	var ip string
	r.GET("/probe", func(c *gin.Context) { ip = c.ClientIP() })
	req := httptest.NewRequest("GET", "/probe", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	req.Header.Set("X-Forwarded-For", "203.0.113.77, 198.51.100.3")
	r.ServeHTTP(httptest.NewRecorder(), req)
	if ip != "198.51.100.3" {
		t.Fatal("trusted chain took spoofed leftmost")
	}
}
