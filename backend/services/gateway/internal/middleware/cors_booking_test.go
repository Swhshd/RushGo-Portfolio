package middleware

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBookingRecoveryHeadersAccessibleToBrowser(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CORSMiddleware())
	r.POST("/api/bookings", func(c *gin.Context) { c.Status(202) })
	request := httptest.NewRequest("OPTIONS", "/api/bookings", nil)
	request.Header.Set("Origin", "http://checkout.fixture")
	request.Header.Set("Access-Control-Request-Headers", "idempotency-key")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, request)
	if w.Code != 204 || !strings.Contains(strings.ToLower(w.Header().Get("Access-Control-Allow-Headers")), "idempotency-key") {
		t.Fatal("browser cannot send stable booking key", w.Header())
	}
	if !strings.Contains(strings.ToLower(w.Header().Get("Access-Control-Expose-Headers")), "retry-after") {
		t.Fatal("browser cannot read recovery retry deadline")
	}
}
