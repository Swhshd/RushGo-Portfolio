package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPaymentHTTPErrorMapsAccountingReadiness(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		name    string
		err     error
		status  int
		message string
	}{
		{"not_ready", status.Error(codes.FailedPrecondition, "accounting_not_ready"), http.StatusConflict, "ticket accounting is not ready for sale"},
		{"other_conflict", status.Error(codes.FailedPrecondition, "financial_state_protected"), http.StatusConflict, "financial_state_protected"},
		{"unavailable", status.Error(codes.Unavailable, "accounting_readiness_unavailable"), http.StatusServiceUnavailable, "financial responsibility unavailable; retry same identity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			paymentHTTPError(c, tc.err)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d", w.Code, tc.status)
			}
			var body map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body["error"] != tc.message {
				t.Fatalf("error=%v want=%s", body["error"], tc.message)
			}
		})
	}
}
