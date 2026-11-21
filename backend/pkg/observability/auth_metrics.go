package observability

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func RecordAuth(m *Metrics, operation, result string) {
	if m == nil {
		return
	}
	switch result {
	case "success", "invalid", "expired", "rate_limited", "error":
	default:
		result = "error"
	}
	var counter metric.Int64Counter
	switch operation {
	case "send":
		counter = m.AuthVerificationSend
	case "verify":
		counter = m.AuthVerificationVerify
	case "request":
		counter = m.AuthResetRequest
	case "complete":
		counter = m.AuthResetComplete
	default:
		return
	}
	if counter != nil {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("result", result)))
	}
}
func IsUserAuthMethod(method string) bool {
	switch method {
	case "/user.v1.UserService/Register", "/user.v1.UserService/RequestRegistrationCode", "/user.v1.UserService/RequestPasswordReset", "/user.v1.UserService/ResetPassword", "/user.v1.UserService/Login", "/user.v1.UserService/RefreshToken", "/user.v1.UserService/Logout", "/user.v1.UserService/ValidateToken":
		return true
	}
	return false
}
