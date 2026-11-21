package handler

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/gateway/internal/authlimiter"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type authClient struct {
	userv1.UserServiceClient
	calls, revoke int
	err           error
}

func (c *authClient) Register(context.Context, *userv1.RegisterRequest, ...grpc.CallOption) (*userv1.RegistrationResponse, error) {
	c.calls++
	return &userv1.RegistrationResponse{AccountCreated: true}, c.err
}
func (c *authClient) RequestRegistrationCode(context.Context, *userv1.RegistrationCodeRequest, ...grpc.CallOption) (*userv1.RegistrationCodeResponse, error) {
	c.calls++
	return &userv1.RegistrationCodeResponse{}, c.err
}
func (c *authClient) RequestPasswordReset(context.Context, *userv1.PasswordResetRequest, ...grpc.CallOption) (*userv1.PasswordResetResponse, error) {
	c.calls++
	return &userv1.PasswordResetResponse{}, c.err
}
func (c *authClient) Logout(context.Context, *userv1.LogoutRequest, ...grpc.CallOption) (*userv1.LogoutResponse, error) {
	c.revoke++
	return &userv1.LogoutResponse{}, c.err
}

type deniedLimiter struct{ err error }

func (l deniedLimiter) Admit(context.Context, authlimiter.Policy, string, string) (time.Duration, error) {
	return time.Minute, l.err
}
func requestAuth(h *AuthHandler, route, body string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/auth", func(c *gin.Context) {
		switch route {
		case "register":
			h.Register(c)
		case "send":
			h.RequestRegistrationCode(c)
		case "forgot":
			h.RequestPasswordReset(c)
		case "logout":
			h.Logout(c)
		}
	})
	req := httptest.NewRequest("POST", "/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer synthetic")
	req.RemoteAddr = "192.0.2.1:1234"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}
func TestAuthUnverifiedAndNoStore(t *testing.T) {
	c := &authClient{}
	h := NewAuthHandler(c, nil)
	w := requestAuth(h, "register", `{"email":"a@example.com","password":"synthetic long input","name":"M6"}`)
	if w.Code != 400 || c.calls != 0 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("unverified or cacheable auth")
	}
}
func TestAuthLimiterOutageDoesNotSend(t *testing.T) {
	c := &authClient{}
	h := NewAuthHandler(c, nil)
	h.limiter = deniedLimiter{authlimiter.ErrUnavailable}
	w := requestAuth(h, "send", `{"email":"a@example.com"}`)
	if w.Code != 503 || c.calls != 0 || strings.Contains(w.Body.String(), "rpc") {
		t.Fatal("mail gate failed open")
	}
	h.limiter = deniedLimiter{authlimiter.ErrLimited}
	w = requestAuth(h, "send", `{"email":"a@example.com"}`)
	if w.Code != 429 || w.Header().Get("Retry-After") != "60" {
		t.Fatal("rate envelope missing")
	}
}
func TestAuthBoundAndSafeProviderError(t *testing.T) {
	c := &authClient{err: status.Error(codes.Internal, "synthetic provider secret")}
	h := NewAuthHandler(c, nil)
	h.limiter = deniedLimiter{}
	w := requestAuth(h, "register", `{"email":"a@example.com","password":"synthetic long input","name":"M6","verification_id":"`+uuid.NewString()+`","verification_code":"000001"}`)
	if w.Code != 503 || strings.Contains(w.Body.String(), "synthetic provider secret") {
		t.Fatal("provider error crossed HTTP")
	}
	w = requestAuth(h, "register", `{"email":"`+strings.Repeat("x", 17000)+`"}`)
	if w.Code != 413 {
		t.Fatal("auth request bound missing")
	}
}
func TestLogoutUserBeforeBlacklistFailure(t *testing.T) {
	c := &authClient{}
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	defer redisClient.Close()
	h := NewAuthHandler(c, redisClient)
	w := requestAuth(h, "logout", `{"refresh_token":"`+uuid.NewString()+`"}`)
	if w.Code != 503 || c.revoke != 1 {
		t.Fatal("blacklist failure claimed full logout")
	}
	c.err = status.Error(codes.Unavailable, "TEMPORARY_UNAVAILABLE")
	w = requestAuth(h, "logout", `{"refresh_token":"`+uuid.NewString()+`"}`)
	if w.Code != 503 {
		t.Fatal("User unavailable claimed success")
	}
}
func TestForgotGenericAdmission(t *testing.T) {
	c := &authClient{}
	h := NewAuthHandler(c, nil)
	h.limiter = deniedLimiter{}
	a := requestAuth(h, "forgot", `{"email":"known@example.com"}`)
	b := requestAuth(h, "forgot", `{"email":"unknown@example.com"}`)
	if a.Code != 202 || b.Code != 202 || a.Body.String() != b.Body.String() {
		t.Fatal("forgot existence response")
	}
}
