package handler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/ticketbox/gateway/internal/authlimiter"
	"github.com/ticketbox/pkg/authpolicy"
	"github.com/ticketbox/pkg/observability"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc/status"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type AuthHandler struct {
	userClient  userv1.UserServiceClient
	redisClient *redis.Client
	limiter     authlimiter.AuthLimiter
	metrics     *observability.Metrics
}

func NewAuthHandler(client userv1.UserServiceClient, redisClient *redis.Client, metrics ...*observability.Metrics) *AuthHandler {
	h := &AuthHandler{userClient: client, redisClient: redisClient, limiter: authlimiter.New(redisClient)}
	if len(metrics) > 0 {
		h.metrics = metrics[0]
	}
	return h
}
func writeAuthError(c *gin.Context, httpStatus int, code, message string) {
	c.Header("Cache-Control", "no-store")
	c.JSON(httpStatus, gin.H{"code": code, "error": message})
}
func bindAuth(c *gin.Context, dst interface{}) bool {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16*1024)
	decoder := json.NewDecoder(c.Request.Body)
	err := decoder.Decode(dst)
	if err == nil {
		var tail interface{}
		if err = decoder.Decode(&tail); err == nil {
			err = errors.New("extra JSON")
		} else if err.Error() == "EOF" {
			err = nil
		}
	}
	if err == nil {
		return true
	}
	var oversized *http.MaxBytesError
	if errors.As(err, &oversized) {
		writeAuthError(c, 413, "VALIDATION_ERROR", "Request is too large.")
	} else {
		writeAuthError(c, 400, "VALIDATION_ERROR", "Please check the required fields.")
	}
	return false
}
func rpcAuthError(c *gin.Context, err error) {
	code := status.Convert(err).Message()
	httpStatus := 400
	message := "Please check your request."
	switch code {
	case "VALIDATION_ERROR":
		message = "Please check the required fields."
	case "INVALID_CODE":
		message = "The verification code is invalid."
	case "CODE_EXPIRED":
		message = "The verification code has expired. Request a new code."
	case "CODE_ATTEMPTS_EXHAUSTED":
		message = "Too many incorrect codes. Request a new code."
	case "ACCOUNT_EXISTS":
		httpStatus = 409
		message = "An account already exists. Please sign in or recover your password."
	case "INVALID_RESET_TOKEN":
		message = "This reset link is invalid. Request a new link."
	case "RESET_TOKEN_EXPIRED":
		message = "This reset link has expired. Request a new link."
	case "RESET_TOKEN_USED":
		message = "This reset link has already been used."
	case "EMAIL_CHANGE_NOT_SUPPORTED":
		message = "Email changes require a separate verification flow."
	case "INVALID_SESSION":
		message = "The current session could not be ended."
	case "INVALID_CREDENTIALS", "TOKEN_EXPIRED":
		httpStatus = 401
		message = "Invalid or expired credentials."
	case "RATE_LIMITED":
		httpStatus = 429
		message = "Please wait before trying again."
		c.Header("Retry-After", "60")
	case "DELIVERY_UNAVAILABLE":
		httpStatus = 503
		message = "Verification mail is temporarily unavailable."
	default:
		code = "TEMPORARY_UNAVAILABLE"
		httpStatus = 503
		message = "Temporarily unavailable. Please try again."
	}
	writeAuthError(c, httpStatus, code, message)
}
func (h *AuthHandler) admit(c *gin.Context, policy authlimiter.Policy, email string) bool {
	retry, err := h.limiter.Admit(c.Request.Context(), policy, email, c.ClientIP())
	if err == nil {
		return true
	}
	operation := map[authlimiter.Policy]string{authlimiter.Send: "send", authlimiter.Verify: "verify", authlimiter.Forgot: "request", authlimiter.Reset: "complete"}[policy]
	result := "error"
	if errors.Is(err, authlimiter.ErrLimited) {
		result = "rate_limited"
	}
	observability.RecordAuth(h.metrics, operation, result)
	if errors.Is(err, authlimiter.ErrLimited) {
		c.Header("Retry-After", strconv.Itoa(int(math.Ceil(retry.Seconds()))))
		writeAuthError(c, 429, "RATE_LIMITED", "Please wait before trying again.")
	} else {
		writeAuthError(c, 503, "TEMPORARY_UNAVAILABLE", "Temporarily unavailable. Please try again.")
	}
	return false
}
func canonicalIngress(c *gin.Context, raw string) (string, bool) {
	email, err := authpolicy.CanonicalEmail(raw)
	if err != nil {
		writeAuthError(c, 400, "VALIDATION_ERROR", "Please enter a valid email address.")
		return "", false
	}
	return email, true
}
func (h *AuthHandler) RequestRegistrationCode(c *gin.Context) {
	var req struct {
		Email string `json:"email"`
	}
	if !bindAuth(c, &req) {
		return
	}
	email, ok := canonicalIngress(c, req.Email)
	if !ok || !h.admit(c, authlimiter.Send, email) {
		return
	}
	resp, err := h.userClient.RequestRegistrationCode(c.Request.Context(), &userv1.RegistrationCodeRequest{Email: email})
	if err != nil {
		rpcAuthError(c, err)
		return
	}
	c.JSON(200, gin.H{"verification_id": resp.VerificationId, "expires_at": resp.ExpiresAt.AsTime().Format(time.RFC3339), "next_resend_at": resp.NextResendAt.AsTime().Format(time.RFC3339), "message": "Verification message accepted for delivery."})
}
func (h *AuthHandler) Register(c *gin.Context) {
	var req struct {
		Email          string `json:"email"`
		Password       string `json:"password"`
		Name           string `json:"name"`
		VerificationID string `json:"verification_id"`
		Code           string `json:"verification_code"`
	}
	if !bindAuth(c, &req) {
		return
	}
	email, ok := canonicalIngress(c, req.Email)
	if !ok {
		return
	}
	id, err := uuid.Parse(req.VerificationID)
	valid := len(req.Code) == 6
	for _, r := range req.Code {
		if r < '0' || r > '9' {
			valid = false
		}
	}
	if err != nil || id == uuid.Nil || !valid || req.Password == "" || strings.TrimSpace(req.Name) == "" {
		writeAuthError(c, 400, "VALIDATION_ERROR", "Please check the required fields.")
		return
	}
	if !h.admit(c, authlimiter.Verify, email) {
		return
	}
	resp, err := h.userClient.Register(c.Request.Context(), &userv1.RegisterRequest{Email: email, Password: req.Password, Name: req.Name, VerificationId: id.String(), VerificationCode: req.Code})
	if err != nil {
		rpcAuthError(c, err)
		return
	}
	c.JSON(201, gin.H{"account_created": resp.AccountCreated, "message": "Account created. Please sign in."})
}
func (h *AuthHandler) RequestPasswordReset(c *gin.Context) {
	var req struct {
		Email string `json:"email"`
	}
	if !bindAuth(c, &req) {
		return
	}
	email, ok := canonicalIngress(c, req.Email)
	if !ok || !h.admit(c, authlimiter.Forgot, email) {
		return
	}
	if _, err := h.userClient.RequestPasswordReset(c.Request.Context(), &userv1.PasswordResetRequest{Email: email}); err != nil {
		rpcAuthError(c, err)
		return
	}
	c.JSON(202, gin.H{"message": "Request received. If this email is eligible for password recovery, a reset message will be sent."})
}
func (h *AuthHandler) ResetPassword(c *gin.Context) {
	var req struct {
		Token    string `json:"token"`
		Password string `json:"new_password"`
	}
	if !bindAuth(c, &req) {
		return
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(req.Token)
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != req.Token || req.Password == "" {
		writeAuthError(c, 400, "VALIDATION_ERROR", "Please check the required fields.")
		return
	}
	if !h.admit(c, authlimiter.Reset, "") {
		return
	}
	if _, err = h.userClient.ResetPassword(c.Request.Context(), &userv1.ResetPasswordRequest{Token: req.Token, NewPassword: req.Password}); err != nil {
		rpcAuthError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Password updated. Please sign in."})
}
func (h *AuthHandler) Login(c *gin.Context) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !bindAuth(c, &req) {
		return
	}
	if req.Email == "" || req.Password == "" {
		writeAuthError(c, 400, "VALIDATION_ERROR", "Please check the required fields.")
		return
	}
	resp, err := h.userClient.Login(c.Request.Context(), &userv1.LoginRequest{Email: authpolicy.NormalizeEmail(req.Email), Password: req.Password})
	if err != nil {
		rpcAuthError(c, err)
		return
	}
	c.JSON(200, gin.H{"access_token": resp.AccessToken, "refresh_token": resp.RefreshToken, "user": toUserJSON(resp.User)})
}
func (h *AuthHandler) RefreshToken(c *gin.Context) {
	var req struct {
		Refresh string `json:"refresh_token"`
	}
	if !bindAuth(c, &req) {
		return
	}
	if req.Refresh == "" {
		writeAuthError(c, 400, "VALIDATION_ERROR", "Please check the required fields.")
		return
	}
	resp, err := h.userClient.RefreshToken(c.Request.Context(), &userv1.RefreshTokenRequest{RefreshToken: req.Refresh})
	if err != nil {
		rpcAuthError(c, err)
		return
	}
	c.JSON(200, gin.H{"access_token": resp.AccessToken, "refresh_token": resp.RefreshToken, "user": toUserJSON(resp.User)})
}
func (h *AuthHandler) Logout(c *gin.Context) {
	var req struct {
		Refresh string `json:"refresh_token"`
	}
	if !bindAuth(c, &req) {
		return
	}
	if _, err := uuid.Parse(req.Refresh); err != nil {
		writeAuthError(c, 400, "VALIDATION_ERROR", "Current refresh credential is required.")
		return
	}
	access := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
	if _, err := h.userClient.Logout(c.Request.Context(), &userv1.LogoutRequest{AccessToken: access, RefreshToken: req.Refresh}); err != nil {
		rpcAuthError(c, err)
		return
	}
	if h.redisClient == nil || h.redisClient.Set(c.Request.Context(), "blacklist:"+access, "1", 24*time.Hour).Err() != nil {
		writeAuthError(c, 503, "TEMPORARY_UNAVAILABLE", "Local session ended. Server logout could not be fully confirmed.")
		return
	}
	c.JSON(200, gin.H{"message": "Logged out."})
}
func toUserJSON(u *userv1.UserProfile) gin.H {
	if u == nil {
		return nil
	}
	return gin.H{"id": u.Id, "email": u.Email, "name": u.Name, "role": u.Role, "locale": u.Locale, "created_at": u.CreatedAt.AsTime().Format(time.RFC3339)}
}
