package handler

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	userv1 "github.com/ticketbox/pkg/proto/user/v1"
)

type UserHandler struct {
	userClient userv1.UserServiceClient
}

func NewUserHandler(userClient userv1.UserServiceClient) *UserHandler {
	return &UserHandler{userClient: userClient}
}

func (h *UserHandler) GetProfile(c *gin.Context) {
	userID, _ := c.Get("user_id")

	resp, err := h.userClient.GetProfile(c.Request.Context(), &userv1.GetProfileRequest{
		UserId: userID.(string),
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		} else {
			rpcAuthError(c, err)
		}
		return
	}

	c.JSON(http.StatusOK, toUserJSON(resp))
}

func (h *UserHandler) UpdateProfile(c *gin.Context) {
	userID, _ := c.Get("user_id")

	var req struct {
		Name   string `json:"name"`
		Email  string `json:"email"`
		Locale string `json:"locale"`
	}
	if !bindAuth(c, &req) {
		return
	}
	if req.Email != "" {
		writeAuthError(c, 400, "EMAIL_CHANGE_NOT_SUPPORTED", "Email changes require a separate verification flow.")
		return
	}

	resp, err := h.userClient.UpdateProfile(c.Request.Context(), &userv1.UpdateProfileRequest{
		UserId: userID.(string),
		Name:   req.Name,
		Email:  req.Email,
		Locale: req.Locale,
	})
	if err != nil {
		rpcAuthError(c, err)
		return
	}

	c.JSON(http.StatusOK, toUserJSON(resp))
}

func (h *UserHandler) ChangePassword(c *gin.Context) {
	var req struct {
		Current string `json:"current_password"`
		Next    string `json:"new_password"`
	}
	if !bindAuth(c, &req) {
		return
	}
	resp, err := h.userClient.ChangePassword(ticketIdentityContext(c), &userv1.ChangePasswordRequest{UserId: c.GetString("user_id"), CurrentPassword: req.Current, NewPassword: req.Next})
	if err != nil {
		rpcAuthError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": resp.GetMessage(), "sign_in_required": true})
}
func (h *UserHandler) RequestEmailChange(c *gin.Context) {
	var req struct {
		Current string `json:"current_password"`
		Email   string `json:"email"`
	}
	if !bindAuth(c, &req) {
		return
	}
	resp, err := h.userClient.RequestEmailChange(ticketIdentityContext(c), &userv1.EmailChangeRequest{UserId: c.GetString("user_id"), CurrentPassword: req.Current, Email: req.Email})
	if err != nil {
		rpcAuthError(c, err)
		return
	}
	if resp == nil || resp.ExpiresAt == nil || resp.NextResendAt == nil {
		writeAuthError(c, 503, "TEMPORARY_UNAVAILABLE", "Verification state unavailable.")
		return
	}
	c.JSON(200, gin.H{"message": resp.Message, "verification_id": resp.VerificationId, "expires_at": resp.ExpiresAt.AsTime().Format(time.RFC3339), "next_resend_at": resp.NextResendAt.AsTime().Format(time.RFC3339)})
}
func (h *UserHandler) CompleteEmailChange(c *gin.Context) {
	var req struct {
		Current        string `json:"current_password"`
		VerificationID string `json:"verification_id"`
		Code           string `json:"verification_code"`
	}
	if !bindAuth(c, &req) {
		return
	}
	resp, err := h.userClient.CompleteEmailChange(ticketIdentityContext(c), &userv1.CompleteEmailChangeRequest{UserId: c.GetString("user_id"), CurrentPassword: req.Current, VerificationId: req.VerificationID, VerificationCode: req.Code})
	if err != nil {
		rpcAuthError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": resp.GetMessage(), "sign_in_required": true})
}
