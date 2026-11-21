package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdminOutboxAuthMiddleware keeps transient User validation failures from
// looking like an expired browser session. It is intentionally separate from
// AuthMiddleware so existing routes retain their established behavior.
func AdminOutboxAuthMiddleware(userClient userv1.UserServiceClient, redisClient *redis.Client) gin.HandlerFunc {
	return adminLiveRoleAuthMiddleware(userClient, redisClient, "outbox source unavailable")
}

// AdminCurrentRoleAuthMiddleware protects privileged mutations with current
// User authority and revocation, including tokens issued before a demotion.
func AdminCurrentRoleAuthMiddleware(userClient userv1.UserServiceClient, redisClient *redis.Client) gin.HandlerFunc {
	return adminLiveRoleAuthMiddleware(userClient, redisClient, "admin authorization unavailable")
}

// AdminEventTicketOperationsAuthMiddleware applies the same revocation and
// live-role checks while keeping endpoint errors specific to this source.
func AdminEventTicketOperationsAuthMiddleware(userClient userv1.UserServiceClient, redisClient *redis.Client) gin.HandlerFunc {
	return adminLiveRoleAuthMiddleware(userClient, redisClient, "ticket operations source unavailable")
}

// AdminEventTicketListAuthMiddleware applies the same token revocation and
// current-role checks as the existing Event Operations reader.
func AdminEventTicketListAuthMiddleware(userClient userv1.UserServiceClient, redisClient *redis.Client) gin.HandlerFunc {
	return adminLiveRoleAuthMiddleware(userClient, redisClient, "ticket list source unavailable")
}

func adminLiveRoleAuthMiddleware(userClient userv1.UserServiceClient, redisClient *redis.Client, unavailableMessage string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			abortAdminOutboxAuthentication(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == "" || token == authHeader || strings.TrimSpace(token) != token || strings.ContainsAny(token, " \t\r\n") {
			abortAdminOutboxAuthentication(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
			return
		}
		if redisClient == nil || userClient == nil {
			abortAdminOutboxAuthentication(c, http.StatusServiceUnavailable, "source_unavailable", unavailableMessage)
			return
		}
		blacklistKey := "blacklist:" + token
		blacklisted, err := redisClient.Exists(c.Request.Context(), blacklistKey).Result()
		if err != nil {
			abortAdminOutboxAuthentication(c, http.StatusServiceUnavailable, "source_unavailable", unavailableMessage)
			return
		}
		if blacklisted > 0 {
			abortAdminOutboxAuthentication(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
			return
		}

		resp, err := userClient.ValidateToken(c.Request.Context(), &userv1.ValidateTokenRequest{AccessToken: token})
		if err != nil {
			if status.Code(err) == codes.Unauthenticated {
				abortAdminOutboxAuthentication(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
				return
			}
			abortAdminOutboxAuthentication(c, http.StatusServiceUnavailable, "source_unavailable", unavailableMessage)
			return
		}
		if resp == nil || !resp.GetValid() {
			abortAdminOutboxAuthentication(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
			return
		}
		if resp.GetRole() != "admin" {
			abortAdminOutboxAuthentication(c, http.StatusForbidden, "forbidden", "admin access required")
			return
		}
		tokenUserID, err := uuid.Parse(resp.GetUserId())
		if err != nil || tokenUserID == uuid.Nil {
			abortAdminOutboxAuthentication(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
			return
		}

		current, err := userClient.ValidateToken(c.Request.Context(), &userv1.ValidateTokenRequest{
			AccessToken: token, RequireCurrentRole: true,
		})
		if err != nil {
			switch status.Code(err) {
			case codes.Unauthenticated:
				abortAdminOutboxAuthentication(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
			case codes.NotFound, codes.PermissionDenied:
				abortAdminOutboxAuthentication(c, http.StatusForbidden, "forbidden", "admin access required")
			default:
				abortAdminOutboxAuthentication(c, http.StatusServiceUnavailable, "source_unavailable", unavailableMessage)
			}
			return
		}
		if current == nil {
			abortAdminOutboxAuthentication(c, http.StatusServiceUnavailable, "source_unavailable", unavailableMessage)
			return
		}
		if !current.GetValid() {
			abortAdminOutboxAuthentication(c, http.StatusUnauthorized, "unauthenticated", "authentication required")
			return
		}
		if !current.GetCurrentRoleVerified() {
			abortAdminOutboxAuthentication(c, http.StatusServiceUnavailable, "source_unavailable", unavailableMessage)
			return
		}
		currentUserID, err := uuid.Parse(current.GetUserId())
		if err != nil || currentUserID == uuid.Nil {
			abortAdminOutboxAuthentication(c, http.StatusServiceUnavailable, "source_unavailable", unavailableMessage)
			return
		}
		if currentUserID != tokenUserID {
			abortAdminOutboxAuthentication(c, http.StatusForbidden, "forbidden", "admin access required")
			return
		}
		if current.GetRole() != "admin" {
			abortAdminOutboxAuthentication(c, http.StatusForbidden, "forbidden", "admin access required")
			return
		}

		c.Set("user_id", resp.GetUserId())
		c.Set("email", resp.GetEmail())
		c.Set("role", current.GetRole())
		c.Next()
	}
}

func abortAdminOutboxAuthentication(c *gin.Context, httpStatus int, code, message string) {
	c.Header("Cache-Control", "no-store")
	c.AbortWithStatusJSON(httpStatus, gin.H{"code": code, "error": message})
}
