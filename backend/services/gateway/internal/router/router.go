package router

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/ticketbox/pkg/observability"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	notificationv1 "github.com/ticketbox/pkg/proto/notification/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	schedulerv1 "github.com/ticketbox/pkg/proto/scheduler/v1"
	userv1 "github.com/ticketbox/pkg/proto/user/v1"

	"github.com/ticketbox/gateway/internal/handler"
	"github.com/ticketbox/gateway/internal/middleware"
	"github.com/ticketbox/gateway/internal/realtime"
)

const (
	adminOutboxRequestTimeout                = 10 * time.Second
	adminEventTicketOperationsRequestTimeout = 10 * time.Second
)

// SetupRouter retains the existing no-proxy integration seam.
func SetupRouter(userClient userv1.UserServiceClient, eventClient eventv1.EventServiceClient, bookingClient bookingv1.BookingServiceClient, paymentClient paymentv1.PaymentServiceClient, schedulerClient schedulerv1.SchedulerServiceClient, redisClient *redis.Client, realtimeHub *realtime.Hub, telemetry ...*observability.Runtime) *gin.Engine {
	r, _ := SetupRouterWithProxies(userClient, eventClient, bookingClient, paymentClient, schedulerClient, redisClient, realtimeHub, "", telemetry...)
	return r
}

func SetupRouterWithProxies(
	userClient userv1.UserServiceClient,
	eventClient eventv1.EventServiceClient,
	bookingClient bookingv1.BookingServiceClient,
	paymentClient paymentv1.PaymentServiceClient,
	schedulerClient schedulerv1.SchedulerServiceClient,
	redisClient *redis.Client,
	realtimeHub *realtime.Hub,
	trustedProxies string,
	telemetry ...*observability.Runtime,
) (*gin.Engine, error) {
	return SetupRouterWithBookingPolicy(userClient, eventClient, bookingClient, paymentClient, schedulerClient, redisClient, realtimeHub, trustedProxies, false, telemetry...)
}

func SetupRouterWithBookingPolicy(userClient userv1.UserServiceClient, eventClient eventv1.EventServiceClient, bookingClient bookingv1.BookingServiceClient, paymentClient paymentv1.PaymentServiceClient, schedulerClient schedulerv1.SchedulerServiceClient, redisClient *redis.Client, realtimeHub *realtime.Hub, trustedProxies string, requireRequestID bool, telemetry ...*observability.Runtime) (*gin.Engine, error) {
	return setupRouter(userClient, eventClient, bookingClient, paymentClient, schedulerClient, redisClient, realtimeHub, trustedProxies, requireRequestID, false, telemetry...)
}

// SetupRouterWithAdminOutboxSecurity enables the read-only Admin outbox route
// only when both the Booking and User clients use their existing mTLS paths.
func SetupRouterWithAdminOutboxSecurity(userClient userv1.UserServiceClient, eventClient eventv1.EventServiceClient, bookingClient bookingv1.BookingServiceClient, paymentClient paymentv1.PaymentServiceClient, schedulerClient schedulerv1.SchedulerServiceClient, redisClient *redis.Client, realtimeHub *realtime.Hub, trustedProxies string, requireRequestID bool, secureAdminOutbox bool, telemetry ...*observability.Runtime) (*gin.Engine, error) {
	return setupRouter(userClient, eventClient, bookingClient, paymentClient, schedulerClient, redisClient, realtimeHub, trustedProxies, requireRequestID, secureAdminOutbox, telemetry...)
}

func setupRouter(userClient userv1.UserServiceClient, eventClient eventv1.EventServiceClient, bookingClient bookingv1.BookingServiceClient, paymentClient paymentv1.PaymentServiceClient, schedulerClient schedulerv1.SchedulerServiceClient, redisClient *redis.Client, realtimeHub *realtime.Hub, trustedProxies string, requireRequestID bool, secureAdminOutbox bool, telemetry ...*observability.Runtime) (*gin.Engine, error) {
	return setupRouterWithNotification(userClient, eventClient, bookingClient, paymentClient, schedulerClient, redisClient, realtimeHub, trustedProxies, requireRequestID, secureAdminOutbox, nil, false, telemetry...)
}

// SetupRouterWithNotificationOutboxSecurity adds an optional, independently
// protected Notification reader. Existing Booking-only entry points retain
// their fail-closed defaults and do not require a Notification connection.
func SetupRouterWithNotificationOutboxSecurity(userClient userv1.UserServiceClient, eventClient eventv1.EventServiceClient, bookingClient bookingv1.BookingServiceClient, paymentClient paymentv1.PaymentServiceClient, schedulerClient schedulerv1.SchedulerServiceClient, redisClient *redis.Client, realtimeHub *realtime.Hub, trustedProxies string, requireRequestID bool, secureAdminOutbox bool, notificationClient notificationv1.NotificationAdminServiceClient, secureNotification bool, telemetry ...*observability.Runtime) (*gin.Engine, error) {
	return setupRouterWithNotification(userClient, eventClient, bookingClient, paymentClient, schedulerClient, redisClient, realtimeHub, trustedProxies, requireRequestID, secureAdminOutbox, notificationClient, secureNotification, telemetry...)
}

func setupRouterWithNotification(userClient userv1.UserServiceClient, eventClient eventv1.EventServiceClient, bookingClient bookingv1.BookingServiceClient, paymentClient paymentv1.PaymentServiceClient, schedulerClient schedulerv1.SchedulerServiceClient, redisClient *redis.Client, realtimeHub *realtime.Hub, trustedProxies string, requireRequestID bool, secureAdminOutbox bool, notificationClient notificationv1.NotificationAdminServiceClient, secureNotification bool, telemetry ...*observability.Runtime) (*gin.Engine, error) {
	r := gin.New()
	if err := middleware.ConfigureTrustedProxies(r, trustedProxies); err != nil {
		return nil, err
	}
	r.Use(gin.Recovery())
	r.Use(func(c *gin.Context) {
		path := c.Request.URL.Path
		refundPath := strings.HasPrefix(path, "/api/") && (strings.Contains(path, "/refund-requests") || strings.Contains(path, "/refund-approvers") || strings.HasSuffix(path, "/refund"))
		if refundPath {
			c.Header("Cache-Control", "no-store")
			if !secureAdminOutbox {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"code": "refund_unavailable", "error": "Refund authority is temporarily unavailable", "retryable": true})
				return
			}
			requestCtx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
			defer cancel()
			c.Request = c.Request.WithContext(requestCtx)
		}
		adminOutboxPath := path == "/api/admin/outbox-events" || strings.HasPrefix(path, "/api/admin/outbox-events/")
		adminEventTicketOperationsPath := false
		adminEventTicketListPath := false
		if strings.HasPrefix(path, "/api/admin/events/") {
			segments := strings.Split(strings.TrimPrefix(path, "/api/admin/events/"), "/")
			adminEventTicketOperationsPath = len(segments) >= 2 && segments[1] == "ticket-operations"
			adminEventTicketListPath = len(segments) >= 2 && segments[1] == "tickets"
		}
		if adminOutboxPath {
			c.Header("Cache-Control", "no-store")
			if !secureAdminOutbox && !(secureNotification && c.Query("source") == "notification") {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"code": "source_unavailable", "error": "outbox source unavailable"})
				return
			}
			requestCtx, cancel := context.WithTimeout(c.Request.Context(), adminOutboxRequestTimeout)
			defer cancel()
			c.Request = c.Request.WithContext(requestCtx)
		}
		if adminEventTicketOperationsPath {
			c.Header("Cache-Control", "no-store")
			if !secureAdminOutbox {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"code": "source_unavailable", "error": "ticket operations source unavailable"})
				return
			}
			requestCtx, cancel := context.WithTimeout(c.Request.Context(), adminEventTicketOperationsRequestTimeout)
			defer cancel()
			c.Request = c.Request.WithContext(requestCtx)
		}
		if adminEventTicketListPath {
			c.Header("Cache-Control", "no-store")
			if !secureAdminOutbox {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"code": "source_unavailable", "error": "ticket list source unavailable"})
				return
			}
			requestCtx, cancel := context.WithTimeout(c.Request.Context(), adminEventTicketOperationsRequestTimeout)
			defer cancel()
			c.Request = c.Request.WithContext(requestCtx)
		}
		adminBookingDetailPath := strings.HasPrefix(path, "/api/admin/bookings/")
		if adminBookingDetailPath {
			c.Header("Cache-Control", "no-store")
			if !secureAdminOutbox {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"code": "source_unavailable", "error": "booking detail source unavailable"})
				return
			}
			requestCtx, cancel := context.WithTimeout(c.Request.Context(), adminEventTicketOperationsRequestTimeout)
			defer cancel()
			c.Request = c.Request.WithContext(requestCtx)
		}
		if adminOutboxPath || adminEventTicketOperationsPath || adminEventTicketListPath || adminBookingDetailPath || strings.HasPrefix(path, "/api/auth/") || strings.HasPrefix(path, "/api/tickets") || strings.Contains(path, "/scanners") || strings.HasSuffix(path, "/check-in") {
			c.Header("Cache-Control", "no-store")
		}
		c.Next()
	})
	r.Use(middleware.CORSMiddleware())
	if len(telemetry) > 0 && telemetry[0] != nil {
		r.Use(telemetry[0].GinMiddleware())
	}

	var authMetrics *observability.Metrics
	if len(telemetry) > 0 && telemetry[0] != nil {
		authMetrics = telemetry[0].Metrics
	}
	authHandler := handler.NewAuthHandler(userClient, redisClient, authMetrics)
	eventHandler := handler.NewEventHandler(eventClient, realtimeHub)
	eventInterestsHandler := handler.NewEventInterestHandler(eventClient)
	bookingHandler := handler.NewBookingHandler(bookingClient)
	ticketHandler := handler.NewTicketHandler(bookingClient)
	refundHandler := handler.NewRefundHandler(bookingClient)
	bookingHandler.RequireRequestID = requireRequestID
	userHandler := handler.NewUserHandler(userClient)
	paymentHandler := handler.NewPaymentHandler(paymentClient, bookingClient)
	schedulerHandler := handler.NewSchedulerHandler(schedulerClient)
	adminOutboxHandler := handler.NewAdminOutboxHandlerWithNotification(bookingClient, secureAdminOutbox, notificationClient, secureNotification)
	notificationInboxHandler := handler.NewNotificationInboxHandler(notificationClient, secureNotification)
	adminEventTicketOperationsHandler := handler.NewAdminEventTicketOperationsHandler(eventClient, bookingClient)
	adminEventTicketsHandler := handler.NewAdminEventTicketsHandler(eventClient, bookingClient)
	adminBookingDetailHandler := handler.NewAdminBookingDetailHandler(bookingClient)

	api := r.Group("/api")
	{
		auth := api.Group("/auth")
		auth.POST("/registration-code", authHandler.RequestRegistrationCode)
		auth.POST("/forgot-password", authHandler.RequestPasswordReset)
		auth.POST("/reset-password", authHandler.ResetPassword)
		auth.POST("/register", authHandler.Register)
		auth.POST("/login", authHandler.Login)
		auth.POST("/refresh", authHandler.RefreshToken)

		api.GET("/events", eventHandler.ListEvents)
		api.GET("/events/:id", eventHandler.GetEvent)
		api.GET("/events/:id/seats/stream", eventHandler.StreamSeats)
		api.GET("/seats", eventHandler.GetSeatsOfEventByTierId)

		protected := api.Group("")
		protected.Use(middleware.AuthMiddleware(userClient, redisClient))
		{
			protected.POST("/auth/logout", authHandler.Logout)
			protected.GET("/users/me", userHandler.GetProfile)
			protected.PUT("/users/me", userHandler.UpdateProfile)
			protected.POST("/users/me/password", userHandler.ChangePassword)
			protected.POST("/users/me/email/code", userHandler.RequestEmailChange)
			protected.POST("/users/me/email/confirm", userHandler.CompleteEmailChange)
			protected.GET("/users/me/favorites", eventInterestsHandler.ListFavorites)
			protected.PUT("/users/me/favorites/:eventID", eventInterestsHandler.SetFavorite)
			protected.DELETE("/users/me/favorites/:eventID", eventInterestsHandler.RemoveFavorite)
			protected.GET("/users/me/event-alerts", eventInterestsHandler.ListAlerts)
			protected.DELETE("/users/me/event-alerts/:kind/email", eventInterestsHandler.DisableAlertEmails)
			protected.GET("/users/me/events/:eventID/interests", eventInterestsHandler.GetInterests)
			protected.PUT("/users/me/events/:eventID/alerts/:kind", eventInterestsHandler.SetAlert)
			protected.DELETE("/users/me/events/:eventID/alerts/:kind", eventInterestsHandler.RemoveAlert)
			protected.GET("/users/me/notifications", notificationInboxHandler.List)
			protected.PATCH("/users/me/notifications/:id/read", notificationInboxHandler.MarkRead)

			protected.POST("/bookings", bookingHandler.CreateBooking)
			protected.GET("/bookings", bookingHandler.ListBookings)
			protected.GET("/bookings/:id", bookingHandler.GetBooking)
			protected.POST("/bookings/:id/cancel", bookingHandler.CancelBooking)
			protected.POST("/bookings/:id/resume", bookingHandler.ResumeBooking)
			protected.GET("/bookings/:id/refund", refundHandler.Get)
			protected.POST("/bookings/:id/refund", refundHandler.Request)
			protected.POST("/refund-requests/:requestId/withdraw", refundHandler.Withdraw)
			// Booking revalidates the current event-scoped approver assignment.
			protected.GET("/events/:id/refund-requests", refundHandler.List)
			protected.POST("/events/:id/refund-requests/:requestId/decision", refundHandler.Decide)

			protected.GET("/tickets", ticketHandler.ListOwned)
			protected.POST("/tickets/:id/credential", ticketHandler.RotateCredential)
			protected.POST("/events/:id/check-in", ticketHandler.CheckIn)

			protected.POST("/payments/search", paymentHandler.SearchPayments)
			protected.POST("/payments", paymentHandler.CreatePayment)
			protected.GET("/payments/:id", paymentHandler.GetPaymentById)
			protected.PATCH("/payments/:id", paymentHandler.UpdatePayment)
			protected.DELETE("/payments/:id", paymentHandler.DeletePayment)

			admin := protected.Group("")
			admin.Use(middleware.AdminCurrentRoleAuthMiddleware(userClient, redisClient), middleware.AdminOnly())
			admin.POST("/events", eventHandler.CreateEvent)
			admin.GET("/admin/events", eventHandler.ListAdminEvents)
			admin.GET("/admin/events/:id", eventHandler.GetAdminEvent)
			admin.GET("/admin/events/:id/engagement", eventInterestsHandler.AdminEventEngagement)
			admin.PATCH("/admin/events/:id/sales", eventHandler.UpdateEventSales)
			admin.PUT("/events/:id", eventHandler.UpdateEvent)
			admin.DELETE("/events/:id", eventHandler.DeleteEvent)
			admin.GET("/events/:id/scanners", ticketHandler.ListScannerAssignments)
			admin.PUT("/events/:id/scanners/:staffId", ticketHandler.SetScannerAssignment(true))
			admin.DELETE("/events/:id/scanners/:staffId", ticketHandler.SetScannerAssignment(false))

			admin.GET("/scheduler/jobs", schedulerHandler.ListSchedulerJobs)
			admin.POST("/scheduler/jobs", schedulerHandler.CreateSchedulerJob)
			admin.PATCH("/scheduler/jobs/:id", schedulerHandler.UpdateSchedulerJob)
		}
	}
	api.GET("/admin/events/:id/refund-approvers", middleware.AdminEventTicketOperationsAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), refundHandler.ListApprovers)
	api.PUT("/admin/events/:id/refund-approvers/:staffId", middleware.AdminEventTicketOperationsAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), refundHandler.SetApprover(true))
	api.DELETE("/admin/events/:id/refund-approvers/:staffId", middleware.AdminEventTicketOperationsAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), refundHandler.SetApprover(false))
	api.GET("/admin/outbox-events", middleware.AdminOutboxAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), adminOutboxHandler.ListEvents)
	api.GET("/admin/events/:id/ticket-operations", middleware.AdminEventTicketOperationsAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), adminEventTicketOperationsHandler.GetTicketOperations)
	api.GET("/admin/events/:id/tickets", middleware.AdminEventTicketListAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), adminEventTicketsHandler.ListTickets)
	api.GET("/admin/bookings/:id", middleware.AdminEventTicketOperationsAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), adminBookingDetailHandler.Get)

	operationsHandler := handler.NewOperationsHandlerWithNotification(bookingClient, secureAdminOutbox, notificationClient, secureNotification)
	api.GET("/admin/operations", middleware.AdminCurrentRoleAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), operationsHandler.List)
	api.GET("/work", middleware.AuthMiddleware(userClient, redisClient), operationsHandler.Work)
	managementHandler := handler.NewManagementHandler(bookingClient, secureAdminOutbox)
	api.GET("/features", managementHandler.PublicFeatures)
	api.GET("/admin/feature-flags", middleware.AdminCurrentRoleAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), managementHandler.Features)
	api.PUT("/admin/feature-flags/:key", middleware.AdminCurrentRoleAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), managementHandler.ChangeFeature)
	api.GET("/admin/metrics", middleware.AdminCurrentRoleAuthMiddleware(userClient, redisClient), middleware.AdminOnly(), managementHandler.Metrics)
	return r, nil
}
