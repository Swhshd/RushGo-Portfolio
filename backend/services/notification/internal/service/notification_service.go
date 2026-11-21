package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/repository"
	"github.com/ticketbox/pkg/authpolicy"
	"github.com/ticketbox/pkg/eventreminder"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type NotificationService struct {
	repo       repository.NotificationRepository
	sender     NotificationSender
	logger     *zap.Logger
	metrics    *observability.Metrics
	recipients RecipientLookup
}

func NewNotificationService(repo repository.NotificationRepository, sender NotificationSender, logger *zap.Logger, metrics ...*observability.Metrics) *NotificationService {
	var m *observability.Metrics
	if len(metrics) > 0 {
		m = metrics[0]
	}
	return &NotificationService{repo: repo, sender: sender, logger: logger, metrics: m}
}

func (s *NotificationService) SetRecipientLookup(lookup RecipientLookup) {
	s.recipients = lookup
}

func (s *NotificationService) SendBookingConfirmation(ctx context.Context, data json.RawMessage) error {
	return s.acceptBookingEmail(ctx, "booking_confirmation", data)
}
func (s *NotificationService) SendBookingFailure(ctx context.Context, data json.RawMessage) error {
	return s.acceptBookingEmail(ctx, "booking_failure", data)
}
func (s *NotificationService) SendBookingCancellation(ctx context.Context, data json.RawMessage) error {
	return s.acceptBookingEmail(ctx, "booking_cancellation", data)
}
func (s *NotificationService) SendWelcomeEmail(ctx context.Context, data json.RawMessage) error {
	return s.accept(ctx, "welcome_email", data)
}
func (s *NotificationService) accept(ctx context.Context, typ string, data json.RawMessage) error {
	return s.acceptWithLocale(ctx, typ, data, "en")
}

func (s *NotificationService) acceptBookingEmail(ctx context.Context, typ string, data json.RawMessage) error {
	if s.recipients == nil {
		return s.accept(ctx, typ, data)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil || payload == nil {
		return errors.Join(pkgkafka.ErrPermanent, domain.ErrInvalidIntent)
	}
	rawUserID, ok := payload["user_id"].(string)
	if !ok {
		return errors.Join(pkgkafka.ErrPermanent, domain.ErrInvalidIntent)
	}
	userID, err := uuid.Parse(rawUserID)
	if err != nil || userID == uuid.Nil {
		return errors.Join(pkgkafka.ErrPermanent, domain.ErrInvalidIntent)
	}
	recipient, err := s.recipients.LookupNotificationRecipient(ctx, userID)
	if err != nil {
		return err
	}
	if recipient.UserID != userID || !recipient.EmailVerified {
		return errors.Join(pkgkafka.ErrPermanent, ErrRecipientUnavailable)
	}
	canonicalEmail, err := authpolicy.CanonicalEmail(recipient.Email)
	if err != nil {
		return errors.Join(pkgkafka.ErrPermanent, ErrRecipientUnavailable)
	}
	payload["email"] = canonicalEmail
	raw, err := json.Marshal(payload)
	if err != nil {
		return errors.Join(pkgkafka.ErrPermanent, domain.ErrInvalidIntent)
	}
	return s.acceptWithLocale(ctx, typ, raw, normalizeRecipientLocale(recipient.Locale))
}

func (s *NotificationService) SendBusinessNotificationV1(ctx context.Context, raw json.RawMessage) error {
	ctx, span := otel.Tracer("github.com/ticketbox/notification").Start(ctx, "notification.business_intent")
	defer span.End()
	event, hash, err := domain.ParseBusinessNotificationV1(raw)
	if err != nil {
		span.SetStatus(codes.Error, "invalid business notification")
		return errors.Join(pkgkafka.ErrPermanent, err)
	}
	if s.recipients == nil {
		span.SetStatus(codes.Error, "notification recipient lookup unavailable")
		return ErrRecipientUnavailable
	}
	lookup, ok := s.repo.(repository.BusinessNotificationRepository)
	if !ok || lookup == nil {
		span.SetStatus(codes.Error, "notification inbox repository unavailable")
		return ErrBusinessNotificationUnavailable
	}
	recipient, err := s.recipients.LookupNotificationRecipient(ctx, event.UserID)
	if err != nil {
		span.SetStatus(codes.Error, "notification recipient lookup failed")
		return err
	}
	if recipient.UserID != event.UserID {
		span.SetStatus(codes.Error, "notification recipient identity mismatch")
		return errors.Join(pkgkafka.ErrPermanent, ErrRecipientUnavailable)
	}
	rendered, err := domain.RenderBusinessNotification(event, recipient.Locale)
	if err != nil {
		span.SetStatus(codes.Error, "notification template unavailable")
		return errors.Join(pkgkafka.ErrPermanent, err)
	}
	verified := recipient.EmailVerified
	canonicalEmail := ""
	if verified {
		canonicalEmail, err = authpolicy.CanonicalEmail(recipient.Email)
		if err != nil {
			// Keep the in-app intent, but never send to an invalid or unverified address.
			verified = false
		}
	}
	created, err := lookup.AcceptBusinessNotification(ctx, event, hash, rendered, canonicalEmail, verified)
	if err != nil {
		span.SetStatus(codes.Error, "business notification persistence failed")
		if errors.Is(err, repository.ErrSemanticConflict) || errors.Is(err, domain.ErrInvalidBusinessNotification) {
			return errors.Join(pkgkafka.ErrPermanent, err)
		}
		s.recordNotification(ctx, event.Type, "failed")
		return err
	}
	result := "duplicate_suppressed"
	if created {
		result = "accepted"
	}
	span.SetAttributes(attribute.String("notification.type", event.Type), attribute.String("notification.result", result))
	s.recordNotification(ctx, event.Type, result)
	return nil
}

func (s *NotificationService) SendEventReminderV1(ctx context.Context, raw json.RawMessage) error {
	ctx, span := otel.Tracer("github.com/ticketbox/notification").Start(ctx, "notification.event_reminder")
	defer span.End()
	event, hash, err := domain.ParseEventReminder(raw)
	if err != nil {
		span.SetStatus(codes.Error, "invalid event reminder")
		return errors.Join(pkgkafka.ErrPermanent, err)
	}
	lookup, ok := s.repo.(repository.EventReminderRepository)
	if !ok || lookup == nil {
		span.SetStatus(codes.Error, "event reminder repository unavailable")
		return ErrBusinessNotificationUnavailable
	}
	locale := "en"
	recipient := ""
	verified := false
	// Inbox language follows the owner's preference even without email consent.
	// Withdrawal-only messages need no profile lookup to advance their fence.
	if event.Action == eventreminder.ActionTrigger || event.EmailOptIn {
		if s.recipients == nil {
			span.SetStatus(codes.Error, "event reminder recipient lookup unavailable")
			return ErrRecipientUnavailable
		}
		user, lookupErr := s.recipients.LookupNotificationRecipient(ctx, event.UserID)
		if lookupErr != nil {
			span.SetStatus(codes.Error, "event reminder recipient lookup failed")
			return lookupErr
		}
		if user.UserID != event.UserID {
			span.SetStatus(codes.Error, "event reminder recipient identity mismatch")
			return errors.Join(pkgkafka.ErrPermanent, ErrRecipientUnavailable)
		}
		locale = normalizeRecipientLocale(user.Locale)
		if event.EmailOptIn && user.EmailVerified {
			if canonical, canonicalErr := authpolicy.CanonicalEmail(user.Email); canonicalErr == nil {
				recipient, verified = canonical, true
			}
		}
	}
	rendered := domain.InboxTemplate{Locale: locale}
	if event.Action == eventreminder.ActionTrigger {
		rendered, err = domain.RenderEventReminder(event, locale)
		if err != nil {
			span.SetStatus(codes.Error, "event reminder template unavailable")
			return errors.Join(pkgkafka.ErrPermanent, err)
		}
	}
	created, err := lookup.AcceptEventReminder(ctx, event, hash, rendered, recipient, verified)
	if err != nil {
		span.SetStatus(codes.Error, "event reminder persistence failed")
		if errors.Is(err, repository.ErrSemanticConflict) || errors.Is(err, domain.ErrInvalidEventReminder) {
			return errors.Join(pkgkafka.ErrPermanent, err)
		}
		s.recordNotification(ctx, event.Type, "failed")
		return err
	}
	result := "duplicate_suppressed"
	if created {
		result = "accepted"
	}
	span.SetAttributes(attribute.String("notification.type", event.Type), attribute.String("notification.action", event.Action), attribute.String("notification.result", result))
	s.recordNotification(ctx, event.Type, result)
	return nil
}

func (s *NotificationService) acceptWithLocale(ctx context.Context, typ string, data json.RawMessage, locale string) error {
	ctx, span := otel.Tracer("github.com/ticketbox/notification").Start(ctx, "notification.process")
	defer span.End()
	key, n, hash, err := domain.PrepareIntent(typ, data)
	if err != nil {
		return errors.Join(pkgkafka.ErrPermanent, err)
	}
	n.Locale = normalizeRecipientLocale(locale)
	_, created, err := s.repo.AcceptIntent(ctx, key, n, hash)
	if err != nil {
		span.SetStatus(codes.Error, "notification acceptance failed")
		s.recordNotification(ctx, typ, "failed")
		if errors.Is(err, repository.ErrSemanticConflict) || errors.Is(err, repository.ErrLegacyAttention) || errors.Is(err, domain.ErrInvalidIntent) {
			return errors.Join(pkgkafka.ErrPermanent, err)
		}
		return err
	}
	result := "duplicate_suppressed"
	if created {
		result = "accepted"
	}
	s.recordNotification(ctx, typ, result)
	return nil
}

var ErrRecipientUnavailable = fmt.Errorf("Notification recipient is unavailable or not eligible for email")
var ErrBusinessNotificationUnavailable = fmt.Errorf("Notification business intent repository unavailable")

func normalizeRecipientLocale(value string) string {
	if domain.NormalizeLocale(value) == "zh" {
		return "zh"
	}
	return "en"
}
func (s *NotificationService) recordNotification(ctx context.Context, eventType, result string) {
	if s.metrics == nil {
		return
	}
	if result == "failed" {
		s.metrics.NotificationFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventType)))
		return
	}
	s.metrics.NotificationProcessed.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventType), attribute.String("result", result)))
}
