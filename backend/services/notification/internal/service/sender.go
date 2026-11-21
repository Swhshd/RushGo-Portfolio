package service

import (
	"context"

	"github.com/ticketbox/notification/internal/domain"
	"go.uber.org/zap"
)

type NotificationSender interface {
	Send(ctx context.Context, notification *domain.Notification) error
}

type LogSender struct {
	logger                      *zap.Logger
	includeNotificationIdentity bool
}

func NewLogSender(logger *zap.Logger) *LogSender {
	return &LogSender{logger: logger}
}

// SetIncludeNotificationIdentity enables identity-bound sender evidence for
// isolated Recovery runs. It is off by default to keep ordinary logs private.
func (s *LogSender) SetIncludeNotificationIdentity(enabled bool) {
	s.includeNotificationIdentity = enabled
}

func (s *LogSender) Send(ctx context.Context, n *domain.Notification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fields := []zap.Field{
		zap.String("adapter", "log"), zap.String("type", n.Type), zap.String("channel", n.Channel),
	}
	if s.includeNotificationIdentity {
		fields = append(fields, zap.String("notification_id", n.ID.String()))
	}
	s.logger.Info("notification adapter accepted", fields...)
	return nil
}
