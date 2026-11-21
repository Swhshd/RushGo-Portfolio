package service

import (
	"context"

	"github.com/google/uuid"
)

type NotificationRecipient struct {
	UserID        uuid.UUID
	Email         string
	Locale        string
	EmailVerified bool
}

type RecipientLookup interface {
	LookupNotificationRecipient(context.Context, uuid.UUID) (NotificationRecipient, error)
}
