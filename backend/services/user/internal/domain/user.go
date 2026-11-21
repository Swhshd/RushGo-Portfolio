package domain

import (
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID                uuid.UUID
	Email             string
	EmailCanonical    string
	EmailVerifiedAt   *time.Time
	PasswordHash      string
	Name              string
	Role              string
	Locale            string
	CredentialVersion int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type RefreshToken struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	TokenHash string
	ExpiresAt time.Time
	RevokedAt *time.Time
	CreatedAt time.Time
}
