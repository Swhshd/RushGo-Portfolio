package domain

import (
	"errors"
	"github.com/google/uuid"
	"time"
)

var (
	ErrValidation              = errors.New("VALIDATION_ERROR")
	ErrInvalidCode             = errors.New("INVALID_CODE")
	ErrCodeExpired             = errors.New("CODE_EXPIRED")
	ErrCodeAttemptsExhausted   = errors.New("CODE_ATTEMPTS_EXHAUSTED")
	ErrRateLimited             = errors.New("RATE_LIMITED")
	ErrDeliveryUnavailable     = errors.New("DELIVERY_UNAVAILABLE")
	ErrEmailExists             = errors.New("ACCOUNT_EXISTS")
	ErrInvalidResetToken       = errors.New("INVALID_RESET_TOKEN")
	ErrResetTokenExpired       = errors.New("RESET_TOKEN_EXPIRED")
	ErrResetTokenUsed          = errors.New("RESET_TOKEN_USED")
	ErrTemporaryUnavailable    = errors.New("TEMPORARY_UNAVAILABLE")
	ErrEmailChangeNotSupported = errors.New("EMAIL_CHANGE_NOT_SUPPORTED")
	ErrInvalidSession          = errors.New("INVALID_SESSION")
	ErrInvalidCredentials      = errors.New("INVALID_CREDENTIALS")
	ErrTokenExpired            = errors.New("TOKEN_EXPIRED")
)

type DeliveryState string

const (
	Prepared            DeliveryState = "PREPARED"
	AcceptedForDelivery DeliveryState = "ACCEPTED"
	Failed              DeliveryState = "FAILED"
	UnknownDelivery     DeliveryState = "UNKNOWN"
)

type DeliveryOutcome int

const (
	Accepted DeliveryOutcome = iota
	ExplicitFailure
	Unknown
	ConfigurationFailure
)

type RegisterInput struct {
	Email, Password, Name, VerificationCode string
	VerificationID                          uuid.UUID
}
type RegisterCompletion struct {
	UserID, VerificationID             uuid.UUID
	CanonicalEmail, Name, PasswordHash string
}
type RegistrationCodeResult struct {
	VerificationID          uuid.UUID
	ExpiresAt, NextResendAt time.Time
}
type RegistrationVerification struct {
	ID                                            uuid.UUID
	CanonicalEmail, Purpose, CredentialHash       string
	Generation                                    int64
	AttemptCount, MaxAttempts                     int
	DeliveryState                                 DeliveryState
	ExpiresAt, NextResendAt, CreatedAt, UpdatedAt time.Time
	ConsumedAt, InvalidatedAt                     *time.Time
}
type PasswordResetRequest struct {
	ID, UserID                                    uuid.UUID
	CanonicalEmail, TokenHash                     string
	Generation                                    int64
	DeliveryState                                 DeliveryState
	ExpiresAt, NextResendAt, CreatedAt, UpdatedAt time.Time
	ConsumedAt, InvalidatedAt                     *time.Time
}
type SessionPair struct {
	AccessToken, RefreshToken string
	StoredRefresh             RefreshToken
}
