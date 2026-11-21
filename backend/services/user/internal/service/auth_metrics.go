package service

import (
	"errors"
	"github.com/ticketbox/pkg/observability"
	"github.com/ticketbox/user/internal/domain"
)

func (s *AuthService) recordAuth(operation string, err error) {
	result := "error"
	switch {
	case err == nil:
		result = "success"
	case errors.Is(err, domain.ErrRateLimited):
		result = "rate_limited"
	case errors.Is(err, domain.ErrCodeExpired) || errors.Is(err, domain.ErrResetTokenExpired):
		result = "expired"
	case errors.Is(err, domain.ErrValidation) || errors.Is(err, domain.ErrInvalidCode) || errors.Is(err, domain.ErrCodeAttemptsExhausted) || errors.Is(err, domain.ErrInvalidResetToken) || errors.Is(err, domain.ErrResetTokenUsed) || errors.Is(err, domain.ErrEmailExists):
		result = "invalid"
	}
	observability.RecordAuth(s.deps.Metrics, operation, result)
}
