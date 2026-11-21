package service

import (
	"context"
	"github.com/ticketbox/user/internal/domain"
	"time"
)

type MailPurpose string

const (
	RegistrationMail MailPurpose = "registration"
	ResetMail        MailPurpose = "reset"
	EmailChangeMail  MailPurpose = "email_change"
)

type AuthMail struct {
	Recipient, Code, ResetURL string
	Purpose                   MailPurpose
	ExpiresAt                 time.Time
}
type DeliveryResult struct {
	Class      domain.DeliveryOutcome
	SafeReason string
}
type MailSender interface {
	Send(context.Context, AuthMail) DeliveryResult
}
type SMTPConfig struct {
	Host, TLSMode, Username, Password, From string
	Port                                    int
	Timeout                                 time.Duration
}
type unavailableMailSender struct{}

func (unavailableMailSender) Send(context.Context, AuthMail) DeliveryResult {
	return DeliveryResult{domain.ConfigurationFailure, "unavailable"}
}
