package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/ticketbox/pkg/authpolicy"
	"github.com/ticketbox/user/internal/domain"
	mail "github.com/wneessen/go-mail"
	"net"
	"strings"
	"sync"
	"time"
	"unicode"
)

type SMTPMailSender struct {
	config SMTPConfig
	roots  *x509.CertPool
	dial   func(context.Context, string, string) (net.Conn, error)
}

func NewSMTPMailSender(config SMTPConfig) (*SMTPMailSender, error) {
	if config.Host == "" && config.TLSMode == "" && config.Username == "" && config.Password == "" && config.From == "" && config.Port == 0 {
		return &SMTPMailSender{config: config}, nil
	}
	if config.Host == "" || strings.ContainsAny(config.Host, " \t\r\n:/") || config.Username == "" || config.Password == "" || config.From == "" {
		return nil, domain.ErrValidation
	}
	for _, r := range config.Username {
		if unicode.IsControl(r) {
			return nil, domain.ErrValidation
		}
	}
	if _, err := authpolicy.CanonicalEmail(config.From); err != nil {
		return nil, domain.ErrValidation
	}
	if !((config.Port == 465 && config.TLSMode == "implicit") || (config.Port == 587 && config.TLSMode == "starttls")) {
		return nil, domain.ErrValidation
	}
	if config.Timeout == 0 {
		config.Timeout = 15 * time.Second
	}
	if config.Timeout <= 0 || config.Timeout > 15*time.Second {
		return nil, domain.ErrValidation
	}
	return &SMTPMailSender{config: config}, nil
}
func (s *SMTPMailSender) Send(parent context.Context, input AuthMail) DeliveryResult {
	if s.config.Host == "" {
		return DeliveryResult{domain.ConfigurationFailure, "unavailable"}
	}
	recipient, err := authpolicy.CanonicalEmail(input.Recipient)
	if err != nil {
		return DeliveryResult{domain.ConfigurationFailure, "invalid_message"}
	}
	subject, body, err := authMailTemplate(input)
	if err != nil {
		return DeliveryResult{domain.ConfigurationFailure, "invalid_message"}
	}
	ctx, cancel := context.WithTimeout(parent, s.config.Timeout)
	defer cancel()
	tlsConfig := &tls.Config{ServerName: s.config.Host, MinVersion: tls.VersionTLS12, RootCAs: s.roots}
	for _, mechanism := range []mail.SMTPAuthType{mail.SMTPAuthPlain, mail.SMTPAuthLogin} {
		var owned net.Conn
		var mu sync.Mutex
		closeSocket := func() {
			mu.Lock()
			defer mu.Unlock()
			if owned != nil {
				_ = owned.Close()
				owned = nil
			}
		}
		stop := context.AfterFunc(ctx, closeSocket)
		dial := func(dialCtx context.Context, network, address string) (net.Conn, error) {
			var conn net.Conn
			var dialErr error
			if s.dial != nil {
				conn, dialErr = s.dial(dialCtx, network, address)
			} else if s.config.TLSMode == "implicit" {
				conn, dialErr = (&tls.Dialer{NetDialer: &net.Dialer{}, Config: tlsConfig}).DialContext(dialCtx, network, address)
			} else {
				conn, dialErr = (&net.Dialer{}).DialContext(dialCtx, network, address)
			}
			if dialErr != nil {
				return nil, dialErr
			}
			mu.Lock()
			owned = conn
			if ctx.Err() != nil {
				conn.Close()
				owned = nil
			}
			mu.Unlock()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if s.config.TLSMode == "implicit" {
				tc, ok := conn.(*tls.Conn)
				if !ok {
					tc = tls.Client(conn, tlsConfig)
				}
				if dialErr = tc.HandshakeContext(dialCtx); dialErr != nil {
					return nil, dialErr
				}
				conn = tc
				// Preserve actual *tls.Conn for go-mail's secure-auth state check.
			}
			return conn, nil
		}
		opts := []mail.Option{mail.WithPort(s.config.Port), mail.WithTimeout(time.Until(deadlineOf(ctx))), mail.WithTLSConfig(tlsConfig), mail.WithSMTPAuth(mechanism), mail.WithUsername(s.config.Username), mail.WithPassword(s.config.Password), mail.WithDialContextFunc(dial)}
		if s.config.TLSMode == "implicit" {
			opts = append(opts, mail.WithSSL())
		} else {
			opts = append(opts, mail.WithTLSPortPolicy(mail.TLSMandatory))
		}
		client, err := mail.NewClient(s.config.Host, opts...)
		if err == nil {
			err = client.DialWithContext(ctx)
		}
		if err != nil {
			// Capability sentinel only. No provider-text/status matching and no credential resend.
			unsupported := mechanism == mail.SMTPAuthPlain && errors.Is(err, mail.ErrPlainAuthNotSupported) && ctx.Err() == nil
			stop()
			closeSocket()
			if unsupported {
				continue
			}
			if ctx.Err() != nil {
				return DeliveryResult{domain.Unknown, "timeout_or_cancelled"}
			}
			var certificate x509.UnknownAuthorityError
			var host x509.HostnameError
			var invalid x509.CertificateInvalidError
			if errors.As(err, &certificate) || errors.As(err, &host) || errors.As(err, &invalid) {
				return DeliveryResult{domain.ConfigurationFailure, "tls"}
			}
			var netErr net.Error
			if errors.As(err, &netErr) {
				return DeliveryResult{domain.Unknown, "network"}
			}
			return DeliveryResult{domain.ConfigurationFailure, "tls_or_auth"}
		}
		msg := mail.NewMsg()
		if err = msg.From(s.config.From); err == nil {
			err = msg.To(recipient)
		}
		if err != nil {
			_ = client.Close()
			stop()
			closeSocket()
			return DeliveryResult{domain.ConfigurationFailure, "invalid_message"}
		}
		msg.Subject(subject)
		msg.SetBodyString(mail.TypeTextPlain, body)
		err = client.Send(msg)
		accepted := msg.IsDelivered()
		_ = client.Close()
		stop()
		closeSocket()
		if accepted {
			return DeliveryResult{domain.Accepted, "accepted"}
		}
		if ctx.Err() != nil {
			return DeliveryResult{domain.Unknown, "timeout_or_cancelled"}
		}
		var sendErr *mail.SendError
		if errors.As(err, &sendErr) && sendErr.ErrorCode() >= 400 && sendErr.ErrorCode() < 600 {
			return DeliveryResult{domain.ExplicitFailure, "rejected"}
		}
		return DeliveryResult{domain.Unknown, "network_or_submission"}
	}
	return DeliveryResult{domain.ConfigurationFailure, "unsupported_auth"}
}
func deadlineOf(ctx context.Context) time.Time { deadline, _ := ctx.Deadline(); return deadline }
