package service

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/pkg/authpolicy"
)

var (
	ErrDefinitelyNotAccepted  = errors.New("Notification email was not accepted by the provider")
	ErrDeliveryOutcomeUnknown = errors.New("Notification email provider outcome is unknown")
	ErrSMTPConfiguration      = errors.New("Notification SMTP configuration is unavailable")
)

type SMTPConfig struct {
	Host           string
	Port           int
	TLSMode        string
	Username       string
	Password       string
	From           string
	FrontendOrigin string
	Timeout        time.Duration
}

type SMTPNotificationSender struct {
	config         SMTPConfig
	frontendOrigin *url.URL
	roots          *x509.CertPool
	dial           func(context.Context, string, string) (net.Conn, error)
}

func NewSMTPNotificationSender(config SMTPConfig) (*SMTPNotificationSender, error) {
	return newSMTPNotificationSender(config, nil)
}

func newSMTPNotificationSender(config SMTPConfig, roots *x509.CertPool) (*SMTPNotificationSender, error) {
	if config.Host == "" || strings.ContainsAny(config.Host, " \t\r\n:/") || config.Username == "" || config.Password == "" || config.From == "" {
		return nil, ErrSMTPConfiguration
	}
	for _, char := range config.Username {
		if unicode.IsControl(char) {
			return nil, ErrSMTPConfiguration
		}
	}
	if _, err := authpolicy.CanonicalEmail(config.From); err != nil {
		return nil, ErrSMTPConfiguration
	}
	frontendOrigin, err := validateSMTPFrontendOrigin(config.FrontendOrigin)
	if err != nil {
		return nil, ErrSMTPConfiguration
	}
	if !((config.Port == 465 && config.TLSMode == "implicit") || (config.Port == 587 && config.TLSMode == "starttls")) {
		return nil, ErrSMTPConfiguration
	}
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}
	if config.Timeout <= 0 || config.Timeout > 15*time.Second {
		return nil, ErrSMTPConfiguration
	}
	return &SMTPNotificationSender{config: config, frontendOrigin: frontendOrigin, roots: roots}, nil
}

func (s *SMTPNotificationSender) Send(parent context.Context, notification *domain.Notification) error {
	if s == nil || notification == nil || notification.ID == uuid.Nil {
		return ErrDefinitelyNotAccepted
	}
	if err := parent.Err(); err != nil {
		return errors.Join(ErrDefinitelyNotAccepted, err)
	}
	recipient, err := authpolicy.CanonicalEmail(notification.Recipient)
	if err != nil {
		return ErrDefinitelyNotAccepted
	}
	from, err := authpolicy.CanonicalEmail(s.config.From)
	if err != nil {
		return ErrDefinitelyNotAccepted
	}
	template, err := renderNotificationEmail(notification, s.frontendOrigin)
	if err != nil {
		return ErrDefinitelyNotAccepted
	}
	message, err := composeMessage(notification.ID, from, recipient, template, time.Now())
	if err != nil {
		return ErrDefinitelyNotAccepted
	}
	ctx, cancel := context.WithTimeout(parent, s.config.Timeout)
	defer cancel()
	address := net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.Port))
	dial := s.dial
	if dial == nil {
		dialer := &net.Dialer{Timeout: s.config.Timeout}
		dial = dialer.DialContext
	}
	conn, err := dial(ctx, "tcp", address)
	if err != nil {
		return ErrDefinitelyNotAccepted
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer func() {
		stopClose()
		_ = conn.Close()
	}()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	tlsConfig := &tls.Config{ServerName: s.config.Host, MinVersion: tls.VersionTLS12, RootCAs: s.roots}
	if s.config.TLSMode == "implicit" {
		tlsConn := tls.Client(conn, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return ErrDefinitelyNotAccepted
		}
		conn = tlsConn
	}
	client, err := smtp.NewClient(conn, s.config.Host)
	if err != nil {
		return ErrDefinitelyNotAccepted
	}
	defer client.Close()
	if s.config.TLSMode == "starttls" {
		if err := client.StartTLS(tlsConfig); err != nil {
			return ErrDefinitelyNotAccepted
		}
	}
	authSupported, authMechanisms := client.Extension("AUTH")
	if !authSupported {
		return ErrDefinitelyNotAccepted
	}
	mechanisms := make(map[string]bool)
	for _, mechanism := range strings.Fields(strings.ToUpper(authMechanisms)) {
		mechanisms[mechanism] = true
	}
	var auth smtp.Auth
	switch {
	case mechanisms["PLAIN"]:
		auth = smtp.PlainAuth("", s.config.Username, s.config.Password, s.config.Host)
	case mechanisms["LOGIN"]:
		auth = loginAuth{username: s.config.Username, password: s.config.Password}
	default:
		return ErrDefinitelyNotAccepted
	}
	if err := client.Auth(auth); err != nil {
		return ErrDefinitelyNotAccepted
	}
	if err := client.Mail(from); err != nil {
		return ErrDefinitelyNotAccepted
	}
	if err := client.Rcpt(recipient); err != nil {
		return ErrDefinitelyNotAccepted
	}
	writer, err := client.Data()
	if err != nil {
		return ErrDefinitelyNotAccepted
	}
	if _, err := writer.Write(message); err != nil {
		_ = client.Close()
		return ErrDefinitelyNotAccepted
	}
	if err := writer.Close(); err != nil {
		var smtpResponse *textproto.Error
		if errors.As(err, &smtpResponse) {
			return ErrDefinitelyNotAccepted
		}
		return ErrDeliveryOutcomeUnknown
	}
	return nil
}

func validateSMTPFrontendOrigin(raw string) (*url.URL, error) {
	if raw == "" || strings.TrimSpace(raw) != raw || strings.ContainsAny(raw, "\\\r\n\t ") {
		return nil, ErrSMTPConfiguration
	}
	origin, err := url.Parse(raw)
	if err != nil || origin == nil || origin.Opaque != "" || origin.User != nil || origin.Host == "" ||
		origin.Path != "" || origin.RawPath != "" || origin.RawQuery != "" || origin.ForceQuery ||
		origin.Fragment != "" || origin.RawFragment != "" {
		return nil, ErrSMTPConfiguration
	}
	origin.Scheme = strings.ToLower(origin.Scheme)
	if origin.Scheme != "https" && origin.Scheme != "http" {
		return nil, ErrSMTPConfiguration
	}
	hostname, port, ok := splitSMTPOriginHost(origin.Host)
	if !ok || !validSMTPOriginHostname(hostname) {
		return nil, ErrSMTPConfiguration
	}
	if port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return nil, ErrSMTPConfiguration
		}
	}
	if origin.Scheme == "http" {
		ip := net.ParseIP(hostname)
		if !strings.EqualFold(hostname, "localhost") && (ip == nil || !ip.IsLoopback()) {
			return nil, ErrSMTPConfiguration
		}
	}
	return origin, nil
}

func splitSMTPOriginHost(host string) (hostname, port string, ok bool) {
	if host == "" || strings.ContainsAny(host, "\\\r\n\t %/@?#") {
		return "", "", false
	}
	if strings.HasPrefix(host, "[") {
		closing := strings.IndexByte(host, ']')
		if closing < 0 {
			return "", "", false
		}
		hostname = host[1:closing]
		remainder := host[closing+1:]
		if remainder != "" {
			if !strings.HasPrefix(remainder, ":") || len(remainder) == 1 {
				return "", "", false
			}
			port = remainder[1:]
		}
		if net.ParseIP(hostname) == nil || !strings.Contains(hostname, ":") {
			return "", "", false
		}
	} else {
		if strings.ContainsAny(host, "[]") || strings.Count(host, ":") > 1 {
			return "", "", false
		}
		if strings.Contains(host, ":") {
			parts := strings.SplitN(host, ":", 2)
			hostname, port = parts[0], parts[1]
		} else {
			hostname = host
		}
	}
	if strings.Contains(host, ":") && port == "" && !strings.HasPrefix(host, "[") {
		return "", "", false
	}
	if port != "" {
		for _, digit := range port {
			if digit < '0' || digit > '9' {
				return "", "", false
			}
		}
	}
	return hostname, port, true
}

func validSMTPOriginHostname(hostname string) bool {
	if hostname == "" || strings.HasSuffix(hostname, ".") || strings.Contains(hostname, "%") {
		return false
	}
	if net.ParseIP(hostname) != nil {
		return true
	}
	if len(hostname) > 253 {
		return false
	}
	for _, label := range strings.Split(hostname, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func composeMessage(id uuid.UUID, from, recipient string, template domain.InboxTemplate, now time.Time) ([]byte, error) {
	if id == uuid.Nil || template.Subject == "" || len(template.Subject) > 160 || len(template.Body) == 0 || len(template.Body) > 4000 {
		return nil, ErrDefinitelyNotAccepted
	}
	fromAddress, err := mail.ParseAddress(from)
	if err != nil || fromAddress.Address != from {
		return nil, ErrDefinitelyNotAccepted
	}
	toAddress, err := mail.ParseAddress(recipient)
	if err != nil || toAddress.Address != recipient {
		return nil, ErrDefinitelyNotAccepted
	}
	domainPart := strings.SplitN(from, "@", 2)[1]
	var body bytes.Buffer
	quoted := quotedprintable.NewWriter(&body)
	if _, err := quoted.Write([]byte(template.Body)); err != nil {
		return nil, ErrDefinitelyNotAccepted
	}
	if err := quoted.Close(); err != nil {
		return nil, ErrDefinitelyNotAccepted
	}
	message := fmt.Sprintf("From: <%s>\r\nTo: <%s>\r\nDate: %s\r\nMessage-ID: <%s@%s>\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n%s",
		from, recipient, now.UTC().Format(time.RFC1123Z), id.String(), domainPart,
		mime.QEncoding.Encode("utf-8", template.Subject), body.String())
	return []byte(message), nil
}

type loginAuth struct {
	username string
	password string
}

func (a loginAuth) Start(*smtp.ServerInfo) (string, []byte, error) {
	return "LOGIN", nil, nil
}

func (a loginAuth) Next(challenge []byte, more bool) ([]byte, error) {
	if !more {
		return nil, nil
	}
	text := strings.ToLower(strings.TrimSpace(string(challenge)))
	switch {
	case strings.Contains(text, "user"):
		return []byte(a.username), nil
	case strings.Contains(text, "pass"):
		return []byte(a.password), nil
	default:
		return nil, errors.New("unsupported SMTP LOGIN challenge")
	}
}

var _ NotificationSender = (*SMTPNotificationSender)(nil)
