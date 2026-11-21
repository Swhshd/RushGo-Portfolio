package service

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"io"
	"math/big"
	"mime/quotedprintable"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
)

func TestSMTPConfigurationFailsClosedAndRequiresVerifiedTransport(t *testing.T) {
	valid := SMTPConfig{Host: "smtp.example.test", Port: 465, TLSMode: "implicit", Username: "user", Password: "password", From: "sender@example.test", FrontendOrigin: "https://rushgo.example.test"}
	if _, err := NewSMTPNotificationSender(valid); err != nil {
		t.Fatalf("valid SMTP config rejected: %v", err)
	}
	for name, mutate := range map[string]func(*SMTPConfig){
		"missing host":             func(c *SMTPConfig) { c.Host = "" },
		"cleartext port":           func(c *SMTPConfig) { c.Port = 25; c.TLSMode = "plain" },
		"TLS mode mismatch":        func(c *SMTPConfig) { c.TLSMode = "starttls" },
		"missing credentials":      func(c *SMTPConfig) { c.Password = "" },
		"invalid sender":           func(c *SMTPConfig) { c.From = "bad\r\nBcc: victim@example.test" },
		"missing frontend origin":  func(c *SMTPConfig) { c.FrontendOrigin = "" },
		"insecure remote frontend": func(c *SMTPConfig) { c.FrontendOrigin = "http://rushgo.example.test" },
		"frontend path":            func(c *SMTPConfig) { c.FrontendOrigin = "https://rushgo.example.test/app" },
		"frontend query":           func(c *SMTPConfig) { c.FrontendOrigin = "https://rushgo.example.test/?next=https://evil.example" },
		"frontend fragment":        func(c *SMTPConfig) { c.FrontendOrigin = "https://rushgo.example.test/#account" },
		"frontend userinfo":        func(c *SMTPConfig) { c.FrontendOrigin = "https://user@rushgo.example.test" },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			if _, err := NewSMTPNotificationSender(config); err == nil {
				t.Fatal("unsafe SMTP configuration was accepted")
			}
		})
	}
	for _, origin := range []string{"http://localhost:3002", "http://127.0.0.1:3002", "http://[::1]:3002"} {
		t.Run("loopback "+origin, func(t *testing.T) {
			config := valid
			config.FrontendOrigin = origin
			if _, err := NewSMTPNotificationSender(config); err != nil {
				t.Fatalf("loopback test origin rejected: %v", err)
			}
		})
	}
}

func TestExplicitProbeUsesExistingTLSAdapterWithoutBusinessClaims(t *testing.T) {
	capture := runSMTPTestServer(t, smtpTestAccept)
	sender := smtpFixtureSender(t, capture)
	id := uuid.New()
	if err := sender.Send(context.Background(), &domain.Notification{ID: id, Type: "smtp_probe", Recipient: "authorized@example.test", Locale: "en"}); err != nil {
		t.Fatal(err)
	}
	message := <-capture.message
	if !strings.Contains(message, "Message-ID: <"+id.String()+"@example.test>") || !strings.Contains(decodeSMTPBody(t, message), "No order, account or payment was created") {
		t.Fatal("incorrect probe content/identity")
	}
}

func TestSMTPAcceptedEmailUsesStableMessageIDAndLocalizedSafeTemplate(t *testing.T) {
	caught := runSMTPTestServer(t, smtpTestAccept)
	sender := smtpFixtureSender(t, caught)
	id := uuid.MustParse("87df179f-43f8-49b2-89c3-74e4b61f6e3b")
	n := businessEmailFixture(id, "zh")
	if err := sender.Send(context.Background(), n); err != nil {
		t.Fatalf("send accepted SMTP message: %v", err)
	}
	message := <-caught.message
	if !strings.Contains(message, "Message-ID: <"+id.String()+"@example.test>") ||
		!strings.Contains(message, "Content-Transfer-Encoding: quoted-printable") ||
		!strings.Contains(decodeSMTPBody(t, message), "https://rushgo.example.test/my-tickets/orders/d25512e4-c46a-4eac-864f-1fd1da30d763") ||
		strings.Contains(strings.ToLower(message), "qr_code") || strings.Contains(message, "private-secret") {
		t.Fatalf("SMTP message missing stable identity or leaked unsafe fields: %s", message)
	}
	if n.ProviderID != "" {
		t.Fatalf("SMTP sender invented provider ID unavailable from net/smtp: %q", n.ProviderID)
	}
	if err := sender.Send(context.Background(), n); err != nil {
		t.Fatalf("same notification retry: %v", err)
	}
	second := <-caught.message
	if !strings.Contains(second, "Message-ID: <"+id.String()+"@example.test>") {
		t.Fatal("retry did not preserve the stable Message-ID")
	}
}

func TestSMTPLegacyEmailAddsSafeLinksAndOnlyDisplaysKnownCurrency(t *testing.T) {
	caught := runSMTPTestServer(t, smtpTestAccept)
	sender := smtpFixtureSender(t, caught)
	bookingID := "d25512e4-c46a-4eac-864f-1fd1da30d763"
	tests := []struct {
		name         string
		typeName     string
		locale       string
		payload      map[string]interface{}
		wantContains []string
		wantAbsent   []string
	}{
		{
			name: "english with currency", typeName: "booking_confirmation", locale: "en",
			payload:      map[string]interface{}{"booking_id": bookingID, "total": int64(72500), "currency": "USD"},
			wantContains: []string{"https://rushgo.example.test/my-tickets/orders/" + bookingID, "USD 725.00"},
			wantAbsent:   []string{"CNY"},
		},
		{
			name: "chinese with currency", typeName: "booking_confirmation", locale: "zh",
			payload:      map[string]interface{}{"booking_id": bookingID, "total": int64(72500), "currency": "CNY"},
			wantContains: []string{"https://rushgo.example.test/my-tickets/orders/" + bookingID, "CNY 725.00"},
		},
		{
			name: "missing currency", typeName: "booking_confirmation", locale: "en",
			payload:      map[string]interface{}{"booking_id": bookingID, "total": int64(72500)},
			wantContains: []string{"https://rushgo.example.test/my-tickets/orders/" + bookingID},
			wantAbsent:   []string{"CNY", "USD", "725.00"},
		},
		{
			name: "invalid currency", typeName: "booking_confirmation", locale: "en",
			payload:      map[string]interface{}{"booking_id": bookingID, "total": int64(72500), "currency": "cny"},
			wantContains: []string{"https://rushgo.example.test/my-tickets/orders/" + bookingID},
			wantAbsent:   []string{"cny", "CNY", "725.00"},
		},
		{
			name: "welcome account link", typeName: "welcome_email", locale: "zh",
			payload:      map[string]interface{}{},
			wantContains: []string{"https://rushgo.example.test/account"},
			wantAbsent:   []string{"/my-tickets/orders/"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			notification := &domain.Notification{
				ID: uuid.New(), Type: test.typeName, Recipient: "recipient@example.test", Locale: test.locale,
				Payload: test.payload,
			}
			if err := sender.Send(context.Background(), notification); err != nil {
				t.Fatalf("send %s email: %v", test.name, err)
			}
			select {
			case message := <-caught.message:
				body := decodeSMTPBody(t, message)
				for _, want := range test.wantContains {
					if !strings.Contains(body, want) {
						t.Errorf("email body missing %q: %s", want, body)
					}
				}
				for _, absent := range test.wantAbsent {
					if strings.Contains(body, absent) {
						t.Errorf("email body unexpectedly contains %q: %s", absent, body)
					}
				}
			default:
				t.Fatal("SMTP catcher did not receive submitted message")
			}
		})
	}
}

func decodeSMTPBody(t *testing.T, message string) string {
	t.Helper()
	separator := strings.Index(message, "\r\n\r\n")
	if separator < 0 {
		t.Fatal("SMTP message is missing body separator")
	}
	decoded, err := io.ReadAll(quotedprintable.NewReader(strings.NewReader(message[separator+4:])))
	if err != nil {
		t.Fatalf("decode quoted-printable body: %v", err)
	}
	return string(decoded)
}

func TestSMTPClassifiesExplicitRejectAndLostFinalResponse(t *testing.T) {
	for _, test := range []struct {
		name string
		mode smtpTestMode
		want error
		data bool
	}{
		{name: "auth rejection before submit", mode: smtpTestAuthReject, want: ErrDefinitelyNotAccepted, data: false},
		{name: "explicit data rejection", mode: smtpTestDataReject, want: ErrDefinitelyNotAccepted, data: true},
		{name: "lost final response after submit", mode: smtpTestDropAfterData, want: ErrDeliveryOutcomeUnknown, data: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			caught := runSMTPTestServer(t, test.mode)
			sender := smtpFixtureSender(t, caught)
			err := sender.Send(context.Background(), businessEmailFixture(uuid.New(), "en"))
			if !errors.Is(err, test.want) {
				t.Fatalf("SMTP result=%v, want classification %v", err, test.want)
			}
			if test.data {
				select {
				case <-caught.message:
				case <-time.After(time.Second):
					t.Fatal("SMTP catcher did not receive submitted message")
				}
			} else if caught.dataSeen.Load() {
				t.Fatal("SMTP sender submitted body after authentication rejection")
			}
		})
	}
}

func businessEmailFixture(id uuid.UUID, locale string) *domain.Notification {
	return &domain.Notification{
		ID: id, Type: "tickets_issued", Recipient: "recipient@example.test", Locale: locale,
		Payload: map[string]interface{}{
			"version": 1, "notification_id": id.String(), "type": "tickets_issued",
			"booking_id": "d25512e4-c46a-4eac-864f-1fd1da30d763", "user_id": "9cd06b91-3ecb-4a49-90dd-37a13fcb138c",
			"event_id": "fe591dda-0b95-4a07-9469-34e4a41a96ef", "generation": 1, "currency": "CNY", "total": 72500, "status": "issued",
		},
	}
}

type smtpTestMode int

const (
	smtpTestAccept smtpTestMode = iota
	smtpTestAuthReject
	smtpTestDataReject
	smtpTestDropAfterData
)

type smtpCapture struct {
	message  chan string
	dataSeen atomic.Bool
	listener net.Listener
	root     *x509.Certificate
}

func runSMTPTestServer(t *testing.T, mode smtpTestMode) *smtpCapture {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	certificate := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "smtp.fixture.test"},
		DNSNames: []string{"smtp.fixture.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true, IsCA: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	tlsCert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{tlsCert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	capture := &smtpCapture{message: make(chan string, 4), listener: listener, root: leaf}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			serveSMTPFixture(conn, capture, mode)
			_ = conn.Close()
		}
	}()
	return capture
}

func serveSMTPFixture(conn net.Conn, capture *smtpCapture, mode smtpTestMode) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	write := func(line string) bool {
		_, err := writer.WriteString(line + "\r\n")
		if err == nil {
			err = writer.Flush()
		}
		return err == nil
	}
	if !write("220 smtp.fixture.test ESMTP") {
		return
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		command := strings.TrimSpace(line)
		upper := strings.ToUpper(command)
		switch {
		case strings.HasPrefix(upper, "EHLO "):
			if !write("250-smtp.fixture.test") || !write("250 AUTH PLAIN") {
				return
			}
		case strings.HasPrefix(upper, "AUTH PLAIN "):
			if mode == smtpTestAuthReject {
				_ = write("535 5.7.8 authentication rejected")
				return
			}
			encodedAuth := strings.TrimSpace(command[len("AUTH PLAIN "):])
			if _, err := base64.StdEncoding.DecodeString(encodedAuth); err != nil {
				_ = write("501 invalid auth")
				return
			}
			if !write("235 2.7.0 authentication accepted") {
				return
			}
		case strings.HasPrefix(upper, "MAIL FROM:") || strings.HasPrefix(upper, "RCPT TO:"):
			if !write("250 2.1.0 accepted") {
				return
			}
		case upper == "DATA":
			if !write("354 end with dot") {
				return
			}
			capture.dataSeen.Store(true)
			var message strings.Builder
			for {
				bodyLine, readErr := reader.ReadString('\n')
				if readErr != nil {
					return
				}
				if strings.TrimRight(bodyLine, "\r\n") == "." {
					break
				}
				message.WriteString(bodyLine)
			}
			capture.message <- message.String()
			if mode == smtpTestDropAfterData {
				return
			}
			if mode == smtpTestDataReject {
				_ = write("550 5.7.1 message rejected")
				return
			}
			if !write("250 2.0.0 queued as fixture-123") {
				return
			}
		case upper == "QUIT":
			_ = write("221 2.0.0 bye")
			return
		default:
			if !write("250 2.0.0 ok") {
				return
			}
		}
	}
}

func smtpFixtureSender(t *testing.T, capture *smtpCapture) *SMTPNotificationSender {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(capture.root)
	config := SMTPConfig{Host: "smtp.fixture.test", Port: 465, TLSMode: "implicit", Username: "synthetic-user", Password: "synthetic-password", From: "sender@example.test", FrontendOrigin: "https://rushgo.example.test", Timeout: 3 * time.Second}
	sender, err := newSMTPNotificationSender(config, pool)
	if err != nil {
		t.Fatal(err)
	}
	address := capture.listener.Addr().String()
	sender.dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	return sender
}
