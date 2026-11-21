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
	"encoding/pem"
	"fmt"
	"github.com/ticketbox/user/internal/domain"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// Fixture retains command categories/mechanism only. AUTH values and DATA are discarded.
type smtpFixture struct {
	listener                              net.Listener
	cert                                  tls.Certificate
	roots                                 *x509.CertPool
	mode, auth, reject, block             string
	mu                                    sync.Mutex
	plain, login, mail, data, connections int
	insecure                              bool
	conns                                 []net.Conn
	wg                                    sync.WaitGroup
	entered                               chan struct{}
	once                                  sync.Once
}

func newSMTPFixture(t *testing.T, mode, auth, reject, block string) *smtpFixture {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal("test CA generation failed")
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "smtp.fixture.test"}, DNSNames: []string{"smtp.fixture.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal("test cert failed")
	}
	private, _ := x509.MarshalECPrivateKey(key)
	cert, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: private}))
	if err != nil {
		t.Fatal("test cert parse failed")
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("smtp listener failed")
	}
	f := &smtpFixture{listener: l, cert: cert, roots: roots, mode: mode, auth: auth, reject: reject, block: block, entered: make(chan struct{})}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns = append(f.conns, c)
			f.connections++
			f.mu.Unlock()
			f.wg.Add(1)
			go func() { defer f.wg.Done(); f.serve(c) }()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		f.mu.Lock()
		for _, c := range f.conns {
			c.Close()
		}
		f.mu.Unlock()
		f.wg.Wait()
	})
	return f
}
func (f *smtpFixture) waitForClose(c net.Conn) {
	f.once.Do(func() { close(f.entered) })
	_, _ = io.Copy(io.Discard, c)
}
func (f *smtpFixture) serve(raw net.Conn) {
	defer raw.Close()
	c := raw
	secure := false
	if f.mode == "implicit" {
		tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{f.cert}, MinVersion: tls.VersionTLS12})
		if tc.Handshake() != nil {
			return
		}
		c = tc
		secure = true
	}
	if f.block == "greeting" {
		f.waitForClose(c)
		return
	}
	_, _ = fmt.Fprint(c, "220 fixture ESMTP\r\n")
	r := bufio.NewReader(c)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			return
		}
		command := strings.ToUpper(parts[0])
		switch command {
		case "EHLO":
			_, _ = fmt.Fprint(c, "250-fixture\r\n")
			if !secure && f.mode == "starttls" {
				_, _ = fmt.Fprint(c, "250-STARTTLS\r\n")
			}
			if secure && f.auth != "absent" {
				_, _ = fmt.Fprintf(c, "250-AUTH %s\r\n", f.auth)
			}
			_, _ = fmt.Fprint(c, "250 8BITMIME\r\n")
		case "STARTTLS":
			_, _ = fmt.Fprint(c, "220 upgrade\r\n")
			tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{f.cert}, MinVersion: tls.VersionTLS12})
			if tc.Handshake() != nil {
				return
			}
			c = tc
			r = bufio.NewReader(c)
			secure = true
		case "AUTH":
			f.mu.Lock()
			if !secure {
				f.insecure = true
			}
			mechanism := ""
			if len(parts) > 1 {
				mechanism = parts[1]
			}
			if mechanism == "PLAIN" {
				f.plain++
			}
			if mechanism == "LOGIN" {
				f.login++
			}
			f.mu.Unlock()
			if f.block == "auth" {
				f.waitForClose(c)
				return
			}
			if f.reject == "auth" {
				_, _ = fmt.Fprint(c, "535 synthetic rejection canary\r\n")
				continue
			}
			if mechanism == "LOGIN" {
				_, _ = fmt.Fprint(c, "334 VXNlcm5hbWU6\r\n")
				if _, err = r.ReadString('\n'); err != nil {
					return
				}
				_, _ = fmt.Fprint(c, "334 UGFzc3dvcmQ6\r\n")
				if _, err = r.ReadString('\n'); err != nil {
					return
				}
			}
			_, _ = fmt.Fprint(c, "235 authenticated\r\n")
		case "MAIL":
			f.mu.Lock()
			f.mail++
			f.mu.Unlock()
			if f.reject == "mail" {
				_, _ = fmt.Fprint(c, "550 synthetic rejection\r\n")
			} else {
				_, _ = fmt.Fprint(c, "250 sender\r\n")
			}
		case "RCPT":
			if f.reject == "rcpt" {
				_, _ = fmt.Fprint(c, "550 synthetic rejection\r\n")
			} else {
				_, _ = fmt.Fprint(c, "250 recipient\r\n")
			}
		case "DATA":
			f.mu.Lock()
			f.data++
			f.mu.Unlock()
			if f.reject == "data" {
				_, _ = fmt.Fprint(c, "550 synthetic rejection\r\n")
				continue
			}
			_, _ = fmt.Fprint(c, "354 content\r\n")
			for {
				body, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if body == ".\r\n" {
					break
				}
			}
			if f.block == "data" {
				f.waitForClose(c)
				return
			}
			_, _ = fmt.Fprint(c, "250 accepted\r\n")
		case "RSET":
			if f.reject == "rset" {
				_, _ = fmt.Fprint(c, "550 cleanup failed\r\n")
			} else {
				_, _ = fmt.Fprint(c, "250 reset\r\n")
			}
		case "QUIT":
			if f.reject == "quit" {
				_, _ = fmt.Fprint(c, "550 cleanup failed\r\n")
			} else {
				_, _ = fmt.Fprint(c, "221 closing\r\n")
			}
			return
		default:
			_, _ = fmt.Fprint(c, "250 ok\r\n")
		}
	}
}
func (f *smtpFixture) sender(t *testing.T) *SMTPMailSender {
	t.Helper()
	port := 465
	if f.mode == "starttls" {
		port = 587
	}
	s, err := NewSMTPMailSender(SMTPConfig{Host: "smtp.fixture.test", TLSMode: f.mode, Port: port, Username: "synthetic", Password: "synthetic", From: "sender@example.com", Timeout: 15 * time.Second})
	if err != nil {
		t.Fatal("valid SMTP config rejected")
	}
	s.roots = f.roots
	s.dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, f.listener.Addr().String())
	}
	return s
}
func syntheticMail() AuthMail {
	return AuthMail{Recipient: "recipient@example.com", Code: "000001", Purpose: RegistrationMail, ExpiresAt: time.Now().Add(time.Minute)}
}
func TestSMTPAuthMatrix(t *testing.T) {
	for _, mode := range []string{"implicit", "starttls"} {
		for _, auth := range []string{"PLAIN", "LOGIN", "PLAIN LOGIN", "CRAM-MD5", "absent"} {
			t.Run(mode+"/"+auth, func(t *testing.T) {
				f := newSMTPFixture(t, mode, auth, "", "")
				result := f.sender(t).Send(context.Background(), syntheticMail())
				f.mu.Lock()
				defer f.mu.Unlock()
				if auth == "CRAM-MD5" || auth == "absent" {
					if result.Class != domain.ConfigurationFailure || f.mail != 0 || f.data != 0 || f.plain != 0 || f.login != 0 {
						t.Fatal("unsupported mechanism escaped allowlist")
					}
					return
				}
				if result.Class != domain.Accepted || f.insecure {
					t.Fatal("TLS authentication delivery failed")
				}
				if auth == "LOGIN" {
					if f.login != 1 || f.plain != 0 || f.connections != 2 {
						t.Fatal("LOGIN fallback selection incorrect")
					}
				} else if f.plain != 1 || f.login != 0 || f.connections != 1 {
					t.Fatal("PLAIN preference incorrect")
				}
			})
		}
	}
}
func TestSMTPAcceptedDataCleanupFailure(t *testing.T) {
	for _, cleanup := range []string{"quit", "rset"} {
		t.Run(cleanup, func(t *testing.T) {
			f := newSMTPFixture(t, "implicit", "PLAIN", cleanup, "")
			if f.sender(t).Send(context.Background(), syntheticMail()).Class != domain.Accepted {
				t.Fatal("cleanup erased positive DATA acceptance")
			}
		})
	}
}
func TestSMTPCredentialRejectionNoFallback(t *testing.T) {
	for _, auth := range []string{"PLAIN LOGIN", "LOGIN"} {
		t.Run(auth, func(t *testing.T) {
			f := newSMTPFixture(t, "implicit", auth, "auth", "")
			r := f.sender(t).Send(context.Background(), syntheticMail())
			f.mu.Lock()
			defer f.mu.Unlock()
			if r.Class != domain.ConfigurationFailure || f.mail != 0 || f.plain+f.login != 1 {
				t.Fatal("credential rejection retried or leaked")
			}
		})
	}
}
func TestSMTPExplicitReject(t *testing.T) {
	for _, phase := range []string{"mail", "rcpt", "data"} {
		t.Run(phase, func(t *testing.T) {
			f := newSMTPFixture(t, "starttls", "PLAIN", phase, "")
			r := f.sender(t).Send(context.Background(), syntheticMail())
			if r.Class != domain.ExplicitFailure {
				t.Fatal("explicit rejection not classified")
			}
		})
	}
}
func TestSMTPCancellationAllPhases(t *testing.T) {
	for _, phase := range []string{"greeting", "auth", "data"} {
		t.Run(phase, func(t *testing.T) {
			f := newSMTPFixture(t, "implicit", "PLAIN LOGIN", "", phase)
			s := f.sender(t)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan DeliveryResult, 1)
			go func() { done <- s.Send(ctx, syntheticMail()) }()
			select {
			case <-f.entered:
			case <-time.After(2 * time.Second):
				cancel()
				t.Fatal("phase not reached")
			}
			cancel()
			select {
			case r := <-done:
				if r.Class != domain.Unknown {
					t.Fatal("cancellation not unknown")
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation failed to close socket")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.connections != 1 || f.login != 0 {
				t.Fatal("cancellation caused auth fallback")
			}
		})
	}
}
func TestSMTPTimeoutNoFallback(t *testing.T) {
	f := newSMTPFixture(t, "implicit", "PLAIN LOGIN", "", "auth")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r := f.sender(t).Send(ctx, syntheticMail())
	if r.Class != domain.Unknown {
		t.Fatal("timeout not unknown")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.connections != 1 || f.login != 0 {
		t.Fatal("timeout reset or retried")
	}
}
func TestSMTPCertificateFailure(t *testing.T) {
	f := newSMTPFixture(t, "implicit", "PLAIN LOGIN", "", "")
	s := f.sender(t)
	s.roots = x509.NewCertPool()
	r := s.Send(context.Background(), syntheticMail())
	if r.Class != domain.ConfigurationFailure {
		t.Fatal("untrusted TLS accepted")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.plain+f.login != 0 || f.mail != 0 {
		t.Fatal("credentials crossed invalid TLS")
	}
}
func TestSMTPNonTLSAndPartialRejected(t *testing.T) {
	for _, cfg := range []SMTPConfig{{Host: "smtp.example.com", Port: 25, TLSMode: "plain", Username: "synthetic", Password: "synthetic", From: "a@example.com"}, {Host: "smtp.example.com", Port: 465, TLSMode: "starttls"}, {Host: "smtp.example.com", Port: 587, TLSMode: "implicit"}} {
		if _, err := NewSMTPMailSender(cfg); err == nil {
			t.Fatal("plaintext/malformed configuration accepted")
		}
	}
}
func TestSMTPHeaderInjectionRejected(t *testing.T) {
	f := newSMTPFixture(t, "implicit", "PLAIN", "", "")
	m := syntheticMail()
	m.Recipient = "a@example.com\r\nBcc:bad@example.com"
	if f.sender(t).Send(context.Background(), m).Class != domain.ConfigurationFailure {
		t.Fatal("header injection accepted")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.connections != 0 {
		t.Fatal("invalid header reached transport")
	}
}
