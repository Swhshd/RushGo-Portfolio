package config

import "testing"

func TestSMTPRawFieldsLoad(t *testing.T) {
	t.Setenv("SMTP_HOST", "smtp.fixture.test")
	t.Setenv("SMTP_PORT", "465")
	t.Setenv("SMTP_TLS_MODE", "implicit")
	t.Setenv("SMTP_USERNAME", "synthetic")
	t.Setenv("SMTP_PASSWORD", "synthetic")
	t.Setenv("SMTP_FROM", "sender@example.com")
	t.Setenv("AUTH_FRONTEND_ORIGIN", "http://localhost:3002")
	cfg, err := Load()
	if err != nil {
		t.Fatal("config load failed")
	}
	if cfg.SMTPHost != "smtp.fixture.test" || cfg.SMTPPort != 465 || cfg.SMTPTLSMode != "implicit" || cfg.SMTPUsername != "synthetic" || cfg.SMTPPassword != "synthetic" || cfg.SMTPFrom != "sender@example.com" || cfg.AuthFrontendOrigin != "http://localhost:3002" {
		t.Fatal("SMTP raw fields not loaded")
	}
}
