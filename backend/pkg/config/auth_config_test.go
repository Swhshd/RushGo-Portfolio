package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigPresenceCheckerNeverPrintsCanary(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "synthetic.env")
	if os.WriteFile(file, []byte("SMTP_HOST=synthetic-canary\nSMTP_PASSWORD=synthetic-canary\nSMTP_FROM=\n"), 0600) != nil {
		t.Fatal("fixture failed")
	}
	script, err := filepath.Abs("../../scripts/m6-auth-env-check.ps1")
	if err != nil {
		t.Fatal("script path failed")
	}
	out, err := exec.Command("powershell.exe", "-NoLogo", "-NoProfile", "-File", script, "-EnvFile", file, "-LocalOnly").CombinedOutput()
	if err != nil {
		t.Fatal("checker failed")
	}
	if strings.Contains(string(out), "synthetic-canary") {
		t.Fatal("checker printed value")
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 7 || !strings.Contains(string(out), "SMTP_HOST = SET") || !strings.Contains(string(out), "SMTP_FROM = NOT_SET") {
		t.Fatal("presence checker contract mismatch")
	}
}
