package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBackupPolicyDeclarations(t *testing.T) {
	valid := `{"status":"CONFIGURED","storage":"operator-owned-repository-reference","encryptionKeyReference":"secret-manager-reference","retentionDays":30,"rpoSeconds":300,"rtoSeconds":3600,"schedule":"operator-managed daily backup","lastVerifiedRestore":null,"scope":["user","event","booking","payment","saga","notification","scheduler"]}`
	for _, tc := range []struct{ name, body, want string }{
		{"complete", valid, "PASS"},
		{"inactive", strings.Replace(valid, "CONFIGURED", "NOT_ACTIVATED", 1), "NOT_CONFIGURED"},
		{"missing storage", strings.Replace(valid, `"operator-owned-repository-reference"`, `null`, 1), "NOT_CONFIGURED"},
		{"missing scheduler", strings.Replace(valid, `,"scheduler"`, "", 1), "FAIL"},
		{"duplicate scope", strings.Replace(valid, `"scheduler"`, `"user"`, 1), "FAIL"},
		{"zero RPO", strings.Replace(valid, `"rpoSeconds":300`, `"rpoSeconds":0`, 1), "FAIL"},
		{"negative RTO", strings.Replace(valid, `"rtoSeconds":3600`, `"rtoSeconds":-1`, 1), "FAIL"},
		{"typo", strings.Replace(valid, "rpoSeconds", "rpoSecond", 1), "FAIL"},
		{"activation unsupported", strings.Replace(valid, "CONFIGURED", "ACTIVE", 1), "FAIL"},
		{"trailing", valid + `{}`, "FAIL"},
		{"oversized", strings.Repeat(" ", 65537), "FAIL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "policy.json")
			if err := os.WriteFile(p, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			if got := verifyBackupPolicy(p); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
	if verifyBackupPolicy("") != "NOT_CONFIGURED" || verifyBackupPolicy(filepath.Join(t.TempDir(), "missing")) != "FAIL" {
		t.Fatal("missing path handling")
	}
}

func TestBackupPolicyTemplateRemainsInactive(t *testing.T) {
	if got := verifyBackupPolicy("../../../deployment/backup-policy.json.example"); got != "NOT_CONFIGURED" {
		t.Fatalf("template: %s", got)
	}
}

func TestDeclaredRestoreDoesNotCertifyRuntime(t *testing.T) {
	t.Setenv("RUSHGO_ENV", "isolated_demo")
	t.Setenv("RUSHGO_PROVIDER_MODE", "SIMULATED_PROVIDER")
	for _, key := range []string{"STRIPE_SECRET_KEY", "STRIPE_SECRET_KEY_FILE", "STRIPE_SECRET_WEBHOOK", "STRIPE_SECRET_WEBHOOK_FILE", "STRIPE_PUBLISHABLE_KEY", "DATABASE_URL", "DATABASE_URL_FILE"} {
		t.Setenv(key, "")
	}
	p := filepath.Join(t.TempDir(), "policy.json")
	data := `{"status":"CONFIGURED","storage":"repo-reference","encryptionKeyReference":"key-reference","retentionDays":30,"rpoSeconds":300,"rtoSeconds":3600,"schedule":"daily","lastVerifiedRestore":{"result":"PASS","productionReady":true},"scope":["user","event","booking","payment","saga","notification","scheduler"]}`
	if err := os.WriteFile(p, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUSHGO_BACKUP_POLICY_FILE", p)
	r := inspect("", "", false)
	if r.Checks["backupPolicy"] != "PASS" || r.Checks["backupRestore"] != "NOT_VERIFIED" || r.Checks["pitr"] != "NOT_VERIFIED" || r.ProductionReady || r.Overall == "PASS" {
		t.Fatal("operator declaration promoted runtime evidence")
	}
}
