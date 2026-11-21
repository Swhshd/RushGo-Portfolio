package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/ticketbox/pkg/config"
	"os"
	"path/filepath"
	"testing"
)

func TestBindingIdentityAndTamperChecks(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "entry")
	os.WriteFile(file, []byte("bound"), 0600)
	hash := sha256.Sum256([]byte("bound"))
	id := config.ReleaseIdentity{SourceCommit: "source", BuildIdentity: "build", ApplicationVersion: "v1", ConfigSchemaVersion: "1", MigrationVersion: "migration"}
	b := binding{SourceCommit: "source", BuildIdentity: "build", ApplicationVersion: "v1", ConfigSchemaVersion: "1", MigrationVersion: "migration", Files: map[string]string{"entry": hex.EncodeToString(hash[:])}}
	path := filepath.Join(dir, "binding.json")
	save := func() string {
		data, _ := json.Marshal(b)
		os.WriteFile(path, data, 0600)
		h := sha256.Sum256(data)
		return hex.EncodeToString(h[:])
	}
	expected := save()
	if verifyBinding(path, expected, id) != "PASS" {
		t.Fatal("valid binding failed")
	}
	if verifyBinding(path, "", id) != "NOT_CONFIGURED" {
		t.Fatal("untrusted binding accepted")
	}
	os.WriteFile(file, []byte("changed"), 0600)
	if verifyBinding(path, expected, id) != "FAIL" {
		t.Fatal("changed artifact accepted")
	}
	b.Files = map[string]string{"../outside": "bad"}
	expected = save()
	if verifyBinding(path, expected, id) != "FAIL" {
		t.Fatal("unsafe path accepted")
	}
	b.Files = map[string]string{"entry": hex.EncodeToString(hash[:])}
	b.BuildIdentity = "different"
	expected = save()
	if verifyBinding(path, expected, id) != "FAIL" {
		t.Fatal("wrong deployment build accepted")
	}
}

func TestMissingExternalConfigurationNeverPasses(t *testing.T) {
	t.Setenv("RUSHGO_ENV", "isolated_demo")
	t.Setenv("RUSHGO_PROVIDER_MODE", "SIMULATED_PROVIDER")
	for _, name := range []string{"STRIPE_SECRET_KEY", "STRIPE_SECRET_KEY_FILE", "STRIPE_SECRET_WEBHOOK", "STRIPE_SECRET_WEBHOOK_FILE", "STRIPE_PUBLISHABLE_KEY", "RUSHGO_GRPC_SERVER_CERT_FILE", "RUSHGO_ALERTMANAGER_CONFIG_FILE", "RUSHGO_BACKUP_POLICY_FILE", "DATABASE_URL", "DATABASE_URL_FILE"} {
		t.Setenv(name, "")
	}
	r := inspect("", "", false)
	if r.Overall == "PASS" || r.ProductionReady || r.Checks["releaseBinding"] != "NOT_CONFIGURED" || r.Checks["backendReadiness"] != "NOT_VERIFIED" {
		t.Fatal("missing setup certified")
	}
	data, _ := json.Marshal(r)
	if string(data) == "" {
		t.Fatal("missing machine report")
	}
}
