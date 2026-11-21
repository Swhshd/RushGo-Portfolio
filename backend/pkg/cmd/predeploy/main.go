// predeploy is an offline-by-default reader of the existing release binding and
// service configuration. --runtime adds one bounded read-only database probe.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/pkg/authpolicy"
	"github.com/ticketbox/pkg/config"
)

type report struct {
	Identity        config.ReleaseIdentity `json:"identity"`
	Checks          map[string]string      `json:"checks"`
	Overall         string                 `json:"overall"`
	ProductionReady bool                   `json:"productionReady"`
}
type binding struct {
	SourceCommit        string            `json:"sourceCommit"`
	BuildIdentity       string            `json:"buildIdentity"`
	ApplicationVersion  string            `json:"applicationVersion"`
	ConfigSchemaVersion string            `json:"configSchemaVersion"`
	MigrationVersion    string            `json:"migrationVersion"`
	Files               map[string]string `json:"files"`
}

func main() {
	manifest := flag.String("binding", "", "existing package binding.json")
	expected := flag.String("binding-sha256", "", "trusted delivery checksum for binding.json")
	runtime := flag.Bool("runtime", false, "also read configured DB readiness/role privileges; never writes business data")
	jsonOnly := flag.Bool("json", false, "JSON only (default prints table to stderr and JSON to stdout)")
	flag.Parse()
	result := inspect(*manifest, *expected, *runtime)
	if !*jsonOnly {
		keys := make([]string, 0, len(result.Checks))
		for k := range result.Checks {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(os.Stderr, "%-30s %s\n", k, result.Checks[k])
		}
	}
	if json.NewEncoder(os.Stdout).Encode(result) != nil {
		os.Exit(1)
	}
	if result.Overall != "PASS" {
		os.Exit(1)
	}
}

func inspect(path, expected string, runtime bool) report {
	r := report{Checks: map[string]string{}, Overall: "NOT_VERIFIED"}
	for _, name := range []string{"releaseBinding", "configuration", "migrationRuntime", "backendReadiness", "httpsHandshake", "mtlsHandshake", "stripeAccount", "smtpDelivery", "alertDelivery", "databasePrivileges", "backupRestore", "pitr", "realDeviceCamera", "capacity"} {
		r.Checks[name] = "NOT_VERIFIED"
	}
	cfg, err := config.Load()
	if err != nil {
		r.Checks["configuration"] = "FAIL"
		r.Overall = "FAIL"
		if errors.Is(err, config.ErrNotConfigured) {
			r.Checks["configuration"] = "NOT_CONFIGURED"
			r.Overall = "NOT_CONFIGURED"
		}
		return r
	}
	r.Identity = cfg.Release
	r.Checks["configuration"] = "PASS"
	if !cfg.Release.External() {
		r.Checks["configuration"] = "FAIL"
	}
	r.Checks["releaseBinding"] = verifyBinding(path, expected, cfg.Release)
	r.Checks["mtlsConfiguration"] = verifyCertificates()
	r.Checks["httpsOrigin"] = "NOT_CONFIGURED"
	if strings.HasPrefix(cfg.AuthFrontendOrigin, "https://") {
		r.Checks["httpsOrigin"] = "PASS"
	}
	r.Checks["stripeConfiguration"] = "NOT_CONFIGURED"
	if cfg.Release.ProviderMode == "STRIPE_SANDBOX_OR_TEST" && cfg.StripeSecretKey != "" && cfg.StripeSecretWebhook != "" && os.Getenv("RUSHGO_STRIPE_EXPECTED_ACCOUNT") != "" {
		r.Checks["stripeConfiguration"] = "PASS"
	}
	if cfg.ServiceName != "payment-service" {
		r.Checks["stripeConfiguration"] = "NOT_APPLICABLE"
		r.Checks["stripeAccount"] = "NOT_APPLICABLE"
	}
	r.Checks["smtpConfiguration"] = "NOT_CONFIGURED"
	_, fromErr := authpolicy.CanonicalEmail(cfg.SMTPFrom)
	origin, originErr := url.Parse(cfg.AuthFrontendOrigin)
	if cfg.SMTPHost != "" && !strings.ContainsAny(cfg.SMTPHost, " \t\r\n:/") && cfg.SMTPUsername != "" && !strings.ContainsAny(cfg.SMTPUsername, "\r\n") && cfg.SMTPPassword != "" && fromErr == nil && originErr == nil && origin.Scheme == "https" && origin.Host != "" && origin.User == nil && origin.Path == "" && origin.RawQuery == "" && origin.Fragment == "" && ((cfg.SMTPPort == 465 && cfg.SMTPTLSMode == "implicit") || (cfg.SMTPPort == 587 && cfg.SMTPTLSMode == "starttls")) {
		r.Checks["smtpConfiguration"] = "PASS"
	}
	if cfg.ServiceName != "notification-service" && cfg.ServiceName != "user-service" {
		r.Checks["smtpConfiguration"] = "NOT_APPLICABLE"
		r.Checks["smtpDelivery"] = "NOT_APPLICABLE"
	}
	r.Checks["alertReceiver"] = "NOT_CONFIGURED"
	if os.Getenv("RUSHGO_ALERTMANAGER_CONFIG_FILE") != "" {
		r.Checks["alertReceiver"] = "NOT_VERIFIED"
	} // amtool + receipt required, a path is not proof.
	r.Checks["backupPolicy"] = verifyBackupPolicy(os.Getenv("RUSHGO_BACKUP_POLICY_FILE"))
	if cfg.DatabaseURL == "" {
		r.Checks["databasePrivileges"] = "NOT_CONFIGURED"
	} else if runtime {
		r.Checks["databasePrivileges"], r.Checks["databaseReadiness"] = probeDatabase(cfg.DatabaseURL)
	}
	for _, state := range r.Checks {
		if state == "FAIL" {
			r.Overall = "FAIL"
			break
		}
	}
	return r // Offline configuration checks never certify deployment or production readiness.
}

func verifyBinding(path, expected string, id config.ReleaseIdentity) string {
	if path == "" || expected == "" {
		return "NOT_CONFIGURED"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "FAIL"
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != expected {
		return "FAIL"
	}
	var b binding
	if json.Unmarshal(data, &b) != nil || len(b.Files) == 0 {
		return "FAIL"
	}
	if b.SourceCommit != id.SourceCommit || b.BuildIdentity != id.BuildIdentity || b.ApplicationVersion != id.ApplicationVersion || b.ConfigSchemaVersion != id.ConfigSchemaVersion || b.MigrationVersion != id.MigrationVersion {
		return "FAIL"
	}
	root, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return "FAIL"
	}
	for name, want := range b.Files {
		if !filepath.IsLocal(filepath.FromSlash(name)) || strings.Contains(name, "\\") {
			return "FAIL"
		}
		candidate := filepath.Join(root, filepath.FromSlash(name))
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil || resolved != candidate {
			return "FAIL"
		}
		info, err := os.Stat(candidate)
		if err != nil || !info.Mode().IsRegular() {
			return "FAIL"
		}
		contents, err := os.ReadFile(candidate)
		if err != nil {
			return "FAIL"
		}
		hash := sha256.Sum256(contents)
		if hex.EncodeToString(hash[:]) != want {
			return "FAIL"
		}
	}
	return "PASS"
}

func verifyCertificates() string {
	for _, pair := range [][2]string{{"RUSHGO_GRPC_SERVER_CERT_FILE", "RUSHGO_GRPC_SERVER_KEY_FILE"}, {"RUSHGO_GRPC_CLIENT_CERT_FILE", "RUSHGO_GRPC_CLIENT_KEY_FILE"}} {
		if os.Getenv(pair[0]) == "" || os.Getenv(pair[1]) == "" {
			return "NOT_CONFIGURED"
		}
		cert, err := tls.LoadX509KeyPair(os.Getenv(pair[0]), os.Getenv(pair[1]))
		if err != nil || len(cert.Certificate) == 0 {
			return "FAIL"
		}
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil || time.Now().Before(leaf.NotBefore) || time.Now().Add(7*24*time.Hour).After(leaf.NotAfter) {
			return "FAIL"
		}
	}
	for _, name := range []string{"RUSHGO_GRPC_CLIENT_CA_FILE", "RUSHGO_GRPC_SERVER_CA_FILE"} {
		if os.Getenv(name) == "" {
			return "NOT_CONFIGURED"
		}
		contents, err := os.ReadFile(os.Getenv(name))
		if err != nil {
			return "FAIL"
		}
		block, _ := pem.Decode(contents)
		if block == nil {
			return "FAIL"
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA || time.Now().Before(cert.NotBefore) || time.Now().Add(7*24*time.Hour).After(cert.NotAfter) {
			return "FAIL"
		}
	}
	return "PASS" // Syntax, key match and lifetime only; handshake is a separate NOT_VERIFIED check.
}

func probeDatabase(dsn string) (string, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "NOT_VERIFIED", "FAIL"
	}
	defer conn.Close(ctx)
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return "NOT_VERIFIED", "FAIL"
	}
	defer tx.Rollback(ctx)
	var unsafe bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles r WHERE pg_has_role(current_user,r.oid,'MEMBER') AND (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls))
 OR EXISTS(SELECT 1 FROM pg_database d WHERE d.datname=current_database() AND pg_has_role(current_user,d.datdba,'MEMBER'))
 OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND pg_has_role(current_user,c.relowner,'MEMBER'))
 OR has_schema_privilege(current_user,'public','CREATE')`).Scan(&unsafe)
	if err != nil {
		return "NOT_VERIFIED", "PASS"
	}
	if unsafe {
		return "FAIL", "PASS"
	}
	return "PASS", "PASS"
}
