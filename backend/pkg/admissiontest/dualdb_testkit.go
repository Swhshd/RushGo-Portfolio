//go:build admission_integration

// Package admissiontest contains test-only plumbing for the real, isolated
// Payment/Booking PostgreSQL and mTLS admission integration tests.
package admissiontest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	OptInEnv   = "RUSHGO_ADMISSION_DUALDB_TEST_ONLY"
	OptInValue = "I_UNDERSTAND_ISOLATED_DATABASES_AND_LOCAL_PROVIDER_ONLY"
)

var (
	projectPattern   = regexp.MustCompile(`^rushgo-ticket-e2e-20[0-9]{6}-[a-f0-9]{12}$`)
	containerPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
	runIDPattern     = regexp.MustCompile(`^20[0-9]{6}T[0-9]{6}Z-[a-f0-9]{12}$`)
	ownerPattern     = regexp.MustCompile(`^[a-f0-9]{32}$`)
	refundDBPattern  = regexp.MustCompile(`^component_refund_[a-f0-9]{12}$`)
)

// PKI creates fresh ephemeral test credentials. Its keys exist only in the
// integration process temp directory and are never accepted by production
// binaries or default-build tests.
type PKI struct {
	caCertificate *x509.Certificate
	caPrivateKey  *ecdsa.PrivateKey
	CAPEM         []byte
}

type Leaf struct {
	CertificatePEM []byte
	PrivateKeyPEM  []byte
}

func NewPKI(t testing.TB, commonName string) PKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal("generate ephemeral test CA key:", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal("generate ephemeral test CA serial:", err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(2 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal("create ephemeral test CA:", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal("parse ephemeral test CA:", err)
	}
	return PKI{
		caCertificate: certificate,
		caPrivateKey:  key,
		CAPEM:         pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// NewLeaf creates a short-lived leaf with the supplied SPIFFE identity and
// optional DNS SAN. serverName must match dnsName in client credentials.
func (p PKI) NewLeaf(t testing.TB, dnsName, identity string, usage x509.ExtKeyUsage) Leaf {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal("generate ephemeral mTLS key:", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal("generate ephemeral mTLS serial:", err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "admission-integration"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(2 * time.Hour),
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{usage},
	}
	if dnsName != "" {
		template.DNSNames = []string{dnsName}
	}
	if identity != "" {
		parsed, err := url.Parse(identity)
		if err != nil || parsed.Scheme != "spiffe" || parsed.Host != "rushgo.internal" || parsed.Path == "" {
			t.Fatal("invalid test service identity")
		}
		template.URIs = []*url.URL{parsed}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, p.caCertificate, &key.PublicKey, p.caPrivateKey)
	if err != nil {
		t.Fatal("create ephemeral mTLS certificate:", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal("marshal ephemeral mTLS key:", err)
	}
	return Leaf{
		CertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		PrivateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}
}

func WriteLeaf(t testing.TB, dir, name string, leaf Leaf) (certificatePath, keyPath string) {
	t.Helper()
	certificatePath = filepath.Join(dir, name+".crt.pem")
	keyPath = filepath.Join(dir, name+".key.pem")
	if err := os.WriteFile(certificatePath, leaf.CertificatePEM, 0o600); err != nil {
		t.Fatal("write ephemeral test certificate:", err)
	}
	if err := os.WriteFile(keyPath, leaf.PrivateKeyPEM, 0o600); err != nil {
		t.Fatal("write ephemeral test key:", err)
	}
	return certificatePath, keyPath
}

func WriteCA(t testing.TB, dir string, caPEM []byte) string {
	t.Helper()
	path := filepath.Join(dir, "ca.crt.pem")
	if err := os.WriteFile(path, caPEM, 0o600); err != nil {
		t.Fatal("write ephemeral test CA:", err)
	}
	return path
}

// RequireOptIn is deliberately separate from target validation so every
// exported fixture setup can fail before even opening a database connection.
func RequireOptIn(t testing.TB) {
	t.Helper()
	if os.Getenv(OptInEnv) != OptInValue {
		t.Fatalf("BLOCKED: set %s=%s only for the owned admission integration run", OptInEnv, OptInValue)
	}
}

// VerifyBookingPostgres accepts only the active C-SIM's isolated Booking
// database after matching both its Docker container and internal network.
func VerifyBookingPostgres(t testing.TB, dsn string) {
	t.Helper()
	parsed := parsePostgresURL(t, dsn, "/ticketbox_booking_test")
	for _, key := range []string{"host", "hostaddr", "port", "database", "dbname", "service", "servicefile", "search_path"} {
		if _, exists := parsed.Query()[key]; exists {
			t.Fatalf("BLOCKED: Booking DSN contains forbidden target override %q", key)
		}
	}
	host := net.ParseIP(parsed.Hostname())
	loopback := parsed.Hostname() == "127.0.0.1" && parsed.Port() == "25435"
	private := host != nil && host.To4() != nil && host.IsPrivate() && parsed.Port() == "5432"
	if !loopback && !private {
		t.Fatal("BLOCKED: Booking DSN is not the isolated C-SIM PostgreSQL target")
	}
	project, containerID := os.Getenv("TICKET_E2E_PROJECT"), os.Getenv("BOOKING_TEST_DATABASE_CONTAINER_ID")
	if !projectPattern.MatchString(project) || !containerPattern.MatchString(containerID) {
		t.Fatal("BLOCKED: verified C-SIM project and Booking container identity are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	containerJSON, err := exec.CommandContext(ctx, "docker", "inspect", containerID).Output()
	if err != nil {
		t.Fatal("BLOCKED: cannot verify owned Booking PostgreSQL container")
	}
	networkName := project + "_ticket-e2e"
	networkJSON, err := exec.CommandContext(ctx, "docker", "network", "inspect", networkName).Output()
	if err != nil {
		t.Fatal("BLOCKED: cannot verify owned Booking PostgreSQL network")
	}
	if !verifiedBookingTarget(dsn, project, containerID, networkName, containerJSON, networkJSON) {
		t.Fatal("BLOCKED: Booking PostgreSQL target does not match its verified isolated container/network")
	}
}

type dockerContainer struct {
	ID    string `json:"Id"`
	State struct {
		Running bool `json:"Running"`
	} `json:"State"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	HostConfig struct {
		PortBindings map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"PortBindings"`
		PublishAllPorts bool `json:"PublishAllPorts"`
	} `json:"HostConfig"`
	NetworkSettings struct {
		Ports map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
		Networks map[string]struct {
			IPAddress string `json:"IPAddress"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
}

type dockerNetwork struct {
	Name     string            `json:"Name"`
	Internal bool              `json:"Internal"`
	Labels   map[string]string `json:"Labels"`
}

func verifiedBookingTarget(dsn, project, containerID, networkName string, containerJSON, networkJSON []byte) bool {
	var containers []dockerContainer
	var networks []dockerNetwork
	if json.Unmarshal(containerJSON, &containers) != nil || len(containers) != 1 || json.Unmarshal(networkJSON, &networks) != nil || len(networks) != 1 {
		return false
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return false
	}
	c := containers[0]
	if c.ID != containerID || !c.State.Running || c.Config.Labels["com.docker.compose.project"] != project ||
		c.Config.Labels["com.docker.compose.service"] != "ticket-e2e-postgres" || c.Config.Labels["rushgo.ticket_e2e.project"] != project ||
		c.HostConfig.PublishAllPorts {
		return false
	}
	attachment, attached := c.NetworkSettings.Networks[networkName]
	if len(c.NetworkSettings.Networks) != 1 || !attached {
		return false
	}
	network := networks[0]
	if network.Name != networkName || !network.Internal || network.Labels["com.docker.compose.project"] != project ||
		network.Labels["com.docker.compose.network"] != "ticket-e2e" || network.Labels["rushgo.ticket_e2e.project"] != project {
		return false
	}
	if u.Hostname() != "127.0.0.1" {
		if u.Port() != "5432" || attachment.IPAddress != u.Hostname() {
			return false
		}
		return verifiedLoopbackOnlyBinding(c.HostConfig.PortBindings)
	}
	bindings := c.NetworkSettings.Ports["5432/tcp"]
	return u.Port() == "25435" && len(bindings) == 1 && bindings[0].HostIP == "127.0.0.1" && bindings[0].HostPort == "25435" &&
		verifiedLoopbackOnlyBinding(c.HostConfig.PortBindings)
}

func verifiedLoopbackOnlyBinding(bindings map[string][]struct {
	HostIP   string `json:"HostIp"`
	HostPort string `json:"HostPort"`
}) bool {
	if len(bindings) > 1 {
		return false
	}
	for port, entries := range bindings {
		if port != "5432/tcp" || len(entries) != 1 || entries[0].HostIP != "127.0.0.1" || entries[0].HostPort != "25435" {
			return false
		}
	}
	return true
}

type refundOwnerManifest struct {
	ResourceID             string `json:"resource_id"`
	ContainerID            string `json:"container_id"`
	Database               string `json:"database"`
	Host                   string `json:"host"`
	Port                   int    `json:"port"`
	DSNSHA256              string `json:"dsn_sha256"`
	RunID                  string `json:"run_id"`
	OwnerNonce             string `json:"owner_nonce"`
	ServerSystemIdentifier string `json:"server_system_identifier"`
	DatabaseMarker         string `json:"database_marker"`
	ServerVersion          string `json:"server_version"`
	ImageID                string `json:"image_id"`
}

// VerifyRefundComponentPostgres checks the resource helper's immutable owner
// record against both the exact URL and PostgreSQL's live system/database
// markers. The test therefore refuses a merely similar or stale database.
func VerifyRefundComponentPostgres(t testing.TB, dsn, manifestPath string) {
	t.Helper()
	if strings.TrimSpace(manifestPath) == "" {
		t.Fatal("BLOCKED: payment PostgreSQL owner manifest is required")
	}
	var owner refundOwnerManifest
	contents, err := os.ReadFile(manifestPath)
	if err != nil || json.Unmarshal(contents, &owner) != nil {
		t.Fatal("BLOCKED: payment PostgreSQL owner manifest is unreadable")
	}
	parsed := parsePostgresURL(t, dsn, "/"+owner.Database)
	for _, key := range []string{"host", "hostaddr", "port", "database", "dbname", "service", "servicefile", "search_path"} {
		if _, exists := parsed.Query()[key]; exists {
			t.Fatalf("BLOCKED: Payment DSN contains forbidden target override %q", key)
		}
	}
	digest := sha256.Sum256([]byte(dsn))
	if owner.Host != "127.0.0.1" || owner.Port < 1 || owner.Port > 65535 || parsed.Hostname() != owner.Host || parsed.Port() != strconv.Itoa(owner.Port) ||
		!refundDBPattern.MatchString(owner.Database) || !containerPattern.MatchString(owner.ContainerID) || !ownerPattern.MatchString(owner.OwnerNonce) ||
		!runIDPattern.MatchString(owner.RunID) || owner.ResourceID != owner.ContainerID || owner.DSNSHA256 != hex.EncodeToString(digest[:]) ||
		owner.ServerSystemIdentifier == "" || owner.DatabaseMarker != "codex-refund-component:v1:"+owner.ContainerID+":"+owner.RunID+":"+owner.OwnerNonce ||
		!strings.HasPrefix(owner.ServerVersion, "16.") || owner.ImageID == "" {
		t.Fatal("BLOCKED: Payment DSN does not match the exact owned refund-component record")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("BLOCKED: cannot connect to the owned Payment PostgreSQL")
	}
	defer pool.Close()
	var database, systemIdentifier, marker, serverVersion string
	err = pool.QueryRow(ctx, `SELECT current_database(), system_identifier::text,
		(SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database()),
		current_setting('server_version') FROM pg_control_system()`).Scan(&database, &systemIdentifier, &marker, &serverVersion)
	if err != nil || database != owner.Database || systemIdentifier != owner.ServerSystemIdentifier || marker != owner.DatabaseMarker ||
		!strings.HasPrefix(serverVersion, "16.") {
		t.Fatal("BLOCKED: live Payment PostgreSQL identity differs from the owned manifest")
	}
}

// RequireDistinctPostgresEndpoints prevents one PostgreSQL server/database
// from being presented as two independent authorities.
func RequireDistinctPostgresEndpoints(t testing.TB, bookingDSN, paymentDSN string) {
	t.Helper()
	booking := parsePostgresURL(t, bookingDSN, "")
	payment := parsePostgresURL(t, paymentDSN, "")
	if strings.EqualFold(booking.Hostname()+":"+booking.Port(), payment.Hostname()+":"+payment.Port()) {
		t.Fatal("BLOCKED: Booking and Payment must use independent PostgreSQL endpoints")
	}
}

type Schema struct {
	Pool *pgxpool.Pool
	DSN  string
	Name string
	base *pgxpool.Pool
}

// OpenMigrationSchema creates a unique, per-test schema in an already
// verified disposable database, runs the service's real migrations against
// that schema, and drops only that generated schema at test cleanup.
func OpenMigrationSchema(t testing.TB, dsn, migrationPath string, maxConnections int32) Schema {
	t.Helper()
	parsed := parsePostgresURL(t, dsn, "")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	base, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("connect to verified isolated PostgreSQL:", err)
	}
	schema := "admission_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = base.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		base.Close()
		t.Fatal("ensure UUID migration extension in isolated database:", err)
	}
	if _, err = base.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		base.Close()
		t.Fatal("create per-run isolated schema:", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	schemaDSN := parsed.String()
	config, err := pgxpool.ParseConfig(schemaDSN)
	if err != nil {
		dropSchema(t, base, schema)
		base.Close()
		t.Fatal("parse per-run schema DSN:", err)
	}
	if maxConnections < 1 {
		maxConnections = 8
	}
	config.MaxConns = maxConnections
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		dropSchema(t, base, schema)
		base.Close()
		t.Fatal("connect per-run isolated schema:", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropSchema(t, base, schema)
		base.Close()
	})
	command := exec.CommandContext(ctx, "migrate", "-path", migrationPath, "-database", schemaDSN, "up")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run real service migrations: %s (target=%s)", strings.ReplaceAll(string(output), schemaDSN, "[isolated schema DSN]"), RedactedTarget(schemaDSN))
	}
	return Schema{Pool: pool, DSN: schemaDSN, Name: schema, base: base}
}

func dropSchema(t testing.TB, pool *pgxpool.Pool, schema string) {
	t.Helper()
	if !regexp.MustCompile(`^admission_test_[a-f0-9]{32}$`).MatchString(schema) {
		t.Errorf("refusing to drop unexpected generated schema name")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`); err != nil {
		t.Errorf("drop owned admission test schema: %v", err)
	}
}

func parsePostgresURL(t testing.TB, dsn, expectedPath string) *url.URL {
	t.Helper()
	u, err := url.Parse(strings.TrimSpace(dsn))
	if err != nil || u.Scheme != "postgres" || u.Hostname() == "" || u.Port() == "" || u.Path == "" ||
		(expectedPath != "" && u.Path != expectedPath) {
		t.Fatal("BLOCKED: PostgreSQL target URL is absent or outside the expected isolated database")
	}
	return u
}

// RedactedTarget gives failures useful target context without passwords or
// query values that may contain credentials.
func RedactedTarget(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "[invalid PostgreSQL target]"
	}
	u.User = nil
	query := u.Query()
	for key := range query {
		query.Set(key, "[redacted]")
	}
	u.RawQuery = query.Encode()
	return u.Scheme + "://" + u.Host + u.Path
}
