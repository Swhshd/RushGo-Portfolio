//go:build reliability_component

package recoverycomponent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errInvalidComponentSchema = errors.New("invalid Saga component schema")

type DatabaseFixture struct {
	Schema string
	URL    string
}

var componentSchemaPattern = regexp.MustCompile(`^saga_component_[0-9a-f]{32}$`)

// CreateDatabaseFixture validates the root-owned database identity before it
// creates one uniquely named schema and applies the repository's migrations.
func CreateDatabaseFixture(t testing.TB) DatabaseFixture {
	t.Helper()
	dsn, manifest := readOwnerConfig(t)
	admin := openOwnerPool(t, dsn, manifest)
	schema := "saga_component_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if !componentSchemaPattern.MatchString(schema) {
		t.Fatalf("generated invalid Saga component schema")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatalf("isolated database extension setup failed")
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("isolated Saga component schema setup failed")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
		if err != nil {
			t.Errorf("isolated Saga component schema cleanup failed")
		}
	})

	fixtureURL := withComponentSchema(dsn, schema)
	migrationPath := filepath.ToSlash(filepath.Join("..", "..", "migrations"))
	migrateBinary := os.Getenv("SAGA_COMPONENT_MIGRATE_BIN")
	if migrateBinary == "" {
		migrateBinary = "migrate"
	}
	cmd := exec.Command(migrateBinary, "-path", migrationPath, "-database", fixtureURL, "up")
	if err := cmd.Run(); err != nil {
		t.Fatalf("repository migrations failed for the owned Saga component schema (connection details redacted)")
	}
	t.Setenv("SAGA_COMPONENT_SCHEMA", schema)
	return DatabaseFixture{Schema: schema, URL: fixtureURL}
}

// OpenComponentPool verifies the owner manifest and PG identity on a fresh
// connection in each process, then opens the schema assigned by the parent test.
func OpenComponentPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn, manifest := readOwnerConfig(t)
	openSchema := os.Getenv("SAGA_COMPONENT_SCHEMA")
	if !componentSchemaPattern.MatchString(openSchema) {
		t.Fatalf("SAGA_COMPONENT_SCHEMA must name an owned fixture schema")
	}
	ownerPool := openOwnerPool(t, dsn, manifest)
	ownerPool.Close()
	fixtureURL := withComponentSchema(dsn, openSchema)
	config, err := pgxpool.ParseConfig(fixtureURL)
	if err != nil {
		t.Fatalf("invalid Saga component database URL")
	}
	config.ConnConfig.RuntimeParams["application_name"] = fmt.Sprintf("saga_component_%d", os.Getpid())
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("could not open owned Saga component database")
	}
	t.Cleanup(pool.Close)
	return pool
}

// OpenIndependentPool creates a separate pool for the observer's committed
// read; it verifies the owner metadata again before connecting.
func OpenIndependentPool(t testing.TB, applicationName string) *pgxpool.Pool {
	t.Helper()
	dsn, manifest := readOwnerConfig(t)
	if _, err := strconv.Atoi(applicationName); err == nil || applicationName == "" {
		t.Fatalf("observer application name must be a descriptive label")
	}
	openSchema := os.Getenv("SAGA_COMPONENT_SCHEMA")
	if !componentSchemaPattern.MatchString(openSchema) {
		t.Fatalf("SAGA_COMPONENT_SCHEMA must name an owned fixture schema")
	}
	ownerPool := openOwnerPool(t, dsn, manifest)
	ownerPool.Close()
	config, err := pgxpool.ParseConfig(withComponentSchema(dsn, openSchema))
	if err != nil {
		t.Fatalf("invalid Saga component observer URL")
	}
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	config.MaxConns = 1
	config.MinConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("could not open independent Saga component observer connection")
	}
	t.Cleanup(pool.Close)
	return pool
}

func readOwnerConfig(t testing.TB) (string, OwnerManifest) {
	t.Helper()
	dsn := os.Getenv("SAGA_COMPONENT_DATABASE_URL")
	manifestPath := os.Getenv("SAGA_COMPONENT_OWNER_MANIFEST")
	if dsn == "" || manifestPath == "" || !filepath.IsAbs(manifestPath) {
		t.Fatalf("SAGA_COMPONENT_DATABASE_URL and absolute SAGA_COMPONENT_OWNER_MANIFEST are required")
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("could not read Saga component owner manifest")
	}
	var manifest OwnerManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("invalid Saga component owner manifest JSON")
	}
	if err := validateOwnerManifest(dsn, manifest); err != nil {
		t.Fatalf("Saga component owner manifest did not bind the supplied database URL")
	}
	if runID := os.Getenv("SAGA_COMPONENT_RUN_ID"); runID != "" && runID != manifest.RunID {
		t.Fatalf("component run ID does not match owner manifest")
	}
	return dsn, manifest
}

func openOwnerPool(t testing.TB, dsn string, manifest OwnerManifest) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("invalid Saga component database URL")
	}
	config.ConnConfig.RuntimeParams["application_name"] = fmt.Sprintf("saga_component_owner_%d", os.Getpid())
	config.MaxConns = 1
	config.MinConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("could not connect to the root-owned Saga component database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var database, marker, systemIdentifier, version string
	err = pool.QueryRow(ctx, `SELECT current_database(), COALESCE(shobj_description(db.oid,'pg_database'),''), control.system_identifier::text, current_setting('server_version_num') FROM pg_database db CROSS JOIN LATERAL pg_control_system() control WHERE db.datname=current_database()`).Scan(&database, &marker, &systemIdentifier, &version)
	if err != nil {
		pool.Close()
		t.Fatalf("could not verify Saga component PostgreSQL identity: %v", err)
	}
	serverVersion, err := strconv.Atoi(version)
	if err != nil || serverVersion < 160000 || serverVersion >= 170000 || database != manifest.Database ||
		systemIdentifier != manifest.ServerSystemIdentifier || marker != manifest.DatabaseMarker {
		pool.Close()
		t.Fatalf("connected PostgreSQL identity does not match the owner manifest")
	}
	t.Cleanup(pool.Close)
	return pool
}

func withComponentSchema(dsn, schema string) string {
	parsed, _ := url.Parse(dsn)
	query := parsed.Query()
	query.Set("search_path", schema+",public")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
