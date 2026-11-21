//go:build recoverycomponents

package repository

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This integration test deliberately creates only disposable schemas inside the
// manifested refund component database. It never runs migrations against the
// component database's public schema or any demo/history database.
func TestRefundProvenanceMigrationFreshUpgradeRepeatAndDisposableDownIntegration(t *testing.T) {
	// Reuse the established owner-manifest gate before creating any additional
	// schema. The returned schema is itself disposable and cleaned by its helper.
	validated := refundComponentDatabase(t)
	_ = validated

	dsn := os.Getenv("REFUND_COMPONENT_DATABASE_URL")
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("BLOCKED: manifested refund database URL is invalid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("BLOCKED: cannot connect to the manifested refund database")
	}
	t.Cleanup(admin.Close)

	migrationPath := filepath.Clean(filepath.Join("..", "..", "migrations"))
	allMigrations, versions := refundProvenanceMigrationFiles(t, migrationPath)
	if len(allMigrations) < 2 {
		t.Fatal("expected the provenance migration after an existing Payment schema")
	}
	latestVersion := versions[len(versions)-1]
	refundMigrationIndex := -1
	for index, name := range allMigrations {
		if strings.Contains(name, "payment_refund_audit") {
			refundMigrationIndex = index
			break
		}
	}
	if refundMigrationIndex <= 0 {
		t.Fatal("cannot locate the refund provenance migration after a prior Payment schema")
	}
	refundVersion := strings.SplitN(allMigrations[refundMigrationIndex], "_", 2)[0]
	refundPreviousVersion := strings.SplitN(allMigrations[refundMigrationIndex-1], "_", 2)[0]
	// Seed the legacy schema through the migration immediately before the
	// refund-audit migration, so the upgrade subtest actually exercises the
	// audit migration rather than only the final additive migration.
	previousFiles := allMigrations[:refundMigrationIndex]
	previousPath := filepath.Join(t.TempDir(), "previous")
	if err := os.MkdirAll(previousPath, 0o700); err != nil {
		t.Fatal("cannot create disposable previous-migration directory")
	}
	for _, name := range previousFiles {
		contents, err := os.ReadFile(filepath.Join(migrationPath, name))
		if err != nil {
			t.Fatal("cannot read prior Payment migration")
		}
		if err := os.WriteFile(filepath.Join(previousPath, name), contents, 0o600); err != nil {
			t.Fatal("cannot copy prior Payment migration into disposable test directory")
		}
	}

	newSchema := func(label string) (string, string) {
		t.Helper()
		schema := "refund_provenance_" + label + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
			t.Fatalf("cannot create disposable %s schema", label)
		}
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
				t.Errorf("cannot drop disposable %s schema", label)
			}
		})
		query := parsed.Query()
		query.Set("search_path", schema+",public")
		copyURL := *parsed
		copyURL.RawQuery = query.Encode()
		return schema, copyURL.String()
	}

	latestPath := filepath.ToSlash(migrationPath)
	migrate := refundProvenanceMigrateBinary(t)

	t.Run("fresh schema and repeat startup preserve migration version", func(t *testing.T) {
		schema, isolatedDSN := newSchema("fresh")
		assertRefundProvenanceUUIDOSSPublic(t, ctx, isolatedDSN, schema)
		runRefundProvenanceMigrate(t, ctx, migrate, latestPath, isolatedDSN, "up")
		assertRefundProvenanceMigrationVersion(t, ctx, isolatedDSN, latestVersion, false)
		assertRefundProvenanceSchema(t, ctx, isolatedDSN)

		// Simulate another application startup against the same disposable schema.
		runRefundProvenanceMigrate(t, ctx, migrate, latestPath, isolatedDSN, "up")
		assertRefundProvenanceMigrationVersion(t, ctx, isolatedDSN, latestVersion, false)
	})

	t.Run("upgrade leaves existing financial row byte-for-byte equivalent as JSON", func(t *testing.T) {
		schema, isolatedDSN := newSchema("upgrade")
		assertRefundProvenanceUUIDOSSPublic(t, ctx, isolatedDSN, schema)
		runRefundProvenanceMigrate(t, ctx, migrate, filepath.ToSlash(previousPath), isolatedDSN, "up")

		id, userID, bookingID := uuid.New(), uuid.New(), uuid.New()
		pool := refundProvenancePoolForSchema(t, ctx, isolatedDSN)
		defer pool.Close()
		if _, err := pool.Exec(ctx, `INSERT INTO payments (id,user_id,booking_id,status,price,currency,payment_intent_id)
			VALUES ($1,$2,$3,'success',725,'usd',$4)`, id, userID, bookingID, "pi_legacy_"+uuid.NewString()); err != nil {
			t.Fatal("cannot seed a disposable pre-upgrade Payment row")
		}
		var before json.RawMessage
		if err := pool.QueryRow(ctx, `SELECT to_jsonb(p) FROM payments p WHERE id=$1`, id).Scan(&before); err != nil {
			t.Fatal("cannot snapshot the disposable pre-upgrade Payment row")
		}

		runRefundProvenanceMigrate(t, ctx, migrate, latestPath, isolatedDSN, "up")
		assertRefundProvenanceMigrationVersion(t, ctx, isolatedDSN, latestVersion, false)
		assertRefundProvenanceSchema(t, ctx, isolatedDSN)
		var after json.RawMessage
		if err := pool.QueryRow(ctx, `SELECT to_jsonb(p) FROM payments p WHERE id=$1`, id).Scan(&after); err != nil {
			t.Fatal("cannot read the disposable Payment row after upgrade")
		}
		if string(before) != string(after) {
			t.Fatalf("additive provenance migration changed existing Payment data: before=%s after=%s", before, after)
		}
	})

	t.Run("down is limited to an empty disposable migration copy", func(t *testing.T) {
		schema, isolatedDSN := newSchema("downcopy")
		assertRefundProvenanceUUIDOSSPublic(t, ctx, isolatedDSN, schema)
		runRefundProvenanceMigrate(t, ctx, migrate, latestPath, isolatedDSN, "up")
		assertRefundProvenanceSchema(t, ctx, isolatedDSN)
		// Later additive Payment migrations exist above the refund-audit
		// migration. Move back to that exact version before testing its down
		// script; assuming it is the latest migration would test a different
		// down script and leave the audit table in place.
		runRefundProvenanceMigrate(t, ctx, migrate, latestPath, isolatedDSN,
			"goto", refundVersion)
		assertRefundProvenanceMigrationVersion(t, ctx, isolatedDSN, refundVersion, false)
		runRefundProvenanceMigrate(t, ctx, migrate, latestPath, isolatedDSN, "down", "1")
		assertRefundProvenanceMigrationVersion(t, ctx, isolatedDSN, refundPreviousVersion, false)
		if refundProvenanceTableExists(t, ctx, isolatedDSN) {
			t.Fatal("down on the disposable empty copy left payment_refund_audit present")
		}
	})
}

func refundProvenanceMigrationFiles(t *testing.T, dir string) ([]string, []string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal("cannot list Payment migration files")
	}
	files := make([]string, 0, len(entries))
	versions := make([]string, 0, len(entries)/2)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		// Migration filenames begin with a numeric version followed by `_`.
		version := strings.SplitN(name, "_", 2)[0]
		files = append(files, name)
		versions = append(versions, version)
	}
	sort.Strings(files)
	sort.Strings(versions)
	return files, versions
}

func refundProvenanceMigrateBinary(t *testing.T) string {
	t.Helper()
	if configured := os.Getenv("REFUND_COMPONENT_MIGRATE_BIN"); configured != "" {
		if info, err := os.Stat(configured); err == nil && !info.IsDir() {
			return configured
		}
		t.Fatal("BLOCKED: REFUND_COMPONENT_MIGRATE_BIN is not an executable file")
	}
	path, err := exec.LookPath("migrate")
	if err != nil {
		t.Fatal("BLOCKED: migrate CLI is unavailable")
	}
	return path
}

func runRefundProvenanceMigrate(t *testing.T, ctx context.Context, binary, source, dsn string, args ...string) {
	t.Helper()
	commandArgs := []string{"-path", source, "-database", dsn}
	commandArgs = append(commandArgs, args...)
	cmd := exec.CommandContext(ctx, binary, commandArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		redacted := strings.ReplaceAll(string(output), dsn, "[isolated DSN]")
		t.Fatalf("isolated Payment migration command failed (%s): %s", strings.Join(args, " "), strings.TrimSpace(redacted))
	}
}

func refundProvenancePoolForSchema(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("cannot connect to disposable migration schema")
	}
	return pool
}

func assertRefundProvenanceMigrationVersion(t *testing.T, ctx context.Context, dsn, want string, dirty bool) {
	t.Helper()
	pool := refundProvenancePoolForSchema(t, ctx, dsn)
	defer pool.Close()
	var got string
	var gotDirty bool
	if err := pool.QueryRow(ctx, `SELECT version::text,dirty FROM schema_migrations`).Scan(&got, &gotDirty); err != nil {
		t.Fatal("cannot inspect disposable Payment migration version")
	}
	if got != want || gotDirty != dirty {
		t.Fatalf("migration version=%s dirty=%v; want %s dirty=%v", got, gotDirty, want, dirty)
	}
}

func assertRefundProvenanceSchema(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	pool := refundProvenancePoolForSchema(t, ctx, dsn)
	defer pool.Close()
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM information_schema.tables
		WHERE table_schema=current_schema() AND table_name='payment_refund_audit')`).Scan(&exists); err != nil || !exists {
		t.Fatal("freshly migrated disposable schema lacks payment_refund_audit")
	}
	var columns []string
	rows, err := pool.Query(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name='payment_refund_audit'`)
	if err != nil {
		t.Fatal("cannot inspect refund audit columns")
	}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			rows.Close()
			t.Fatal("cannot read refund audit column name")
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal("cannot finish refund audit column inspection")
	}
	rows.Close()
	for _, required := range []string{"record_id", "record_kind", "payment_id", "booking_id", "generation", "command_digest", "application_attempt_id", "provider_request_id", "provider_refund_id", "provider_event_id", "payload", "recorded_at"} {
		if !containsRefundProvenance(columns, required) {
			t.Fatalf("audit table is missing required association column %q", required)
		}
	}
	var primaryColumns string
	if err := pool.QueryRow(ctx, `SELECT string_agg(a.attname,',' ORDER BY k.ordinality)
		FROM pg_constraint c
		JOIN unnest(c.conkey) WITH ORDINALITY AS k(attnum,ordinality) ON true
		JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum
		WHERE c.conrelid='payment_refund_audit'::regclass AND c.contype='p'`).Scan(&primaryColumns); err != nil {
		t.Fatal("cannot inspect payment_refund_audit primary key")
	}
	if primaryColumns != "record_id" {
		t.Fatalf("audit primary key columns=%q; want only record_id", primaryColumns)
	}
	var uniqueCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_index i
		WHERE i.indrelid='payment_refund_audit'::regclass AND i.indisunique`).Scan(&uniqueCount); err != nil {
		t.Fatal("cannot inspect audit uniqueness constraints")
	}
	if uniqueCount != 1 {
		t.Fatalf("audit unique-index count=%d; want only the record_id primary key", uniqueCount)
	}
	var usefulIndexes int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes
		WHERE schemaname=current_schema() AND tablename='payment_refund_audit' AND indexdef LIKE 'CREATE INDEX%'`).Scan(&usefulIndexes); err != nil {
		t.Fatal("cannot inspect audit query indexes")
	}
	if usefulIndexes < 8 {
		t.Fatalf("audit secondary-index count=%d; want indexed payment/booking/command/attempt/provider IDs/kind associations", usefulIndexes)
	}
	var hasKindCheck bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM pg_constraint WHERE conrelid='payment_refund_audit'::regclass
		AND contype='c' AND pg_get_constraintdef(oid) ILIKE '%record_kind%')`).Scan(&hasKindCheck); err != nil || !hasKindCheck {
		t.Fatal("audit schema lacks a record-kind check constraint")
	}
}

func assertRefundProvenanceUUIDOSSPublic(t *testing.T, ctx context.Context, dsn, wantSchema string) {
	t.Helper()
	pool := refundProvenancePoolForSchema(t, ctx, dsn)
	defer pool.Close()
	var currentSchema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil || currentSchema != wantSchema {
		t.Fatalf("disposable search_path current_schema=%q err=%v; want %q", currentSchema, err, wantSchema)
	}
	var extensionSchema string
	if err := pool.QueryRow(ctx, `SELECT n.nspname
		FROM pg_catalog.pg_extension e
		JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace
		WHERE e.extname='uuid-ossp'`).Scan(&extensionSchema); err != nil || extensionSchema != "public" {
		t.Fatalf("uuid-ossp extension namespace=%q err=%v; want public for disposable schema %q", extensionSchema, err, wantSchema)
	}
	var generatedID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT uuid_generate_v4()`).Scan(&generatedID); err != nil || generatedID == uuid.Nil {
		t.Fatalf("uuid_generate_v4 unavailable through disposable schema %q search_path: id=%s err=%v", wantSchema, generatedID, err)
	}
}

func containsRefundProvenance(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func refundProvenanceTableExists(t *testing.T, ctx context.Context, dsn string) bool {
	t.Helper()
	connection := refundProvenancePoolForSchema(t, ctx, dsn)
	defer connection.Close()
	var exists bool
	if err := connection.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM information_schema.tables
		WHERE table_schema=current_schema() AND table_name='payment_refund_audit')`).Scan(&exists); err != nil {
		t.Fatal("cannot inspect disposable migration schema after down")
	}
	return exists
}
