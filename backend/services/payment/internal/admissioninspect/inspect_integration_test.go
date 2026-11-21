package admissioninspect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestInspectAgainstIsolatedPostgres uses only disposable schemas in explicitly
// configured isolated databases. It never applies RushGo migrations or touches
// the database's application schema. Admin DSNs are used only to create and
// drop fixtures; the inspector always connects with the corresponding RO DSN.
func TestInspectAgainstIsolatedPostgres(t *testing.T) {
	paymentAdminDSN := os.Getenv("ADMISSION_INSPECT_PAYMENT_TEST_ADMIN_DSN")
	paymentDSN := os.Getenv("ADMISSION_INSPECT_PAYMENT_TEST_READONLY_DSN")
	bookingAdminDSN := os.Getenv("ADMISSION_INSPECT_BOOKING_TEST_ADMIN_DSN")
	bookingDSN := os.Getenv("ADMISSION_INSPECT_BOOKING_TEST_READONLY_DSN")
	if paymentAdminDSN == "" || paymentDSN == "" || bookingAdminDSN == "" || bookingDSN == "" {
		t.Skip("isolated Payment/Booking admin and read-only test DSNs are not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	paymentFixture := provisionFixture(t, ctx, paymentAdminDSN, paymentDSN, "payment")
	bookingFixture := provisionFixture(t, ctx, bookingAdminDSN, bookingDSN, "booking")
	paymentBefore := fixtureSnapshot(t, ctx, paymentFixture.adminDSN, paymentFixture.schema,
		"schema_migrations", "payment_admission_mode", "payment_admission_permits", "payments")
	bookingBefore := fixtureSnapshot(t, ctx, bookingFixture.adminDSN, bookingFixture.schema,
		"schema_migrations", "booking_admission_mode", "ticket_admission_intents")

	report := Inspect(ctx, Options{
		Source:           SourceBoth,
		PaymentDSN:       paymentFixture.dsn,
		BookingDSN:       bookingFixture.dsn,
		SourceVersion:    "integration-test-build",
		TotalTimeout:     10 * time.Second,
		StatementTimeout: 2 * time.Second,
	})
	if report.Payment == nil || report.Booking == nil {
		t.Fatal("inspection omitted a selected database")
	}
	if report.Payment.Status != "available" || report.Booking.Status != "available" {
		t.Fatal("inspection did not complete both isolated database snapshots")
	}
	if report.CrossDatabaseAtomicSnapshot {
		t.Fatal("independent database observations were reported as an atomic snapshot")
	}
	if report.Payment.ObservedAt == nil || report.Booking.ObservedAt == nil {
		t.Fatal("database observations did not retain independent observation timestamps")
	}
	if report.Payment.Identity == nil || report.Booking.Identity == nil ||
		report.Payment.Identity.DatabaseName != databaseName(t, paymentDSN) ||
		report.Booking.Identity.DatabaseName != databaseName(t, bookingDSN) {
		t.Fatal("inspection did not report the database identities")
	}
	if report.Payment.Admission == nil || report.Booking.Admission == nil {
		t.Fatal("inspection omitted admission coordination facts")
	}
	assertAvailableCount(t, report.Payment.Admission.PendingCount, 2)
	assertAvailableCount(t, report.Payment.Admission.TerminalCount, 2)
	assertAvailableCount(t, report.Payment.Admission.UnknownStateCount, 1)
	assertAvailableCount(t, report.Payment.Admission.RecoveryDueCount, 1)
	if report.Payment.Admission.OldestPendingAgeSeconds == nil || *report.Payment.Admission.OldestPendingAgeSeconds < 50 {
		t.Fatal("inspection did not report the oldest pending permit age")
	}
	if report.Payment.Mode.Active == nil || !*report.Payment.Mode.Active {
		t.Fatal("Payment admission mode was not reported as active")
	}
	if report.Payment.Migration.Version == nil || *report.Payment.Migration.Version != paymentAdmissionMigrationVersion {
		t.Fatal("Payment migration version was not reported")
	}
	if report.Payment.RefundQueueCandidatesWithPendingAdmission == nil {
		t.Fatal("inspection omitted refund candidate count")
	}
	assertAvailableCount(t, *report.Payment.RefundQueueCandidatesWithPendingAdmission, 1)
	assertAvailableCount(t, report.Booking.Admission.PendingCount, 1)
	assertAvailableCount(t, report.Booking.Admission.TerminalCount, 2)
	assertAvailableCount(t, report.Booking.Admission.UnknownStateCount, 1)
	if report.Booking.Admission.RecoveryDueCount.Status != "not_persisted" || report.Booking.Admission.RecoveryDueCount.Value != nil {
		t.Fatal("Booking recovery due was invented despite no persisted due field")
	}

	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal("inspection report could not be encoded")
	}
	output := string(encoded)
	for _, secret := range []string{paymentDSN, bookingDSN, "private-sentinel@example.com", "private-sentinel-"} {
		if strings.Contains(output, secret) {
			t.Fatal("inspection output exposed a DSN or private fixture value")
		}
	}

	if after := fixtureSnapshot(t, ctx, paymentFixture.adminDSN, paymentFixture.schema,
		"schema_migrations", "payment_admission_mode", "payment_admission_permits", "payments"); after != paymentBefore {
		t.Fatal("Payment fixture changed during the read-only inspection")
	}
	if after := fixtureSnapshot(t, ctx, bookingFixture.adminDSN, bookingFixture.schema,
		"schema_migrations", "booking_admission_mode", "ticket_admission_intents"); after != bookingBefore {
		t.Fatal("Booking fixture changed during the read-only inspection")
	}

	assertReadOnlyRoleCannotWrite(t, ctx, paymentFixture)
	assertEmptyDatabaseFacts(t, ctx, paymentFixture, bookingFixture)
	assertPermissionFailureIsNotZero(t, ctx, paymentFixture)
}

type isolatedFixture struct {
	dsn      string
	adminDSN string
	schema   string
}

func provisionFixture(t *testing.T, ctx context.Context, adminDSN, readonlyDSN, service string) isolatedFixture {
	t.Helper()
	if databaseName(t, adminDSN) != databaseName(t, readonlyDSN) ||
		databaseUser(t, adminDSN) == databaseUser(t, readonlyDSN) {
		t.Fatal("fixture and read-only DSNs must use separate roles in the same isolated database")
	}
	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatal("cannot connect to the explicitly configured isolated test database")
	}
	t.Cleanup(admin.Close)
	if err := admin.Ping(ctx); err != nil {
		t.Fatal("cannot reach the explicitly configured isolated test database")
	}

	schema := "admission_inspect_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	qualifiedSchema := `"` + schema + `"`
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+qualifiedSchema); err != nil {
		t.Fatal("cannot create a disposable test schema")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+qualifiedSchema+" CASCADE"); err != nil {
			t.Error("cannot drop the disposable test schema")
		}
	})

	isolatedDSN := dsnWithSearchPath(t, readonlyDSN, schema)
	if service == "payment" {
		createPaymentFixture(t, ctx, admin, qualifiedSchema)
	} else {
		createBookingFixture(t, ctx, admin, qualifiedSchema)
	}
	readOnlyRole := pgx.Identifier{databaseUser(t, readonlyDSN)}.Sanitize()
	execFixtureStatements(t, ctx, admin,
		"GRANT USAGE ON SCHEMA "+qualifiedSchema+" TO "+readOnlyRole,
		"GRANT SELECT ON ALL TABLES IN SCHEMA "+qualifiedSchema+" TO "+readOnlyRole)
	return isolatedFixture{dsn: isolatedDSN, adminDSN: adminDSN, schema: schema}
}

func createPaymentFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string) {
	t.Helper()
	statements := []string{
		`CREATE TABLE ` + schema + `.schema_migrations (version BIGINT NOT NULL, dirty BOOLEAN NOT NULL)`,
		`INSERT INTO ` + schema + `.schema_migrations VALUES (` + intString(paymentAdmissionMigrationVersion) + `, FALSE)`,
		`CREATE TABLE ` + schema + `.payment_admission_mode (singleton BOOLEAN PRIMARY KEY, active BOOLEAN NOT NULL)`,
		`INSERT INTO ` + schema + `.payment_admission_mode VALUES (TRUE, TRUE)`,
		`CREATE TABLE ` + schema + `.payment_admission_permits (
 request_id UUID PRIMARY KEY, payment_id UUID NOT NULL, state TEXT NOT NULL,
 next_attempt_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL, last_error TEXT
);`,
		`CREATE TABLE ` + schema + `.payments (
 id UUID PRIMARY KEY, managed BOOLEAN NOT NULL, refund_state TEXT NOT NULL,
 due_at TIMESTAMPTZ NOT NULL, lease_until TIMESTAMPTZ
);`,
	}
	execFixtureStatements(t, ctx, pool, statements...)
	execFixtureStatements(t, ctx, pool, `
INSERT INTO `+schema+`.payments(id,managed,refund_state,due_at,lease_until) VALUES
 ('00000000-0000-4000-8000-000000000001',TRUE,'pending',clock_timestamp()-INTERVAL '1 minute',NULL),
 ('00000000-0000-4000-8000-000000000002',TRUE,'pending',clock_timestamp()-INTERVAL '1 minute',clock_timestamp()+INTERVAL '1 minute')`, `
INSERT INTO `+schema+`.payment_admission_permits(request_id,payment_id,state,next_attempt_at,created_at,last_error) VALUES
 ('00000000-0000-4000-8000-000000000101','00000000-0000-4000-8000-000000000001','PENDING',clock_timestamp()-INTERVAL '1 second',clock_timestamp()-INTERVAL '2 minutes','booking_intent_pending'),
 ('00000000-0000-4000-8000-000000000102','00000000-0000-4000-8000-000000000002','PENDING',clock_timestamp()+INTERVAL '1 minute',clock_timestamp()-INTERVAL '20 seconds','private-sentinel@example.com'),
 ('00000000-0000-4000-8000-000000000104','00000000-0000-4000-8000-000000000001','COMMITTED',clock_timestamp(),clock_timestamp()-INTERVAL '3 minutes',NULL),
 ('00000000-0000-4000-8000-000000000105','00000000-0000-4000-8000-000000000001','ABORTED',clock_timestamp(),clock_timestamp()-INTERVAL '4 minutes',NULL),
 ('00000000-0000-4000-8000-000000000106','00000000-0000-4000-8000-000000000001','REVIEWING',clock_timestamp(),clock_timestamp()-INTERVAL '5 minutes',NULL)`)
}

func createBookingFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string) {
	t.Helper()
	statements := []string{
		`CREATE TABLE ` + schema + `.schema_migrations (version BIGINT NOT NULL, dirty BOOLEAN NOT NULL)`,
		`INSERT INTO ` + schema + `.schema_migrations VALUES (` + intString(bookingAdmissionMigrationVersion) + `, FALSE)`,
		`CREATE TABLE ` + schema + `.booking_admission_mode (singleton BOOLEAN PRIMARY KEY, active BOOLEAN NOT NULL)`,
		`INSERT INTO ` + schema + `.booking_admission_mode VALUES (TRUE, TRUE)`,
		`CREATE TABLE ` + schema + `.ticket_admission_intents (
 request_id UUID PRIMARY KEY, state TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
);`,
	}
	execFixtureStatements(t, ctx, pool, statements...)
	execFixtureStatements(t, ctx, pool, `
INSERT INTO `+schema+`.ticket_admission_intents(request_id,state,created_at,updated_at) VALUES
 ('00000000-0000-4000-8000-000000000201','PENDING',clock_timestamp()-INTERVAL '2 minutes',clock_timestamp()-INTERVAL '1 minute'),
 ('00000000-0000-4000-8000-000000000202','COMMITTED',clock_timestamp()-INTERVAL '3 minutes',clock_timestamp()-INTERVAL '3 minutes'),
 ('00000000-0000-4000-8000-000000000203','ABORTED',clock_timestamp()-INTERVAL '4 minutes',clock_timestamp()-INTERVAL '4 minutes'),
 ('00000000-0000-4000-8000-000000000204','REVIEWING',clock_timestamp()-INTERVAL '5 minutes',clock_timestamp()-INTERVAL '5 minutes')`)
}

func execFixtureStatements(t *testing.T, ctx context.Context, pool *pgxpool.Pool, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal("cannot prepare isolated PostgreSQL fixture")
		}
	}
}

func fixtureSnapshot(t *testing.T, ctx context.Context, adminDSN, schema string, tables ...string) string {
	t.Helper()
	dsn := dsnWithSearchPath(t, adminDSN, schema)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("cannot inspect disposable fixture")
	}
	defer pool.Close()
	qualifiedSchema := `"` + schema + `"`
	var snapshots []string
	for _, table := range tables {
		query := "SELECT COALESCE(jsonb_agg(to_jsonb(row_value) ORDER BY to_jsonb(row_value)::text)::text,'[]') FROM " + qualifiedSchema + "." + table + " AS row_value"
		var snapshot string
		if err := pool.QueryRow(ctx, query).Scan(&snapshot); err != nil {
			t.Fatal("cannot snapshot disposable fixture")
		}
		snapshots = append(snapshots, table+":"+snapshot)
	}
	return strings.Join(snapshots, "\n")
}

func assertReadOnlyRoleCannotWrite(t *testing.T, ctx context.Context, fixture isolatedFixture) {
	t.Helper()
	before := fixtureSnapshot(t, ctx, fixture.adminDSN, fixture.schema, "payment_admission_mode")
	pool, err := pgxpool.New(ctx, fixture.dsn)
	if err != nil {
		t.Fatal("cannot connect with the read-only fixture role")
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `UPDATE payment_admission_mode SET active=FALSE WHERE singleton=TRUE`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.SQLState() != "42501" {
		t.Fatal("read-only fixture role could write the admission mode table")
	}
	if after := fixtureSnapshot(t, ctx, fixture.adminDSN, fixture.schema, "payment_admission_mode"); after != before {
		t.Fatal("denied read-only role write changed the fixture")
	}
}

func assertEmptyDatabaseFacts(t *testing.T, ctx context.Context, payment, booking isolatedFixture) {
	t.Helper()
	paymentAdmin, err := pgxpool.New(ctx, dsnWithSearchPath(t, payment.adminDSN, payment.schema))
	if err != nil {
		t.Fatal("cannot connect to Payment fixture owner")
	}
	defer paymentAdmin.Close()
	if _, err := paymentAdmin.Exec(ctx, `DELETE FROM payment_admission_permits`); err != nil {
		t.Fatal("cannot empty Payment admission fixture")
	}
	bookingAdmin, err := pgxpool.New(ctx, dsnWithSearchPath(t, booking.adminDSN, booking.schema))
	if err != nil {
		t.Fatal("cannot connect to Booking fixture owner")
	}
	defer bookingAdmin.Close()
	if _, err := bookingAdmin.Exec(ctx, `DELETE FROM ticket_admission_intents`); err != nil {
		t.Fatal("cannot empty Booking admission fixture")
	}

	paymentBefore := fixtureSnapshot(t, ctx, payment.adminDSN, payment.schema,
		"schema_migrations", "payment_admission_mode", "payment_admission_permits", "payments")
	bookingBefore := fixtureSnapshot(t, ctx, booking.adminDSN, booking.schema,
		"schema_migrations", "booking_admission_mode", "ticket_admission_intents")
	report := Inspect(ctx, Options{
		Source:           SourceBoth,
		PaymentDSN:       payment.dsn,
		BookingDSN:       booking.dsn,
		SourceVersion:    "integration-test-build",
		TotalTimeout:     10 * time.Second,
		StatementTimeout: 2 * time.Second,
	})
	if report.Payment == nil || report.Booking == nil || report.Payment.Status != "available" || report.Booking.Status != "available" {
		t.Fatal("empty fixture inspection did not complete")
	}
	if report.Payment.Admission == nil || report.Booking.Admission == nil {
		t.Fatal("empty fixture inspection omitted admission metrics")
	}
	for _, metric := range []CountMetric{
		report.Payment.Admission.PendingCount,
		report.Payment.Admission.TerminalCount,
		report.Payment.Admission.UnknownStateCount,
		report.Payment.Admission.RecoveryDueCount,
		report.Booking.Admission.PendingCount,
		report.Booking.Admission.TerminalCount,
		report.Booking.Admission.UnknownStateCount,
	} {
		assertAvailableCount(t, metric, 0)
	}
	if report.Payment.Admission.OldestPendingCreatedAt != nil || report.Booking.Admission.OldestPendingCreatedAt != nil {
		t.Fatal("empty fixture reported an oldest pending timestamp")
	}
	if report.Payment.RefundQueueCandidatesWithPendingAdmission == nil {
		t.Fatal("empty fixture omitted refund candidate count")
	}
	assertAvailableCount(t, *report.Payment.RefundQueueCandidatesWithPendingAdmission, 0)
	if report.Booking.Admission.RecoveryDueCount.Status != "not_persisted" || report.Booking.Admission.RecoveryDueCount.Value != nil {
		t.Fatal("Booking recovery due became a numeric zero despite having no persisted due field")
	}
	if after := fixtureSnapshot(t, ctx, payment.adminDSN, payment.schema,
		"schema_migrations", "payment_admission_mode", "payment_admission_permits", "payments"); after != paymentBefore {
		t.Fatal("Payment fixture changed during empty read-only inspection")
	}
	if after := fixtureSnapshot(t, ctx, booking.adminDSN, booking.schema,
		"schema_migrations", "booking_admission_mode", "ticket_admission_intents"); after != bookingBefore {
		t.Fatal("Booking fixture changed during empty read-only inspection")
	}
}

func assertPermissionFailureIsNotZero(t *testing.T, ctx context.Context, payment isolatedFixture) {
	t.Helper()
	admin, err := pgxpool.New(ctx, payment.adminDSN)
	if err != nil {
		t.Fatal("cannot connect to Payment fixture owner")
	}
	defer admin.Close()
	role := pgx.Identifier{databaseUser(t, payment.dsn)}.Sanitize()
	qualifiedSchema := `"` + payment.schema + `"`
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON "+qualifiedSchema+`.payment_admission_permits FROM `+role); err != nil {
		t.Fatal("cannot prepare the permission-denied fixture")
	}
	before := fixtureSnapshot(t, ctx, payment.adminDSN, payment.schema,
		"schema_migrations", "payment_admission_mode", "payment_admission_permits", "payments")

	report := Inspect(ctx, Options{
		Source:           SourcePayment,
		PaymentDSN:       payment.dsn,
		SourceVersion:    "integration-test-build",
		TotalTimeout:     10 * time.Second,
		StatementTimeout: 2 * time.Second,
	})
	if report.Payment == nil || report.Payment.Status != "partial" ||
		report.Payment.FailurePhase != "payment_admission_aggregate" || report.Payment.FailureCategory != "permission_denied" {
		t.Fatal("permission failure was not reported with its safe category")
	}
	if report.Payment.Admission == nil || report.Payment.Admission.PendingCount.Status != "unavailable" || report.Payment.Admission.PendingCount.Value != nil {
		t.Fatal("permission failure was represented as an admission count")
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal("permission failure report could not be encoded")
	}
	if strings.Contains(string(encoded), payment.dsn) || strings.Contains(string(encoded), "permission denied for table") {
		t.Fatal("permission failure output exposed a DSN or raw database error")
	}
	if after := fixtureSnapshot(t, ctx, payment.adminDSN, payment.schema,
		"schema_migrations", "payment_admission_mode", "payment_admission_permits", "payments"); after != before {
		t.Fatal("permission failure inspection changed the Payment fixture")
	}
}

func dsnWithSearchPath(t *testing.T, raw, schema string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatal("isolated integration DSNs must use a PostgreSQL URI")
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func databaseName(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal("isolated integration DSN is not a valid PostgreSQL URI")
	}
	return strings.TrimPrefix(parsed.Path, "/")
}

func databaseUser(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User == nil {
		t.Fatal("test DSN must specify a database role")
	}
	return parsed.User.Username()
}

func assertAvailableCount(t *testing.T, metric CountMetric, want int64) {
	t.Helper()
	if metric.Status != "available" || metric.Value == nil || *metric.Value != want {
		t.Fatalf("count metric = %+v, want available value %d", metric, want)
	}
}

func intString(value int64) string {
	return fmt.Sprintf("%d", value)
}
