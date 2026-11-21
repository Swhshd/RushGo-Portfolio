//go:build recoverycomponents

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
)

const (
	refundExplainAdminDSNEnv   = "REFUND_COMPONENT_DATABASE_URL"
	refundExplainOwnerEnv      = "REFUND_COMPONENT_OWNER_MANIFEST"
	refundExplainEvidenceEnv   = "REFUND_EXPLAIN_EVIDENCE_DIR"
	refundExplainGoBinaryEnv   = "REFUND_EXPLAIN_GO_BIN"
	refundExplainMigrateEnv    = "REFUND_COMPONENT_MIGRATE_BIN"
	refundExplainMaxTestWindow = 10 * time.Minute
)

type refundExplainIntegrationOwnerManifest struct {
	ManifestVersion        int    `json:"manifest_version"`
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

type refundExplainTableSnapshot struct {
	Rows      int    `json:"rows"`
	SHA256Hex string `json:"sha256"`
}

type refundExplainReadOnlyProof struct {
	RoleName                    string `json:"role_name"`
	RoleCanLogin                bool   `json:"role_can_login"`
	RoleIsSuperuser             bool   `json:"role_is_superuser"`
	RoleCanCreateDatabase       bool   `json:"role_can_create_database"`
	RoleCanCreateRole           bool   `json:"role_can_create_role"`
	RoleCanReplicate            bool   `json:"role_can_replicate"`
	RoleCanBypassRLS            bool   `json:"role_can_bypass_rls"`
	PublicSchemaUsage           bool   `json:"public_schema_usage"`
	AuditSelect                 bool   `json:"audit_select"`
	AuditInsert                 bool   `json:"audit_insert"`
	AuditUpdate                 bool   `json:"audit_update"`
	AuditDelete                 bool   `json:"audit_delete"`
	AuditTruncate               bool   `json:"audit_truncate"`
	AuditReferences             bool   `json:"audit_references"`
	AuditTrigger                bool   `json:"audit_trigger"`
	PaymentsSelect              bool   `json:"payments_select"`
	PaymentsInsert              bool   `json:"payments_insert"`
	PaymentsUpdate              bool   `json:"payments_update"`
	PaymentsDelete              bool   `json:"payments_delete"`
	PGControlSystemExecute      bool   `json:"pg_control_system_execute"`
	EndpointHostLoopback        bool   `json:"endpoint_host_loopback"`
	ConnectionFallbacks         int    `json:"connection_fallbacks"`
	CurrentUser                 string `json:"current_user"`
	DefaultTransactionReadOnly  string `json:"default_transaction_read_only"`
	ObservedTransactionReadOnly string `json:"observed_transaction_read_only"`
}

type refundExplainCLICaseEvidence struct {
	Name             string   `json:"name"`
	RoleName         string   `json:"database_role"`
	Argv             []string `json:"sanitized_argv"`
	ExpectedExitCode int      `json:"expected_exit_code"`
	ActualExitCode   int      `json:"actual_exit_code"`
	ExitCodeMatches  bool     `json:"exit_code_matches"`
	StdoutFile       string   `json:"stdout_file"`
	StdoutSHA256     string   `json:"stdout_sha256"`
	StderrFile       string   `json:"stderr_file"`
	StderrSHA256     string   `json:"stderr_sha256"`
	JSONDecoded      bool     `json:"json_decoded"`
}

type refundExplainRealCLIEvidence struct {
	SchemaVersion            int                                   `json:"schema_version"`
	Scope                    string                                `json:"scope"`
	TestSelector             string                                `json:"test_selector"`
	StartedAtUTC             string                                `json:"started_at_utc"`
	FinishedAtUTC            string                                `json:"finished_at_utc,omitempty"`
	TestStatus               string                                `json:"test_status"`
	SourceRevision           string                                `json:"source_revision,omitempty"`
	SourceFilesSHA256        map[string]string                     `json:"source_files_sha256"`
	MigrationFilesSHA256     map[string]string                     `json:"payment_migration_files_sha256"`
	BinaryFilename           string                                `json:"binary_filename"`
	BinarySizeBytes          int64                                 `json:"binary_size_bytes"`
	BinarySHA256             string                                `json:"binary_sha256"`
	GoVersion                string                                `json:"go_version,omitempty"`
	BuildArgv                []string                              `json:"sanitized_build_argv"`
	BuildExitCode            int                                   `json:"build_exit_code"`
	BuildStdoutFile          string                                `json:"build_stdout_file"`
	BuildStderrFile          string                                `json:"build_stderr_file"`
	MigrationCommand         []string                              `json:"sanitized_migration_argv"`
	MigrationExitCode        int                                   `json:"migration_exit_code"`
	MigrationLogFile         string                                `json:"migration_log_file"`
	OwnerManifestValidated   bool                                  `json:"owner_manifest_validated"`
	DatabaseSchema           string                                `json:"database_schema"`
	DatabaseFreshBeforeSetup bool                                  `json:"database_fresh_before_migrations"`
	CLIEnvironmentNames      []string                              `json:"cli_child_environment_names"`
	ForbiddenEnvironment     []string                              `json:"forbidden_environment_absent"`
	ReadOnlyRole             refundExplainReadOnlyProof            `json:"read_only_role_proof"`
	NoSelectRole             refundExplainReadOnlyProof            `json:"no_select_role_proof"`
	FixtureConflictAppend    string                                `json:"conflicting_response_append"`
	RepeatedEventAppend      string                                `json:"identical_event_replay_append"`
	SnapshotBeforeCLI        map[string]refundExplainTableSnapshot `json:"business_snapshot_before_cli"`
	SnapshotAfterCLI         map[string]refundExplainTableSnapshot `json:"business_snapshot_after_cli"`
	SnapshotUnchanged        bool                                  `json:"business_snapshot_unchanged"`
	CLIInvocations           []refundExplainCLICaseEvidence        `json:"cli_invocations"`
}

type refundExplainTestRole struct {
	name         string
	password     string
	dsn          string
	manifestPath string
}

type refundExplainCaseRunner func(
	name string,
	role refundExplainTestRole,
	argv []string,
	expectedExit int,
) (explanation, int, bool)

func TestRefundExplainRealPostgresCLIIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), refundExplainMaxTestWindow)
	defer cancel()

	evidenceRoot, err := requiredEvidenceRoot()
	if err != nil {
		t.Fatal(err)
	}
	repoRoot, err := refundExplainRepositoryRoot()
	if err != nil {
		t.Fatal("cannot identify the refund-explain source tree")
	}
	if refundExplainPathIsInside(repoRoot, evidenceRoot) {
		t.Fatal("BLOCKED: evidence must be saved outside the source tree")
	}
	evidenceDir, err := os.MkdirTemp(evidenceRoot, "refund-explain-cli-")
	if err != nil {
		t.Fatal("cannot create a fresh external refund-explain evidence directory")
	}
	_ = os.Chmod(evidenceDir, 0o700)
	privateDir, err := os.MkdirTemp("", "refund-explain-private-")
	if err != nil {
		t.Fatal("cannot create private refund-explain fixture material")
	}
	_ = os.Chmod(privateDir, 0o700)
	if refundExplainPathIsInside(repoRoot, privateDir) {
		t.Fatal("BLOCKED: private credential fixtures must remain outside the source tree")
	}
	defer os.RemoveAll(privateDir)

	evidence := refundExplainRealCLIEvidence{
		SchemaVersion:        1,
		Scope:                "SYNTHETIC_REFUND_EXPLAIN_REAL_POSTGRES_CLI",
		TestSelector:         "^TestRefundExplainRealPostgresCLIIntegration$",
		StartedAtUTC:         time.Now().UTC().Format(time.RFC3339Nano),
		TestStatus:           "FAIL",
		BuildExitCode:        -1,
		MigrationExitCode:    -1,
		SourceFilesSHA256:    map[string]string{},
		MigrationFilesSHA256: map[string]string{},
		SnapshotBeforeCLI:    map[string]refundExplainTableSnapshot{},
		SnapshotAfterCLI:     map[string]refundExplainTableSnapshot{},
		CLIInvocations:       []refundExplainCLICaseEvidence{},
	}
	var secrets []string
	evidenceFile := filepath.Join(evidenceDir, "refund-explain-cli-evidence.json")
	t.Cleanup(func() {
		evidence.FinishedAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if t.Failed() {
			evidence.TestStatus = "FAIL"
		} else {
			evidence.TestStatus = "PASS"
		}
		content, marshalErr := json.MarshalIndent(evidence, "", "  ")
		if marshalErr != nil {
			t.Errorf("cannot encode refund-explain evidence")
			return
		}
		if writeErr := writeEvidenceFile(evidenceFile, append(content, '\n'), false); writeErr != nil {
			t.Errorf("cannot save refund-explain evidence")
			return
		}
		t.Logf("refund-explain evidence saved: %s", evidenceFile)
	})

	if err := rejectAmbientPostgresOverrides(); err != nil {
		t.Fatal(err)
	}
	adminDSN := os.Getenv(refundExplainAdminDSNEnv)
	ownerPath := os.Getenv(refundExplainOwnerEnv)
	if adminDSN == "" || ownerPath == "" {
		t.Fatalf("BLOCKED: %s and %s are required", refundExplainAdminDSNEnv, refundExplainOwnerEnv)
	}
	if refundExplainPathIsInside(repoRoot, ownerPath) {
		t.Fatal("BLOCKED: owner manifest must come from the external owned-resource evidence directory")
	}
	secrets = append(secrets, adminDSN, ownerPath, privateDir)
	if err := writePrivateFixture(privateDir, "admin-dsn.txt", []byte(adminDSN)); err != nil {
		t.Fatal("cannot save private owner DSN fixture")
	}
	rawOwnerManifest, err := os.ReadFile(ownerPath)
	if err != nil {
		t.Fatal("cannot read the owner manifest")
	}
	if err := writePrivateFixture(privateDir, "admin-owner.json", rawOwnerManifest); err != nil {
		t.Fatal("cannot save private owner manifest fixture")
	}
	ownerManifest, err := decodeRefundExplainOwnerManifest(rawOwnerManifest)
	if err != nil {
		t.Fatal("owner manifest has an invalid shape")
	}
	ownerAdmin, err := repository.OpenRefundComponentDatabase(ctx, adminDSN, ownerPath)
	if err != nil {
		t.Fatal("BLOCKED: owner-admin DSN failed the manifested loopback PostgreSQL identity gate")
	}
	evidence.OwnerManifestValidated = true
	defer ownerAdmin.Close()

	var currentSchema string
	if err := ownerAdmin.QueryRow(ctx, "SELECT current_schema()").Scan(&currentSchema); err != nil {
		t.Fatal("cannot identify the migration target schema")
	}
	evidence.DatabaseSchema = currentSchema
	if currentSchema != "public" {
		t.Fatal("BLOCKED: refund-explain integration requires the fresh database public schema")
	}
	var paymentTablesPresent bool
	if err := ownerAdmin.QueryRow(ctx, "SELECT to_regclass('public.payments') IS NOT NULL OR to_regclass('public.payment_refund_audit') IS NOT NULL").Scan(&paymentTablesPresent); err != nil {
		t.Fatal("cannot establish whether the manifested Payment database is fresh")
	}
	if paymentTablesPresent {
		t.Fatal("BLOCKED: Payment tables already exist; refusing to migrate a non-fresh database")
	}
	evidence.DatabaseFreshBeforeSetup = true

	fixtures, err := refundExplainFixtureIDs()
	if err != nil {
		t.Fatal("cannot allocate synthetic fixture identifiers")
	}
	evidence.MigrationFilesSHA256, err = refundExplainMigrationHashes(filepath.Join(repoRoot, "backend", "services", "payment", "migrations"))
	if err != nil {
		t.Fatal("cannot hash the Payment migration sources")
	}
	migrateBinary, err := refundExplainMigrationBinary()
	if err != nil {
		t.Fatal(err)
	}
	migrationArgs := []string{
		"-path", filepath.ToSlash(filepath.Join(repoRoot, "backend", "services", "payment", "migrations")),
		"-database", adminDSN, "up",
	}
	migrationStdout, migrationStderr, migrationExit, err := runRefundExplainSetupCommand(ctx, migrateBinary, migrationArgs, refundExplainSetupEnvironment())
	if err != nil {
		t.Fatal("BLOCKED: candidate Payment migrations could not be run against the fresh manifested database")
	}
	evidence.MigrationCommand = []string{
		filepath.Base(migrateBinary), "-path", "backend/services/payment/migrations",
		"-database", "[OWNER_ADMIN_DSN]", "up",
	}
	evidence.MigrationExitCode = migrationExit
	migrationLog := sanitizeRefundExplainText(migrationStdout+migrationStderr, secrets...)
	if err := writeEvidenceFile(filepath.Join(evidenceDir, "payment-migrations.log"), []byte(migrationLog), true); err != nil {
		t.Fatal("cannot save sanitized migration output")
	}
	evidence.MigrationLogFile = "payment-migrations.log"
	if migrationExit != 0 {
		t.Fatalf("candidate Payment migrations exited %d", migrationExit)
	}

	var auditTablePresent bool
	if err := ownerAdmin.QueryRow(ctx, "SELECT to_regclass('public.payment_refund_audit') IS NOT NULL").Scan(&auditTablePresent); err != nil || !auditTablePresent {
		t.Fatal("candidate migration did not create payment_refund_audit in public")
	}
	var finalSchema string
	if err := ownerAdmin.QueryRow(ctx, "SELECT current_schema()").Scan(&finalSchema); err != nil || finalSchema != "public" {
		t.Fatal("candidate migration did not remain in the public schema")
	}
	if err := refundExplainSeedFixtures(t, ctx, ownerAdmin, fixtures); err != nil {
		t.Fatal("cannot seed synthetic refund-explain fixtures")
	}
	evidence.FixtureConflictAppend = "REJECTED_AS_EXPECTED"
	evidence.RepeatedEventAppend = "IDEMPOTENT_AS_EXPECTED"
	boundedWindowOrder, err := refundExplainBoundedWindowOrder(ctx, ownerAdmin, fixtures.bounded)
	if err != nil {
		t.Fatal("cannot capture the database order for the bounded-window fixture")
	}
	if len(boundedWindowOrder) != refundExplainProbeLimit {
		t.Fatalf("bounded-window fixture has %d rows, want %d", len(boundedWindowOrder), refundExplainProbeLimit)
	}

	readOnlyRole, err := refundExplainCreateRole(ctx, ownerAdmin, ownerManifest, adminDSN, privateDir, "ro", true)
	if err != nil {
		t.Fatal("cannot provision a temporary SELECT-only refund-explain role")
	}
	defer func() {
		if err := refundExplainCleanupRole(ownerAdmin, readOnlyRole.name); err != nil {
			t.Errorf("cannot drop the temporary SELECT-only CLI role")
		}
	}()
	noSelectRole, err := refundExplainCreateRole(ctx, ownerAdmin, ownerManifest, adminDSN, privateDir, "noselect", false)
	if err != nil {
		t.Fatal("cannot provision a temporary no-SELECT refund-explain role")
	}
	defer func() {
		if err := refundExplainCleanupRole(ownerAdmin, noSelectRole.name); err != nil {
			t.Errorf("cannot drop the temporary no-SELECT CLI role")
		}
	}()
	secrets = append(secrets, readOnlyRole.dsn, readOnlyRole.manifestPath, noSelectRole.dsn, noSelectRole.manifestPath)

	readOnlyPool, err := repository.OpenRefundComponentDatabase(ctx, readOnlyRole.dsn, readOnlyRole.manifestPath)
	if err != nil {
		t.Fatal("SELECT-only role failed the same manifested database identity gate used by the CLI")
	}
	defer readOnlyPool.Close()
	evidence.ReadOnlyRole, err = refundExplainReadOnlyProofForRole(ctx, ownerAdmin, readOnlyPool, readOnlyRole.name)
	if err != nil {
		t.Fatal("could not prove read-only role and transaction settings")
	}
	if !refundExplainProofIsSelectOnly(evidence.ReadOnlyRole) {
		t.Fatal("temporary CLI role does not have the required SELECT-only/default-read-only privileges")
	}
	noSelectPool, err := repository.OpenRefundComponentDatabase(ctx, noSelectRole.dsn, noSelectRole.manifestPath)
	if err != nil {
		t.Fatal("no-SELECT role failed the manifested database identity gate before its safe CLI failure check")
	}
	evidence.NoSelectRole, err = refundExplainReadOnlyProofForRole(ctx, ownerAdmin, noSelectPool, noSelectRole.name)
	noSelectPool.Close()
	if err != nil {
		t.Fatal("could not inspect the temporary no-SELECT role")
	}
	if evidence.NoSelectRole.AuditSelect || evidence.NoSelectRole.AuditInsert || evidence.NoSelectRole.AuditUpdate ||
		evidence.NoSelectRole.AuditDelete || evidence.NoSelectRole.AuditTruncate || evidence.NoSelectRole.AuditReferences ||
		evidence.NoSelectRole.AuditTrigger || evidence.NoSelectRole.PaymentsSelect || evidence.NoSelectRole.PaymentsInsert ||
		evidence.NoSelectRole.PaymentsUpdate || evidence.NoSelectRole.PaymentsDelete ||
		!evidence.NoSelectRole.RoleCanLogin || evidence.NoSelectRole.RoleIsSuperuser ||
		!evidence.NoSelectRole.PublicSchemaUsage || !evidence.NoSelectRole.PGControlSystemExecute ||
		!evidence.NoSelectRole.EndpointHostLoopback || evidence.NoSelectRole.ConnectionFallbacks != 0 ||
		evidence.NoSelectRole.DefaultTransactionReadOnly != "on" || evidence.NoSelectRole.ObservedTransactionReadOnly != "on" {
		t.Fatal("no-SELECT fixture unexpectedly received Payment table privileges")
	}

	unreachableRole, err := refundExplainUnreachableRole(readOnlyRole, ownerManifest, privateDir)
	if err != nil {
		t.Fatal("cannot prepare a manifested loopback-only unavailable database endpoint")
	}
	secrets = append(secrets, unreachableRole.dsn, unreachableRole.manifestPath)

	evidence.SnapshotBeforeCLI, err = refundExplainSnapshot(ctx, ownerAdmin)
	if err != nil {
		t.Fatal("cannot snapshot business and audit rows before CLI execution")
	}
	goBinary, err := refundExplainGoBinary()
	if err != nil {
		t.Fatal(err)
	}
	goVersion, _, _, goVersionErr := runRefundExplainSetupCommand(ctx, goBinary, []string{"version"}, refundExplainBuildEnvironment())
	if goVersionErr == nil {
		evidence.GoVersion = strings.TrimSpace(goVersion)
	}
	binaryName := "refund-explain"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binaryPath := filepath.Join(privateDir, binaryName)
	buildArgs := []string{"build", "-trimpath", "-buildvcs=false", "-mod=readonly", "-o", binaryPath, "."}
	buildStdout, buildStderr, buildExit, buildErr := runRefundExplainSetupCommand(ctx, goBinary, buildArgs, refundExplainBuildEnvironment())
	evidence.BuildArgv = []string{"go", "build", "-trimpath", "-buildvcs=false", "-mod=readonly", "-o", binaryName, "."}
	evidence.BuildExitCode = buildExit
	if err := writeEvidenceFile(filepath.Join(evidenceDir, "refund-explain-build.stdout.log"), []byte(sanitizeRefundExplainText(buildStdout, secrets...)), true); err != nil {
		t.Fatal("cannot save sanitized binary build stdout")
	}
	if err := writeEvidenceFile(filepath.Join(evidenceDir, "refund-explain-build.stderr.log"), []byte(sanitizeRefundExplainText(buildStderr, secrets...)), true); err != nil {
		t.Fatal("cannot save sanitized binary build stderr")
	}
	evidence.BuildStdoutFile = "refund-explain-build.stdout.log"
	evidence.BuildStderrFile = "refund-explain-build.stderr.log"
	if buildErr != nil || buildExit != 0 {
		t.Fatalf("actual refund-explain binary build failed with exit %d", buildExit)
	}
	binaryInfo, err := os.Stat(binaryPath)
	if err != nil {
		t.Fatal("built refund-explain binary is unavailable")
	}
	binaryDigest, err := refundExplainFileSHA256(binaryPath)
	if err != nil {
		t.Fatal("cannot identify the built refund-explain binary")
	}
	evidence.BinaryFilename = binaryName
	evidence.BinarySizeBytes = binaryInfo.Size()
	evidence.BinarySHA256 = binaryDigest
	evidence.SourceFilesSHA256, err = refundExplainSourceHashes(repoRoot)
	if err != nil {
		t.Fatal("cannot hash refund-explain source identity")
	}
	evidence.SourceRevision = refundExplainGitHead(ctx, repoRoot)

	cliEnvironmentNames := refundExplainCLIEnvironmentNames()
	evidence.CLIEnvironmentNames = cliEnvironmentNames
	evidence.ForbiddenEnvironment = []string{"PG* overrides", "Stripe/provider credentials", "ambient database fallback variables"}
	for _, forbidden := range []string{"PG", "STRIPE", "PAYMENT_DATABASE_URL", "DATABASE_URL"} {
		for _, name := range cliEnvironmentNames {
			if strings.HasPrefix(strings.ToUpper(name), forbidden) {
				t.Fatalf("CLI child environment unexpectedly contains a forbidden variable")
			}
		}
	}
	runCase := refundExplainCaseRunner(func(name string, role refundExplainTestRole, argv []string, expectedExit int) (explanation, int, bool) {
		exitCode, stdout, stderr, runErr := runRefundExplainBinary(ctx, binaryPath, argv, role.dsn, role.manifestPath)
		if runErr != nil {
			t.Errorf("%s: could not start the built refund-explain binary", name)
			exitCode = -1
		}
		stdout = sanitizeRefundExplainText(stdout, secrets...)
		stderr = sanitizeRefundExplainText(stderr, secrets...)
		stdoutName := "cli-" + name + ".stdout.log"
		stderrName := "cli-" + name + ".stderr.log"
		if writeErr := writeEvidenceFile(filepath.Join(evidenceDir, stdoutName), []byte(stdout), true); writeErr != nil {
			t.Errorf("%s: could not save sanitized stdout", name)
		}
		if writeErr := writeEvidenceFile(filepath.Join(evidenceDir, stderrName), []byte(stderr), true); writeErr != nil {
			t.Errorf("%s: could not save sanitized stderr", name)
		}
		var parsed explanation
		jsonOK := json.Unmarshal([]byte(stdout), &parsed) == nil
		evidence.CLIInvocations = append(evidence.CLIInvocations, refundExplainCLICaseEvidence{
			Name: name, RoleName: role.name, Argv: append([]string{binaryName}, argv...),
			ExpectedExitCode: expectedExit, ActualExitCode: exitCode, ExitCodeMatches: exitCode == expectedExit,
			StdoutFile: stdoutName, StdoutSHA256: refundExplainSHA256([]byte(stdout)),
			StderrFile: stderrName, StderrSHA256: refundExplainSHA256([]byte(stderr)), JSONDecoded: jsonOK,
		})
		if exitCode != expectedExit {
			t.Errorf("%s: exit code %d, want %d", name, exitCode, expectedExit)
		}
		return parsed, exitCode, jsonOK
	})

	complete, completeExit, completeJSON := runCase("complete_linked_chain", readOnlyRole,
		[]string{"--payment-id", fixtures.complete.String()}, 0)
	if !completeJSON || completeExit != 0 || complete.Status != "EXPLAINED" || len(complete.Attempts) != 1 ||
		complete.Attempts[0].Status != "RESPONSE_RECORDED" || complete.Attempts[0].Response == nil ||
		len(complete.Attempts[0].VerifiedEvents) != 1 || len(complete.Commands) != 1 {
		t.Errorf("complete local chain did not explain its prepared response and linked event")
	}

	preparedOnly, preparedExit, preparedJSON := runCase("prepared_only", readOnlyRole,
		[]string{"--payment-id", fixtures.preparedOnly.String()}, 0)
	if !preparedJSON || preparedExit != 0 || preparedOnly.Status != "PARTIAL" || len(preparedOnly.Attempts) != 1 ||
		preparedOnly.Attempts[0].Status != "PREPARED_ONLY" || preparedOnly.Attempts[0].Response != nil {
		t.Errorf("prepared-only command was not reported with unknown response outcome")
	}

	responseOnly, responseOnlyExit, responseOnlyJSON := runCase("response_without_preparation", readOnlyRole,
		[]string{"--payment-id", fixtures.responseOnly.String()}, 0)
	if !responseOnlyJSON || responseOnlyExit != 0 || responseOnly.Status != "PARTIAL" || len(responseOnly.Attempts) != 1 ||
		responseOnly.Attempts[0].Status != "RESPONSE_WITHOUT_PREPARATION" {
		t.Errorf("response without its preparation was not reported as partial")
	}

	duplicateConflict, duplicateExit, duplicateJSON := runCase("duplicate_event_and_content_conflict", readOnlyRole,
		[]string{"--payment-id", fixtures.duplicateConflict.String()}, 0)
	if !duplicateJSON || duplicateExit != 0 || duplicateConflict.Status != "PARTIAL" ||
		len(duplicateConflict.Attempts) != 1 || len(duplicateConflict.Attempts[0].VerifiedEvents) != 1 ||
		len(duplicateConflict.RelatedEventCandidates) != 1 ||
		duplicateConflict.RelatedEventCandidates[0].SamePaymentScope ||
		duplicateConflict.RelatedEventCandidates[0].EventID != fixtures.conflictingEventID {
		t.Errorf("duplicate/conflicting event observation was not kept separate from the exact attempt link")
	}

	legacy, legacyExit, legacyJSON := runCase("legacy_payment_without_audit", readOnlyRole,
		[]string{"--payment-id", fixtures.legacy.String()}, 0)
	if !legacyJSON || legacyExit != 0 || legacy.Status != "LEGACY_UNVERIFIED" || legacy.PrimaryRowsIncluded != 0 {
		t.Errorf("legacy Payment row without audit records did not retain the existing legacy result")
	}

	multiple, multipleExit, multipleJSON := runCase("multiple_application_attempts", readOnlyRole,
		[]string{"--payment-id", fixtures.multipleAttempts.String()}, 0)
	if !multipleJSON || multipleExit != 0 || multiple.Status != "EXPLAINED" || len(multiple.Commands) != 1 ||
		len(multiple.Commands[0].AttemptIDs) != 2 || len(multiple.Attempts) != 2 {
		t.Errorf("multiple attempts for one frozen command were not grouped deterministically")
	}

	nonexistent, nonexistentExit, nonexistentJSON := runCase("valid_uuid_without_payment_row", readOnlyRole,
		[]string{"--payment-id", fixtures.nonexistent.String()}, 0)
	if !nonexistentJSON || nonexistentExit != 0 || nonexistent.Status != "LEGACY_UNVERIFIED" ||
		nonexistent.Status != legacy.Status || nonexistent.PrimaryRowsIncluded != 0 {
		t.Errorf("valid UUID without a Payment row did not expose the existing empty-audit legacy limitation")
	}

	invalid, invalidExit, invalidJSON := runCase("invalid_payment_id", readOnlyRole,
		[]string{"--payment-id", "not-a-uuid"}, 2)
	if !invalidJSON || invalidExit != 2 || invalid.Status != "AUDIT_UNAVAILABLE" ||
		len(invalid.Findings) == 0 || !strings.Contains(invalid.Findings[0], "valid --payment-id UUID") {
		t.Errorf("invalid Payment UUID did not fail safely")
	}

	invalidArgs, invalidArgsExit, invalidArgsJSON := runCase("unexpected_positional_argument", readOnlyRole,
		[]string{"--payment-id", fixtures.complete.String(), "unexpected"}, 2)
	if invalidArgsExit != 2 || invalidArgsJSON || invalidArgs.Status != "" {
		t.Errorf("unexpected positional argument did not exit safely without an explanation")
	}

	noSelect, noSelectExit, noSelectJSON := runCase("role_without_select", noSelectRole,
		[]string{"--payment-id", fixtures.complete.String()}, 2)
	if !noSelectJSON || noSelectExit != 2 || noSelect.Status != "AUDIT_UNAVAILABLE" {
		t.Errorf("missing SELECT permission did not fail closed")
	}

	unreachable, unreachableExit, unreachableJSON := runCase("loopback_database_unreachable", unreachableRole,
		[]string{"--payment-id", fixtures.complete.String()}, 2)
	if !unreachableJSON || unreachableExit != 2 || unreachable.Status != "AUDIT_UNAVAILABLE" {
		t.Errorf("unavailable manifested database did not fail closed without fallback")
	}

	bounded, boundedExit, boundedJSON := runCase("bounded_window_and_ordering", readOnlyRole,
		[]string{"--payment-id", fixtures.bounded.String()}, 0)
	if !boundedJSON || boundedExit != 0 || bounded.Status != "PARTIAL" ||
		bounded.QueryLimit != refundExplainRecordLimit || bounded.PrimaryRowsIncluded != refundExplainRecordLimit ||
		!bounded.MayBeTruncated || len(bounded.Attempts) != refundExplainRecordLimit {
		t.Errorf("bounded audit window was not marked partial at the established query limit")
	} else {
		expectedNewest := make(map[string]struct{}, refundExplainRecordLimit)
		for _, attemptID := range boundedWindowOrder[:refundExplainRecordLimit] {
			expectedNewest[attemptID.String()] = struct{}{}
		}
		actualIncluded := make(map[string]struct{}, len(bounded.Attempts))
		for _, attempt := range bounded.Attempts {
			if _, duplicate := actualIncluded[attempt.AttemptID]; duplicate {
				t.Errorf("bounded window repeated attempt %s", attempt.AttemptID)
			}
			actualIncluded[attempt.AttemptID] = struct{}{}
		}
		if len(actualIncluded) != refundExplainRecordLimit {
			t.Errorf("bounded window has %d unique attempts, want %d", len(actualIncluded), refundExplainRecordLimit)
		}
		for expectedID := range expectedNewest {
			if _, included := actualIncluded[expectedID]; !included {
				t.Errorf("bounded window omitted expected newest attempt %s", expectedID)
			}
		}
		for actualID := range actualIncluded {
			if _, expected := expectedNewest[actualID]; !expected {
				t.Errorf("bounded window included unexpected attempt %s", actualID)
			}
		}
		oldestOmittedID := boundedWindowOrder[refundExplainRecordLimit].String()
		if _, included := actualIncluded[oldestOmittedID]; included {
			t.Errorf("bounded window included the oldest omitted attempt %s", oldestOmittedID)
		}

		// Result presentation is sorted independently of the database window selection.
		for index := 1; index < len(bounded.Attempts); index++ {
			if bounded.Attempts[index-1].AttemptID > bounded.Attempts[index].AttemptID {
				t.Errorf("attempts are not ordered lexically at index %d", index)
				break
			}
		}
	}

	evidence.SnapshotAfterCLI, err = refundExplainSnapshot(ctx, ownerAdmin)
	if err != nil {
		t.Fatal("cannot snapshot business and audit rows after CLI execution")
	}
	evidence.SnapshotUnchanged = refundExplainSnapshotsEqual(evidence.SnapshotBeforeCLI, evidence.SnapshotAfterCLI)
	if !evidence.SnapshotUnchanged {
		t.Error("refund-explain CLI changed Payment business rows or immutable audit rows")
	}
	t.Logf("refund-explain binary sha256: %s", evidence.BinarySHA256)
}

type refundExplainFixtureSet struct {
	complete           uuid.UUID
	preparedOnly       uuid.UUID
	responseOnly       uuid.UUID
	duplicateConflict  uuid.UUID
	legacy             uuid.UUID
	multipleAttempts   uuid.UUID
	nonexistent        uuid.UUID
	bounded            uuid.UUID
	conflictingEventID string
}

func refundExplainSeedFixtures(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixtures refundExplainFixtureSet) error {
	t.Helper()
	repo := repository.NewPostgresPaymentRepository(pool)
	paymentIDs := []uuid.UUID{
		fixtures.complete, fixtures.preparedOnly, fixtures.responseOnly, fixtures.duplicateConflict,
		fixtures.legacy, fixtures.multipleAttempts, fixtures.bounded,
	}
	for _, paymentID := range paymentIDs {
		if _, err := pool.Exec(ctx, "INSERT INTO payments(id,user_id,status,price,currency) VALUES($1,$2,'success',$3,'usd')",
			paymentID, uuid.New(), int64(700)); err != nil {
			return err
		}
	}

	prepared, response := refundExplainAuditPair(t, fixtures.complete, "re_complete_fixture", "req_complete_fixture")
	if err := refundExplainAppendRecords(ctx, repo, prepared, response); err != nil {
		return err
	}
	completeEvent, err := refundExplainLinkedEvent(response, "evt_complete_fixture", 700)
	if err != nil {
		return err
	}
	if err := repo.AppendRefundAudit(ctx, completeEvent); err != nil {
		return err
	}

	prepared, _ = refundExplainAuditPair(t, fixtures.preparedOnly, "re_prepared_fixture", "req_prepared_fixture")
	if err := repo.AppendRefundAudit(ctx, prepared); err != nil {
		return err
	}

	_, response = refundExplainAuditPair(t, fixtures.responseOnly, "re_response_fixture", "req_response_fixture")
	if err := repo.AppendRefundAudit(ctx, response); err != nil {
		return err
	}

	prepared, response = refundExplainAuditPair(t, fixtures.duplicateConflict, "re_duplicate_fixture", "req_duplicate_fixture")
	if err := refundExplainAppendRecords(ctx, repo, prepared, response); err != nil {
		return err
	}
	conflictingResponse, err := domain.NewRefundResponseAudit(prepared, domain.RefundAuditResponse{
		Classification:    domain.RefundAuditResponseObserved,
		ProviderRequestID: "req_changed_fixture", RefundID: "re_changed_fixture",
		PaymentIntentID: response.Response.PaymentIntentID, Currency: "usd", AmountCents: refundExplainInt64(700),
		ObservedProviderAccount: refundExplainString("acct_fixture"), ObservedLiveMode: refundExplainBool(false),
		TransportObservationState: "unknown", ObservedTransportCalls: -1,
	})
	if err != nil {
		return err
	}
	if err := repo.AppendRefundAudit(ctx, conflictingResponse); !errors.Is(err, repository.ErrRefundAuditConflict) {
		return fmt.Errorf("response identity conflict was not rejected")
	}
	exactEvent, err := refundExplainLinkedEvent(response, fixtures.conflictingEventID, 700)
	if err != nil {
		return err
	}
	if err := repo.AppendRefundAudit(ctx, exactEvent); err != nil {
		return err
	}
	if err := repo.AppendRefundAudit(ctx, exactEvent); err != nil {
		return err
	}
	unlinkedConflictingEvent, err := refundExplainUnlinkedEvent(response, fixtures.conflictingEventID, 701, exactEvent.VerifiedEvent.ReceiptID)
	if err != nil {
		return err
	}
	if err := repo.AppendRefundAudit(ctx, unlinkedConflictingEvent); err != nil {
		return err
	}

	firstPrepared, firstResponse := refundExplainAuditPair(t, fixtures.multipleAttempts, "re_attempt_one", "req_attempt_one")
	secondPrepared, secondResponse := refundExplainAuditPair(t, fixtures.multipleAttempts, "re_attempt_two", "req_attempt_two")
	if firstPrepared.CommandDigest != secondPrepared.CommandDigest || firstPrepared.AttemptID == secondPrepared.AttemptID {
		return fmt.Errorf("multiple attempt fixtures do not share a command identity")
	}
	if err := refundExplainAppendRecords(ctx, repo, firstPrepared, firstResponse, secondPrepared, secondResponse); err != nil {
		return err
	}

	for index := 0; index < refundExplainProbeLimit; index++ {
		boundedPrepared, _ := refundExplainAuditPair(t, fixtures.bounded,
			fmt.Sprintf("re_bounded_%03d", index), fmt.Sprintf("req_bounded_%03d", index))
		if err := repo.AppendRefundAudit(ctx, boundedPrepared); err != nil {
			return err
		}
	}
	return nil
}

func refundExplainFixtureIDs() (refundExplainFixtureSet, error) {
	values := make([]uuid.UUID, 8)
	for index := range values {
		id, err := uuid.NewRandom()
		if err != nil {
			return refundExplainFixtureSet{}, err
		}
		values[index] = id
	}
	return refundExplainFixtureSet{
		complete: values[0], preparedOnly: values[1], responseOnly: values[2], duplicateConflict: values[3],
		legacy: values[4], multipleAttempts: values[5], nonexistent: values[6], bounded: values[7],
		conflictingEventID: "evt_duplicate_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
	}, nil
}

func refundExplainAuditPair(t *testing.T, paymentID uuid.UUID, refundID, requestID string) (domain.RefundAuditRecord, domain.RefundAuditRecord) {
	t.Helper()
	prepared, _ := explainFixture(t, paymentID, refundID, requestID)
	response, err := domain.NewRefundResponseAudit(prepared, domain.RefundAuditResponse{
		Classification: domain.RefundAuditResponseObserved, ProviderRequestID: requestID, RefundID: refundID,
		PaymentIntentID: prepared.PaymentIntentID, ProviderStatus: "succeeded", Currency: "usd",
		AmountCents: refundExplainInt64(700), ObservedProviderAccount: refundExplainString("acct_fixture"),
		ObservedLiveMode: refundExplainBool(false), TransportObservationState: "unknown", ObservedTransportCalls: -1,
	})
	if err != nil {
		t.Fatalf("cannot build a valid synthetic response audit record")
	}
	return prepared, response
}

func refundExplainAppendRecords(ctx context.Context, repo *repository.PostgresPaymentRepository, records ...domain.RefundAuditRecord) error {
	for _, record := range records {
		if err := repo.AppendRefundAudit(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func refundExplainLinkedEvent(response domain.RefundAuditRecord, eventID string, amount int64) (domain.RefundAuditRecord, error) {
	if response.Response == nil {
		return domain.RefundAuditRecord{}, domain.ErrInvalidRefundAuditRecord
	}
	account := "acct_fixture"
	liveMode := false
	event := domain.VerifiedRefundEventAudit{
		ReceiptID: uuid.New(), EventID: eventID, EventType: "refund.updated",
		ObjectType: "refund", ObjectID: response.Response.RefundID,
		ProviderRequestID: response.Response.ProviderRequestID, ObservedProviderAccount: &account,
		ObservedLiveMode: &liveMode, RefundID: response.Response.RefundID, PaymentIntentID: response.Response.PaymentIntentID,
		ProviderStatus: "succeeded", Currency: response.Response.Currency, AmountCents: refundExplainInt64(amount),
		LinkClassification: domain.RefundAuditLinkExactRefundIdentity, LinkSource: domain.RefundAuditLinkSourceRequestAndID,
	}
	return domain.NewRefundVerifiedEventRecord(event, &response)
}

func refundExplainUnlinkedEvent(response domain.RefundAuditRecord, eventID string, amount int64, receiptID uuid.UUID) (domain.RefundAuditRecord, error) {
	if response.Response == nil {
		return domain.RefundAuditRecord{}, domain.ErrInvalidRefundAuditRecord
	}
	account := "acct_fixture"
	liveMode := false
	event := domain.VerifiedRefundEventAudit{
		ReceiptID: receiptID, EventID: eventID, EventType: "refund.updated",
		ObjectType: "refund", ObjectID: response.Response.RefundID,
		ProviderRequestID: response.Response.ProviderRequestID, ObservedProviderAccount: &account,
		ObservedLiveMode: &liveMode, RefundID: response.Response.RefundID, PaymentIntentID: response.Response.PaymentIntentID,
		ProviderStatus: "succeeded", Currency: response.Response.Currency, AmountCents: refundExplainInt64(amount),
		LinkClassification: domain.RefundAuditLinkUncorrelated,
	}
	return domain.NewRefundVerifiedEventRecord(event, nil)
}

func refundExplainCreateRole(
	ctx context.Context,
	admin *pgxpool.Pool,
	owner refundExplainIntegrationOwnerManifest,
	adminDSN string,
	privateDir string,
	prefix string,
	selectTables bool,
) (refundExplainTestRole, error) {
	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return refundExplainTestRole{}, err
	}
	name := "refund_explain_" + prefix + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	password := hex.EncodeToString(randomBytes)
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE ROLE "+identifier+" LOGIN PASSWORD '"+password+"'"); err != nil {
		return refundExplainTestRole{}, err
	}
	cleanup := func() {
		_ = refundExplainCleanupRole(admin, name)
	}
	setup := []string{
		"GRANT CONNECT ON DATABASE " + pgx.Identifier{owner.Database}.Sanitize() + " TO " + identifier,
		"GRANT USAGE ON SCHEMA public TO " + identifier,
		"GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO " + identifier,
		"ALTER ROLE " + identifier + " SET default_transaction_read_only = 'on'",
	}
	if selectTables {
		setup = append(setup, "GRANT SELECT ON TABLE public.payment_refund_audit TO "+identifier)
	}
	for _, statement := range setup {
		if _, err := admin.Exec(ctx, statement); err != nil {
			cleanup()
			return refundExplainTestRole{}, err
		}
	}
	adminURL, err := url.Parse(adminDSN)
	if err != nil {
		cleanup()
		return refundExplainTestRole{}, err
	}
	adminURL.User = url.UserPassword(name, password)
	dsn := adminURL.String()
	owner.DSNSHA256 = refundExplainSHA256([]byte(dsn))
	manifestPath := filepath.Join(privateDir, prefix+"-owner.json")
	manifestBytes, err := json.Marshal(owner)
	if err != nil {
		cleanup()
		return refundExplainTestRole{}, err
	}
	if err := writePrivateFixture(privateDir, prefix+"-dsn.txt", []byte(dsn)); err != nil {
		cleanup()
		return refundExplainTestRole{}, err
	}
	if err := writePrivateFixture(privateDir, prefix+"-owner.json", manifestBytes); err != nil {
		cleanup()
		return refundExplainTestRole{}, err
	}
	return refundExplainTestRole{name: name, password: password, dsn: dsn, manifestPath: manifestPath}, nil
}

func refundExplainUnreachableRole(
	role refundExplainTestRole,
	owner refundExplainIntegrationOwnerManifest,
	privateDir string,
) (refundExplainTestRole, error) {
	parsed, err := url.Parse(role.dsn)
	if err != nil {
		return refundExplainTestRole{}, err
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(owner.Host, "0"))
	if err != nil {
		return refundExplainTestRole{}, err
	}
	_, portText, splitErr := net.SplitHostPort(listener.Addr().String())
	closeErr := listener.Close()
	port, portErr := strconv.Atoi(portText)
	if splitErr != nil || closeErr != nil || portErr != nil || port == owner.Port {
		return refundExplainTestRole{}, fmt.Errorf("could not allocate a closed loopback port")
	}
	parsed.Host = net.JoinHostPort(owner.Host, strconv.Itoa(port))
	dsn := parsed.String()
	owner.Port = port
	owner.DSNSHA256 = refundExplainSHA256([]byte(dsn))
	manifest, err := json.Marshal(owner)
	if err != nil {
		return refundExplainTestRole{}, err
	}
	manifestPath := filepath.Join(privateDir, "unreachable-owner.json")
	if err := writePrivateFixture(privateDir, "unreachable-dsn.txt", []byte(dsn)); err != nil {
		return refundExplainTestRole{}, err
	}
	if err := writePrivateFixture(privateDir, "unreachable-owner.json", manifest); err != nil {
		return refundExplainTestRole{}, err
	}
	return refundExplainTestRole{name: role.name, password: role.password, dsn: dsn, manifestPath: manifestPath}, nil
}

func refundExplainCleanupRole(admin *pgxpool.Pool, roleName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	identifier := pgx.Identifier{roleName}.Sanitize()
	if _, err := admin.Exec(ctx, "DROP OWNED BY "+identifier); err != nil {
		return err
	}
	_, err := admin.Exec(ctx, "DROP ROLE "+identifier)
	return err
}

func refundExplainReadOnlyProofForRole(
	ctx context.Context,
	admin *pgxpool.Pool,
	rolePool *pgxpool.Pool,
	roleName string,
) (refundExplainReadOnlyProof, error) {
	proof := refundExplainReadOnlyProof{RoleName: roleName}
	if err := admin.QueryRow(ctx, "SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls FROM pg_catalog.pg_roles WHERE rolname=$1", roleName).
		Scan(&proof.RoleCanLogin, &proof.RoleIsSuperuser, &proof.RoleCanCreateDatabase, &proof.RoleCanCreateRole, &proof.RoleCanReplicate, &proof.RoleCanBypassRLS); err != nil {
		return refundExplainReadOnlyProof{}, err
	}
	if err := admin.QueryRow(ctx, "SELECT has_schema_privilege($1,'public','USAGE'),has_table_privilege($1,'public.payment_refund_audit','SELECT'),"+
		"has_table_privilege($1,'public.payment_refund_audit','INSERT'),has_table_privilege($1,'public.payment_refund_audit','UPDATE'),"+
		"has_table_privilege($1,'public.payment_refund_audit','DELETE'),has_table_privilege($1,'public.payment_refund_audit','TRUNCATE'),"+
		"has_table_privilege($1,'public.payment_refund_audit','REFERENCES'),has_table_privilege($1,'public.payment_refund_audit','TRIGGER'),"+
		"has_table_privilege($1,'public.payments','SELECT'),has_table_privilege($1,'public.payments','INSERT'),"+
		"has_table_privilege($1,'public.payments','UPDATE'),has_table_privilege($1,'public.payments','DELETE'),"+
		"has_function_privilege($1,'pg_catalog.pg_control_system()','EXECUTE')", roleName).
		Scan(&proof.PublicSchemaUsage, &proof.AuditSelect, &proof.AuditInsert, &proof.AuditUpdate, &proof.AuditDelete,
			&proof.AuditTruncate, &proof.AuditReferences, &proof.AuditTrigger, &proof.PaymentsSelect, &proof.PaymentsInsert,
			&proof.PaymentsUpdate, &proof.PaymentsDelete, &proof.PGControlSystemExecute); err != nil {
		return refundExplainReadOnlyProof{}, err
	}
	tx, err := rolePool.Begin(ctx)
	if err != nil {
		return refundExplainReadOnlyProof{}, err
	}
	defer tx.Rollback(ctx)
	if err := tx.QueryRow(ctx, "SELECT current_user,current_setting('default_transaction_read_only'),current_setting('transaction_read_only')").
		Scan(&proof.CurrentUser, &proof.DefaultTransactionReadOnly, &proof.ObservedTransactionReadOnly); err != nil {
		return refundExplainReadOnlyProof{}, err
	}
	connectionConfig := rolePool.Config().ConnConfig
	if connectionConfig == nil {
		return refundExplainReadOnlyProof{}, fmt.Errorf("connection config is unavailable")
	}
	address := net.ParseIP(connectionConfig.Host)
	proof.EndpointHostLoopback = address != nil && address.IsLoopback()
	proof.ConnectionFallbacks = len(connectionConfig.Fallbacks)
	if proof.CurrentUser != roleName {
		return refundExplainReadOnlyProof{}, fmt.Errorf("role identity mismatch")
	}
	return proof, nil
}

func refundExplainProofIsSelectOnly(proof refundExplainReadOnlyProof) bool {
	return proof.RoleCanLogin && !proof.RoleIsSuperuser && !proof.RoleCanCreateDatabase && !proof.RoleCanCreateRole &&
		!proof.RoleCanReplicate && !proof.RoleCanBypassRLS && proof.PublicSchemaUsage && proof.AuditSelect &&
		!proof.AuditInsert && !proof.AuditUpdate && !proof.AuditDelete && !proof.AuditTruncate &&
		!proof.AuditReferences && !proof.AuditTrigger && !proof.PaymentsSelect && !proof.PaymentsInsert &&
		!proof.PaymentsUpdate && !proof.PaymentsDelete && proof.PGControlSystemExecute &&
		proof.EndpointHostLoopback && proof.ConnectionFallbacks == 0 &&
		proof.DefaultTransactionReadOnly == "on" && proof.ObservedTransactionReadOnly == "on"
}

func refundExplainSnapshot(ctx context.Context, pool *pgxpool.Pool) (map[string]refundExplainTableSnapshot, error) {
	result := map[string]refundExplainTableSnapshot{}
	for name, query := range map[string]string{
		"payments":             "SELECT to_jsonb(p)::text FROM public.payments AS p ORDER BY p.id",
		"payment_refund_audit": "SELECT to_jsonb(a)::text FROM public.payment_refund_audit AS a ORDER BY a.record_id",
	} {
		rows, err := pool.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		hash := sha256.New()
		count := 0
		for rows.Next() {
			var serialized string
			if err := rows.Scan(&serialized); err != nil {
				rows.Close()
				return nil, err
			}
			_, _ = hash.Write([]byte(strconv.Itoa(len(serialized))))
			_, _ = hash.Write([]byte{':'})
			_, _ = hash.Write([]byte(serialized))
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		result[name] = refundExplainTableSnapshot{Rows: count, SHA256Hex: hex.EncodeToString(hash.Sum(nil))}
	}
	return result, nil
}

func refundExplainBoundedWindowOrder(ctx context.Context, pool *pgxpool.Pool, paymentID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := pool.Query(ctx,
		"SELECT application_attempt_id FROM public.payment_refund_audit "+
			"WHERE payment_id=$1 AND record_kind='PREPARED' "+
			"ORDER BY recorded_at DESC,record_id LIMIT $2",
		paymentID, refundExplainProbeLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attemptIDs := make([]uuid.UUID, 0, refundExplainProbeLimit)
	for rows.Next() {
		var attemptID uuid.UUID
		if err := rows.Scan(&attemptID); err != nil {
			return nil, err
		}
		attemptIDs = append(attemptIDs, attemptID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return attemptIDs, nil
}

func refundExplainSnapshotsEqual(
	before map[string]refundExplainTableSnapshot,
	after map[string]refundExplainTableSnapshot,
) bool {
	if len(before) != len(after) {
		return false
	}
	for name, snapshot := range before {
		if after[name] != snapshot {
			return false
		}
	}
	return true
}

func runRefundExplainBinary(
	ctx context.Context,
	binaryPath string,
	argv []string,
	dsn string,
	manifestPath string,
) (int, string, string, error) {
	command := exec.CommandContext(ctx, binaryPath, argv...)
	command.Env = refundExplainCLIEnvironment(dsn, manifestPath)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	exitCode := 0
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		} else {
			return -1, stdout.String(), stderr.String(), fmt.Errorf("binary process could not be started")
		}
	}
	return exitCode, stdout.String(), stderr.String(), nil
}

func refundExplainCLIEnvironment(dsn, manifestPath string) []string {
	values := refundExplainBaseEnvironment()
	values = append(values,
		refundExplainAdminDSNEnv+"="+dsn,
		refundExplainOwnerEnv+"="+manifestPath,
	)
	return values
}

func refundExplainCLIEnvironmentNames() []string {
	names := make([]string, 0, len(refundExplainBaseEnvironment())+2)
	seen := make(map[string]struct{})
	for _, value := range refundExplainBaseEnvironment() {
		name, _, _ := strings.Cut(value, "=")
		key := strings.ToUpper(name)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			names = append(names, name)
		}
	}
	for _, name := range []string{refundExplainAdminDSNEnv, refundExplainOwnerEnv} {
		key := strings.ToUpper(name)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func refundExplainBaseEnvironment() []string {
	keys := []string{"PATH", "SystemRoot", "WINDIR", "TEMP", "TMP"}
	result := make([]string, 0, len(keys))
	seen := make(map[string]struct{})
	for _, key := range keys {
		name := strings.ToUpper(key)
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if value, ok := os.LookupEnv(key); ok {
			result = append(result, key+"="+value)
		}
	}
	return result
}

func refundExplainBuildEnvironment() []string {
	result := refundExplainBaseEnvironment()
	for _, key := range []string{"HOME", "USERPROFILE", "LOCALAPPDATA", "APPDATA", "GOMODCACHE", "GOCACHE", "GOMAXPROCS", "GOMEMLIMIT", "GOGC", "GOFLAGS"} {
		if value, ok := os.LookupEnv(key); ok {
			result = append(result, key+"="+value)
		}
	}
	result = append(result, "GO111MODULE=on", "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	return result
}

func refundExplainSetupEnvironment() []string {
	return refundExplainBaseEnvironment()
}

func refundExplainGoBinary() (string, error) {
	if binary := os.Getenv(refundExplainGoBinaryEnv); binary != "" {
		if _, err := os.Stat(binary); err != nil {
			return "", fmt.Errorf("BLOCKED: %s does not identify an accessible Go executable", refundExplainGoBinaryEnv)
		}
		return binary, nil
	}
	binary, err := exec.LookPath("go")
	if err != nil {
		return "", fmt.Errorf("BLOCKED: Go executable is unavailable; set %s", refundExplainGoBinaryEnv)
	}
	return binary, nil
}

func refundExplainMigrationBinary() (string, error) {
	if binary := os.Getenv(refundExplainMigrateEnv); binary != "" {
		if _, err := os.Stat(binary); err != nil {
			return "", fmt.Errorf("BLOCKED: %s does not identify an accessible migrate executable", refundExplainMigrateEnv)
		}
		return binary, nil
	}
	binary, err := exec.LookPath("migrate")
	if err != nil {
		return "", fmt.Errorf("BLOCKED: migrate executable is unavailable; set %s", refundExplainMigrateEnv)
	}
	return binary, nil
}

func runRefundExplainSetupCommand(
	ctx context.Context,
	binary string,
	args []string,
	environment []string,
) (string, string, int, error) {
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = environment
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	exitCode := 0
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		} else {
			return stdout.String(), stderr.String(), -1, fmt.Errorf("setup process could not be started")
		}
	}
	return stdout.String(), stderr.String(), exitCode, nil
}

func refundExplainRepositoryRoot() (string, error) {
	packageDir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	repoRoot := filepath.Clean(filepath.Join(packageDir, "..", "..", "..", "..", ".."))
	if _, err := os.Stat(filepath.Join(repoRoot, "backend", "services", "payment", "go.mod")); err != nil {
		return "", err
	}
	return repoRoot, nil
}

func refundExplainPathIsInside(root, path string) bool {
	absoluteRoot, rootErr := filepath.Abs(root)
	absolutePath, pathErr := filepath.Abs(path)
	if rootErr != nil || pathErr != nil {
		return true
	}
	relative, err := filepath.Rel(absoluteRoot, absolutePath)
	if err != nil {
		return true
	}
	if relative == "." {
		return true
	}
	if filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return false
	}
	return true
}

func requiredEvidenceRoot() (string, error) {
	root := os.Getenv(refundExplainEvidenceEnv)
	if root == "" {
		return "", fmt.Errorf("BLOCKED: %s must point to an existing directory outside the source tree", refundExplainEvidenceEnv)
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("BLOCKED: evidence directory is not a valid path")
	}
	info, err := os.Stat(absolute)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("BLOCKED: evidence directory must already exist")
	}
	return absolute, nil
}

func rejectAmbientPostgresOverrides() error {
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(name), "PG") {
			return fmt.Errorf("BLOCKED: remove ambient PostgreSQL override variables before running this test")
		}
	}
	return nil
}

func refundExplainMigrationHashes(migrationsDir string) (map[string]string, error) {
	paths, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	if err != nil || len(paths) == 0 {
		return nil, fmt.Errorf("Payment migration files are unavailable")
	}
	sort.Strings(paths)
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		digest, err := refundExplainFileSHA256(path)
		if err != nil {
			return nil, err
		}
		result[filepath.Base(path)] = digest
	}
	return result, nil
}

func refundExplainSourceHashes(repoRoot string) (map[string]string, error) {
	paths := []string{
		"backend/services/payment/cmd/refund-explain/main.go",
		"backend/services/payment/cmd/refund-explain/main_test.go",
		"backend/services/payment/cmd/refund-explain/refund_explain_real_cli_integration_test.go",
		"backend/services/payment/go.mod",
		"backend/services/payment/go.sum",
	}
	result := make(map[string]string, len(paths))
	for _, relative := range paths {
		digest, err := refundExplainFileSHA256(filepath.Join(repoRoot, filepath.FromSlash(relative)))
		if err != nil {
			return nil, err
		}
		result[filepath.ToSlash(relative)] = digest
	}
	return result, nil
}

func refundExplainGitHead(ctx context.Context, repoRoot string) string {
	command := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	command.Dir = repoRoot
	command.Env = refundExplainSetupEnvironment()
	output, err := command.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func refundExplainFileSHA256(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return refundExplainSHA256(content), nil
}

func refundExplainSHA256(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func decodeRefundExplainOwnerManifest(raw []byte) (refundExplainIntegrationOwnerManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest refundExplainIntegrationOwnerManifest
	if err := decoder.Decode(&manifest); err != nil {
		return refundExplainIntegrationOwnerManifest{}, err
	}
	if manifest.ManifestVersion != 1 || manifest.ResourceID == "" || manifest.ContainerID == "" ||
		manifest.Database == "" || manifest.Host == "" || manifest.Port < 1 || manifest.Port > 65535 ||
		manifest.DSNSHA256 == "" || manifest.RunID == "" || manifest.OwnerNonce == "" ||
		manifest.ServerSystemIdentifier == "" || manifest.DatabaseMarker == "" {
		return refundExplainIntegrationOwnerManifest{}, fmt.Errorf("owner manifest is incomplete")
	}
	return manifest, nil
}

func writePrivateFixture(directory, name string, content []byte) error {
	path := filepath.Join(directory, name)
	return writeEvidenceFile(path, content, true)
}

func writeEvidenceFile(path string, content []byte, exclusive bool) error {
	flags := os.O_CREATE | os.O_WRONLY
	if exclusive {
		flags |= os.O_EXCL
	} else {
		flags |= os.O_TRUNC
	}
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func sanitizeRefundExplainText(value string, secrets ...string) string {
	replacements := make([]string, 0, len(secrets)*2)
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		replacements = append(replacements, secret, "[REDACTED]")
		if parsed, err := url.Parse(secret); err == nil && parsed.User != nil {
			if password, ok := parsed.User.Password(); ok && password != "" {
				replacements = append(replacements, password, "[REDACTED]")
				replacements = append(replacements, url.QueryEscape(password), "[REDACTED]")
			}
		}
	}
	return strings.NewReplacer(replacements...).Replace(value)
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("cannot read private owner manifest fixture")
	}
	return string(content)
}

func refundExplainInt64(value int64) *int64 { return &value }
func refundExplainBool(value bool) *bool    { return &value }
func refundExplainString(value string) *string {
	return &value
}
