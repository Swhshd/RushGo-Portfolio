//go:build recoverycomponents

package repository_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/booking/internal/service"
	"go.uber.org/zap"
)

type bookingOwnerManifest struct {
	ManifestVersion        string `json:"manifest_version"`
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
}

// TestBookingAbortConfirmationCandidateEvidence drives the real Booking
// service and repository through claim_confirmation followed by
// abort_confirmation, then reads the committed row through a separate pool.
// The emitted observation is component-only and is not formal evidence.
func TestBookingAbortConfirmationCandidateEvidence(t *testing.T) {
	evidenceDir := os.Getenv("BOOKING_COMPONENT_EVIDENCE_DIR")
	if evidenceDir == "" {
		t.Skip("booking candidate evidence emission is disabled")
	}
	sourceSHA := os.Getenv("BOOKING_COMPONENT_SOURCE_SHA256")
	sourceCommit := os.Getenv("BOOKING_COMPONENT_SOURCE_COMMIT")
	sourceTree := os.Getenv("BOOKING_COMPONENT_SOURCE_TREE")
	runID := os.Getenv("BOOKING_COMPONENT_RUN_ID")
	attempt := os.Getenv("BOOKING_COMPONENT_ATTEMPT")
	if attempt == "" {
		attempt = "1"
	}
	if len(sourceSHA) != 64 || len(sourceCommit) != 40 || len(sourceTree) != 40 || runID == "" {
		t.Fatal("gated booking evidence requires BOOKING_COMPONENT_SOURCE_SHA256/COMMIT/TREE and RUN_ID")
	}
	bookingID := strings.TrimSpace(os.Getenv("BOOKING_COMPONENT_BOOKING_ID"))
	if bookingID == "" {
		bookingID = uuid.NewString()
	}
	parsedBookingID, err := uuid.Parse(bookingID)
	if err != nil || parsedBookingID == uuid.Nil {
		t.Fatal("BOOKING_COMPONENT_BOOKING_ID must be a non-nil UUID")
	}

	pool, reader, manifest, schema := bookingComponentPools(t)
	bookingID = parsedBookingID.String()
	userID, eventID := uuid.New(), uuid.New()
	deadline := time.Now().UTC().Add(10 * time.Minute)
	if _, err := pool.Exec(context.Background(), `INSERT INTO bookings
		(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,
		 lifecycle_phase,currency,booking_mode,reservation_deadline)
		VALUES($1,$2,$3,'PENDING',725,1,true,1,'OPEN','usd','pessimistic',$4)`,
		parsedBookingID, userID, eventID, deadline); err != nil {
		t.Fatalf("seed legal OPEN Booking fixture: %v", err)
	}
	repo := repository.NewPostgresBookingRepository(pool)
	svc := service.NewBookingService(repo, nil, nil, zap.NewNop())
	claimDecision := domain.BookingDecisionID(parsedBookingID, "claim_confirmation")
	if _, err := svc.DecideLifecycle(context.Background(), domain.LifecycleDecision{
		Ref:        domain.LifecycleRef{BookingID: parsedBookingID, Generation: 1},
		DecisionID: claimDecision, Kind: "claim_confirmation",
		Reason: "saga_claim_confirmation", ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("claim_confirmation through real service: %v", err)
	}
	view, err := svc.GetLifecycle(context.Background(), domain.LifecycleRef{
		BookingID: parsedBookingID, Generation: 1,
	})
	if err != nil || view.Phase != "CONFIRMING" || view.ConfirmationDecisionID == nil ||
		*view.ConfirmationDecisionID != claimDecision {
		t.Fatalf("claim_confirmation did not commit CONFIRMING state: view=%+v err=%v", view, err)
	}
	abortDecision := domain.BookingDecisionID(parsedBookingID, "abort_confirmation")
	if _, err := svc.DecideLifecycle(context.Background(), domain.LifecycleDecision{
		Ref:        domain.LifecycleRef{BookingID: parsedBookingID, Generation: 1},
		DecisionID: abortDecision, Kind: "abort_confirmation",
		ConfirmationDecisionID: &claimDecision,
		Reason:                 "saga_abort_confirmation", ExpectedVersion: view.Booking.Version,
	}); err != nil {
		t.Fatalf("abort_confirmation through real service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var status, phase, decisionKind, rowDecisionID, rowConfirmationID string
	var generation int64
	var version int
	var readerPID int
	var observedAt float64
	query := `SELECT status,lifecycle_phase,lifecycle_generation,decision_kind,
		decision_id::text,confirmation_decision_id::text,version,pg_backend_pid(),
		EXTRACT(EPOCH FROM clock_timestamp()) FROM bookings WHERE id=$1`
	if err := reader.QueryRow(ctx, query, parsedBookingID).Scan(
		&status, &phase, &generation, &decisionKind, &rowDecisionID,
		&rowConfirmationID, &version, &readerPID, &observedAt); err != nil {
		t.Fatal(err)
	}
	var writerPID int
	if err := pool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&writerPID); err != nil {
		t.Fatal(err)
	}
	if readerPID == writerPID {
		t.Fatal("booking candidate reader reused the writer backend")
	}
	var outboxID, outboxType, outboxKey string
	var published bool
	var eventCreatedAt float64
	if err := reader.QueryRow(ctx, `SELECT id::text,event_type,event_key,published,
		EXTRACT(EPOCH FROM created_at) FROM outbox WHERE id=$1`,
		domain.BookingOutboxID(parsedBookingID, "abort_confirmation")).Scan(
		&outboxID, &outboxType, &outboxKey, &published, &eventCreatedAt); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.StatusFailed) || phase != "TERMINAL" || generation != 1 ||
		decisionKind != "abort_confirmation" || rowDecisionID != abortDecision.String() ||
		rowConfirmationID != claimDecision.String() || version < 3 {
		t.Fatalf("committed Booking row does not satisfy abort-confirmation predicate: status=%s phase=%s generation=%d kind=%s decision=%s confirmation=%s version=%d",
			status, phase, generation, decisionKind, rowDecisionID, rowConfirmationID, version)
	}
	if outboxType != "BookingFailed" || outboxKey != parsedBookingID.String() || published {
		t.Fatalf("abort-confirmation outbox is not the unpublished failed event: %s %s published=%v", outboxType, outboxKey, published)
	}
	if eventCreatedAt > observedAt {
		t.Fatalf("decision event timestamp is after the independent read: event=%f read=%f", eventCreatedAt, observedAt)
	}

	observation := map[string]any{
		"schemaVersion":                  1,
		"kind":                           "booking_abort_confirmation_read",
		"bookingId":                      parsedBookingID.String(),
		"generation":                     generation,
		"status":                         status,
		"lifecyclePhase":                 phase,
		"decisionKind":                   decisionKind,
		"decisionId":                     rowDecisionID,
		"confirmationDecisionId":         rowConfirmationID,
		"expectedDecisionId":             abortDecision.String(),
		"expectedConfirmationDecisionId": claimDecision.String(),
		"version":                        version,
		"command": map[string]any{
			"method": "BookingService.DecideLifecycle", "kind": "abort_confirmation",
			"reason": "saga_abort_confirmation", "invoked": true,
			"claimDecisionId": claimDecision.String(),
		},
		"outbox": map[string]any{
			"eventId": outboxID, "eventType": outboxType, "eventKey": outboxKey,
			"published": published, "createdAt": eventCreatedAt,
		},
		"database": map[string]any{
			"database": manifest.Database, "systemIdentifier": manifest.ServerSystemIdentifier,
			"markerSha256": fmt.Sprintf("%x", sha256.Sum256([]byte(manifest.DatabaseMarker))),
			"schema":       schema,
		},
		"reader": map[string]any{
			"backendPid": readerPID, "writerBackendPid": writerPID,
			"applicationName": "booking_candidate_observer", "observedAt": observedAt,
		},
		"observedAt":  observedAt,
		"timeBinding": "DECISION_OUTBOX_CREATED_TO_INDEPENDENT_READ_INTERVAL",
		"source":      map[string]any{"sha256": sourceSHA, "commit": sourceCommit, "tree": sourceTree},
		"runId":       runID,
		"attemptId":   attempt,
		"evidenceMeta": map[string]any{
			"componentOnly": true, "formalEvidence": false,
			"source":        "REAL_BOOKING_SERVICE_AND_PG16_COMPONENT",
			"sourceBinding": "EMITTER_TEST_FILE_SHA256",
		},
	}
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(evidenceDir, "booking-abort-confirmation-"+runID+".json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create booking candidate evidence: %v", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(observation); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBookingCancelledCandidateEvidence emits the terminal CANCELLED Booking
// fact used by the refund reconciliation chain. The cancellation is performed
// through the real Booking service and the row is read by a separate pool.
func TestBookingCancelledCandidateEvidence(t *testing.T) {
	evidenceDir := os.Getenv("BOOKING_COMPONENT_EVIDENCE_DIR")
	if evidenceDir == "" {
		t.Skip("booking candidate evidence emission is disabled")
	}
	sourceSHA := os.Getenv("BOOKING_COMPONENT_SOURCE_SHA256")
	sourceCommit := os.Getenv("BOOKING_COMPONENT_SOURCE_COMMIT")
	sourceTree := os.Getenv("BOOKING_COMPONENT_SOURCE_TREE")
	runID := os.Getenv("BOOKING_COMPONENT_RUN_ID")
	attempt := os.Getenv("BOOKING_COMPONENT_ATTEMPT")
	if attempt == "" {
		attempt = "1"
	}
	if len(sourceSHA) != 64 || len(sourceCommit) != 40 || len(sourceTree) != 40 || runID == "" {
		t.Fatal("gated booking evidence requires source identity and run ID")
	}
	bookingID := strings.TrimSpace(os.Getenv("BOOKING_COMPONENT_BOOKING_ID"))
	if bookingID == "" {
		bookingID = uuid.NewString()
	}
	parsed, err := uuid.Parse(bookingID)
	if err != nil || parsed == uuid.Nil {
		t.Fatal("BOOKING_COMPONENT_BOOKING_ID must be a non-nil UUID")
	}
	pool, reader, manifest, schema := bookingComponentPools(t)
	userID, eventID := uuid.New(), uuid.New()
	if _, err := pool.Exec(context.Background(), `INSERT INTO bookings
		(id,user_id,event_id,status,total_amount_cents,version,managed,lifecycle_generation,
		 lifecycle_phase,currency,booking_mode,reservation_deadline)
		VALUES($1,$2,$3,'PENDING',725,1,true,1,'OPEN','usd','pessimistic',$4)`,
		parsed, userID, eventID, time.Now().UTC().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewPostgresBookingRepository(pool)
	svc := service.NewBookingService(repo, nil, nil, zap.NewNop())
	cancelDecision := domain.BookingDecisionID(parsed, "cancel")
	if _, err := svc.DecideLifecycle(context.Background(), domain.LifecycleDecision{
		Ref:        domain.LifecycleRef{BookingID: parsed, Generation: 1},
		DecisionID: cancelDecision, Kind: "cancel", Reason: "user_cancelled",
		ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("cancel through real service: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var status, phase, kind, rowDecisionID string
	var version, readerPID, writerPID int
	var generation int64
	var observedAt float64
	if err := reader.QueryRow(ctx, `SELECT status,lifecycle_phase,decision_kind,decision_id::text,
		version,lifecycle_generation,pg_backend_pid(),EXTRACT(EPOCH FROM clock_timestamp())
		FROM bookings WHERE id=$1`, parsed).Scan(
		&status, &phase, &kind, &rowDecisionID, &version, &generation, &readerPID, &observedAt); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&writerPID); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.StatusCancelled) || phase != "TERMINAL" || kind != "cancel" ||
		rowDecisionID != cancelDecision.String() || generation != 1 || version != 2 ||
		readerPID == writerPID {
		t.Fatalf("cancelled Booking row mismatch: status=%s phase=%s kind=%s decision=%s version=%d generation=%d reader=%d writer=%d",
			status, phase, kind, rowDecisionID, version, generation, readerPID, writerPID)
	}
	observation := map[string]any{
		"schemaVersion": 1, "kind": "booking_cancelled_read",
		"bookingId": parsed.String(), "generation": generation, "status": status,
		"lifecyclePhase": phase, "decisionKind": kind, "decisionId": rowDecisionID,
		"expectedDecisionId": cancelDecision.String(), "version": version,
		"command": map[string]any{"method": "BookingService.DecideLifecycle",
			"kind": "cancel", "reason": "user_cancelled", "invoked": true},
		"database": map[string]any{"database": manifest.Database,
			"systemIdentifier": manifest.ServerSystemIdentifier,
			"markerSha256":     fmt.Sprintf("%x", sha256.Sum256([]byte(manifest.DatabaseMarker))),
			"schema":           schema},
		"reader": map[string]any{"backendPid": readerPID, "writerBackendPid": writerPID,
			"applicationName": "booking_candidate_observer", "observedAt": observedAt},
		"observedAt":  observedAt,
		"timeBinding": "DECISION_TO_INDEPENDENT_READ_INTERVAL",
		"source":      map[string]any{"sha256": sourceSHA, "commit": sourceCommit, "tree": sourceTree},
		"runId":       runID, "attemptId": attempt,
		"evidenceMeta": map[string]any{"componentOnly": true, "formalEvidence": false,
			"source":        "REAL_BOOKING_SERVICE_AND_PG16_COMPONENT",
			"sourceBinding": "EMITTER_TEST_FILE_SHA256"},
	}
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(evidenceDir, "booking-cancelled-"+runID+".json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(observation); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func bookingComponentPools(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool, bookingOwnerManifest, string) {
	t.Helper()
	dsn := os.Getenv("BOOKING_COMPONENT_DATABASE_URL")
	manifestPath := os.Getenv("BOOKING_COMPONENT_OWNER_MANIFEST")
	if dsn == "" || manifestPath == "" || !filepath.IsAbs(manifestPath) {
		t.Fatal("BOOKING_COMPONENT_DATABASE_URL and absolute BOOKING_COMPONENT_OWNER_MANIFEST are required")
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal("read booking owner manifest")
	}
	var manifest bookingOwnerManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal("parse booking owner manifest")
	}
	expectedMarker := "codex-booking-component:v1:" + manifest.ResourceID + ":" +
		manifest.RunID + ":" + manifest.OwnerNonce
	if manifest.ManifestVersion != "rushgo.booking.component-pg-owner.v1" ||
		manifest.DatabaseMarker != expectedMarker ||
		manifest.ResourceID == "" || manifest.ContainerID != manifest.ResourceID ||
		manifest.Database == "" || manifest.Port < 1 || manifest.ServerSystemIdentifier == "" {
		t.Fatal("booking owner manifest is incomplete or unbound")
	}
	sum := sha256.Sum256([]byte(dsn))
	if hex.EncodeToString(sum[:]) != manifest.DSNSHA256 {
		t.Fatal("booking DSN does not match owner manifest")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("open manifested booking database")
	}
	t.Cleanup(admin.Close)
	var versionNum int
	var database, systemID, marker string
	if err := admin.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer,
		current_database(), system_identifier::text,
		COALESCE(shobj_description(oid,'pg_database'),'')
		FROM pg_database CROSS JOIN LATERAL pg_control_system()
		WHERE datname=current_database()`).Scan(&versionNum, &database, &systemID, &marker); err != nil {
		t.Fatal("identify manifested booking database")
	}
	if versionNum < 160000 || versionNum >= 170000 || database != manifest.Database ||
		systemID != manifest.ServerSystemIdentifier || marker != manifest.DatabaseMarker {
		t.Fatal("booking database identity differs from owner manifest")
	}
	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal("booking UUID extension setup")
	}
	schema := "booking_component_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal("create disposable booking schema")
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop booking component schema: %v", err)
		}
	})
	parsedDSN, err := url.Parse(dsn)
	if err != nil || (parsedDSN.Scheme != "postgres" && parsedDSN.Scheme != "postgresql") {
		t.Fatal("parse booking DSN")
	}
	query := parsedDSN.Query()
	query.Set("search_path", schema+",public")
	parsedDSN.RawQuery = query.Encode()
	isolated := parsedDSN.String()
	migrateBinary, err := exec.LookPath("migrate")
	if err != nil {
		t.Fatal("migrate CLI is not on PATH")
	}
	migrationPath := filepath.ToSlash(filepath.Join("..", "..", "migrations"))
	cmd := exec.CommandContext(ctx, migrateBinary, "-path", migrationPath, "-database", isolated, "up")
	if output, err := cmd.CombinedOutput(); err != nil {
		safe := strings.ReplaceAll(string(output), isolated, "[isolated DSN]")
		t.Fatalf("booking migrations failed: %s", safe)
	}
	writerConfig, _ := pgxpool.ParseConfig(isolated)
	writerConfig.ConnConfig.RuntimeParams["application_name"] = "booking_candidate_writer"
	writer, err := pgxpool.NewWithConfig(ctx, writerConfig)
	if err != nil {
		t.Fatal("open booking writer pool")
	}
	t.Cleanup(writer.Close)
	readerConfig, _ := pgxpool.ParseConfig(isolated)
	readerConfig.ConnConfig.RuntimeParams["application_name"] = "booking_candidate_observer"
	reader, err := pgxpool.NewWithConfig(ctx, readerConfig)
	if err != nil {
		t.Fatal("open booking independent reader pool")
	}
	t.Cleanup(reader.Close)
	return writer, reader, manifest, schema
}
