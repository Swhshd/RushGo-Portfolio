//go:build admin_event_operations_e2e && ticket_e2e

// This standalone tagged command exercises the existing repository check-in
// transaction against the run-scoped database. Its provider evidence is a
// synthetic test fixture and must never be represented as a live payment call.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/database"
	"go.uber.org/zap"
)

const fixtureAck = "I_ACKNOWLEDGE_ISOLATED_ADMIN_EVENT_OPERATIONS_CHECKIN_FIXTURE"

var runIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
var postgresURLPasswordPattern = regexp.MustCompile(`(?i)(postgres(ql)?://[^:/@?#\s]+:)[^@/?#\s]*(@)`)
var observedCheckinDeltaMillis int64

type ticketFixture struct {
	TicketID            string `json:"ticket_id"`
	CredentialDigestHex string `json:"credential_digest_hex"`
	IdempotencyKey      string `json:"idempotency_key"`
}

type fixtureManifest struct {
	Acknowledgement   string          `json:"acknowledgement"`
	RunID             string          `json:"run_id"`
	PrivateRuntimeDir string          `json:"private_runtime_dir"`
	DatabaseURL       string          `json:"database_url"`
	EventID           string          `json:"event_id"`
	StaffUserID       string          `json:"staff_user_id"`
	ActorUserID       string          `json:"actor_user_id"`
	Tickets           []ticketFixture `json:"tickets"`
}

func main() {
	if len(os.Args) != 3 || os.Args[1] != "seed-checkins" {
		refuse("usage")
	}
	if os.Getenv("ADMIN_EVENT_OPERATIONS_CHECKIN_FIXTURE_ACK") != fixtureAck {
		refuse("acknowledgement")
	}
	if err := run(os.Args[2]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, maskURLpassword(err.Error()))
		refuse("fixture")
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"status": "PASS", "accepted": 2, "replayed": 1,
		"different_key": "already_checked_in", "audit_rows_after_replay": 1,
		"distinct_ticket_audit_rows": 1, "concurrent_same_key_one_replay": true,
		"concurrent_distinct_tickets": true, "checkin_delta_ms": observedCheckinDeltaMillis,
		"authority": "synthetic_repository_fixture",
	})
}

func refuse(stage string) {
	_, _ = fmt.Fprintf(os.Stdout, "{\"status\":\"FAIL\",\"stage\":%q}\n", stage)
	os.Exit(2)
}

func run(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("absolute fixture path required")
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) > 64*1024 {
		return errors.New("fixture manifest unavailable")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var manifest fixtureManifest
	if err := decoder.Decode(&manifest); err != nil {
		return errors.New("fixture manifest invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("fixture manifest has trailing data")
	}
	runtimeDir, err := filepath.Abs(manifest.PrivateRuntimeDir)
	if err != nil || filepath.Base(filepath.Clean(runtimeDir)) != manifest.RunID || !runIDPattern.MatchString(manifest.RunID) || !pathWithin(runtimeDir, path) {
		return errors.New("private runtime identity mismatch")
	}
	if manifest.Acknowledgement != fixtureAck || len(manifest.Tickets) != 2 {
		return errors.New("fixture scope mismatch")
	}
	databaseURL, err := url.Parse(manifest.DatabaseURL)
	if err != nil {
		return fmt.Errorf("fixture database URL invalid: %w", err)
	}
	if err := validateFixtureDatabaseScope(databaseURL, manifest.RunID); err != nil {
		return err
	}
	eventID, err := uuid.Parse(manifest.EventID)
	if err != nil || eventID == uuid.Nil {
		return errors.New("event identity invalid")
	}
	staffID, err := uuid.Parse(manifest.StaffUserID)
	if err != nil || staffID == uuid.Nil {
		return errors.New("scanner identity invalid")
	}
	actorID, err := uuid.Parse(manifest.ActorUserID)
	if err != nil || actorID == uuid.Nil {
		return errors.New("actor identity invalid")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := database.NewPostgresPool(ctx, manifest.DatabaseURL, zap.NewNop())
	if err != nil {
		return fmt.Errorf("Booking database unavailable: %w", err)
	}
	defer pool.Close()
	repo := repository.NewPostgresBookingRepository(pool)
	if _, err := repo.SetScannerAssignment(ctx, eventID, staffID, actorID, true); err != nil {
		return fmt.Errorf("scanner assignment seed failed: %w", err)
	}

	commands := make([]domain.TicketScanCommand, 0, len(manifest.Tickets))
	for _, fixture := range manifest.Tickets {
		ticketID, parseErr := uuid.Parse(fixture.TicketID)
		if parseErr != nil || ticketID == uuid.Nil || fixture.IdempotencyKey == "" {
			return errors.New("ticket fixture identity invalid")
		}
		digest, decodeErr := hex.DecodeString(fixture.CredentialDigestHex)
		if decodeErr != nil || len(digest) != sha256.Size {
			return errors.New("ticket credential digest invalid")
		}
		fingerprint := sha256.Sum256([]byte(eventID.String() + "\n" + ticketID.String() + "\n" + fixture.CredentialDigestHex))
		commands = append(commands, scanCommand(eventID, staffID, ticketID, digest, fingerprint[:], fixture.IdempotencyKey))
	}
	if commands[0].TicketID == commands[1].TicketID {
		return errors.New("fixture tickets must be distinct")
	}
	type attemptResult struct {
		index  int
		result domain.TicketScanResult
		err    error
	}
	started := make(chan struct{})
	results := make(chan attemptResult, 3)
	for _, index := range []int{0, 0, 1} {
		go func(index int) {
			<-started
			attemptCtx, attemptCancel := context.WithTimeout(ctx, 8*time.Second)
			defer attemptCancel()
			result, callErr := repo.CommitTicketCheckIn(attemptCtx, commands[index])
			results <- attemptResult{index: index, result: result, err: callErr}
		}(index)
	}
	close(started)
	var sameKey []domain.TicketScanResult
	var secondTicket *domain.TicketScanResult
	for range 3 {
		attempt := <-results
		if attempt.err != nil {
			return fmt.Errorf("concurrent repository check-in failed: %w", attempt.err)
		}
		if attempt.result.ResultCode != "accepted" || attempt.result.CheckedInAt == nil {
			return errors.New("concurrent repository check-in failed")
		}
		if attempt.index == 0 {
			sameKey = append(sameKey, attempt.result)
		} else {
			copy := attempt.result
			secondTicket = &copy
		}
	}
	if len(sameKey) != 2 || secondTicket == nil || sameKey[0].Replayed == sameKey[1].Replayed {
		return errors.New("concurrent same-key calls did not yield one replay")
	}
	var original, replay domain.TicketScanResult
	if sameKey[0].Replayed {
		original, replay = sameKey[1], sameKey[0]
	} else {
		original, replay = sameKey[0], sameKey[1]
	}
	if !original.CheckedInAt.Equal(*replay.CheckedInAt) {
		return errors.New("concurrent same-key replay changed the check-in time")
	}
	checkinDelta := original.CheckedInAt.Sub(*secondTicket.CheckedInAt)
	if checkinDelta < 0 {
		checkinDelta = -checkinDelta
	}
	if checkinDelta > 5*time.Second {
		return errors.New("concurrent fixture check-ins were not temporally close")
	}
	observedCheckinDeltaMillis = checkinDelta.Milliseconds()
	count, err := acceptedAuditCount(ctx, pool, commands[0].TicketID)
	if err != nil {
		return fmt.Errorf("concurrent same-key audit query failed: %w", err)
	}
	if count != 1 {
		return errors.New("concurrent same-key calls created multiple audits")
	}
	otherCount, err := acceptedAuditCount(ctx, pool, commands[1].TicketID)
	if err != nil {
		return fmt.Errorf("concurrent distinct-ticket audit query failed: %w", err)
	}
	if otherCount != 1 {
		return errors.New("concurrent distinct-ticket call did not create one audit")
	}
	differentKey := commands[0]
	differentKey.IdempotencyKey += "-second"
	secondAttempt, err := repo.CommitTicketCheckIn(ctx, differentKey)
	if err != nil {
		return fmt.Errorf("different-key repository recheck failed: %w", err)
	}
	if secondAttempt.ResultCode != "already_checked_in" || secondAttempt.Replayed {
		return errors.New("different-key repository recheck result mismatch")
	}
	count, err = acceptedAuditCount(ctx, pool, commands[0].TicketID)
	if err != nil {
		return fmt.Errorf("different-key audit query failed: %w", err)
	}
	if count != 1 {
		return errors.New("different-key recheck created an extra audit")
	}
	return nil
}

func validateFixtureDatabaseScope(databaseURL *url.URL, runID string) error {
	if databaseURL == nil || !runIDPattern.MatchString(runID) {
		return errors.New("owner-specific loopback Booking database required")
	}
	if (databaseURL.Scheme != "postgres" && databaseURL.Scheme != "postgresql") || databaseURL.User == nil ||
		!strings.HasPrefix(databaseURL.User.Username(), "evopsbook_") || !loopbackHost(databaseURL.Hostname()) || databaseURL.Port() == "" {
		return errors.New("owner-specific loopback Booking database required")
	}
	legacyDatabase := "/admin_event_operations_" + runID[:12] + "_booking"
	unifiedDatabase := "/admin_unified_preview_" + runID[:12] + "_booking"
	if databaseURL.Path != legacyDatabase && databaseURL.Path != unifiedDatabase {
		return errors.New("owner-specific loopback Booking database required")
	}
	return nil
}

func maskURLpassword(message string) string {
	return postgresURLPasswordPattern.ReplaceAllString(message, `${1}[REDACTED]${3}`)
}

func scanCommand(eventID, staffID, ticketID uuid.UUID, credentialDigest, fingerprint []byte, key string) domain.TicketScanCommand {
	now := time.Now()
	return domain.TicketScanCommand{
		EventID: eventID, StaffUserID: staffID, TicketID: ticketID,
		CredentialDigest:   append([]byte(nil), credentialDigest...),
		RequestFingerprint: append([]byte(nil), fingerprint...), IdempotencyKey: key,
		Decision: domain.TicketIssuanceDecision{Eligible: true},
		Evidence: domain.TicketAuthorityEvidence{
			SagaEligible: true, PaymentEligible: true, EventEligible: true, PaymentDecisionKnown: true,
			DisputeState: domain.DisputeNoDispute, RefundState: domain.RefundNoRefund,
			ObservedAt: now.UTC(), PaymentQueryStartedAt: now, PaymentQueryWallStartedAt: now,
			Source: "stripe_test_api",
		},
	}
}

func acceptedAuditCount(ctx context.Context, pool *pgxpool.Pool, ticketID uuid.UUID) (int, error) {
	var count int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1 AND result_code='accepted'`, ticketID).Scan(&count)
	return count, err
}

func pathWithin(root, candidate string) bool {
	rootPath, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	candidatePath, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(rootPath, candidatePath)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func loopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}
