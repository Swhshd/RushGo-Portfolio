package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
)

func TestTicketOperationReceiptRejectsIncompleteCompletionIntegration(t *testing.T) {
	p, _ := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	now := time.Now().UTC()
	for _, tc := range []struct {
		name, operation                string
		event, result, version, expiry any
	}{
		{"rotation_missing_result", "ROTATE_CREDENTIAL", nil, nil, int64(2), now},
		{"rotation_missing_version", "ROTATE_CREDENTIAL", nil, "ROTATED", nil, now},
		{"rotation_missing_expiry", "ROTATE_CREDENTIAL", nil, "ROTATED", int64(2), nil},
		{"rotation_missing_all_results", "ROTATE_CREDENTIAL", nil, nil, nil, nil},
		{"checkin_missing_result", "CHECK_IN", f.ticketEventID, nil, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := sha256.Sum256([]byte(tc.name))
			_, err := p.Exec(context.Background(), `INSERT INTO ticket_operation_idempotency
				(actor_user_id,idempotency_key_digest,operation,request_fingerprint,ticket_id,event_id,result_code,credential_version,credential_expires_at,completed_at)
				VALUES($1,$2,$3,$2,$4,$5,$6,$7,$8,$9)`, f.userID, key[:], tc.operation, f.ticketID, tc.event, tc.result, tc.version, tc.expiry, now)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "ticket_operation_idempotency_result_ck" {
				t.Fatalf("incomplete completed receipt must violate result constraint, got %v", err)
			}
		})
	}
}

func TestConcurrentTicketCredentialRotationsWithSameKeyApplyOnceIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	const requests = 8
	start := make(chan struct{})
	type outcome struct {
		result domain.TicketCredentialRotationResult
		err    error
	}
	results := make(chan outcome, requests)
	for i := range requests {
		command := credentialRotationCommand(f.userID, f.ticketID, "same-key", fmt.Sprintf("synthetic-credential-%d", i))
		go func() {
			<-start
			result, err := repo.RotateTicketCredential(context.Background(), command)
			results <- outcome{result: result, err: err}
		}()
	}
	close(start)
	rotated, alreadyApplied := 0, 0
	for range requests {
		got := <-results
		if got.err != nil {
			t.Fatalf("same-key rotation: %v", got.err)
		}
		switch got.result.Outcome {
		case domain.TicketCredentialRotationRotated:
			rotated++
		case domain.TicketCredentialRotationAlreadyApplied:
			alreadyApplied++
		default:
			t.Fatalf("unexpected same-key outcome: %+v", got.result)
		}
	}
	if rotated != 1 || alreadyApplied != requests-1 {
		t.Fatalf("same-key concurrent outcomes rotated=%d already_applied=%d, want 1 and %d", rotated, alreadyApplied, requests-1)
	}
	if err := assertCredentialRotationState(t, p, f.ticketID, 2, 1); err != nil {
		t.Fatal(err)
	}
	t.Logf("same-key concurrency: requests=%d ROTATED=%d ALREADY_APPLIED=%d current_credential_rows=1 database_version=2 completed_rotation_receipts=1", requests, rotated, alreadyApplied)
	keyDigest := sha256.Sum256([]byte("same-key"))
	var storedKeyDigest []byte
	var storedVersion int64
	var storedOperation string
	if err := p.QueryRow(context.Background(), `SELECT idempotency_key_digest,credential_version,operation FROM ticket_operation_idempotency WHERE actor_user_id=$1 AND idempotency_key_digest=$2`, f.userID, keyDigest[:]).Scan(&storedKeyDigest, &storedVersion, &storedOperation); err != nil {
		t.Fatal(err)
	}
	if string(storedKeyDigest) != string(keyDigest[:]) || storedVersion != 2 || storedOperation != "ROTATE_CREDENTIAL" {
		t.Fatalf("rotation receipt identity/version mismatch: digest_len=%d version=%d operation=%q", len(storedKeyDigest), storedVersion, storedOperation)
	}
}

func TestTicketCredentialRotationReplayDoesNotRotateAndBindsTicketParametersIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	firstCommand := credentialRotationCommand(f.userID, f.ticketID, "lost-response-key", "first-credential")
	first, err := repo.RotateTicketCredential(context.Background(), firstCommand)
	if err != nil || first.Outcome != domain.TicketCredentialRotationRotated || first.CredentialVersion != 2 {
		t.Fatalf("first rotation=%+v error=%v", first, err)
	}

	retryCommand := credentialRotationCommand(f.userID, f.ticketID, "lost-response-key", "different-discarded-credential")
	retry, err := repo.RotateTicketCredential(context.Background(), retryCommand)
	if err != nil || retry.Outcome != domain.TicketCredentialRotationAlreadyApplied || retry.CredentialVersion != first.CredentialVersion || !retry.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("same-key retry=%+v error=%v, want ALREADY_APPLIED with the committed non-sensitive result", retry, err)
	}
	differentParameters := credentialRotationCommand(f.userID, f.ticketID, "lost-response-key", "different-parameters")
	differentFingerprint := sha256.Sum256([]byte("same key, different rotation request"))
	differentParameters.RequestFingerprint = differentFingerprint[:]
	if _, err = repo.RotateTicketCredential(context.Background(), differentParameters); !errors.Is(err, ErrTicketIdempotencyConflict) {
		t.Fatalf("same key with a different request fingerprint error=%v, want idempotency conflict", err)
	}
	if err := assertCredentialRotationState(t, p, f.ticketID, 2, 1); err != nil {
		t.Fatal(err)
	}

	otherTicket := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	if _, err = p.Exec(context.Background(), `UPDATE bookings SET user_id=$2 WHERE id=$1`, otherTicket.bookingID, f.userID); err != nil {
		t.Fatal(err)
	}
	otherTicketCommand := credentialRotationCommand(f.userID, otherTicket.ticketID, "lost-response-key", "other-ticket-credential")
	if _, err = repo.RotateTicketCredential(context.Background(), otherTicketCommand); !errors.Is(err, ErrTicketIdempotencyConflict) {
		t.Fatalf("same user/key on a different Ticket error=%v, want idempotency conflict", err)
	}
	if err = assertCredentialRotationState(t, p, otherTicket.ticketID, 1, 0); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentTicketCredentialRotationReportsRetryableInProgressIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	if _, err := p.Exec(context.Background(), `CREATE FUNCTION ticket_test_delay_credential_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP='INSERT' THEN PERFORM pg_sleep(2); END IF; RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), `CREATE TRIGGER ticket_test_delay_credential_write BEFORE INSERT OR UPDATE ON ticket_credentials FOR EACH ROW EXECUTE FUNCTION ticket_test_delay_credential_write()`); err != nil {
		t.Fatal(err)
	}
	firstCommand := credentialRotationCommand(f.userID, f.ticketID, "slow-operation-key", "slow-first-credential")
	type result struct {
		value domain.TicketCredentialRotationResult
		err   error
	}
	firstDone := make(chan result, 1)
	go func() {
		value, err := repo.RotateTicketCredential(context.Background(), firstCommand)
		firstDone <- result{value: value, err: err}
	}()

	deadline := time.Now().Add(3 * time.Second)
	rotationIsPaused := false
	for time.Now().Before(deadline) {
		var waiting int
		if err := p.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='PgSleep' AND query ILIKE '%INSERT INTO ticket_credentials%'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			rotationIsPaused = true
			break
		}
		select {
		case got := <-firstDone:
			t.Fatalf("first rotation finished before the controlled pause: result=%+v error=%v", got.value, got.err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !rotationIsPaused {
		got := <-firstDone
		t.Fatalf("did not observe the first rotation inside its write transaction: result=%+v error=%v", got.value, got.err)
	}
	secondCommand := credentialRotationCommand(f.userID, f.ticketID, firstCommand.IdempotencyKey, "slow-discarded-credential")
	if _, err := repo.RotateTicketCredential(context.Background(), secondCommand); !errors.Is(err, ErrTicketIdempotencyInProgress) {
		t.Fatalf("concurrent same-key request error=%v, want retryable in-progress", err)
	}
	first := <-firstDone
	if first.err != nil || first.value.Outcome != domain.TicketCredentialRotationRotated {
		t.Fatalf("first rotation result=%+v error=%v", first.value, first.err)
	}
	if err := assertCredentialRotationState(t, p, f.ticketID, 2, 1); err != nil {
		t.Fatal(err)
	}
	t.Logf("same-key in-progress result: first=ROTATED second=IN_PROGRESS current_credential_rows=1 database_version=2 completed_rotation_receipts=1")
}

func TestTicketOperationIdempotencyRejectsCredentialRotationAndCheckInKeyReuseIntegration(t *testing.T) {
	t.Run("rotation then check-in", func(t *testing.T) {
		p, repo := newBookingOutboxIntegrationFixture(t)
		f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
		key := "shared-operation-key"
		if _, err := repo.RotateTicketCredential(context.Background(), credentialRotationCommand(f.userID, f.ticketID, key, "rotation-token")); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(context.Background(), `INSERT INTO event_scanner_assignments(event_id,staff_user_id,active,granted_by,granted_at) VALUES($1,$2,true,$3,clock_timestamp())`, f.ticketEventID, f.userID, uuid.New()); err != nil {
			t.Fatal(err)
		}
		command := acceptedTicketScanFixture(f, time.Now().UTC())
		command.StaffUserID = f.userID
		command.IdempotencyKey = key
		if _, err := repo.CommitTicketCheckIn(context.Background(), command); !errors.Is(err, ErrTicketIdempotencyConflict) {
			t.Fatalf("same user/key reused for check-in after rotation: %v, want conflict", err)
		}
		var status string
		if err := p.QueryRow(context.Background(), `SELECT status FROM tickets WHERE id=$1`, f.ticketID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != string(domain.TicketIssued) {
			t.Fatalf("conflicting check-in changed Ticket status to %s", status)
		}
	})

	t.Run("check-in then rotation", func(t *testing.T) {
		p, repo := newBookingOutboxIntegrationFixture(t)
		f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
		if _, err := p.Exec(context.Background(), `INSERT INTO event_scanner_assignments(event_id,staff_user_id,active,granted_by,granted_at) VALUES($1,$2,true,$3,clock_timestamp())`, f.ticketEventID, f.userID, uuid.New()); err != nil {
			t.Fatal(err)
		}
		command := acceptedTicketScanFixture(f, time.Now().UTC())
		command.StaffUserID = f.userID
		command.IdempotencyKey = "shared-operation-key"
		if result, err := repo.CommitTicketCheckIn(context.Background(), command); err != nil || result.ResultCode != "accepted" {
			t.Fatalf("first check-in=%+v error=%v", result, err)
		}
		if _, err := repo.RotateTicketCredential(context.Background(), credentialRotationCommand(f.userID, f.ticketID, command.IdempotencyKey, "rotation-token")); !errors.Is(err, ErrTicketIdempotencyConflict) {
			t.Fatalf("same user/key reused for rotation after check-in: %v, want conflict", err)
		}
		if err := assertCredentialRotationState(t, p, f.ticketID, 1, 0); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTicketCredentialRotationFailureLeavesNoIdempotencyReceiptIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	if _, err := p.Exec(context.Background(), `CREATE FUNCTION ticket_test_fail_credential_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic credential write failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), `CREATE TRIGGER ticket_test_fail_credential_write BEFORE INSERT OR UPDATE ON ticket_credentials FOR EACH ROW EXECUTE FUNCTION ticket_test_fail_credential_write()`); err != nil {
		t.Fatal(err)
	}
	command := credentialRotationCommand(f.userID, f.ticketID, "rollback-key", "must-rollback")
	if _, err := repo.RotateTicketCredential(context.Background(), command); err == nil {
		t.Fatal("synthetic credential write failure unexpectedly succeeded")
	}
	if err := assertCredentialRotationState(t, p, f.ticketID, 1, 0); err != nil {
		t.Fatal(err)
	}
	keyDigest := sha256.Sum256([]byte(command.IdempotencyKey))
	var receipts int
	if err := p.QueryRow(context.Background(), `SELECT count(*) FROM ticket_operation_idempotency WHERE actor_user_id=$1 AND idempotency_key_digest=$2`, f.userID, keyDigest[:]).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 0 {
		t.Fatalf("failed rotation left %d idempotency receipts", receipts)
	}
}

func TestTicketCredentialRotationRejectsLegacyCheckInKeyIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	command := acceptedTicketScanFixture(f, time.Now().UTC())
	command.IdempotencyKey = "legacy-scan-key"
	if _, err := p.Exec(context.Background(), `INSERT INTO ticket_scan_requests(staff_user_id,idempotency_key,request_fingerprint,event_id,ticket_id,result_code,reason_code,dispute_state,refund_state,evidence_source)
		VALUES($1,$2,$3,$4,$5,'denied','credential_expired','provider_observed_no_dispute','provider_observed_no_refund','stripe_test_api')`, f.userID, command.IdempotencyKey, command.RequestFingerprint, f.ticketEventID, f.ticketID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.RotateTicketCredential(context.Background(), credentialRotationCommand(f.userID, f.ticketID, command.IdempotencyKey, "rotation-token")); !errors.Is(err, ErrTicketIdempotencyConflict) {
		t.Fatalf("rotation reused a key already held by a legacy check-in: %v", err)
	}
}

func credentialRotationCommand(userID, ticketID uuid.UUID, key, credential string) domain.TicketCredentialRotationCommand {
	fingerprint := sha256.Sum256(append([]byte("ROTATE_CREDENTIAL\x00"), ticketID[:]...))
	digest := sha256.Sum256([]byte(credential))
	return domain.TicketCredentialRotationCommand{
		UserID: userID, TicketID: ticketID, IdempotencyKey: key,
		RequestFingerprint: fingerprint[:], CredentialDigest: digest[:],
		ExpiresAt: time.Now().UTC().Add(10 * time.Minute),
	}
}

func assertCredentialRotationState(t *testing.T, p *pgxpool.Pool, ticketID uuid.UUID, wantVersion int64, wantReceipts int) error {
	t.Helper()
	var version int64
	var credentialRows int
	if err := p.QueryRow(context.Background(), `SELECT credential_version,(SELECT count(*) FROM ticket_credentials WHERE ticket_id=$1) FROM ticket_credentials WHERE ticket_id=$1`, ticketID).Scan(&version, &credentialRows); err != nil {
		return fmt.Errorf("read current Ticket credential: %w", err)
	}
	if version != wantVersion || credentialRows != 1 {
		return fmt.Errorf("credential rows=%d version=%d, want one row version %d", credentialRows, version, wantVersion)
	}
	var receipts int
	if err := p.QueryRow(context.Background(), `SELECT count(*) FROM ticket_operation_idempotency WHERE ticket_id=$1 AND operation='ROTATE_CREDENTIAL' AND completed_at IS NOT NULL`, ticketID).Scan(&receipts); err != nil {
		return fmt.Errorf("read completed rotation receipts: %w", err)
	}
	if receipts != wantReceipts {
		return fmt.Errorf("completed rotation receipts=%d, want %d", receipts, wantReceipts)
	}
	return nil
}
