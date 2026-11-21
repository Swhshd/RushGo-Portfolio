package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/admission"
)

var bookingAdmissionMatrixFence atomic.Int64

func TestBookingAdmissionActiveMatrixIntegration(t *testing.T) {
	p, repo := newBookingOutboxIntegrationFixture(t)
	ctx := context.Background()
	if _, err := p.Exec(ctx, `UPDATE booking_admission_mode SET active=TRUE WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}

	t.Run("committed check-in wins over late abort", func(t *testing.T) {
		f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
		command, identity, permit := prepareBookingAdmissionMatrixAttempt(t, p, repo, f, f.staffID, "matrix-late-abort")

		result, err := repo.CommitTicketCheckIn(ctx, command)
		if err != nil || result.ResultCode != "accepted" {
			t.Fatalf("check-in=%+v error=%v, want accepted", result, err)
		}
		for range 2 {
			outcome, resolveErr := repo.ResolveAdmission(ctx, identity, permit.Token, permit.Fence, true)
			if resolveErr != nil || outcome.State != admission.Committed || outcome.ResultCode != "accepted" || outcome.Token != permit.Token || outcome.Fence != permit.Fence {
				t.Fatalf("late abort outcome=%+v error=%v, want immutable committed result", outcome, resolveErr)
			}
		}
		assertBookingAdmissionIntentMatrix(t, p, identity.RequestID, admission.Committed, "accepted", permit)
		assertBookingAdmissionTicketMatrix(t, p, f.ticketID, string(domain.TicketCheckedIn), 1)
	})

	t.Run("two scanners receive one accepted and one already-checked-in outcome", func(t *testing.T) {
		f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
		secondScanner := uuid.New()
		if _, err := repo.SetScannerAssignment(ctx, f.ticketEventID, secondScanner, uuid.New(), true); err != nil {
			t.Fatal(err)
		}
		first, firstIdentity, firstPermit := prepareBookingAdmissionMatrixAttempt(t, p, repo, f, f.staffID, "matrix-two-scanners-a")
		second, secondIdentity, secondPermit := prepareBookingAdmissionMatrixAttempt(t, p, repo, f, secondScanner, "matrix-two-scanners-b")

		firstResult, err := repo.CommitTicketCheckIn(ctx, first)
		if err != nil || firstResult.ResultCode != "accepted" {
			t.Fatalf("first scanner check-in=%+v error=%v, want accepted", firstResult, err)
		}
		secondResult, err := repo.CommitTicketCheckIn(ctx, second)
		if err != nil || secondResult.ResultCode != "already_checked_in" {
			t.Fatalf("second scanner check-in=%+v error=%v, want already_checked_in", secondResult, err)
		}
		firstOutcome, err := repo.ResolveAdmission(ctx, firstIdentity, firstPermit.Token, firstPermit.Fence, false)
		if err != nil || firstOutcome.State != admission.Committed || firstOutcome.ResultCode != "accepted" {
			t.Fatalf("first scanner outcome=%+v error=%v, want committed accepted", firstOutcome, err)
		}
		secondOutcome, err := repo.ResolveAdmission(ctx, secondIdentity, secondPermit.Token, secondPermit.Fence, false)
		if err != nil || secondOutcome.State != admission.Aborted {
			t.Fatalf("second scanner outcome=%+v error=%v, want aborted already-checked-in", secondOutcome, err)
		}
		assertBookingAdmissionIntentMatrix(t, p, firstIdentity.RequestID, admission.Committed, "accepted", firstPermit)
		assertBookingAdmissionIntentMatrix(t, p, secondIdentity.RequestID, admission.Aborted, "already_checked_in", secondPermit)
		assertBookingAdmissionTicketMatrix(t, p, f.ticketID, string(domain.TicketCheckedIn), 1)
		var acceptedReceipts, alreadyCheckedInReceipts int
		if err := p.QueryRow(ctx, `SELECT count(*) FILTER (WHERE result_code='accepted'),count(*) FILTER (WHERE result_code='already_checked_in')
			FROM ticket_scan_requests WHERE ticket_id=$1`, f.ticketID).Scan(&acceptedReceipts, &alreadyCheckedInReceipts); err != nil {
			t.Fatal(err)
		}
		if acceptedReceipts != 1 || alreadyCheckedInReceipts != 1 {
			t.Fatalf("scan receipts accepted=%d already_checked_in=%d, want one each", acceptedReceipts, alreadyCheckedInReceipts)
		}
	})

	t.Run("a check-in leaves another ticket permit for the same payment unchanged", func(t *testing.T) {
		firstFixture := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
		secondFixture := seedSecondTicketForAdmissionMatrix(t, p, firstFixture)
		first, firstIdentity, firstPermit := prepareBookingAdmissionMatrixAttempt(t, p, repo, firstFixture, firstFixture.staffID, "matrix-shared-payment-a")
		second, secondIdentity, secondPermit := prepareBookingAdmissionMatrixAttempt(t, p, repo, secondFixture, secondFixture.staffID, "matrix-shared-payment-b")
		if firstIdentity.PaymentID != secondIdentity.PaymentID {
			t.Fatal("fixture Tickets do not share their authoritative Payment identity")
		}

		result, err := repo.CommitTicketCheckIn(ctx, first)
		if err != nil || result.ResultCode != "accepted" {
			t.Fatalf("first Ticket check-in=%+v error=%v, want accepted", result, err)
		}
		assertBookingAdmissionIntentMatrix(t, p, secondIdentity.RequestID, admission.Pending, "", secondPermit)
		assertBookingAdmissionTicketMatrix(t, p, secondFixture.ticketID, string(domain.TicketIssued), 0)
		result, err = repo.CommitTicketCheckIn(ctx, second)
		if err != nil || result.ResultCode != "accepted" {
			t.Fatalf("second Ticket check-in=%+v error=%v, want accepted", result, err)
		}
		assertBookingAdmissionIntentMatrix(t, p, firstIdentity.RequestID, admission.Committed, "accepted", firstPermit)
		assertBookingAdmissionIntentMatrix(t, p, secondIdentity.RequestID, admission.Committed, "accepted", secondPermit)
		assertBookingAdmissionTicketMatrix(t, p, firstFixture.ticketID, string(domain.TicketCheckedIn), 1)
		assertBookingAdmissionTicketMatrix(t, p, secondFixture.ticketID, string(domain.TicketCheckedIn), 1)
	})

	for _, tc := range []struct {
		name       string
		key        string
		mutate     func(*testing.T, ticketRepositoryFixture, *PostgresBookingRepository)
		wantErr    error
		wantReason string
		wantStatus string
	}{
		{
			name: "scanner revoked after permit",
			key:  "matrix-denial-scanner-revoked",
			mutate: func(t *testing.T, f ticketRepositoryFixture, repo *PostgresBookingRepository) {
				if _, err := repo.SetScannerAssignment(ctx, f.ticketEventID, f.staffID, uuid.New(), false); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: ErrScannerNotAssigned, wantStatus: string(domain.TicketIssued),
		},
		{
			name: "Ticket revoked after permit",
			key:  "matrix-denial-ticket-revoked",
			mutate: func(t *testing.T, f ticketRepositoryFixture, _ *PostgresBookingRepository) {
				if _, err := p.Exec(ctx, `UPDATE tickets SET status='REVOKED' WHERE id=$1`, f.ticketID); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: "ticket_revoked", wantStatus: string(domain.TicketRevoked),
		},
		{
			name: "Booking cancelled after permit",
			key:  "matrix-denial-booking-cancelled",
			mutate: func(t *testing.T, f ticketRepositoryFixture, _ *PostgresBookingRepository) {
				if _, err := p.Exec(ctx, `UPDATE bookings SET status='CANCELLED' WHERE id=$1`, f.bookingID); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: "booking_not_eligible", wantStatus: string(domain.TicketIssued),
		},
		{
			name: "credential rotated after permit",
			key:  "matrix-denial-credential-rotated",
			mutate: func(t *testing.T, f ticketRepositoryFixture, repo *PostgresBookingRepository) {
				newCredential := sha256.Sum256([]byte("matrix rotated credential:" + f.ticketID.String()))
				fingerprint := sha256.Sum256([]byte("matrix credential rotation:" + f.ticketID.String()))
				_, err := repo.RotateTicketCredential(ctx, domain.TicketCredentialRotationCommand{
					UserID: f.userID, TicketID: f.ticketID, IdempotencyKey: "matrix-credential-rotation",
					RequestFingerprint: fingerprint[:], CredentialDigest: newCredential[:], ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
				})
				if err != nil {
					t.Fatal(err)
				}
			},
			wantReason: "credential_rotated", wantStatus: string(domain.TicketIssued),
		},
		{
			name:       "payment evidence stale after permit",
			key:        "matrix-denial-stale-evidence",
			mutate:     func(*testing.T, ticketRepositoryFixture, *PostgresBookingRepository) {},
			wantReason: "payment_evidence_stale", wantStatus: string(domain.TicketIssued),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
			command, identity, permit := prepareBookingAdmissionMatrixAttempt(t, p, repo, f, f.staffID, tc.key)
			tc.mutate(t, f, repo)
			if tc.name == "payment evidence stale after permit" {
				command.Evidence.ObservedAt = time.Now().UTC().Add(-domain.MaxPaymentEvidenceAge - time.Second)
			}

			result, err := repo.CommitTicketCheckIn(ctx, command)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("check-in error=%v, want %v", err, tc.wantErr)
				}
				assertBookingAdmissionIntentMatrix(t, p, identity.RequestID, admission.Pending, "", permit)
				outcome, resolveErr := repo.ResolveAdmission(ctx, identity, permit.Token, permit.Fence, true)
				if resolveErr != nil || outcome.State != admission.Aborted || outcome.Token != permit.Token || outcome.Fence != permit.Fence {
					t.Fatalf("explicit abort outcome=%+v error=%v, want durable abort for pending permit", outcome, resolveErr)
				}
				assertBookingAdmissionIntentMatrix(t, p, identity.RequestID, admission.Aborted, "", permit)
				var receiptCount int
				if err := p.QueryRow(ctx, `SELECT count(*) FROM ticket_scan_requests WHERE ticket_id=$1 AND staff_user_id=$2`, f.ticketID, f.staffID).Scan(&receiptCount); err != nil {
					t.Fatal(err)
				}
				if receiptCount != 0 {
					t.Fatalf("revoked scanner wrote %d scan receipts, want none", receiptCount)
				}
			} else {
				if err != nil || result.ResultCode != "denied" || result.ReasonCode != tc.wantReason {
					t.Fatalf("check-in=%+v error=%v, want denied %s", result, err, tc.wantReason)
				}
				assertBookingAdmissionIntentMatrix(t, p, identity.RequestID, admission.Aborted, "denied", permit)
				var receiptCount int
				if err := p.QueryRow(ctx, `SELECT count(*) FROM ticket_scan_requests WHERE ticket_id=$1 AND staff_user_id=$2 AND result_code='denied'`, f.ticketID, f.staffID).Scan(&receiptCount); err != nil {
					t.Fatal(err)
				}
				if receiptCount != 1 {
					t.Fatalf("denial receipt count=%d, want one", receiptCount)
				}
			}
			assertBookingAdmissionTicketMatrix(t, p, f.ticketID, tc.wantStatus, 0)
		})
	}

	t.Run("wrong identity binding cannot resolve an intent", func(t *testing.T) {
		f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
		_, identity, permit := prepareBookingAdmissionMatrixAttempt(t, p, repo, f, f.staffID, "matrix-wrong-identity")
		wrong := identity
		wrong.BookingID = uuid.New()
		if _, err := repo.ResolveAdmission(ctx, wrong, permit.Token, permit.Fence, true); !errors.Is(err, admission.ErrConflict) {
			t.Fatalf("wrong identity resolve error=%v, want identity conflict", err)
		}
		assertBookingAdmissionIntentMatrix(t, p, identity.RequestID, admission.Pending, "", permit)
	})

	t.Run("wrong Event binding cannot check in", func(t *testing.T) {
		f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
		command, identity, permit := prepareBookingAdmissionMatrixAttempt(t, p, repo, f, f.staffID, "matrix-wrong-event")
		command.EventID = uuid.New()
		if _, err := repo.CommitTicketCheckIn(ctx, command); !errors.Is(err, admission.ErrConflict) {
			t.Fatalf("wrong Event check-in error=%v, want identity conflict", err)
		}
		assertBookingAdmissionIntentMatrix(t, p, identity.RequestID, admission.Pending, "", permit)
		assertBookingAdmissionTicketMatrix(t, p, f.ticketID, string(domain.TicketIssued), 0)
	})

	t.Run("wrong Payment binding cannot attach a permit", func(t *testing.T) {
		f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
		_, identity, originalPermit := prepareBookingAdmissionMatrixAttempt(t, p, repo, f, f.staffID, "matrix-wrong-payment")
		wrong := identity
		wrong.PaymentID = uuid.New()
		permit := admission.Permit{Identity: wrong, Token: uuid.New(), Fence: 71, State: admission.Pending}
		if err := repo.AttachAdmissionPermit(ctx, permit); !errors.Is(err, admission.ErrConflict) {
			t.Fatalf("wrong Payment permit attach error=%v, want identity conflict", err)
		}
		stored, err := loadAdmissionMatrixPermit(t, p, identity.RequestID)
		if err != nil || stored != originalPermit {
			t.Fatalf("stored permit after rejected binding=%+v error=%v, want original permit unchanged", stored, err)
		}
	})

	t.Run("wrong fence cannot consume an attached permit", func(t *testing.T) {
		f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
		command, identity, permit := prepareBookingAdmissionMatrixAttempt(t, p, repo, f, f.staffID, "matrix-wrong-fence")
		wrong := permit
		wrong.Fence++
		command.Admission = &wrong
		if _, err := repo.CommitTicketCheckIn(ctx, command); !errors.Is(err, admission.ErrStale) {
			t.Fatalf("wrong fence check-in error=%v, want stale permit", err)
		}
		assertBookingAdmissionIntentMatrix(t, p, identity.RequestID, admission.Pending, "", permit)
		assertBookingAdmissionTicketMatrix(t, p, f.ticketID, string(domain.TicketIssued), 0)
	})
}

func prepareBookingAdmissionMatrixAttempt(t *testing.T, p *pgxpool.Pool, repo *PostgresBookingRepository, f ticketRepositoryFixture, actorID uuid.UUID, key string) (domain.TicketScanCommand, admission.Identity, admission.Permit) {
	t.Helper()
	command := acceptedTicketScanFixture(f, time.Now().UTC())
	command.StaffUserID = actorID
	command.IdempotencyKey = key
	fingerprint := sha256.Sum256([]byte("booking admission matrix fingerprint:" + key))
	command.RequestFingerprint = append([]byte(nil), fingerprint[:]...)
	identity := admissionIdentityForTicketFixture(f, actorID, key, command.RequestFingerprint)
	permit := admission.Permit{Identity: identity, Token: uuid.New(), Fence: bookingAdmissionMatrixFence.Add(1), State: admission.Pending}
	if err := repo.EnsureAdmissionIntent(context.Background(), identity); err != nil {
		t.Fatalf("persist admission intent: %v", err)
	}
	if err := repo.AttachAdmissionPermit(context.Background(), permit); err != nil {
		t.Fatalf("attach admission permit: %v", err)
	}
	command.Admission = &permit
	return command, identity, permit
}

func seedSecondTicketForAdmissionMatrix(t *testing.T, p *pgxpool.Pool, original ticketRepositoryFixture) ticketRepositoryFixture {
	t.Helper()
	ctx := context.Background()
	var itemID, sagaID, paymentID, eventID, tierID uuid.UUID
	if err := p.QueryRow(ctx, `SELECT booking_item_id,saga_id,payment_id,event_id,ticket_tier_id FROM tickets WHERE id=$1`, original.ticketID).
		Scan(&itemID, &sagaID, &paymentID, &eventID, &tierID); err != nil {
		t.Fatal(err)
	}
	seatID, ticketID := uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `UPDATE booking_items SET quantity=quantity+1,seat_ids=array_append(seat_ids,$2) WHERE id=$1`, itemID, seatID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation,status)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,1,'ISSUED')`, ticketID, original.bookingID, itemID, sagaID, paymentID, eventID, tierID, seatID); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("matrix second ticket credential:" + ticketID.String()))
	if _, err := p.Exec(ctx, `INSERT INTO ticket_credentials(ticket_id,token_digest,expires_at,credential_version)
		VALUES($1,$2,clock_timestamp()+interval '5 minutes',1)`, ticketID, digest[:]); err != nil {
		t.Fatal(err)
	}
	second := original
	second.ticketID = ticketID
	second.digest = append([]byte(nil), digest[:]...)
	return second
}

func assertBookingAdmissionIntentMatrix(t *testing.T, p *pgxpool.Pool, requestID uuid.UUID, wantState admission.State, wantResult string, wantPermit admission.Permit) {
	t.Helper()
	var state, result string
	var token uuid.UUID
	var fence int64
	err := p.QueryRow(context.Background(), `SELECT state,COALESCE(result_code,''),COALESCE(permit_token,'00000000-0000-0000-0000-000000000000'::uuid),COALESCE(permit_fence,0)
		FROM ticket_admission_intents WHERE request_id=$1`, requestID).Scan(&state, &result, &token, &fence)
	if err != nil {
		t.Fatal(err)
	}
	if admission.State(state) != wantState || result != wantResult || token != wantPermit.Token || fence != wantPermit.Fence {
		t.Fatalf("intent=%s result=%q permit=%s/%d, want %s result=%q permit=%s/%d", state, result, token, fence, wantState, wantResult, wantPermit.Token, wantPermit.Fence)
	}
}

func assertBookingAdmissionTicketMatrix(t *testing.T, p *pgxpool.Pool, ticketID uuid.UUID, wantStatus string, wantAcceptedAudits int) {
	t.Helper()
	var status string
	if err := p.QueryRow(context.Background(), `SELECT status FROM tickets WHERE id=$1`, ticketID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	var acceptedAudits int
	if err := p.QueryRow(context.Background(), `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1 AND result_code='accepted'`, ticketID).Scan(&acceptedAudits); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || acceptedAudits != wantAcceptedAudits {
		t.Fatalf("Ticket status=%q accepted audits=%d, want status=%q audits=%d", status, acceptedAudits, wantStatus, wantAcceptedAudits)
	}
}

func loadAdmissionMatrixPermit(t *testing.T, p *pgxpool.Pool, requestID uuid.UUID) (admission.Permit, error) {
	t.Helper()
	var permit admission.Permit
	var state string
	var token *uuid.UUID
	var fence *int64
	var keyDigest, fingerprint []byte
	err := p.QueryRow(context.Background(), `SELECT payment_id,booking_id,ticket_id,event_id,user_id,actor_id,generation,key_digest,request_fingerprint,state,permit_token,permit_fence
		FROM ticket_admission_intents WHERE request_id=$1`, requestID).Scan(
		&permit.Identity.PaymentID, &permit.Identity.BookingID, &permit.Identity.TicketID, &permit.Identity.EventID,
		&permit.Identity.UserID, &permit.Identity.ActorID, &permit.Identity.Generation, &keyDigest,
		&fingerprint, &state, &token, &fence)
	if err != nil {
		return admission.Permit{}, err
	}
	permit.Identity.RequestID = requestID
	copy(permit.Identity.KeyDigest[:], keyDigest)
	copy(permit.Identity.Fingerprint[:], fingerprint)
	permit.State = admission.State(state)
	if token != nil {
		permit.Token = *token
		permit.Fence = *fence
	}
	return permit, nil
}
