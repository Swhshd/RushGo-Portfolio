package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/pkg/admission"
	"testing"
	"time"
)

type unusedRefundFixture struct {
	ticketRepositoryFixture
	approver uuid.UUID
	request  domain.BookingRefundRequest
	decision domain.DecideUnusedRefundCommand
}

func seedUnusedRefund(t *testing.T, p *pgxpool.Pool, r *PostgresBookingRepository) unusedRefundFixture {
	t.Helper()
	ctx := context.Background()
	f := seedTicketRepositoryRows(t, p, domain.TicketIssued, nil, true)
	if _, err := p.Exec(ctx, `INSERT INTO ticket_issuance_receipts(event_id,saga_id,booking_id,payment_id,event_ref_id,lifecycle_generation,outcome,reason_code,dispute_state,refund_state,evidence_source,observed_at,ticket_count) VALUES($1,$2,$3,$4,$5,1,'issued','eligible','provider_observed_no_dispute','provider_observed_no_refund','stripe_test_api',clock_timestamp(),1)`, uuid.New(), f.sagaID, f.bookingID, f.paymentID, f.bookingEventID); err != nil {
		t.Fatal(err)
	}
	approver := uuid.New()
	if _, err := r.SetEventRefundApprover(ctx, f.bookingEventID, approver, uuid.New(), true); err != nil {
		t.Fatal(err)
	}
	req, err := r.RequestUnusedRefund(ctx, domain.RequestUnusedRefundCommand{RequestID: uuid.New(), BookingID: f.bookingID, BuyerID: f.userID, Reason: "unused", KeyDigest: sha256.Sum256([]byte("request")), Fingerprint: sha256.Sum256([]byte(f.bookingID.String()))})
	if err != nil {
		t.Fatal(err)
	}
	command := domain.DecideUnusedRefundCommand{DecisionID: uuid.New(), RequestID: req.RequestID, EventID: f.bookingEventID, ApproverID: approver, Kind: domain.RefundDecisionApproved, Reason: "approved unused", KeyDigest: sha256.Sum256([]byte("approve")), Fingerprint: sha256.Sum256([]byte(req.RequestID.String())), Event: domain.RefundEventSnapshot{EventID: f.bookingEventID, StartsAt: time.Now().Add(time.Hour)}, Payment: domain.UnusedRefundPaymentSnapshot{BookingID: f.bookingID, BuyerID: f.userID, PaymentID: f.paymentID, PaymentIntentID: "pi_fixture", ProviderAccount: "acct_fixture", Generation: 1, AmountCents: 725, Currency: "usd", RefundState: "not_required", Collected: true, EligibleForApproval: true}}
	return unusedRefundFixture{f, approver, req, command}
}
func activateUnusedRefund(t *testing.T, p *pgxpool.Pool) {
	t.Helper()
	if _, err := p.Exec(context.Background(), `UPDATE booking_admission_mode SET active=TRUE WHERE singleton`); err != nil {
		t.Fatal(err)
	}
}
func approvedUnusedRefund(t *testing.T, r *PostgresBookingRepository, f unusedRefundFixture) domain.BookingRefundRequest {
	t.Helper()
	v, err := r.DecideUnusedRefund(context.Background(), f.decision)
	if err != nil || v.Status != domain.RefundRequestApproved {
		t.Fatalf("approval %+v %v", v, err)
	}
	return v
}

func TestUnusedRefundBothCheckInOrderingsIntegration(t *testing.T) {
	for _, refundFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "checkin_first", true: "refund_first"}[refundFirst], func(t *testing.T) {
			p, r := newBookingOutboxIntegrationFixture(t)
			f := seedUnusedRefund(t, p, r)
			activateUnusedRefund(t, p)
			ctx := context.Background()
			scan := acceptedTicketScanFixture(f.ticketRepositoryFixture, time.Now().UTC())
			identity := admissionIdentityForTicketFixture(f.ticketRepositoryFixture, scan.StaffUserID, scan.IdempotencyKey, scan.RequestFingerprint)
			if refundFirst {
				approvedUnusedRefund(t, r, f)
				scan.Decision = domain.TicketIssuanceDecision{ReasonCode: "refund_approved"}
				if err := r.EnsureAdmissionIntent(ctx, identity); !errors.Is(err, ErrAdmissionRefundApproved) {
					t.Fatalf("approval failed to fence intent: %v", err)
				}
				result, err := r.CommitTicketCheckIn(ctx, scan)
				if err != nil || result.ResultCode != "denied" || result.ReasonCode != "refund_approved" {
					t.Fatalf("refund scan denial %+v %v", result, err)
				}
				replay, err := r.CommitTicketCheckIn(ctx, scan)
				if err != nil || replay.ResultCode != "denied" {
					t.Fatalf("denial replay %+v %v", replay, err)
				}
				var count int
				if err := p.QueryRow(ctx, `SELECT count(*) FROM ticket_checkins WHERE ticket_id=$1`, f.ticketID).Scan(&count); err != nil || count != 0 {
					t.Fatalf("approved order was admitted: %d %v", count, err)
				}
			} else {
				if err := r.EnsureAdmissionIntent(ctx, identity); err != nil {
					t.Fatal(err)
				}
				permit := admission.Permit{Identity: identity, Token: uuid.New(), Fence: 1, State: admission.Pending}
				if err := r.AttachAdmissionPermit(ctx, permit); err != nil {
					t.Fatal(err)
				}
				scan.Admission = &permit
				result, err := r.CommitTicketCheckIn(ctx, scan)
				if err != nil || result.ResultCode != "accepted" {
					t.Fatalf("request froze checkin: %+v %v", result, err)
				}
				v, err := r.DecideUnusedRefund(ctx, f.decision)
				if err != nil || v.Status != domain.RefundRequestRejected {
					t.Fatalf("used order approved: %+v %v", v, err)
				}
			}
			var status string
			if err := p.QueryRow(ctx, `SELECT status FROM bookings WHERE id=$1`, f.bookingID).Scan(&status); err != nil || status != "CONFIRMED" {
				t.Fatalf("history changed %s %v", status, err)
			}
		})
	}
}

func TestUnusedRefundPendingNilAuthorityAndReplayIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	f := seedUnusedRefund(t, p, r)
	activateUnusedRefund(t, p)
	ctx := context.Background()
	scan := acceptedTicketScanFixture(f.ticketRepositoryFixture, time.Now().UTC())
	identity := admissionIdentityForTicketFixture(f.ticketRepositoryFixture, scan.StaffUserID, scan.IdempotencyKey, scan.RequestFingerprint)
	if err := r.EnsureAdmissionIntent(ctx, identity); err != nil {
		t.Fatal(err)
	}
	permit := admission.Permit{Identity: identity, Token: uuid.New(), Fence: 1, State: admission.Pending}
	if err := r.AttachAdmissionPermit(ctx, permit); err != nil {
		t.Fatal(err)
	}
	missing := f.decision
	missing.Event = domain.RefundEventSnapshot{}
	missing.Payment = domain.UnusedRefundPaymentSnapshot{}
	waiting, err := r.DecideUnusedRefund(ctx, missing)
	if err != nil || waiting.Status != domain.RefundRequestWaitingCheckInResolution || waiting.DecisionID != uuid.Nil {
		t.Fatalf("pending without upstream must wait: %+v %v", waiting, err)
	}
	var commands int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_type=$1`, domain.TypeUnusedTicketRefundApprovedV1).Scan(&commands); err != nil || commands != 0 {
		t.Fatalf("waiting emitted command %d %v", commands, err)
	}
	if _, err := r.ResolveAdmission(ctx, identity, permit.Token, permit.Fence, true); err != nil {
		t.Fatal(err)
	}
	approved := approvedUnusedRefund(t, r, f)
	replay, err := r.DecideUnusedRefund(ctx, f.decision)
	if err != nil || replay.DecisionID != approved.DecisionID {
		t.Fatalf("same key did not replay: %+v %v", replay, err)
	}
	changed := f.decision
	changed.Fingerprint = sha256.Sum256([]byte("changed reason"))
	if _, err = r.DecideUnusedRefund(ctx, changed); !errors.Is(err, ErrRefundRequestIdempotencyConflict) {
		t.Fatalf("changed fingerprint accepted: %v", err)
	}
	if err := p.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE id=$1`, approved.DecisionID).Scan(&commands); err != nil || commands != 1 {
		t.Fatalf("duplicate outbox %d %v", commands, err)
	}
}

func TestUnusedRefundApprovalIntentRaceIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	activateUnusedRefund(t, p)
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		f := seedUnusedRefund(t, p, r)
		scan := acceptedTicketScanFixture(f.ticketRepositoryFixture, time.Now().UTC())
		identity := admissionIdentityForTicketFixture(f.ticketRepositoryFixture, scan.StaffUserID, scan.IdempotencyKey, scan.RequestFingerprint)
		barrier, err := p.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = barrier.Exec(ctx, `SELECT id FROM bookings WHERE id=$1 FOR UPDATE`, f.bookingID); err != nil {
			t.Fatal(err)
		}
		type decisionResult struct {
			v domain.BookingRefundRequest
			e error
		}
		decisions := make(chan decisionResult, 1)
		intents := make(chan error, 1)
		start := make(chan struct{})
		go func() { <-start; v, e := r.DecideUnusedRefund(ctx, f.decision); decisions <- decisionResult{v, e} }()
		go func() { <-start; intents <- r.EnsureAdmissionIntent(ctx, identity) }()
		close(start)
		if err := barrier.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var d decisionResult
		var intentErr error
		select {
		case d = <-decisions:
		case <-time.After(5 * time.Second):
			t.Fatal("approval deadlock")
		}
		select {
		case intentErr = <-intents:
		case <-time.After(5 * time.Second):
			t.Fatal("intent deadlock")
		}
		if d.e != nil {
			t.Fatal(d.e)
		}
		if d.v.Status == domain.RefundRequestApproved {
			if !errors.Is(intentErr, ErrAdmissionRefundApproved) {
				t.Fatalf("both won: %v", intentErr)
			}
		} else if d.v.Status == domain.RefundRequestWaitingCheckInResolution {
			if intentErr != nil {
				t.Fatalf("intent winner failed %v", intentErr)
			}
		} else {
			t.Fatalf("unexpected race state %s", d.v.Status)
		}
	}
}

func TestUnusedRefundAuthorityAndHistoryGatesIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	ctx := context.Background()
	inactive := seedUnusedRefund(t, p, r)
	if _, err := r.DecideUnusedRefund(ctx, inactive.decision); err == nil {
		t.Fatal("inactive approval allowed")
	}
	activateUnusedRefund(t, p)
	for _, kind := range []string{"owner", "self", "scanner", "revoked", "started", "receipt", "committed", "multiticket_history"} {
		t.Run(kind, func(t *testing.T) {
			f := seedUnusedRefund(t, p, r)
			command := f.decision
			switch kind {
			case "owner":
				_, err := r.RequestUnusedRefund(ctx, domain.RequestUnusedRefundCommand{RequestID: uuid.New(), BookingID: f.bookingID, BuyerID: uuid.New(), Reason: "other", KeyDigest: sha256.Sum256([]byte("other")), Fingerprint: sha256.Sum256([]byte("other"))})
				if !errors.Is(err, ErrRefundRequestOwnerDenied) {
					t.Fatalf("foreign buyer %v", err)
				}
				return
			case "self":
				command.ApproverID = f.userID
			case "scanner":
				if _, err := r.SetEventRefundApprover(ctx, f.bookingEventID, f.staffID, uuid.New(), true); err != nil {
					t.Fatal(err)
				}
				command.ApproverID = f.staffID
			case "revoked":
				if _, err := r.SetEventRefundApprover(ctx, f.bookingEventID, f.approver, uuid.New(), false); err != nil {
					t.Fatal(err)
				}
			case "started":
				command.Event.StartsAt = time.Now().Add(-time.Minute)
			case "receipt":
				if _, err := p.Exec(ctx, `UPDATE ticket_issuance_receipts SET payment_id=$2 WHERE booking_id=$1`, f.bookingID, uuid.New()); err != nil {
					t.Fatal(err)
				}
			case "committed", "multiticket_history":
				scan := acceptedTicketScanFixture(f.ticketRepositoryFixture, time.Now().UTC())
				id := admissionIdentityForTicketFixture(f.ticketRepositoryFixture, scan.StaffUserID, scan.IdempotencyKey, scan.RequestFingerprint)
				if err := r.EnsureAdmissionIntent(ctx, id); err != nil {
					t.Fatal(err)
				}
				permit := admission.Permit{Identity: id, Token: uuid.New(), Fence: 1, State: admission.Pending}
				if err := r.AttachAdmissionPermit(ctx, permit); err != nil {
					t.Fatal(err)
				}
				scan.Admission = &permit
				if _, err := r.CommitTicketCheckIn(ctx, scan); err != nil {
					t.Fatal(err)
				}
				// Current status alone must not erase committed/historical use.
				if _, err := p.Exec(ctx, `UPDATE tickets SET status='ISSUED',checked_in_at=NULL WHERE id=$1`, f.ticketID); err != nil {
					t.Fatal(err)
				}
				if kind == "multiticket_history" {
					seat := uuid.New()
					if _, err := p.Exec(ctx, `UPDATE booking_items SET quantity=2,seat_ids=array_append(seat_ids,$2) WHERE booking_id=$1`, f.bookingID, seat); err != nil {
						t.Fatal(err)
					}
					if _, err := p.Exec(ctx, `INSERT INTO tickets(id,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,seat_id,lifecycle_generation,status) SELECT $2,booking_id,booking_item_id,saga_id,payment_id,event_id,ticket_tier_id,$3,1,'ISSUED' FROM tickets WHERE id=$1`, f.ticketID, uuid.New(), seat); err != nil {
						t.Fatal(err)
					}
					if _, err := p.Exec(ctx, `UPDATE ticket_issuance_receipts SET ticket_count=2 WHERE booking_id=$1`, f.bookingID); err != nil {
						t.Fatal(err)
					}
				}
			}
			v, err := r.DecideUnusedRefund(ctx, command)
			if err == nil && v.Status != domain.RefundRequestRejected {
				t.Fatalf("gate %s approved %+v", kind, v)
			}
			var block *uuid.UUID
			if e := p.QueryRow(ctx, `SELECT refund_decision_id FROM bookings WHERE id=$1`, f.bookingID).Scan(&block); e != nil || block != nil {
				t.Fatalf("denied gate created block %v %v", block, e)
			}
		})
	}
}

func TestUnusedRefundSuccessCrashGapAndFairRecoveryIntegration(t *testing.T) {
	p, r := newBookingOutboxIntegrationFixture(t)
	activateUnusedRefund(t, p)
	ctx := context.Background()
	var last unusedRefundFixture
	for i := 0; i < 52; i++ {
		f := seedUnusedRefund(t, p, r)
		f.request = approvedUnusedRefund(t, r, f)
		last = f
	}
	seen := map[uuid.UUID]bool{}
	for page := 0; page < 2; page++ {
		rows, err := r.ListUnusedRefundsForRecovery(ctx, 50)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range rows {
			seen[v.RequestID] = true
		}
	}
	if len(seen) != 52 {
		t.Fatalf("recovery starvation: saw %d of52", len(seen))
	}
	// Model crash after authoritative success projection commit, before Ticket update.
	if _, err := p.Exec(ctx, `UPDATE booking_refund_requests SET status='REFUNDED',payment_refund_state='succeeded',payment_refund_amount_cents=725,ticket_revocation_state='pending_payment' WHERE request_id=$1`, last.request.RequestID); err != nil {
		t.Fatal(err)
	}
	stale := last.decision.Payment
	stale.RefundDecisionID = last.request.DecisionID
	stale.RefundState = "pending"
	stale.RefundAmountCents = 725
	v, err := r.ApplyUnusedRefundPaymentSnapshot(ctx, last.request, stale)
	if err != nil || v.Status != domain.RefundRequestRefunded || v.TicketRevocationState != "revoked" {
		t.Fatalf("crash gap did not converge %+v %v", v, err)
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM tickets WHERE booking_id=$1 AND status='REVOKED'`, last.bookingID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("revocation %d %v", count, err)
	}
	if _, err := r.ApplyUnusedRefundPaymentSnapshot(ctx, last.request, stale); err != nil {
		t.Fatal(err)
	}
}
