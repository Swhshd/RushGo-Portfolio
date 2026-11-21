package service

import (
	"context"
	"encoding/json"
	"github.com/ticketbox/pkg/providerevidence"
	"strings"
	"time"

	"github.com/google/uuid"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
)

const paidRecoveryEvidenceMaxAge = 2 * time.Minute

type RecoverPaidUnissuedBookingRequest struct {
	BookingID, SagaID, OperationKey uuid.UUID
	Operator, Reason                string
	ExpectedProviderAccount         string
	Execute                         bool
}

type PaidUnissuedRecoveryReport struct {
	AuditID    string `json:"audit_id"`
	SagaID     string `json:"saga_id"`
	BookingID  string `json:"booking_id"`
	PaymentID  string `json:"payment_id,omitempty"`
	Mode       string `json:"mode"`
	ResultCode string `json:"result_code"`
	Eligible   bool   `json:"eligible"`
	Requeued   bool   `json:"requeued"`
	Replayed   bool   `json:"replayed"`
}

type paidUnissuedAuthoritySnapshot struct {
	Booking paidUnissuedBookingObservation `json:"booking"`
	Payment paidUnissuedPaymentObservation `json:"payment"`
	Event   paidUnissuedEventObservation   `json:"event"`
	Tickets paidUnissuedTicketObservation  `json:"tickets"`
}

type paidUnissuedBookingObservation struct {
	Checked bool   `json:"checked"`
	Phase   string `json:"phase,omitempty"`
	Status  string `json:"status,omitempty"`
	Version int32  `json:"version,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

type paidUnissuedPaymentObservation struct {
	Checked                  bool      `json:"checked"`
	PaymentID                string    `json:"payment_id,omitempty"`
	PaymentIntentID          string    `json:"payment_intent_id,omitempty"`
	ProviderAccount          string    `json:"provider_account,omitempty"`
	Livemode                 bool      `json:"livemode"`
	ProviderIdentityVerified bool      `json:"provider_identity_verified"`
	Status                   string    `json:"status,omitempty"`
	ProviderOutcome          string    `json:"provider_outcome,omitempty"`
	ProviderStatus           string    `json:"provider_status,omitempty"`
	ReconciliationState      string    `json:"reconciliation_state,omitempty"`
	RefundState              string    `json:"refund_state,omitempty"`
	RefundAmountCents        int64     `json:"refund_amount_cents,omitempty"`
	AdmissionEligibility     string    `json:"admission_eligibility,omitempty"`
	DisputeEvidence          string    `json:"dispute_evidence,omitempty"`
	RefundEvidence           string    `json:"refund_evidence,omitempty"`
	EvidenceObservedAt       time.Time `json:"evidence_observed_at,omitempty"`
	EvidenceSource           string    `json:"evidence_source,omitempty"`
	EvidenceReason           string    `json:"evidence_reason,omitempty"`
}

type paidUnissuedEventObservation struct {
	Checked            bool   `json:"checked"`
	EventID            string `json:"event_id,omitempty"`
	Status             string `json:"status,omitempty"`
	AccountingReady    bool   `json:"accounting_ready"`
	SeatEligibility    string `json:"seat_eligibility,omitempty"`
	SeatEligibilityWhy string `json:"seat_eligibility_reason,omitempty"`
	RequiredTierCount  int    `json:"required_tier_count"`
	ReadyTierCount     int    `json:"ready_tier_count"`
}

type paidUnissuedTicketObservation struct {
	Checked              bool      `json:"checked"`
	TicketCount          int64     `json:"ticket_count"`
	IssuanceReceiptCount int64     `json:"issuance_receipt_count"`
	ObservedAt           time.Time `json:"observed_at,omitempty"`
}

// RecoverPaidUnissuedBooking evaluates one existing managed Saga. Execute only
// atomically requeues that same forward graph; the normal RecoveryWorker claims
// it and the existing final step creates the unique Ticket Issuance Outbox.
func (s *SagaService) RecoverPaidUnissuedBooking(ctx context.Context, req RecoverPaidUnissuedBookingRequest) (*PaidUnissuedRecoveryReport, error) {
	if req.BookingID == uuid.Nil || req.SagaID == uuid.Nil || req.OperationKey == uuid.Nil ||
		strings.TrimSpace(req.Operator) == "" || len(req.Operator) > 128 ||
		strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 500 ||
		strings.TrimSpace(req.ExpectedProviderAccount) == "" || len(req.ExpectedProviderAccount) > 128 {
		return nil, repository.ErrInvalidRecoveryInput
	}
	recoveryRepo, ok := s.repo.(repository.PaidUnissuedRecoveryRepository)
	if !ok {
		return nil, repository.ErrRecoveryAttention
	}
	mode := "dry_run"
	if req.Execute {
		mode = "execute"
	}
	command := repository.PaidUnissuedRecoveryCommand{
		SagaID: req.SagaID, BookingID: req.BookingID, OperationKey: req.OperationKey,
		Operator: strings.TrimSpace(req.Operator), Reason: strings.TrimSpace(req.Reason),
		ExpectedProviderAccount: strings.TrimSpace(req.ExpectedProviderAccount), Mode: mode,
	}
	if prior, err := recoveryRepo.LookupPaidUnissuedRecoveryAudit(ctx, command); err != nil {
		return nil, err
	} else if prior != nil {
		return paidUnissuedRecoveryReport(req, command, prior), nil
	}

	state, err := recoveryRepo.GetPaidUnissuedSagaState(ctx, req.SagaID)
	if err != nil {
		return nil, err
	}
	command.Expected = *state
	authority := paidUnissuedAuthoritySnapshot{}
	resultCode := ""
	var paymentID *uuid.UUID
	var paymentIntentID string
	var bookingView *bookingv1.LifecycleDisposition
	var paymentView *paymentv1.PaidRecoveryPaymentSnapshot
	var eventView *eventv1.EventDetail
	var seatView *eventv1.VerifyBookingTicketEligibilityResponse
	var ticketSnapshot *bookingv1.PaidRecoveryTicketSnapshot

	if state.Saga == nil || state.Saga.Input == nil || state.Saga.ID != req.SagaID || state.Saga.BookingID != req.BookingID {
		resultCode = "saga_booking_identity_mismatch"
	} else if state.OutboxCount > 0 {
		if state.Saga.Status == domain.SAGA_COMPLETED {
			resultCode = "issuance_already_queued"
		} else {
			resultCode = "issuance_outbox_already_exists"
		}
	} else if state.Saga.Status == domain.SAGA_COMPLETED {
		resultCode = "completed_without_issuance_outbox"
	} else if state.Saga.RecoveryState == "ready" {
		resultCode = "already_processing"
	} else if !paidUnissuedAttentionCandidate(state.Saga) {
		resultCode = "saga_not_eligible_attention_state"
	} else if state.OutboxClaimActive {
		resultCode = "issuance_outbox_claim_active"
	} else {
		bookingView, err = s.bookingAuthority(ctx, *state.Saga.Input)
		if err != nil {
			resultCode = "booking_authority_unavailable"
		} else {
			authority.Booking = paidUnissuedBookingObservation{Checked: true, Phase: bookingView.Phase, Status: bookingView.Booking.Status, Version: bookingView.Version}
			if bookingView.Phase != "TERMINAL" || bookingView.Booking.Status != "CONFIRMED" {
				resultCode = "booking_not_confirmed"
			}
		}
	}

	if resultCode == "" {
		paymentRecord, readErr := s.financialAuthority(ctx, *state.Saga.Input)
		if readErr != nil {
			resultCode = "payment_recovery_authority_unavailable"
		} else {
			candidatePaymentID, parseErr := uuid.Parse(paymentRecord.Id)
			if parseErr != nil || candidatePaymentID == uuid.Nil {
				resultCode = "payment_identity_unavailable"
			} else {
				paymentID = &candidatePaymentID
				paymentView, err = s.paymentClient.GetPaidRecoveryPaymentSnapshot(ctx, &paymentv1.FinancialPaymentRequest{
					PaymentId: candidatePaymentID.String(), BookingId: req.BookingID.String(), Generation: 1,
				})
				if err != nil {
					resultCode = "payment_recovery_authority_unavailable"
				} else if paymentView == nil || paymentView.Payment == nil || paymentView.Payment.Id != candidatePaymentID.String() {
					resultCode = "payment_identity_changed_during_qualification"
				} else {
					resultCode = paidRecoveryPaymentReason(paymentView, *state.Saga, command.ExpectedProviderAccount, time.Now())
					paymentIntentID = paymentView.Payment.PaymentIntentId
				}
			}
			if resultCode == "" {
				authority.Payment = paymentObservation(paymentView)
			}
		}
	}

	if resultCode == "" {
		eventView, err = s.eventClient.GetEvent(ctx, &eventv1.GetEventRequest{EventId: state.Saga.Input.EventID.String()})
		if err != nil || eventView == nil || eventView.Id != state.Saga.Input.EventID.String() {
			resultCode = "event_authority_unavailable_or_mismatched"
		} else {
			authority.Event, resultCode = paidRecoveryEventObservation(eventView, bookingView)
			if resultCode == "" {
				seatIDs := make([]string, len(state.Saga.Input.SeatIDs))
				for i, id := range state.Saga.Input.SeatIDs {
					seatIDs[i] = id.String()
				}
				seatView, err = s.eventClient.VerifyBookingTicketEligibility(ctx, &eventv1.VerifyBookingTicketEligibilityRequest{
					BookingId: req.BookingID.String(), EventId: state.Saga.Input.EventID.String(), Generation: 1, SeatIds: seatIDs,
				})
				if err != nil || seatView == nil {
					resultCode = "event_seat_authority_unavailable"
				} else if seatView.State != eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_ELIGIBLE {
					resultCode = "event_seat_not_eligible"
				} else {
					authority.Event.SeatEligibility = seatView.State.String()
					authority.Event.SeatEligibilityWhy = seatView.ReasonCode
				}
			}
		}
	}

	if resultCode == "" {
		ticketSnapshot, err = s.bookingClient.GetPaidRecoveryTicketSnapshot(ctx, &bookingv1.LifecycleRequest{BookingId: req.BookingID.String(), Generation: 1})
		if err != nil || ticketSnapshot == nil {
			resultCode = "ticket_authority_unavailable"
		} else if ticketCode := paidRecoveryTicketReason(ticketSnapshot, req.BookingID, req.SagaID, time.Now()); ticketCode != "" {
			resultCode = ticketCode
		} else {
			authority.Tickets = ticketObservation(ticketSnapshot)
		}
	}

	if resultCode == "" {
		command.Eligible = true
		if req.Execute {
			command.ResultCode = "requeued"
		} else {
			command.ResultCode = "dry_run_eligible"
		}
	} else {
		command.ResultCode = resultCode
		if bookingView != nil && bookingView.Booking != nil {
			authority.Booking = paidUnissuedBookingObservation{Checked: true, Phase: bookingView.Phase, Status: bookingView.Booking.Status, Version: bookingView.Version}
		}
		if paymentView != nil {
			authority.Payment = paymentObservation(paymentView)
		}
		if eventView != nil {
			authority.Event.EventID, authority.Event.Status = eventView.Id, eventView.Status
		}
		if ticketSnapshot != nil {
			authority.Tickets = ticketObservation(ticketSnapshot)
		}
	}
	command.PaymentID, command.PaymentIntentID = paymentID, paymentIntentID
	authorityJSON, err := json.Marshal(authority)
	if err != nil {
		return nil, err
	}
	command.AuthoritySnapshot = authorityJSON
	result, err := recoveryRepo.RecordPaidUnissuedRecovery(ctx, command)
	if err != nil {
		return nil, err
	}
	return paidUnissuedRecoveryReport(req, command, result), nil
}

func paidUnissuedAttentionCandidate(v *domain.Saga) bool {
	return v != nil && v.RecoveryManaged && v.Generation == 1 && v.PaymentID == uuid.Nil && v.Input != nil && v.RecoveryState == "attention" &&
		v.Direction == "forward" && v.Status == domain.SAGA_PROCESSING && v.CurrentStepIndex == 4 &&
		(v.ErrorCategory == "paid_unissued_binding_missing" || v.ErrorCategory == "invalid_persisted_graph")
}

func paidRecoveryPaymentReason(snapshot *paymentv1.PaidRecoveryPaymentSnapshot, saga domain.Saga, expectedProviderAccount string, now time.Time) string {
	if snapshot == nil || snapshot.Payment == nil || snapshot.Admission == nil {
		return "payment_snapshot_incomplete"
	}
	p, admission := snapshot.Payment, snapshot.Admission
	if !snapshot.ProviderIdentityVerified || snapshot.Livemode || snapshot.ProviderAccount == "" || snapshot.ProviderAccount != expectedProviderAccount {
		return "payment_test_account_identity_mismatch"
	}
	if p.BookingId != saga.BookingID.String() || p.UserId != saga.Input.UserID.String() || p.PaymentGeneration != 1 ||
		int64(p.Price) != saga.Input.AmountCents || p.Currency != saga.Input.Currency || p.Id == "" || p.PaymentIntentId == "" ||
		(p.BookingDisposition != "" && p.BookingDisposition != "CONFIRMED") {
		return "payment_booking_identity_mismatch"
	}
	paymentID, err := uuid.Parse(p.Id)
	if err != nil || paymentID == uuid.Nil || (saga.PaymentID != uuid.Nil && saga.PaymentID != paymentID) ||
		(saga.PaymentIntentId != "" && saga.PaymentIntentId != p.PaymentIntentId) {
		return "payment_binding_mismatch"
	}
	if p.Status != "success" || p.ProviderOutcome != "collected" || p.ProviderStatus != "succeeded" ||
		p.ReconciliationState != "sale_verified" || p.RefundState != "not_required" || p.RefundAmountCents != 0 ||
		p.RefundId != "" || p.RefundStatus != "" {
		return "payment_not_cleanly_collected"
	}
	if admission.EligibilityState != paymentv1.AdmissionEligibilityState_ADMISSION_ELIGIBILITY_STATE_ELIGIBLE ||
		admission.DisputeState != paymentv1.AdmissionDisputeEvidenceState_ADMISSION_DISPUTE_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_DISPUTE ||
		admission.RefundState != paymentv1.AdmissionRefundEvidenceState_ADMISSION_REFUND_EVIDENCE_STATE_PROVIDER_OBSERVED_NO_REFUND ||
		!providerevidence.AllowedSource(admission.Source) || admission.ObservedAt == nil || admission.ObservedAt.CheckValid() != nil {
		return "payment_refund_dispute_or_evidence_unknown"
	}
	observed := admission.ObservedAt.AsTime()
	if observed.After(now.Add(30*time.Second)) || now.Sub(observed) > paidRecoveryEvidenceMaxAge {
		return "payment_evidence_stale"
	}
	return ""
}

func paidRecoveryEventObservation(event *eventv1.EventDetail, booking *bookingv1.LifecycleDisposition) (paidUnissuedEventObservation, string) {
	observation := paidUnissuedEventObservation{}
	if event == nil || booking == nil || booking.Booking == nil {
		return observation, "event_authority_unavailable_or_mismatched"
	}
	observation.Checked, observation.EventID, observation.Status = true, event.Id, event.Status
	for _, item := range booking.Booking.Items {
		if item == nil {
			return observation, "booking_item_invalid"
		}
		observation.RequiredTierCount++
		found := false
		for _, tier := range event.Tiers {
			if tier != nil && tier.Id == item.TicketTierId && tier.EventId == event.Id {
				found = true
				if !tier.AccountingReady {
					return observation, "event_tier_accounting_not_ready"
				}
				observation.ReadyTierCount++
				break
			}
		}
		if !found {
			return observation, "event_ticket_tier_mismatch"
		}
	}
	observation.AccountingReady = observation.RequiredTierCount > 0 && observation.ReadyTierCount == observation.RequiredTierCount
	return observation, ""
}

func paidRecoveryTicketReason(snapshot *bookingv1.PaidRecoveryTicketSnapshot, bookingID, sagaID uuid.UUID, now time.Time) string {
	if snapshot == nil || snapshot.BookingId != bookingID.String() || snapshot.SagaId != sagaID.String() || snapshot.Generation != 1 ||
		snapshot.ObservedAt == nil || snapshot.ObservedAt.CheckValid() != nil || snapshot.TicketCount < 0 || snapshot.IssuanceReceiptCount < 0 {
		return "ticket_snapshot_identity_or_shape_mismatch"
	}
	observed := snapshot.ObservedAt.AsTime()
	if observed.After(now.Add(30*time.Second)) || now.Sub(observed) > paidRecoveryEvidenceMaxAge {
		return "ticket_snapshot_stale"
	}
	if snapshot.TicketCount != 0 {
		return "ticket_already_exists"
	}
	if snapshot.IssuanceReceiptCount != 0 {
		return "issuance_receipt_already_exists"
	}
	return ""
}

func paymentObservation(snapshot *paymentv1.PaidRecoveryPaymentSnapshot) paidUnissuedPaymentObservation {
	observation := paidUnissuedPaymentObservation{Checked: snapshot != nil}
	if snapshot == nil {
		return observation
	}
	observation.ProviderAccount, observation.Livemode = snapshot.ProviderAccount, snapshot.Livemode
	observation.ProviderIdentityVerified = snapshot.ProviderIdentityVerified
	if snapshot.Payment != nil {
		p := snapshot.Payment
		observation.PaymentID, observation.PaymentIntentID = p.Id, p.PaymentIntentId
		observation.Status, observation.ProviderOutcome, observation.ProviderStatus = p.Status, p.ProviderOutcome, p.ProviderStatus
		observation.ReconciliationState, observation.RefundState = p.ReconciliationState, p.RefundState
		observation.RefundAmountCents = p.RefundAmountCents
	}
	if snapshot.Admission != nil {
		a := snapshot.Admission
		observation.AdmissionEligibility = a.EligibilityState.String()
		observation.DisputeEvidence, observation.RefundEvidence = a.DisputeState.String(), a.RefundState.String()
		observation.EvidenceSource, observation.EvidenceReason = a.Source, a.ReasonCode
		if a.ObservedAt != nil && a.ObservedAt.CheckValid() == nil {
			observation.EvidenceObservedAt = a.ObservedAt.AsTime().UTC()
		}
	}
	return observation
}

func ticketObservation(snapshot *bookingv1.PaidRecoveryTicketSnapshot) paidUnissuedTicketObservation {
	observation := paidUnissuedTicketObservation{Checked: snapshot != nil}
	if snapshot == nil {
		return observation
	}
	observation.TicketCount, observation.IssuanceReceiptCount = snapshot.TicketCount, snapshot.IssuanceReceiptCount
	if snapshot.ObservedAt != nil && snapshot.ObservedAt.CheckValid() == nil {
		observation.ObservedAt = snapshot.ObservedAt.AsTime().UTC()
	}
	return observation
}

func paidUnissuedRecoveryReport(req RecoverPaidUnissuedBookingRequest, command repository.PaidUnissuedRecoveryCommand, result *repository.PaidUnissuedRecoveryAuditResult) *PaidUnissuedRecoveryReport {
	report := &PaidUnissuedRecoveryReport{SagaID: req.SagaID.String(), BookingID: req.BookingID.String(), Mode: command.Mode}
	if result != nil {
		report.AuditID, report.ResultCode = result.AuditID.String(), result.ResultCode
		report.Eligible, report.Requeued, report.Replayed = result.Eligible, result.Requeued, result.Replayed
	}
	if command.PaymentID != nil {
		report.PaymentID = command.PaymentID.String()
	}
	return report
}
