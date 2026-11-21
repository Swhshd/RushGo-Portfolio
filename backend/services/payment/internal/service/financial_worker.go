package service

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	paymentgateway "github.com/ticketbox/payment/internal/gateway"
	"github.com/ticketbox/payment/internal/repository"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	"github.com/ticketbox/pkg/observability"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	"github.com/ticketbox/pkg/reliabilityhook"
	"github.com/ticketbox/pkg/topics"
	"github.com/ticketbox/pkg/typed"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type FinancialWorker struct {
	repo          repository.FinancialRepository
	provider      paymentgateway.FinancialProvider
	booking       bookingv1.BookingServiceClient
	accounting    AccountingReadinessChecker
	publisher     ReceiptPublisher
	metrics       *observability.Metrics
	reliability   RecoveryHook
	w28RefundHook W28RefundHook
	logger        *zap.Logger
}

type RecoveryHook interface {
	Await(context.Context, reliabilityhook.Boundary) (reliabilityhook.Decision, bool, error)
}

type ReceiptPublisher interface {
	PublishEvent(context.Context, string, string, pkgkafka.Event) error
}

func (w *FinancialWorker) SetReceiptPublisher(publisher ReceiptPublisher) { w.publisher = publisher }

func (w *FinancialWorker) SetReliabilityHook(hook RecoveryHook) { w.reliability = hook }

type W28RefundHook interface {
	AwaitW28Refund(context.Context, reliabilityhook.Boundary) (reliabilityhook.Decision, bool, error)
}

func (w *FinancialWorker) SetW28RefundHook(hook W28RefundHook) { w.w28RefundHook = hook }

func (w *FinancialWorker) SetMetrics(metrics *observability.Metrics) { w.metrics = metrics }

func (w *FinancialWorker) RunReceiptsOnce(ctx context.Context, limit int) (int, error) {
	if limit > 4 {
		limit = 4
	}
	claims, err := w.repo.ClaimReceiptWork(ctx, limit, 30*time.Second)
	if err != nil {
		return 0, err
	}
	errorsCh := make(chan error, len(claims))
	for _, claim := range claims {
		go func(c domain.ReceiptClaim) { errorsCh <- w.executeReceipt(ctx, c) }(claim)
	}
	var first error
	for range claims {
		if err := <-errorsCh; err != nil && first == nil {
			first = err
		}
	}
	return len(claims), first
}

func (w *FinancialWorker) executeReceipt(ctx context.Context, c domain.ReceiptClaim) error {
	r := c.Receipt
	if r.CorrelationState == "pending" {
		applied, err := w.repo.ApplyReceiptObservation(ctx, c, r.Observation)
		if err != nil {
			return err
		}
		if !applied {
			_, err := w.repo.FinishReceiptWork(ctx, c, "pending", r.RelayState, "correlation_pending", time.Now().Add(time.Second))
			return err
		}
		r.CorrelationState = "applied"
	}
	relay := r.RelayState
	category := ""
	if relay != "not_applicable" && relay != "published" {
		if w.publisher == nil {
			relay, category = "pending", "publisher_unavailable"
		} else {
			kind := topics.TypePaymentFail
			if r.EventType == "payment_intent.succeeded" {
				kind = topics.TypePaymentSucceed
			}
			data, err := json.Marshal(typed.VerifiedPaymentEvent(r.Observation.EventID, r.Observation.SafeObject))
			if err != nil {
				return err
			}
			if _, _, err := w.awaitReceiptReliabilityBoundary(
				ctx, c, r, "payment_receipt_before_broker_publish", reliabilityhook.ActionPause); err != nil {
				return err
			}
			bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = w.publisher.PublishEvent(bounded, topics.TopicPaymentEvents, r.EnvelopeID, pkgkafka.Event{ID: r.EnvelopeID, Type: kind, Timestamp: r.ReceivedAt, Data: data})
			cancel()
			if err != nil {
				relay, category = "pending", "publish_outcome_unknown"
				if _, _, err := w.awaitReceiptReliabilityBoundary(
					ctx, c, r, "payment_broker_ack_dropped_before_relay_marker", reliabilityhook.ActionAckGate); err != nil {
					return err
				}
			} else {
				relay = "published"
			}
		}
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	_, err := w.repo.FinishReceiptWork(finishCtx, c, r.CorrelationState, relay, category, time.Now().Add(time.Second))
	return err
}

func (w *FinancialWorker) awaitReceiptReliabilityBoundary(
	ctx context.Context, c domain.ReceiptClaim, receipt domain.ProviderReceipt,
	checkpoint, action string,
) (reliabilityhook.Decision, bool, error) {
	if w == nil || w.reliability == nil {
		return reliabilityhook.DecisionContinue, false, nil
	}
	// The production Controller exposes Enabled. A custom RecoveryHook without
	// that optional capability is treated as explicitly active, so malformed
	// claim identity cannot silently bypass an installed hook.
	if configured, ok := w.reliability.(interface{ Enabled() bool }); ok && !configured.Enabled() {
		return reliabilityhook.DecisionContinue, false, nil
	}
	if receipt.ID == uuid.Nil || c.ReceiptID != receipt.ID ||
		receipt.Observation.PaymentID == uuid.Nil || receipt.Observation.BookingID == uuid.Nil ||
		receipt.Observation.EventID == "" || c.Token == uuid.Nil || c.Fence < 1 ||
		!c.LeaseUntil.After(time.Now()) {
		return reliabilityhook.DecisionContinue, false, errors.New("enabled Payment hook has an incomplete receipt claim identity")
	}
	if _, err := uuid.Parse(receipt.EnvelopeID); err != nil {
		return reliabilityhook.DecisionContinue, false, errors.New("enabled Payment hook has an invalid receipt envelope identity")
	}
	return w.reliability.Await(ctx, reliabilityhook.Boundary{
		WindowID:    "payment.relay_before_marker",
		OperationID: receipt.Observation.PaymentID.String(),
		Checkpoint:  checkpoint,
		Action:      action,
		Facts: map[string]string{
			"payment_id":       receipt.Observation.PaymentID.String(),
			"booking_id":       receipt.Observation.BookingID.String(),
			"receipt_event_id": receipt.Observation.EventID,
			"envelope_id":      receipt.EnvelopeID,
			"topic":            topics.TopicPaymentEvents,
			"claim_token":      c.Token.String(),
			"claim_fence":      strconv.FormatInt(c.Fence, 10),
			"lease_until":      c.LeaseUntil.UTC().Format(time.RFC3339Nano),
		},
	})
}

func NewFinancialWorker(s *PaymentService) *FinancialWorker {
	return &FinancialWorker{repo: s.repo, provider: s.provider, booking: s.booking, accounting: s.accounting, logger: s.logger}
}

func (w *FinancialWorker) executeCreate(ctx context.Context, c domain.FinancialClaim) (string, error) {
	result := domain.FinancialResult{State: "create_unknown", ErrorCategory: "provider_outcome_unknown", NextAttemptAt: time.Now().Add(time.Second)}
	bookingCtx, bookingCancel := context.WithTimeout(ctx, 5*time.Second)
	view, err := w.booking.GetLifecycleDisposition(bookingCtx, &bookingv1.LifecycleRequest{BookingId: c.Operation.Identity.BookingID.String(), Generation: c.Operation.Identity.Generation})
	bookingCancel()
	if err != nil {
		finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer finishCancel()
		_, finishErr := w.repo.FinishFinancialWork(finishCtx, c, result)
		return "", finishErr
	}
	if view == nil || view.Booking == nil || view.Generation != c.Operation.Identity.Generation || view.Booking.Id != c.Operation.Identity.BookingID.String() || view.Booking.UserId != c.Operation.Identity.UserID.String() || view.Booking.TotalAmountCents != c.Operation.Identity.AmountCents || view.Currency != c.Operation.Identity.Currency {
		result.State, result.ErrorCategory = "create_attention", "booking_snapshot_conflict"
	} else if view.Phase == "TERMINAL" {
		result.State, result.ErrorCategory = "create_attention", "booking_terminal"
	} else if (view.Phase != "OPEN" && view.Phase != "CONFIRMING") || view.ReservationDeadline == nil || !view.ReservationDeadline.AsTime().After(time.Now()) {
		result.State, result.ErrorCategory = "create_attention", "booking_not_payable"
	}
	accountingBlocked := false
	if result.State != "create_attention" && view != nil && (view.Phase == "OPEN" || view.Phase == "CONFIRMING") {
		if err := checkAccountingReadiness(ctx, w.accounting, view); err != nil {
			accountingBlocked = true
			if errors.Is(err, ErrAccountingUnavailable) {
				// The authority could not be consulted, so no provider call was
				// made. Keep creation retryable instead of wedging the payment in
				// a terminal attention state after one transient outage.
				result.State, result.ErrorCategory = "create_unknown", "accounting_readiness_unavailable"
				result.NextAttemptAt = time.Now().Add(time.Second)
			} else {
				result.State, result.ErrorCategory = "create_attention", "accounting_not_ready"
			}
		}
	}
	var observation *domain.ProviderObservation
	var secret string
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if result.State != "create_attention" && !accountingBlocked {
		if c.FirstAttemptAt == nil || time.Since(*c.FirstAttemptAt) >= 20*time.Hour {
			observations, lookupErr := w.provider.FindIntent(bounded, c.Operation)
			if lookupErr == nil && len(observations) == 1 {
				observation = &observations[0]
			} else {
				result.State, result.ErrorCategory = "create_attention", "create_retention_ambiguity"
			}
		} else {
			observation, secret, err = w.provider.CreateIntent(bounded, c.Operation)
			if err != nil {
				observation = nil
				secret = ""
			}
		}
	}
	if observation != nil {
		result.State, result.ErrorCategory, result.Observation = "create_ready", "", observation
	}
	// Persistence has its own bounded context: cancellation during a remote
	// request cannot erase possible effects or leave no durable retry state.
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer finishCancel()
	owned, err := w.repo.FinishFinancialWork(finishCtx, c, result)
	if err != nil {
		return "", err
	}
	if !owned || result.State != "create_ready" {
		return "", nil
	}
	return secret, nil
}

func (w *FinancialWorker) executeReconcile(ctx context.Context, c domain.FinancialClaim) error {
	result := domain.FinancialResult{State: "pending", ErrorCategory: "booking_disposition_unavailable", NextAttemptAt: time.Now().Add(5 * time.Second)}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	view, err := w.booking.GetLifecycleDisposition(bounded, &bookingv1.LifecycleRequest{BookingId: c.Operation.Identity.BookingID.String(), Generation: c.Operation.Identity.Generation})
	cancel()
	if err == nil {
		if view == nil || view.Booking == nil || view.Booking.Id != c.Operation.Identity.BookingID.String() || view.Booking.UserId != c.Operation.Identity.UserID.String() || view.Generation != c.Operation.Identity.Generation || view.Booking.TotalAmountCents != c.Operation.Identity.AmountCents || view.Currency != c.Operation.Identity.Currency {
			result.State, result.ErrorCategory = "attention", "booking_snapshot_conflict"
		} else if view.Phase == "OPEN" || view.Phase == "CONFIRMING" {
			// A collected payment is settled by the Saga within about a second
			// and the ticket issuance consumer only retries for roughly 1.5s.
			// A five-second park can outlast that window, so leave a collected
			// row due for the next worker tick (the tick is the rate limit).
			// Uncollected open checkouts keep the default backoff so abandoned
			// carts cannot occupy every tick's claim budget.
			result.ErrorCategory = ""
			if c.View != nil && c.View.ProviderOutcome == "collected" {
				result.NextAttemptAt = time.Now()
			}
		} else if view.Phase == "TERMINAL" {
			decision, parseErr := uuid.Parse(view.DecisionId)
			result.DecisionID = decision
			if parseErr != nil {
				result.State, result.ErrorCategory = "attention", "booking_decision_missing"
			} else if view.Booking.Status == "CONFIRMED" && decision == uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+view.Booking.Id+":g1:confirm")) {
				if c.View.ProviderOutcome == "collected" && c.View.RefundState == "not_required" {
					result.State, result.ErrorCategory, result.BookingDisposition = "sale_verified", "", "CONFIRMED"
				} else {
					result.State, result.ErrorCategory = "attention", "confirmed_financial_conflict"
				}
			} else if disposition := repository.TerminalDisposition(c.Operation.Identity.BookingID, decision); disposition != "" && disposition == view.Booking.Status {
				result.BookingDisposition = disposition
				result.ErrorCategory = ""
				if c.View.ProviderOutcome == "collected" {
					var financial *domain.FinancialView
					financial, err = w.repo.RequireRefund(ctx, domain.RefundObligation{PaymentID: c.PaymentID, DecisionID: decision, Generation: c.Operation.Identity.Generation, AmountCents: c.Operation.Identity.AmountCents, Currency: c.Operation.Identity.Currency, Reason: disposition, Key: "ticketbox:payment:" + c.PaymentID.String() + ":g1:refund"})
					result.State = "refund_required"
					if err == nil && financial.RefundState == "succeeded" {
						result.State = "resolved"
					}
				} else {
					var financial *domain.FinancialView
					financial, err = w.repo.RequireCancel(ctx, c.PaymentID, decision)
					if err == nil && financial.CancelState == "cancelled" {
						result.State = "resolved"
					}
				}
				if err != nil {
					result.State, result.ErrorCategory = "pending", "financial_obligation_persistence_retry"
				}
			} else {
				result.State, result.ErrorCategory = "attention", "booking_terminal_conflict"
			}
		} else {
			result.State, result.ErrorCategory = "attention", "booking_phase_invalid"
		}
	} else {
		// The Booking authority was briefly unavailable. A collected payment
		// may have issuance waiting on sale_verified, so retry on the next
		// worker tick instead of the five-second default. Uncollected checkouts
		// keep the default backoff and do not amplify a Booking outage.
		if c.View != nil && c.View.ProviderOutcome == "collected" {
			result.NextAttemptAt = time.Now()
		}
	}
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer finishCancel()
	_, finishErr := w.repo.FinishFinancialWork(finishCtx, c, result)
	return finishErr
}

func (w *FinancialWorker) executeRefund(ctx context.Context, c domain.FinancialClaim) error {
	result := domain.FinancialResult{State: "retryable", ErrorCategory: "provider_refund_unknown", NextAttemptAt: financialRetryAt(c.Attempts)}
	var responsePrepared domain.RefundAuditRecord
	var responseCapture *domain.RefundResponseCapture
	var responseSnapshot *domain.RefundAuditResponse
	preparedAuditAvailable := false
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	observation, err := w.provider.InspectSucceededRefunds(bounded, c.Operation.Identity.ProviderAccount, c.Operation.Identity.LiveMode, c.View.Payment.PaymentIntentId)
	if err == nil && observation != nil {
		switch {
		case !observation.Collected:
			result.State, result.ErrorCategory = "attention", "provider_collection_conflict"
		case observation.SucceededRefundCents == c.Operation.Identity.AmountCents:
			result.State, result.ErrorCategory, result.Observation = "succeeded", "", observation
		case observation.Status == "pending":
			result.State, result.ErrorCategory, result.Observation = "provider_pending", "", observation
			result.NextAttemptAt = time.Now().Add(time.Minute)
		case observation.Status == "partial_attention" || observation.Status == "provider_attention":
			result.State, result.ErrorCategory, result.Observation = "attention", "provider_refund_not_full_succeeded", observation
		case c.View.RefundID != "":
			observation, err = w.provider.RetrieveRefund(bounded, c.Operation.Identity.ProviderAccount, c.Operation.Identity.LiveMode, c.View.RefundID)
			if err == nil && observation != nil {
				result.Observation = observation
				result.State = "provider_pending"
				if observation.Status == "failed" || observation.Status == "canceled" || observation.Status == "requires_action" {
					result.State, result.ErrorCategory = "attention", "provider_refund_terminal"
				}
			} else {
				result.State, result.ErrorCategory = "retryable", "provider_refund_retrieve_unknown"
			}
		case c.FirstAttemptAt == nil || time.Since(*c.FirstAttemptAt) >= 20*time.Hour:
			result.State, result.ErrorCategory = "attention", "refund_retention_ambiguity"
		default:
			guard, guardErr := w.repo.CheckRefundPreSend(bounded, c)
			if guardErr != nil {
				result.State, result.ErrorCategory = "retryable", "refund_pre_send_db_unknown"
			} else {
				switch guard.Decision {
				case repository.RefundPreSendAllow:
					if guard.AttemptID == uuid.Nil {
						result.State, result.ErrorCategory = "retryable", "refund_pre_send_attempt_missing"
						break
					}
					responsePrepared, err = domain.NewRefundPreparedAudit(c, guard.AttemptID)
					if err != nil {
						w.logRefundAuditFailure("prepared_identity", "invalid_prepared_identity")
					} else {
						preparedAuditAvailable = true
					}
					responseCapture = domain.NewRefundResponseCapture(guard.AttemptID)
					createCtx := domain.WithRefundResponseCapture(bounded, responseCapture)
					created, createErr := w.provider.CreateFullRefund(createCtx, c, c.View.Payment.PaymentIntentId)
					if createErr != nil {
						if responseCapture.Snapshot().Classification != domain.RefundAuditAdapterFailure {
							responseCapture.SetResult(domain.RefundAuditOutcomeUnknown, "provider_refund_unknown")
						}
					} else if created == nil {
						responseCapture.SetResult(domain.RefundAuditOutcomeUnknown, "incomplete_response")
						result.State, result.ErrorCategory = "retryable", "provider_refund_response_incomplete"
					} else {
						if responseCapture.Snapshot().Classification != domain.RefundAuditAdapterFailure {
							responseCapture.SetResult(domain.RefundAuditResponseObserved, "")
						}
						result.Observation = created
						result.State = "provider_pending"
						result.ErrorCategory = ""
						if created.Status == "failed" || created.Status == "canceled" || created.Status == "requires_action" {
							result.State, result.ErrorCategory = "attention", "provider_refund_terminal"
						}
					}
					// Snapshot the Create-only facts before the follow-up inspection.
					captured := responseCapture.Snapshot()
					responseSnapshot = &captured
					if createErr == nil {
						verified, verifyErr := w.provider.InspectSucceededRefunds(bounded, c.Operation.Identity.ProviderAccount, c.Operation.Identity.LiveMode, c.View.Payment.PaymentIntentId)
						if verifyErr == nil && verified != nil && verified.Status != "no_refunds" {
							result.Observation = verified
							switch {
							case verified.SucceededRefundCents == c.Operation.Identity.AmountCents:
								result.State, result.ErrorCategory = "succeeded", ""
							case verified.Status == "partial_attention" || verified.Status == "provider_attention":
								result.State, result.ErrorCategory = "attention", "provider_refund_not_full_succeeded"
							}
						}
					}
				case repository.RefundPreSendSucceeded:
					// FinishFinancialWork preserves the succeeded/resolved row while
					// releasing this still-owned claim, without fabricating evidence.
					result.State, result.ErrorCategory = "provider_pending", ""
				case repository.RefundPreSendExistingResult:
					if guard.RefundID == "" {
						result.State, result.ErrorCategory = "retryable", "refund_pre_send_result_without_id"
						break
					}
					existing, retrieveErr := w.provider.RetrieveRefund(bounded, c.Operation.Identity.ProviderAccount, c.Operation.Identity.LiveMode, guard.RefundID)
					if retrieveErr != nil || existing == nil {
						result.State, result.ErrorCategory = "retryable", "refund_pre_send_retrieve_unknown"
						break
					}
					result.Observation, result.State, result.ErrorCategory = existing, "provider_pending", ""
					if existing.Status == "failed" || existing.Status == "canceled" || existing.Status == "requires_action" {
						result.State, result.ErrorCategory = "attention", "provider_refund_terminal"
					}
				case repository.RefundPreSendStaleOwner:
					result.State, result.ErrorCategory = "retryable", "refund_pre_send_owner_lost"
				case repository.RefundPreSendCommandMismatch:
					result.State, result.ErrorCategory = "attention", "refund_pre_send_"+guard.Reason
				case repository.RefundPreSendNotAllowed:
					result.State, result.ErrorCategory = "attention", "refund_pre_send_not_allowed"
				case repository.RefundPreSendCoordinationWait:
					deferRepo, ok := w.repo.(repository.RefundAdmissionWorkRepository)
					if !ok {
						return errors.New("refund admission neutral defer is unavailable")
					}
					deferCtx, deferCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
					defer deferCancel()
					_, deferErr := deferRepo.DeferRefundForAdmission(deferCtx, c, guard, time.Now().UTC().Add(15*time.Second))
					return deferErr
				case repository.RefundPreSendAdmissionConflict:
					result.State, result.ErrorCategory = "attention", "committed_admission_conflicts_with_unused_refund"
				default:
					result.State, result.ErrorCategory = "retryable", "refund_pre_send_unknown_decision"
				}
			}
		}
	}
	if result.State == "retryable" && financialExhausted(c) {
		result.State, result.ErrorCategory = "attention", "refund_recovery_exhausted"
	}
	if result.State == "provider_pending" {
		result.NextAttemptAt = time.Now().Add(time.Minute)
	}
	if err := w.awaitVerifiedFullRefundBoundary(ctx, c, result); err != nil {
		return err
	}
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer finishCancel()
	owned, finishErr := w.repo.FinishFinancialWork(finishCtx, c, result)
	if responseCapture != nil {
		finishOutcome := domain.RefundAuditFinishCommitUnknown
		if finishErr == nil {
			if owned {
				finishOutcome = domain.RefundAuditFinishPersisted
			} else {
				finishOutcome = domain.RefundAuditFinishNotOwned
				w.logRefundAuditFailure("financial_finish", "financial_finish_not_owned")
			}
		} else {
			w.logRefundAuditFailure("financial_finish", "financial_finish_commit_unknown")
		}
		responseCapture.SetFinancialFinishOutcome(finishOutcome)
		if responseSnapshot != nil {
			responseSnapshot.FinancialFinishOutcome = finishOutcome
		}
		if preparedAuditAvailable && responseSnapshot != nil {
			w.appendRefundResponseAudit(ctx, responsePrepared, *responseSnapshot)
		}
	}
	return finishErr
}

// awaitVerifiedFullRefundBoundary is the opt-in acceptance barrier at which the
// provider has verified a full succeeded refund but the local refund marker has
// not been committed. The product claim columns (claim_token/claim_fence/
// lease_until/operation_kind='refund') are the marker authority; the append-only
// audit table is evidence, not a lease. Disabled controllers never enter here.
func (w *FinancialWorker) awaitVerifiedFullRefundBoundary(
	ctx context.Context, c domain.FinancialClaim, result domain.FinancialResult,
) error {
	if w == nil || (w.reliability == nil && w.w28RefundHook == nil) || c.Kind != "refund" {
		return nil
	}
	if configured, ok := w.reliability.(interface{ Enabled() bool }); ok && !configured.Enabled() {
		return nil
	}
	if result.State != "succeeded" || result.Observation == nil ||
		c.Refund == nil || c.View == nil || c.View.Payment == nil {
		return nil
	}
	observation := result.Observation
	if c.Token == uuid.Nil || c.Fence < 1 || !c.LeaseUntil.After(time.Now()) ||
		observation.RefundID == "" ||
		observation.SucceededRefundCents != c.Operation.Identity.AmountCents ||
		observation.SucceededRefundCents != c.Refund.AmountCents ||
		observation.IntentID != c.View.Payment.PaymentIntentId {
		return errors.New("enabled refund hook has an incomplete verified claim identity")
	}
	boundary := reliabilityhook.Boundary{
		WindowID:    "saga.late_collection.compensation_verified_full_refund",
		OperationID: c.PaymentID.String(),
		Checkpoint:  "payment_verified_full_refund_before_marker",
		Action:      reliabilityhook.ActionPause,
		Facts: map[string]string{
			"payment_id":          c.PaymentID.String(),
			"booking_id":          c.Operation.Identity.BookingID.String(),
			"payment_intent_id":   c.View.Payment.PaymentIntentId,
			"refund_id":           observation.RefundID,
			"refund_amount_cents": strconv.FormatInt(observation.SucceededRefundCents, 10),
			"refund_currency":     c.Operation.Identity.Currency,
			"claim_token":         c.Token.String(),
			"claim_fence":         strconv.FormatInt(c.Fence, 10),
			"lease_until":         c.LeaseUntil.UTC().Format(time.RFC3339Nano),
			"provider_account":    c.Operation.Identity.ProviderAccount,
			"livemode":            strconv.FormatBool(c.Operation.Identity.LiveMode),
		},
	}
	if w.reliability != nil {
		_, matched, err := w.reliability.Await(ctx, boundary)
		if matched || err != nil {
			return err
		}
	}
	if w.w28RefundHook == nil {
		return nil
	}
	boundary.Checkpoint = "payment_w28_verified_refund_before_marker"
	_, _, err := w.w28RefundHook.AwaitW28Refund(ctx, boundary)
	return err
}

func (w *FinancialWorker) appendRefundResponseAudit(ctx context.Context, prepared domain.RefundAuditRecord, response domain.RefundAuditResponse) {
	auditRepo, ok := w.repo.(repository.RefundAuditRepository)
	if !ok {
		w.logRefundAuditFailure("response_append", "audit_repository_unavailable")
		return
	}
	record, err := domain.NewRefundResponseAudit(prepared, response)
	if err != nil {
		w.logRefundAuditFailure("response_append", "invalid_response_record")
		return
	}
	appendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := auditRepo.AppendRefundAudit(appendCtx, record); err != nil {
		w.logRefundAuditFailure("response_append", "audit_append_failed")
	}
}

func (w *FinancialWorker) logRefundAuditFailure(stage, category string) {
	if w == nil || w.logger == nil {
		return
	}
	w.logger.Warn("refund provenance sidecar diagnostic", zap.String("stage", stage), zap.String("category", category))
}

func (w *FinancialWorker) executeCancel(ctx context.Context, c domain.FinancialClaim) error {
	result := domain.FinancialResult{State: "cancel_unknown", ErrorCategory: "provider_cancel_unknown", NextAttemptAt: financialRetryAt(c.Attempts)}
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	intentID := c.View.Payment.PaymentIntentId
	if intentID == "" {
		observations, err := w.provider.FindIntent(bounded, c.Operation)
		if err == nil && len(observations) == 1 {
			intentID = observations[0].IntentID
			result.Observation = &observations[0]
		} else {
			if len(observations) > 1 || financialExhausted(c) {
				result.State, result.ErrorCategory = "cancel_attention", "create_correlation_ambiguity"
			}
			finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer finishCancel()
			_, finishErr := w.repo.FinishFinancialWork(finishCtx, c, result)
			return finishErr
		}
	}
	observation, _, err := w.provider.RetrieveIntent(bounded, c.Operation.Identity.ProviderAccount, c.Operation.Identity.LiveMode, intentID)
	if err == nil {
		if observation.Collected {
			result.Observation = observation
		} else if observation.Status == "canceled" {
			result.State, result.ErrorCategory, result.Observation = "cancelled", "", observation
		} else if c.FirstAttemptAt == nil || time.Since(*c.FirstAttemptAt) >= 20*time.Hour {
			result.State, result.ErrorCategory = "cancel_attention", "cancel_retention_ambiguity"
		} else {
			observation, err = w.provider.CancelIntent(bounded, c, intentID)
			if err == nil {
				result.Observation = observation
				if observation.Status == "canceled" {
					result.State, result.ErrorCategory = "cancelled", ""
				}
			}
		}
	}
	if result.State == "cancel_unknown" && result.Observation == nil && financialExhausted(c) {
		result.State, result.ErrorCategory = "cancel_attention", "cancel_recovery_exhausted"
	}
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer finishCancel()
	_, err = w.repo.FinishFinancialWork(finishCtx, c, result)
	return err
}

func financialExhausted(c domain.FinancialClaim) bool {
	return c.Attempts >= 32 || c.FirstAttemptAt == nil || time.Since(*c.FirstAttemptAt) >= 20*time.Hour
}

func financialRetryAt(attempts int) time.Time {
	shift := max(0, min(attempts-1, 8))
	delay := time.Second * time.Duration(1<<shift)
	return time.Now().Add(delay + time.Duration(rand.Int64N(int64(delay/4)+1)))
}

func (w *FinancialWorker) RunOnce(ctx context.Context, limit int) (int, error) {
	if limit > 4 {
		limit = 4
	}
	claims, err := w.repo.ClaimFinancialWork(ctx, limit, 30*time.Second)
	if err != nil {
		return 0, err
	}
	// Claim only currently executable slots; no queued unrenewed leases.
	errorsCh := make(chan error, len(claims))
	for _, claim := range claims {
		go func(c domain.FinancialClaim) {
			if w.metrics != nil && w.metrics.PaymentFinancialAttempts != nil {
				w.metrics.PaymentFinancialAttempts.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", c.Kind)))
			}
			if c.Kind == "refund" {
				errorsCh <- w.executeRefund(ctx, c)
				return
			}
			if c.Kind == "cancel" {
				errorsCh <- w.executeCancel(ctx, c)
				return
			}
			if c.Kind == "reconcile" {
				errorsCh <- w.executeReconcile(ctx, c)
				return
			}
			_, err := w.executeCreate(ctx, c)
			errorsCh <- err
		}(claim)
	}
	var first error
	for range claims {
		if err := <-errorsCh; err != nil && first == nil {
			first = err
		}
	}
	if w.metrics != nil {
		due, attention, age, err := w.repo.FinancialBacklog(ctx)
		if err != nil {
			if first == nil {
				first = err
			}
		} else {
			if w.metrics.PaymentFinancialDue != nil {
				w.metrics.PaymentFinancialDue.Record(ctx, due)
			}
			if w.metrics.PaymentFinancialAttention != nil {
				w.metrics.PaymentFinancialAttention.Record(ctx, attention)
			}
			if w.metrics.PaymentFinancialOldestAge != nil {
				w.metrics.PaymentFinancialOldestAge.Record(ctx, age)
			}
		}
	}
	return len(claims), first
}

func (w *FinancialWorker) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if w.publisher != nil {
			_, _ = w.RunReceiptsOnce(ctx, 4)
		}
		if _, err := w.RunOnce(ctx, 4); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
