package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	pkgkafka "github.com/ticketbox/pkg/kafka"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	eventv1 "github.com/ticketbox/pkg/proto/event/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"github.com/ticketbox/saga/internal/domain"
	"github.com/ticketbox/saga/internal/repository"
)

const (
	StalledIssuanceAlreadyIssued       = "already_issued"
	StalledIssuanceDenied              = "issuance_denied"
	StalledIssuanceNeedsManual         = "needs_manual"
	StalledIssuanceBlocked             = "blocked"
	StalledIssuanceProcessing          = "processing"
	StalledIssuanceStalledConsumer     = "stalled_consumer"
	StalledIssuanceConsumerDown        = "consumer_down"
	StalledIssuanceConsumerNotAssigned = "consumer_not_assigned"
	StalledIssuanceConsumedNoTicket    = "consumed_without_ticket"
	StalledIssuanceRecordNotRetained   = "record_not_retained"
	StalledIssuanceScanTruncated       = "scan_truncated"
	StalledIssuanceMultiInstance       = "multi_instance_manual"
	StalledIssuanceKafkaUnavailable    = "kafka_unavailable"
	StalledIssuanceNotApplicable       = "not_applicable"

	stalledIssuanceMinAge      = 10 * time.Second
	stalledIssuanceDefaultScan = 1000
)

type StalledIssuanceInspectRequest struct {
	BookingID, SagaID       uuid.UUID
	ExpectedProviderAccount string
	Operator, Reason        string
	Topic, Group            string
	SampleInterval          time.Duration
	MaxScan                 int
	IncludeKafkaOnIssued    bool
}

type StalledIssuanceOutboxObservation struct {
	Count         int        `json:"count"`
	Published     int        `json:"published"`
	EventID       string     `json:"event_id,omitempty"`
	PublishedAt   *time.Time `json:"published_at,omitempty"`
	ElapsedMillis int64      `json:"elapsed_millis,omitempty"`
}

type StalledIssuanceTicketObservation struct {
	TicketCount          int64     `json:"ticket_count"`
	IssuanceReceiptCount int64     `json:"issuance_receipt_count"`
	ObservedAt           time.Time `json:"observed_at,omitempty"`
}

type StalledIssuanceKafkaPartition struct {
	Partition       int   `json:"partition"`
	FirstOffset     int64 `json:"first_offset"`
	CommittedOffset int64 `json:"committed_offset"`
	LogEndOffset    int64 `json:"log_end_offset"`
}

type StalledIssuanceKafkaObservation struct {
	Topic             string                          `json:"topic"`
	Group             string                          `json:"group"`
	ObservedAt        time.Time                       `json:"observed_at"`
	Members           int                             `json:"members"`
	MemberAssignments []StalledIssuanceMember         `json:"member_assignments,omitempty"`
	Partitions        []StalledIssuanceKafkaPartition `json:"partitions"`
	RecordFound       bool                            `json:"record_found"`
	RecordPartition   int                             `json:"record_partition"`
	RecordOffset      int64                           `json:"record_offset"`
	RecordUncommitted bool                            `json:"record_uncommitted"`
	RecordConsumed    bool                            `json:"record_consumed"`
	StableNoProgress  bool                            `json:"stable_no_progress"`
	ScanTruncated     bool                            `json:"scan_truncated"`
	Scanned           int                             `json:"scanned"`
}

type StalledIssuanceMember struct {
	MemberID   string `json:"member_id"`
	ClientID   string `json:"client_id,omitempty"`
	ClientHost string `json:"client_host,omitempty"`
}

type StalledIssuancePlan struct {
	Action                 string   `json:"action"`
	TargetService          string   `json:"target_service"`
	Impact                 string   `json:"impact"`
	RequiresIsolatedTarget bool     `json:"requires_isolated_target"`
	Preconditions          []string `json:"preconditions"`
	Verification           []string `json:"verification"`
}

type StalledIssuanceReport struct {
	SagaID             string                           `json:"saga_id"`
	BookingID          string                           `json:"booking_id"`
	PaymentID          string                           `json:"payment_id,omitempty"`
	Classification     string                           `json:"classification"`
	Reasons            []string                         `json:"reasons,omitempty"`
	EligibleForRestart bool                             `json:"eligible_for_restart"`
	ObservedAt         time.Time                        `json:"observed_at"`
	Outbox             StalledIssuanceOutboxObservation `json:"outbox"`
	Tickets            StalledIssuanceTicketObservation `json:"tickets"`
	Authorities        paidUnissuedAuthoritySnapshot    `json:"authorities"`
	Kafka              *StalledIssuanceKafkaObservation `json:"kafka,omitempty"`
	Plan               *StalledIssuancePlan             `json:"plan,omitempty"`
}

type IssuanceGroupInspector interface {
	InspectGroup(context.Context, string, string) (*pkgkafka.GroupSnapshot, error)
	FindRecord(context.Context, string, int, int64, int64, int, func([]byte) bool) (int64, bool, int, error)
}

type kafkaGroupInspector struct{ brokers []string }

func NewKafkaGroupInspector(brokers []string) IssuanceGroupInspector {
	return kafkaGroupInspector{brokers: append([]string(nil), brokers...)}
}

func (i kafkaGroupInspector) InspectGroup(ctx context.Context, topic, group string) (*pkgkafka.GroupSnapshot, error) {
	return pkgkafka.InspectGroup(ctx, i.brokers, topic, group)
}

func (i kafkaGroupInspector) FindRecord(ctx context.Context, topic string, partition int, from, to int64, max int, match func([]byte) bool) (int64, bool, int, error) {
	return pkgkafka.FindRecord(ctx, i.brokers, topic, partition, from, to, max, match)
}

// InspectStalledIssuance classifies a paid, completed Saga whose ticket
// issuance outbox is already published but whose Booking has no ticket. It is
// read-only: it never commits offsets, resets a group, publishes a message, or
// writes business state. A restart plan is only a recommendation; the caller
// must verify the isolated target before acting.
func (s *SagaService) InspectStalledIssuance(ctx context.Context, req StalledIssuanceInspectRequest, inspector IssuanceGroupInspector) (*StalledIssuanceReport, error) {
	if req.BookingID == uuid.Nil || req.SagaID == uuid.Nil || strings.TrimSpace(req.ExpectedProviderAccount) == "" ||
		strings.TrimSpace(req.Topic) == "" || strings.TrimSpace(req.Group) == "" {
		return nil, repository.ErrInvalidRecoveryInput
	}
	if inspector == nil {
		return nil, errors.New("issuance group inspector is required")
	}
	if req.SampleInterval <= 0 {
		req.SampleInterval = 3 * time.Second
	}
	if req.MaxScan <= 0 || req.MaxScan > 10000 {
		req.MaxScan = stalledIssuanceDefaultScan
	}
	repo, ok := s.repo.(repository.PaidUnissuedRecoveryRepository)
	if !ok {
		return nil, repository.ErrRecoveryAttention
	}
	state, err := repo.GetPaidUnissuedSagaState(ctx, req.SagaID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	report := &StalledIssuanceReport{
		SagaID: req.SagaID.String(), BookingID: req.BookingID.String(), ObservedAt: now,
		Outbox: StalledIssuanceOutboxObservation{Count: state.OutboxCount, Published: state.OutboxPublished},
	}
	if state.OutboxPublishedAt != nil {
		report.Outbox.PublishedAt = state.OutboxPublishedAt
		report.Outbox.ElapsedMillis = now.Sub(*state.OutboxPublishedAt).Milliseconds()
	}
	if state.OutboxEventID != nil {
		report.Outbox.EventID = state.OutboxEventID.String()
	}
	if state.Saga == nil || state.Saga.Input == nil || state.Saga.ID != req.SagaID || state.Saga.BookingID != req.BookingID {
		return finishedInspection(report, StalledIssuanceNotApplicable, "saga_booking_identity_mismatch", nil), nil
	}
	if state.Saga.Status != domain.SAGA_COMPLETED {
		return finishedInspection(report, StalledIssuanceNotApplicable, "saga_not_completed", nil), nil
	}
	if state.OutboxCount != 1 || state.OutboxPublishedAt == nil || state.OutboxEventID == nil {
		return finishedInspection(report, StalledIssuanceNotApplicable, "issuance_outbox_not_published_or_ambiguous", nil), nil
	}

	bookingView, err := s.bookingAuthority(ctx, *state.Saga.Input)
	if err != nil {
		return finishedInspection(report, StalledIssuanceBlocked, "booking_authority_unavailable", nil), nil
	}
	report.Authorities.Booking = paidUnissuedBookingObservation{Checked: true, Phase: bookingView.Phase, Status: bookingView.Booking.Status, Version: bookingView.Version}
	if bookingView.Phase != "TERMINAL" || bookingView.Booking.Status != "CONFIRMED" {
		return finishedInspection(report, StalledIssuanceBlocked, "booking_not_confirmed", nil), nil
	}

	paymentRecord, err := s.financialAuthority(ctx, *state.Saga.Input)
	if err != nil {
		return finishedInspection(report, StalledIssuanceBlocked, "payment_recovery_authority_unavailable", nil), nil
	}
	paymentID, parseErr := uuid.Parse(paymentRecord.Id)
	if parseErr != nil || paymentID == uuid.Nil {
		return finishedInspection(report, StalledIssuanceBlocked, "payment_identity_unavailable", nil), nil
	}
	paymentView, err := s.paymentClient.GetPaidRecoveryPaymentSnapshot(ctx, &paymentv1.FinancialPaymentRequest{
		PaymentId: paymentID.String(), BookingId: req.BookingID.String(), Generation: 1,
	})
	if err != nil || paymentView == nil || paymentView.Payment == nil || paymentView.Payment.Id != paymentID.String() {
		return finishedInspection(report, StalledIssuanceBlocked, "payment_recovery_authority_unavailable", nil), nil
	}
	if reason := paidRecoveryPaymentReason(paymentView, *state.Saga, strings.TrimSpace(req.ExpectedProviderAccount), now); reason != "" {
		report.Authorities.Payment = paymentObservation(paymentView)
		return finishedInspection(report, StalledIssuanceBlocked, reason, nil), nil
	}
	report.PaymentID = paymentID.String()
	report.Authorities.Payment = paymentObservation(paymentView)

	eventView, err := s.eventClient.GetEvent(ctx, &eventv1.GetEventRequest{EventId: state.Saga.Input.EventID.String()})
	if err != nil || eventView == nil || eventView.Id != state.Saga.Input.EventID.String() {
		return finishedInspection(report, StalledIssuanceBlocked, "event_authority_unavailable_or_mismatched", nil), nil
	}
	eventObservation, reason := paidRecoveryEventObservation(eventView, bookingView)
	report.Authorities.Event = eventObservation
	if reason != "" {
		return finishedInspection(report, StalledIssuanceBlocked, reason, nil), nil
	}
	seatIDs := make([]string, len(state.Saga.Input.SeatIDs))
	for i, id := range state.Saga.Input.SeatIDs {
		seatIDs[i] = id.String()
	}
	seatView, err := s.eventClient.VerifyBookingTicketEligibility(ctx, &eventv1.VerifyBookingTicketEligibilityRequest{
		BookingId: req.BookingID.String(), EventId: state.Saga.Input.EventID.String(), Generation: 1, SeatIds: seatIDs,
	})
	if err != nil || seatView == nil {
		return finishedInspection(report, StalledIssuanceBlocked, "event_seat_authority_unavailable", nil), nil
	}
	if seatView.State != eventv1.BookingTicketEligibilityState_BOOKING_TICKET_ELIGIBILITY_STATE_ELIGIBLE {
		report.Authorities.Event.SeatEligibility = seatView.State.String()
		report.Authorities.Event.SeatEligibilityWhy = seatView.ReasonCode
		return finishedInspection(report, StalledIssuanceBlocked, "event_seat_not_eligible", nil), nil
	}
	report.Authorities.Event.SeatEligibility = seatView.State.String()
	report.Authorities.Event.SeatEligibilityWhy = seatView.ReasonCode

	ticketSnapshot, err := s.bookingClient.GetPaidRecoveryTicketSnapshot(ctx, &bookingv1.LifecycleRequest{BookingId: req.BookingID.String(), Generation: 1})
	if err != nil || ticketSnapshot == nil {
		return finishedInspection(report, StalledIssuanceBlocked, "ticket_authority_unavailable", nil), nil
	}
	if reason := stalledIssuanceTicketSnapshotReason(ticketSnapshot, req.BookingID, req.SagaID, now); reason != "" {
		return finishedInspection(report, StalledIssuanceBlocked, reason, nil), nil
	}
	report.Authorities.Tickets = ticketObservation(ticketSnapshot)
	report.Tickets = StalledIssuanceTicketObservation{
		TicketCount: ticketSnapshot.TicketCount, IssuanceReceiptCount: ticketSnapshot.IssuanceReceiptCount,
		ObservedAt: ticketSnapshot.ObservedAt.AsTime().UTC(),
	}
	switch {
	case ticketSnapshot.TicketCount > 0 && ticketSnapshot.IssuanceReceiptCount == 1:
		if req.IncludeKafkaOnIssued {
			if snapshot, inspectErr := inspector.InspectGroup(ctx, req.Topic, req.Group); inspectErr == nil {
				report.Kafka = kafkaObservation(snapshot, snapshot)
			}
		}
		return finishedInspection(report, StalledIssuanceAlreadyIssued, "ticket_and_issued_receipt_present", nil), nil
	case ticketSnapshot.TicketCount == 0 && ticketSnapshot.IssuanceReceiptCount == 1:
		return finishedInspection(report, StalledIssuanceDenied, "denied_issuance_receipt_present", nil), nil
	case ticketSnapshot.TicketCount > 0 || ticketSnapshot.IssuanceReceiptCount > 1:
		return finishedInspection(report, StalledIssuanceNeedsManual, "partial_or_conflicting_ticket_state", nil), nil
	}

	first, err := inspector.InspectGroup(ctx, req.Topic, req.Group)
	if err != nil {
		return finishedInspection(report, StalledIssuanceKafkaUnavailable, "kafka_group_state_unavailable", nil), nil
	}
	if err := waitInspectInterval(ctx, req.SampleInterval); err != nil {
		return nil, err
	}
	second, err := inspector.InspectGroup(ctx, req.Topic, req.Group)
	if err != nil {
		return finishedInspection(report, StalledIssuanceKafkaUnavailable, "kafka_group_state_unavailable", nil), nil
	}
	kafka := kafkaObservation(first, second)
	report.Kafka = kafka
	matcher := issuanceEventMatcher(*state.OutboxEventID)
	for _, partition := range first.Partitions {
		from := partition.CommittedOffset
		if from < 0 {
			from = partition.FirstOffset
		}
		if from < partition.FirstOffset {
			from = partition.FirstOffset
		}
		if from >= partition.LogEndOffset {
			continue
		}
		offset, found, scanned, scanErr := inspector.FindRecord(ctx, req.Topic, partition.Partition, from, partition.LogEndOffset, req.MaxScan, matcher)
		kafka.Scanned += scanned
		if scanErr != nil {
			return finishedInspection(report, StalledIssuanceKafkaUnavailable, "kafka_record_scan_unavailable", nil), nil
		}
		if found {
			kafka.RecordFound, kafka.RecordPartition, kafka.RecordOffset, kafka.RecordUncommitted = true, partition.Partition, offset, true
			break
		}
		if partition.LogEndOffset-from > int64(req.MaxScan) {
			kafka.ScanTruncated = true
		}
	}
	if !kafka.RecordFound {
		for _, partition := range first.Partitions {
			to := partition.CommittedOffset
			if to <= partition.FirstOffset {
				continue
			}
			from := to - int64(req.MaxScan)
			if from < partition.FirstOffset {
				from = partition.FirstOffset
			}
			offset, found, scanned, scanErr := inspector.FindRecord(ctx, req.Topic, partition.Partition, from, to, req.MaxScan, matcher)
			kafka.Scanned += scanned
			if scanErr != nil {
				return finishedInspection(report, StalledIssuanceKafkaUnavailable, "kafka_record_scan_unavailable", nil), nil
			}
			if found {
				kafka.RecordFound, kafka.RecordPartition, kafka.RecordOffset, kafka.RecordConsumed = true, partition.Partition, offset, true
				break
			}
			if to-partition.FirstOffset > int64(req.MaxScan) {
				kafka.ScanTruncated = true
			}
		}
	}

	elapsed := time.Duration(0)
	if state.OutboxPublishedAt != nil {
		elapsed = now.Sub(*state.OutboxPublishedAt)
	}
	classification, reason := classifyStalledIssuance(kafka, first, elapsed)
	plan := stalledIssuancePlan(classification, req.Topic, req.Group)
	report.Plan = plan
	if classification == StalledIssuanceStalledConsumer || classification == StalledIssuanceConsumerDown {
		report.EligibleForRestart = len(first.Members) <= 1 && kafka.RecordUncommitted && report.Tickets.TicketCount == 0 && report.Tickets.IssuanceReceiptCount == 0
	}
	return finishedInspection(report, classification, reason, plan), nil
}

func kafkaObservation(first, second *pkgkafka.GroupSnapshot) *StalledIssuanceKafkaObservation {
	observation := &StalledIssuanceKafkaObservation{Topic: first.Topic, Group: first.Group, ObservedAt: first.ObservedAt}
	observation.Members = len(first.Members)
	for _, member := range first.Members {
		observation.MemberAssignments = append(observation.MemberAssignments, StalledIssuanceMember{
			MemberID: member.MemberID, ClientID: member.ClientID, ClientHost: member.ClientHost,
		})
	}
	for _, partition := range first.Partitions {
		observation.Partitions = append(observation.Partitions, StalledIssuanceKafkaPartition{
			Partition: partition.Partition, FirstOffset: partition.FirstOffset,
			CommittedOffset: partition.CommittedOffset, LogEndOffset: partition.LogEndOffset,
		})
	}
	observation.StableNoProgress = committedOffsetsStable(first, second)
	return observation
}

func committedOffsetsStable(first, second *pkgkafka.GroupSnapshot) bool {
	if first == nil || second == nil || len(first.Partitions) == 0 || len(second.Partitions) != len(first.Partitions) {
		return false
	}
	for _, a := range first.Partitions {
		matched := false
		for _, b := range second.Partitions {
			if a.Topic == b.Topic && a.Partition == b.Partition && a.CommittedOffset == b.CommittedOffset {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func classifyStalledIssuance(kafka *StalledIssuanceKafkaObservation, snapshot *pkgkafka.GroupSnapshot, elapsed time.Duration) (string, string) {
	if kafka == nil || snapshot == nil {
		return StalledIssuanceKafkaUnavailable, "kafka_group_state_unavailable"
	}
	if kafka.RecordConsumed {
		return StalledIssuanceConsumedNoTicket, "record_committed_without_ticket"
	}
	if !kafka.RecordUncommitted {
		if kafka.ScanTruncated {
			return StalledIssuanceScanTruncated, "record_not_found_within_configured_scan_bound"
		}
		return StalledIssuanceRecordNotRetained, "outbox_record_not_found_in_retained_ranges"
	}
	if elapsed < stalledIssuanceMinAge || !kafka.StableNoProgress {
		return StalledIssuanceProcessing, "issuance_recent_or_offset_progressing"
	}
	if kafka.Members == 0 {
		return StalledIssuanceConsumerDown, "no_group_member_and_outbox_record_uncommitted"
	}
	if kafka.Members > 1 {
		return StalledIssuanceMultiInstance, "multiple_group_members"
	}
	if !memberAssigned(snapshot, kafka.Topic, kafka.RecordPartition) {
		return StalledIssuanceConsumerNotAssigned, "sole_member_not_assigned_to_record_partition"
	}
	return StalledIssuanceStalledConsumer, "outbox_record_uncommitted_member_idle_and_offset_stable"
}

func memberAssigned(snapshot *pkgkafka.GroupSnapshot, topic string, partition int) bool {
	for _, member := range snapshot.Members {
		for _, assignment := range member.Topics {
			if assignment.Topic != topic {
				continue
			}
			for _, assigned := range assignment.Partitions {
				if assigned == partition {
					return true
				}
			}
		}
	}
	return false
}

func issuanceEventMatcher(eventID uuid.UUID) func([]byte) bool {
	return func(value []byte) bool {
		var envelope struct {
			ID string `json:"id"`
		}
		if len(value) == 0 || json.Unmarshal(value, &envelope) != nil {
			return false
		}
		return envelope.ID == eventID.String()
	}
}

func stalledIssuanceTicketSnapshotReason(snapshot *bookingv1.PaidRecoveryTicketSnapshot, bookingID, sagaID uuid.UUID, now time.Time) string {
	if snapshot == nil || snapshot.BookingId != bookingID.String() || snapshot.SagaId != sagaID.String() || snapshot.Generation != 1 ||
		snapshot.ObservedAt == nil || snapshot.ObservedAt.CheckValid() != nil || snapshot.TicketCount < 0 || snapshot.IssuanceReceiptCount < 0 {
		return "ticket_snapshot_identity_or_shape_mismatch"
	}
	observed := snapshot.ObservedAt.AsTime()
	if observed.After(now.Add(30*time.Second)) || now.Sub(observed) > paidRecoveryEvidenceMaxAge {
		return "ticket_snapshot_stale"
	}
	return ""
}

func waitInspectInterval(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func finishedInspection(report *StalledIssuanceReport, classification, reason string, plan *StalledIssuancePlan) *StalledIssuanceReport {
	report.Classification = classification
	if reason != "" {
		report.Reasons = append(report.Reasons, reason)
	}
	if plan != nil {
		report.Plan = plan
	}
	return report
}

func stalledIssuancePlan(classification, topic, group string) *StalledIssuancePlan {
	switch classification {
	case StalledIssuanceStalledConsumer, StalledIssuanceConsumerDown, StalledIssuanceMultiInstance, StalledIssuanceConsumerNotAssigned:
	default:
		return nil
	}
	return &StalledIssuancePlan{
		Action: "restart_booking_consumer", TargetService: "booking-service",
		Impact:                 fmt.Sprintf("Restarts every consumer and background task in the booking process; group %s resumes topic %s from its committed offset. The restart is not scoped to one ticket.", group, topic),
		RequiresIsolatedTarget: true,
		Preconditions: []string{
			"the target is this run's booking-service instance and its ownership labels match",
			"the outbox event is still uncommitted and the group has no more than one member",
			"no ticket or issuance receipt exists for the booking",
			"the payment, event, and booking authorities are eligible",
		},
		Verification: []string{
			"the original outbox event is processed by the restarted consumer",
			"one ticket per booked seat and one issued issuance receipt exist",
			"the group committed offset advances past the outbox event",
			"a following message on the partition is processed",
			"a repeat inspection reports already_issued without another restart",
		},
	}
}
