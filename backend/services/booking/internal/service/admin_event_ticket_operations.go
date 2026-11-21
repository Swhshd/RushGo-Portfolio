package service

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/repository"
)

var (
	ErrAdminEventTicketOperationsInvalidEventID   = errors.New("invalid Admin event ticket operations event id")
	ErrAdminEventTicketOperationsInvalidSnapshot  = errors.New("invalid Admin event ticket operations snapshot")
	ErrAdminEventTicketOperationsCapacityExceeded = repository.ErrAdminEventTicketOperationsCapacityExceeded
	ErrAdminEventTicketOperationsReadFailed       = errors.New("Admin event ticket operations source unavailable")
)

const AdminEventAfterSalesDeliveryPending = "CONFIRMED_TICKET_DELIVERY_PENDING"

type AdminEventTicketOperations struct {
	EventID                string
	ObservedAt             time.Time
	TotalRecordedCount     int32
	IssuedCount            int32
	CheckedInCount         int32
	RevokedCount           int32
	SuccessfulCheckinCount int32
	LatestCheckinAt        *time.Time
	RecentCheckins         []time.Time
	SourceRecordLimit      int32
	DeliveryExceptions     []AdminEventAfterSalesDeliveryException
}

type AdminEventAfterSalesDeliveryException struct {
	BookingID              string
	State                  string
	BookingStatus          string
	ExpectedTicketCount    int64
	RecordedTicketCount    int64
	IssuanceReceiptOutcome string
	IssuanceReasonCode     string
	BookingCreatedAt       time.Time
}

type AdminEventTicketOperationsService struct {
	reader repository.AdminEventTicketOperationsReader
}

func NewAdminEventTicketOperationsService(reader repository.AdminEventTicketOperationsReader) *AdminEventTicketOperationsService {
	return &AdminEventTicketOperationsService{reader: reader}
}

func (s *AdminEventTicketOperationsService) Get(ctx context.Context, eventID uuid.UUID) (*AdminEventTicketOperations, error) {
	if s == nil || s.reader == nil {
		return nil, ErrAdminEventTicketOperationsReadFailed
	}
	if eventID == uuid.Nil {
		return nil, ErrAdminEventTicketOperationsInvalidEventID
	}
	snapshot, err := s.reader.ReadAdminEventTicketOperations(ctx, eventID)
	if errors.Is(err, repository.ErrAdminEventTicketOperationsCapacityExceeded) {
		return nil, ErrAdminEventTicketOperationsCapacityExceeded
	}
	if err != nil {
		return nil, ErrAdminEventTicketOperationsReadFailed
	}
	result, err := summarizeAdminEventTicketOperations(snapshot, eventID)
	if err != nil {
		if errors.Is(err, ErrAdminEventTicketOperationsCapacityExceeded) {
			return nil, ErrAdminEventTicketOperationsCapacityExceeded
		}
		return nil, ErrAdminEventTicketOperationsInvalidSnapshot
	}
	return result, nil
}

func summarizeAdminEventTicketOperations(snapshot repository.AdminEventTicketOperationsSnapshot, eventID uuid.UUID) (*AdminEventTicketOperations, error) {
	if eventID == uuid.Nil || snapshot.EventID != eventID || !validAdminEventTicketOperationsTime(snapshot.ObservedAt) {
		return nil, ErrAdminEventTicketOperationsInvalidSnapshot
	}
	if len(snapshot.Tickets) > repository.AdminEventTicketOperationsCap || len(snapshot.Audits) > repository.AdminEventTicketOperationsCap ||
		len(snapshot.DeliveryExceptions) > repository.AdminEventTicketOperationsCap {
		return nil, ErrAdminEventTicketOperationsCapacityExceeded
	}

	ticketsByID := make(map[uuid.UUID]repository.AdminEventTicketOperationsTicket, len(snapshot.Tickets))
	for _, ticket := range snapshot.Tickets {
		if ticket.TicketID == uuid.Nil || ticket.EventID != eventID {
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
		if _, exists := ticketsByID[ticket.TicketID]; exists {
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
		if ticket.CheckedInAt != nil && (!validAdminEventTicketOperationsTime(*ticket.CheckedInAt) || ticket.CheckedInAt.After(snapshot.ObservedAt)) {
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
		if ticket.RevokedAt != nil && !validAdminEventTicketOperationsTime(*ticket.RevokedAt) {
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
		ticketsByID[ticket.TicketID] = ticket
	}
	deliveryExceptions := make([]AdminEventAfterSalesDeliveryException, 0, len(snapshot.DeliveryExceptions))
	for _, exception := range snapshot.DeliveryExceptions {
		identityConflict := exception.IssuanceReceiptOutcome == "CONFLICT" && exception.IssuanceReasonCode != ""
		if exception.BookingID == uuid.Nil || exception.State != AdminEventAfterSalesDeliveryPending || exception.BookingStatus != "CONFIRMED" ||
			exception.ExpectedTicketCount < 0 || exception.RecordedTicketCount < 0 ||
			(exception.ExpectedTicketCount == exception.RecordedTicketCount && !identityConflict) ||
			!validAdminEventTicketOperationsTime(exception.BookingCreatedAt) || exception.BookingCreatedAt.After(snapshot.ObservedAt) {
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
		switch exception.IssuanceReceiptOutcome {
		case "NOT_RECORDED", "issued", "denied":
		case "CONFLICT":
			if exception.IssuanceReasonCode == "" {
				return nil, ErrAdminEventTicketOperationsInvalidSnapshot
			}
		default:
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
		deliveryExceptions = append(deliveryExceptions, AdminEventAfterSalesDeliveryException{
			BookingID: exception.BookingID.String(), State: exception.State, BookingStatus: exception.BookingStatus,
			ExpectedTicketCount: exception.ExpectedTicketCount, RecordedTicketCount: exception.RecordedTicketCount,
			IssuanceReceiptOutcome: exception.IssuanceReceiptOutcome, IssuanceReasonCode: exception.IssuanceReasonCode,
			BookingCreatedAt: exception.BookingCreatedAt.UTC(),
		})
	}
	sort.Slice(deliveryExceptions, func(i, j int) bool {
		if !deliveryExceptions[i].BookingCreatedAt.Equal(deliveryExceptions[j].BookingCreatedAt) {
			return deliveryExceptions[i].BookingCreatedAt.Before(deliveryExceptions[j].BookingCreatedAt)
		}
		return deliveryExceptions[i].BookingID < deliveryExceptions[j].BookingID
	})

	auditsByTicket := make(map[uuid.UUID]repository.AdminEventTicketOperationsAudit, len(snapshot.Audits))
	for _, audit := range snapshot.Audits {
		if audit.TicketID == uuid.Nil || audit.EventID != eventID || audit.ResultCode != "accepted" || !validAdminEventTicketOperationsTime(audit.CheckedInAt) || audit.CheckedInAt.After(snapshot.ObservedAt) {
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
		ticket, exists := ticketsByID[audit.TicketID]
		if !exists || ticket.CheckedInAt == nil || !ticket.CheckedInAt.Equal(audit.CheckedInAt) || ticket.Status == "ISSUED" {
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
		if _, exists := auditsByTicket[audit.TicketID]; exists {
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
		auditsByTicket[audit.TicketID] = audit
	}

	result := &AdminEventTicketOperations{
		EventID:            eventID.String(),
		ObservedAt:         snapshot.ObservedAt.UTC(),
		SourceRecordLimit:  repository.AdminEventTicketOperationsCap,
		DeliveryExceptions: deliveryExceptions,
	}
	for _, ticket := range snapshot.Tickets {
		switch ticket.Status {
		case "ISSUED":
			if ticket.CheckedInAt != nil || ticket.RevokedAt != nil {
				return nil, ErrAdminEventTicketOperationsInvalidSnapshot
			}
			result.IssuedCount++
		case "CHECKED_IN":
			if ticket.CheckedInAt == nil || ticket.RevokedAt != nil {
				return nil, ErrAdminEventTicketOperationsInvalidSnapshot
			}
			if _, exists := auditsByTicket[ticket.TicketID]; !exists {
				return nil, ErrAdminEventTicketOperationsInvalidSnapshot
			}
			result.CheckedInCount++
		case "REVOKED":
			if ticket.RevokedAt == nil {
				return nil, ErrAdminEventTicketOperationsInvalidSnapshot
			}
			if ticket.CheckedInAt != nil {
				if _, exists := auditsByTicket[ticket.TicketID]; !exists {
					return nil, ErrAdminEventTicketOperationsInvalidSnapshot
				}
			}
			result.RevokedCount++
		default:
			return nil, ErrAdminEventTicketOperationsInvalidSnapshot
		}
	}
	result.TotalRecordedCount = result.IssuedCount + result.CheckedInCount + result.RevokedCount
	result.SuccessfulCheckinCount = int32(len(auditsByTicket))
	if result.TotalRecordedCount > repository.AdminEventTicketOperationsCap || result.SuccessfulCheckinCount > result.TotalRecordedCount {
		return nil, ErrAdminEventTicketOperationsInvalidSnapshot
	}

	audits := append([]repository.AdminEventTicketOperationsAudit(nil), snapshot.Audits...)
	sort.Slice(audits, func(i, j int) bool { return audits[i].CheckedInAt.After(audits[j].CheckedInAt) })
	recentCount := len(audits)
	if recentCount > 5 {
		recentCount = 5
	}
	result.RecentCheckins = make([]time.Time, 0, recentCount)
	for _, audit := range audits[:recentCount] {
		result.RecentCheckins = append(result.RecentCheckins, audit.CheckedInAt.UTC())
	}
	if len(result.RecentCheckins) > 0 {
		latest := result.RecentCheckins[0]
		result.LatestCheckinAt = &latest
	}
	return result, nil
}

func validAdminEventTicketOperationsTime(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999
}
