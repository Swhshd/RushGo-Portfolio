package domain

import (
	"github.com/google/uuid"
	"time"
)

type OperationsQuery struct {
	From, To      time.Time
	Limit, Offset int
}
type OperationIssue struct {
	Kind, Source, State, NextAction string
	ObservedAt                      time.Time
}
type OperationsBooking struct {
	BookingID, EventID                                uuid.UUID
	Status                                            string
	CreatedAt                                         time.Time
	Generation, TicketCount, ExpectedTickets          int64
	IssuanceDenied                                    bool
	RefundStatus, RevocationState, PaymentSourceState string
	RefundDecidedAt                                   *time.Time
	OutboxStale, AdmissionStale                       bool
	Issues                                            []OperationIssue
}
type StaffWork struct {
	EventID        uuid.UUID
	Access         string
	PendingRefunds int64
}
