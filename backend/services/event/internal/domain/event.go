package domain

import (
	"time"

	"github.com/google/uuid"
)

const DefaultReservationTimeInMinutes int32 = 15

const (
	PublicationDraft     = "draft"
	PublicationPublished = "published"

	SalesScheduled = "scheduled"
	SalesSelling   = "selling"
	SalesPaused    = "paused"
	SalesEnded     = "ended"
	SalesCancelled = "cancelled"
)

func NormalizeReservationTimeInMinutes(minutes int32) int32 {
	if minutes <= 0 {
		return DefaultReservationTimeInMinutes
	}
	return minutes
}

type Event struct {
	ID                uuid.UUID
	Title             string
	Description       string
	Category          string
	Venue             string
	Location          string
	Date              time.Time
	DateVersion       int64
	ImageURL          string
	Status            string
	PublicationStatus string
	SalesStatus       string
	SalesStartAt      *time.Time
	SalesEndAt        *time.Time
	UpdateSalesWindow bool
	Tiers             []TicketTier
	Seats             []*Seat
	CreationRequest   *EventCreationRequest
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type EventCreationRequest struct {
	ActorID     uuid.UUID
	RequestID   string
	RequestHash []byte
}

// EventSaleEligibility applies the server-owned sale window. Empty lifecycle
// fields are accepted only for legacy active rows while the compatibility
// migration is being applied; every other unknown state fails closed.
func EventSaleEligibility(event *Event, now time.Time) (bool, string) {
	if event == nil {
		return false, "event_not_found"
	}

	publication := event.PublicationStatus
	sales := event.SalesStatus
	if publication == "" && sales == "" && event.Status == "active" {
		publication = PublicationPublished
		sales = SalesSelling
	}
	if publication != PublicationPublished {
		if publication == PublicationDraft {
			return false, "event_not_published"
		}
		return false, "sale_state_unknown"
	}
	if event.Date.IsZero() {
		return false, "event_date_unknown"
	}

	switch sales {
	case SalesPaused:
		return false, "sales_paused"
	case SalesCancelled:
		return false, "event_cancelled"
	case SalesEnded:
		return false, "sales_ended"
	case SalesScheduled:
		if event.SalesStartAt == nil || now.Before(*event.SalesStartAt) {
			return false, "sales_not_started"
		}
	case SalesSelling:
	default:
		return false, "sale_state_unknown"
	}

	if event.SalesStartAt != nil && now.Before(*event.SalesStartAt) {
		return false, "sales_not_started"
	}
	if event.SalesEndAt != nil && !now.Before(*event.SalesEndAt) {
		return false, "sales_ended"
	}
	if !now.Before(event.Date) {
		return false, "event_ended"
	}
	return true, ""
}

func EventIsPublished(event *Event) bool {
	if event == nil {
		return false
	}
	if event.PublicationStatus == "" && event.SalesStatus == "" && event.Status == "active" {
		return true
	}
	return event.PublicationStatus == PublicationPublished
}

// EffectiveSalesStatus reports a lifecycle status suitable for public and
// admin event views without mutating the persisted pause/cancel controls.
func EffectiveSalesStatus(event *Event, now time.Time) string {
	if event == nil {
		return "unknown"
	}
	publication, sales := event.PublicationStatus, event.SalesStatus
	if publication == "" && sales == "" && event.Status == "active" {
		publication, sales = PublicationPublished, SalesSelling
	}
	if publication != PublicationPublished {
		if publication == PublicationDraft {
			return SalesScheduled
		}
		return "unknown"
	}
	if event.Date.IsZero() {
		return "unknown"
	}
	switch sales {
	case SalesPaused, SalesCancelled, SalesEnded:
		return sales
	case SalesScheduled, SalesSelling:
	default:
		return "unknown"
	}
	if event.SalesEndAt != nil && !now.Before(*event.SalesEndAt) || !event.Date.IsZero() && !now.Before(event.Date) {
		return SalesEnded
	}
	if event.SalesStartAt != nil && now.Before(*event.SalesStartAt) || sales == SalesScheduled && event.SalesStartAt == nil {
		return SalesScheduled
	}
	return SalesSelling
}

type TicketTier struct {
	ID                uuid.UUID
	EventID           uuid.UUID
	Name              string
	PriceCents        int64
	TotalQuantity     int32
	AvailableQuantity int32
	Version           int32
	CreatedAt         time.Time
}

type SeatStatus string

const (
	SeatStatusAvailable SeatStatus = "available"
	SeatStatusReserved  SeatStatus = "reserved"
	SeatStatusBooked    SeatStatus = "booked"
)

type Seat struct {
	ID                   uuid.UUID
	EventID              uuid.UUID
	TicketTierID         uuid.UUID
	Status               SeatStatus
	SeatRevision         int64
	BookingID            *uuid.UUID
	OrderID              *uuid.UUID
	Position             Position
	CreatedAt            time.Time
	UpdatedAt            time.Time
	ReservationExpiredAt *time.Time
	ReservedByBookingID  *uuid.UUID
}

type Position struct {
	SectionID string `json:"sectionId"`
	Row       string `json:"row"`
	Seat      int    `json:"seat"`
	X         int    `json:"x"`
	Y         int    `json:"y"`
}

type ReserveSeatAction string

const RESERVED_SEAT ReserveSeatAction = "reserved"
const COMPENSATE_SEAT ReserveSeatAction = "compensate"
