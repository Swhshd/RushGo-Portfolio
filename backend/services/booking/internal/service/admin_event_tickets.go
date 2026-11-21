package service

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/repository"
	"github.com/ticketbox/pkg/adminticketlist"
)

var (
	ErrAdminEventTicketListInvalidRequest   = errors.New("invalid Admin event ticket list request")
	ErrAdminEventTicketListInvalidSnapshot  = errors.New("invalid Admin event ticket list snapshot")
	ErrAdminEventTicketListCapacityExceeded = repository.ErrAdminEventTicketOperationsCapacityExceeded
	ErrAdminEventTicketListReadFailed       = errors.New("Admin event ticket list source unavailable")
)

type AdminEventTicketListRecord struct {
	TicketID    string
	Status      string
	IssuedAt    time.Time
	CheckedInAt *time.Time
	RevokedAt   *time.Time
}

type AdminEventTicketList struct {
	EventID           string
	ObservedAt        time.Time
	StatusFilter      string
	PageSize          int32
	HasMore           bool
	NextCursor        string
	SourceRecordLimit int32
	Tickets           []AdminEventTicketListRecord
}

type AdminEventTicketListService struct {
	reader repository.AdminEventTicketListReader
}

func NewAdminEventTicketListService(reader repository.AdminEventTicketListReader) *AdminEventTicketListService {
	return &AdminEventTicketListService{reader: reader}
}

func (s *AdminEventTicketListService) List(ctx context.Context, eventID uuid.UUID, statusFilter string, requestedPageSize int32, cursorValue string) (*AdminEventTicketList, error) {
	if s == nil || s.reader == nil {
		return nil, ErrAdminEventTicketListReadFailed
	}
	if eventID == uuid.Nil {
		return nil, ErrAdminEventTicketListInvalidRequest
	}
	filter, err := adminticketlist.NormalizeStatusFilter(statusFilter)
	if err != nil {
		return nil, ErrAdminEventTicketListInvalidRequest
	}
	pageSize, err := adminticketlist.NormalizePageSize(requestedPageSize)
	if err != nil {
		return nil, ErrAdminEventTicketListInvalidRequest
	}
	var after *adminticketlist.Cursor
	if cursorValue != "" {
		decoded, err := adminticketlist.DecodeCursor(cursorValue, eventID, filter)
		if err != nil {
			return nil, ErrAdminEventTicketListInvalidRequest
		}
		after = &decoded
	}

	snapshot, err := s.reader.ReadAdminEventTickets(ctx, eventID, repository.AdminEventTicketListQuery{
		StatusFilter: filter,
		PageSize:     pageSize,
		After:        after,
	})
	if errors.Is(err, repository.ErrAdminEventTicketOperationsCapacityExceeded) {
		return nil, ErrAdminEventTicketListCapacityExceeded
	}
	if err != nil {
		return nil, ErrAdminEventTicketListReadFailed
	}
	page, err := projectAdminEventTicketListSnapshot(snapshot, eventID, filter, pageSize, after)
	if err != nil {
		return nil, ErrAdminEventTicketListInvalidSnapshot
	}
	return page, nil
}

func projectAdminEventTicketListSnapshot(snapshot repository.AdminEventTicketListSnapshot, eventID uuid.UUID, filter string, pageSize int32, after *adminticketlist.Cursor) (*AdminEventTicketList, error) {
	if eventID == uuid.Nil || snapshot.EventID != eventID || !validAdminEventTicketOperationsTime(snapshot.ObservedAt) || len(snapshot.Tickets) > int(pageSize+1) {
		return nil, ErrAdminEventTicketListInvalidSnapshot
	}
	seen := make(map[uuid.UUID]struct{}, len(snapshot.Tickets))
	for index, record := range snapshot.Tickets {
		if record.TicketID == uuid.Nil || record.EventID != eventID || !validAdminEventTicketOperationsTime(record.IssuedAt) || record.IssuedAt.After(snapshot.ObservedAt) {
			return nil, ErrAdminEventTicketListInvalidSnapshot
		}
		if filter != adminticketlist.StatusAll && record.Status != filter {
			return nil, ErrAdminEventTicketListInvalidSnapshot
		}
		if _, exists := seen[record.TicketID]; exists {
			return nil, ErrAdminEventTicketListInvalidSnapshot
		}
		seen[record.TicketID] = struct{}{}
		if record.CheckedInAt != nil && (!validAdminEventTicketOperationsTime(*record.CheckedInAt) || record.CheckedInAt.Before(record.IssuedAt) || record.CheckedInAt.After(snapshot.ObservedAt)) {
			return nil, ErrAdminEventTicketListInvalidSnapshot
		}
		if record.RevokedAt != nil && (!validAdminEventTicketOperationsTime(*record.RevokedAt) || record.RevokedAt.Before(record.IssuedAt) || record.RevokedAt.After(snapshot.ObservedAt)) {
			return nil, ErrAdminEventTicketListInvalidSnapshot
		}
		switch record.Status {
		case adminticketlist.StatusIssued:
			if record.CheckedInAt != nil || record.RevokedAt != nil {
				return nil, ErrAdminEventTicketListInvalidSnapshot
			}
		case adminticketlist.StatusCheckedIn:
			if record.CheckedInAt == nil || record.RevokedAt != nil {
				return nil, ErrAdminEventTicketListInvalidSnapshot
			}
		case adminticketlist.StatusRevoked:
			if record.RevokedAt == nil {
				return nil, ErrAdminEventTicketListInvalidSnapshot
			}
		default:
			return nil, ErrAdminEventTicketListInvalidSnapshot
		}
		if index > 0 && !adminEventTicketListIsBefore(snapshot.Tickets[index-1], record) {
			return nil, ErrAdminEventTicketListInvalidSnapshot
		}
		if after != nil && !adminEventTicketListIsAfterCursor(record, *after) {
			return nil, ErrAdminEventTicketListInvalidSnapshot
		}
	}

	pageCount := len(snapshot.Tickets)
	hasMore := pageCount > int(pageSize)
	if hasMore {
		pageCount = int(pageSize)
	}
	page := &AdminEventTicketList{
		EventID:           eventID.String(),
		ObservedAt:        snapshot.ObservedAt.UTC(),
		StatusFilter:      filter,
		PageSize:          pageSize,
		HasMore:           hasMore,
		SourceRecordLimit: adminticketlist.SourceRecordCap,
		Tickets:           make([]AdminEventTicketListRecord, 0, pageCount),
	}
	for _, record := range snapshot.Tickets[:pageCount] {
		page.Tickets = append(page.Tickets, AdminEventTicketListRecord{
			TicketID:    record.TicketID.String(),
			Status:      record.Status,
			IssuedAt:    record.IssuedAt.UTC(),
			CheckedInAt: utcAdminEventTicketListTime(record.CheckedInAt),
			RevokedAt:   utcAdminEventTicketListTime(record.RevokedAt),
		})
	}
	if hasMore {
		last := snapshot.Tickets[pageCount-1]
		cursor, err := adminticketlist.EncodeCursor(adminticketlist.Cursor{
			EventID: eventID, StatusFilter: filter, IssuedAt: last.IssuedAt, TicketID: last.TicketID,
		})
		if err != nil {
			return nil, ErrAdminEventTicketListInvalidSnapshot
		}
		page.NextCursor = cursor
	}
	return page, nil
}

func adminEventTicketListIsBefore(previous, current repository.AdminEventTicketListRecord) bool {
	if previous.IssuedAt.Equal(current.IssuedAt) {
		return bytes.Compare(previous.TicketID[:], current.TicketID[:]) > 0
	}
	return previous.IssuedAt.After(current.IssuedAt)
}

func adminEventTicketListIsAfterCursor(record repository.AdminEventTicketListRecord, cursor adminticketlist.Cursor) bool {
	if record.IssuedAt.Equal(cursor.IssuedAt) {
		return bytes.Compare(record.TicketID[:], cursor.TicketID[:]) < 0
	}
	return record.IssuedAt.Before(cursor.IssuedAt)
}

func utcAdminEventTicketListTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}
