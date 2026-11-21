package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/repository"
)

const (
	AdminOutboxSource          = "notification"
	AdminOutboxRecordLimit     = repository.AdminOutboxProjectionCap
	AdminOutboxDefaultPageSize = 20
	AdminOutboxMaxPageSize     = 100
	AdminOutboxMaxRange        = 7 * 24 * time.Hour
	adminOutboxCursorVersion   = 1
	adminOutboxMaxCursorBytes  = 4096
)

var (
	ErrAdminOutboxInvalidQuery     = errors.New("invalid Notification admin outbox query")
	ErrAdminOutboxCapacityExceeded = repository.ErrAdminOutboxCapacityExceeded
	ErrAdminOutboxReadFailed       = errors.New("Notification admin outbox source unavailable")
)

var supportedAdminOutboxTypes = []string{
	"booking_confirmation",
	"booking_failure",
	"booking_cancellation",
	"welcome_email",
	"tickets_issued",
	"refund_requested",
	"refund_approved",
	"refund_rejected",
	"refund_succeeded",
	"manual_attention",
	"event_onsale",
	"event_restock",
	"Other",
}

var filterableAdminOutboxTypes = map[string]struct{}{
	"booking_confirmation": {},
	"booking_failure":      {},
	"booking_cancellation": {},
	"welcome_email":        {},
	"tickets_issued":       {},
	"refund_requested":     {},
	"refund_approved":      {},
	"refund_rejected":      {},
	"refund_succeeded":     {},
	"manual_attention":     {},
	"event_onsale":         {},
	"event_restock":        {},
}

var adminOutboxStatuses = map[string]string{
	"PENDING":    "pending",
	"DELIVERING": "delivering",
	"SENT":       "sent",
	"RETRYABLE":  "retryable",
	"UNKNOWN":    "unknown",
	"ATTENTION":  "attention",
	"CANCELLED":  "cancelled",
}

var filterableAdminOutboxStatuses = map[string]struct{}{
	"pending": {}, "delivering": {}, "sent": {}, "retryable": {}, "unknown": {}, "attention": {}, "cancelled": {},
}

type AdminOutboxQuery struct {
	Source    string
	Status    string
	EventType string
	From      string
	To        string
	Limit     int32
	Cursor    string
}

type AdminOutboxEvent struct {
	EventID         string    `json:"event_id"`
	SourceService   string    `json:"source_service"`
	EventType       string    `json:"event_type"`
	CreatedAt       time.Time `json:"created_at"`
	RecordedStatus  string    `json:"recorded_status"`
	ConsumerOutcome string    `json:"consumer_outcome"`
}

type AdminOutboxPage struct {
	Records             []AdminOutboxEvent `json:"records"`
	NextCursor          string             `json:"next_cursor"`
	HasMore             bool               `json:"has_more"`
	ObservedAt          time.Time          `json:"observed_at"`
	From                time.Time          `json:"from"`
	To                  time.Time          `json:"to"`
	SupportedEventTypes []string           `json:"supported_event_types"`
	SourceRecordLimit   int                `json:"source_record_limit"`
	ConsumerOutcome     string             `json:"consumer_outcome"`
}

type AdminOutboxService struct {
	reader repository.AdminOutboxProjectionReader
	now    func() time.Time
}

func NewAdminOutboxService(reader repository.AdminOutboxProjectionReader) *AdminOutboxService {
	return &AdminOutboxService{reader: reader, now: time.Now}
}

type adminOutboxCursorQuery struct {
	Source    string `json:"source"`
	Status    string `json:"status"`
	EventType string `json:"event_type"`
	From      string `json:"from"`
	To        string `json:"to"`
	Limit     int32  `json:"limit"`
}

type adminOutboxCursor struct {
	Version   int    `json:"v"`
	Source    string `json:"source"`
	QueryHash string `json:"query_hash"`
	Status    string `json:"status"`
	EventType string `json:"event_type"`
	From      string `json:"from"`
	To        string `json:"to"`
	Limit     int32  `json:"limit"`
	CreatedAt string `json:"created_at"`
	EventID   string `json:"event_id"`
}

type adminOutboxPosition struct {
	createdAt time.Time
	eventID   uuid.UUID
}

func (s *AdminOutboxService) List(ctx context.Context, query AdminOutboxQuery) (*AdminOutboxPage, error) {
	if s == nil || s.reader == nil {
		return nil, ErrAdminOutboxReadFailed
	}
	limit, from, to, fromBinding, toBinding, err := validateAdminOutboxQuery(query)
	if err != nil {
		return nil, ErrAdminOutboxInvalidQuery
	}
	queryHash, err := adminOutboxQueryHash(query, fromBinding, toBinding, limit)
	if err != nil {
		return nil, ErrAdminOutboxInvalidQuery
	}
	var after *adminOutboxPosition
	if query.Cursor != "" {
		after, err = decodeAdminOutboxCursor(query.Cursor, query, fromBinding, toBinding, queryHash)
		if err != nil {
			return nil, ErrAdminOutboxInvalidQuery
		}
	}

	rows, err := s.reader.ReadAdminOutboxProjection(ctx)
	if errors.Is(err, repository.ErrAdminOutboxCapacityExceeded) {
		return nil, ErrAdminOutboxCapacityExceeded
	}
	if err != nil {
		return nil, ErrAdminOutboxReadFailed
	}
	if len(rows) > AdminOutboxRecordLimit {
		return nil, ErrAdminOutboxCapacityExceeded
	}

	filtered := make([]repository.AdminOutboxProjection, 0, len(rows))
	for _, row := range rows {
		if !row.Managed {
			continue
		}
		if row.ID == uuid.Nil || row.CreatedAt.IsZero() {
			return nil, ErrAdminOutboxReadFailed
		}
		recordedStatus, ok := projectAdminOutboxStatus(row.Status)
		if !ok {
			return nil, ErrAdminOutboxReadFailed
		}
		if row.CreatedAt.Before(from) || row.CreatedAt.After(to) {
			continue
		}
		if query.Status != "all" && query.Status != recordedStatus {
			continue
		}
		eventType := projectAdminOutboxEventType(row.EventType)
		if query.EventType != "" && query.EventType != eventType {
			continue
		}
		if after != nil && !adminOutboxKeyBefore(row, *after) {
			continue
		}
		filtered = append(filtered, row)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if !filtered[i].CreatedAt.Equal(filtered[j].CreatedAt) {
			return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
		}
		return bytes.Compare(filtered[i].ID[:], filtered[j].ID[:]) > 0
	})

	pageSize := int(limit)
	hasMore := len(filtered) > pageSize
	if hasMore {
		filtered = filtered[:pageSize]
	}
	page := &AdminOutboxPage{
		Records:             make([]AdminOutboxEvent, 0, len(filtered)),
		HasMore:             hasMore,
		ObservedAt:          s.currentTime().UTC(),
		From:                from,
		To:                  to,
		SupportedEventTypes: append([]string(nil), supportedAdminOutboxTypes...),
		SourceRecordLimit:   AdminOutboxRecordLimit,
		ConsumerOutcome:     "not_verified",
	}
	for _, row := range filtered {
		recordedStatus, _ := projectAdminOutboxStatus(row.Status)
		page.Records = append(page.Records, AdminOutboxEvent{
			EventID:         row.ID.String(),
			SourceService:   AdminOutboxSource,
			EventType:       projectAdminOutboxEventType(row.EventType),
			CreatedAt:       row.CreatedAt.UTC(),
			RecordedStatus:  recordedStatus,
			ConsumerOutcome: "not_verified",
		})
	}
	if hasMore && len(filtered) > 0 {
		last := filtered[len(filtered)-1]
		page.NextCursor, err = encodeAdminOutboxCursor(last, query, fromBinding, toBinding, limit, queryHash)
		if err != nil {
			return nil, ErrAdminOutboxReadFailed
		}
	}
	return page, nil
}

func (s *AdminOutboxService) currentTime() time.Time {
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

func validateAdminOutboxQuery(query AdminOutboxQuery) (int32, time.Time, time.Time, string, string, error) {
	if query.Source != AdminOutboxSource {
		return 0, time.Time{}, time.Time{}, "", "", ErrAdminOutboxInvalidQuery
	}
	if query.Status != "all" {
		if _, ok := filterableAdminOutboxStatuses[query.Status]; !ok {
			return 0, time.Time{}, time.Time{}, "", "", ErrAdminOutboxInvalidQuery
		}
	}
	if query.EventType != "" {
		if _, ok := filterableAdminOutboxTypes[query.EventType]; !ok {
			return 0, time.Time{}, time.Time{}, "", "", ErrAdminOutboxInvalidQuery
		}
	}
	if query.Limit < 1 || query.Limit > AdminOutboxMaxPageSize {
		return 0, time.Time{}, time.Time{}, "", "", ErrAdminOutboxInvalidQuery
	}
	if len(query.Cursor) > adminOutboxMaxCursorBytes {
		return 0, time.Time{}, time.Time{}, "", "", ErrAdminOutboxInvalidQuery
	}
	if query.From == "" || query.To == "" {
		return 0, time.Time{}, time.Time{}, "", "", ErrAdminOutboxInvalidQuery
	}
	from, err := time.Parse(time.RFC3339Nano, query.From)
	if err != nil {
		return 0, time.Time{}, time.Time{}, "", "", ErrAdminOutboxInvalidQuery
	}
	to, err := time.Parse(time.RFC3339Nano, query.To)
	if err != nil || !to.After(from) || to.Sub(from) > AdminOutboxMaxRange {
		return 0, time.Time{}, time.Time{}, "", "", ErrAdminOutboxInvalidQuery
	}
	from = from.UTC()
	to = to.UTC()
	return query.Limit, from, to, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), nil
}

func projectAdminOutboxStatus(status string) (string, bool) {
	projected, ok := adminOutboxStatuses[status]
	return projected, ok
}

func projectAdminOutboxEventType(eventType string) string {
	if _, ok := filterableAdminOutboxTypes[eventType]; ok {
		return eventType
	}
	return "Other"
}

func adminOutboxKeyBefore(row repository.AdminOutboxProjection, position adminOutboxPosition) bool {
	if row.CreatedAt.Before(position.createdAt) {
		return true
	}
	return row.CreatedAt.Equal(position.createdAt) && bytes.Compare(row.ID[:], position.eventID[:]) < 0
}

func adminOutboxQueryHash(query AdminOutboxQuery, from, to string, limit int32) (string, error) {
	payload, err := json.Marshal(adminOutboxCursorQuery{Source: query.Source, Status: query.Status, EventType: query.EventType, From: from, To: to, Limit: limit})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func encodeAdminOutboxCursor(row repository.AdminOutboxProjection, query AdminOutboxQuery, from, to string, limit int32, queryHash string) (string, error) {
	payload, err := json.Marshal(adminOutboxCursor{
		Version: adminOutboxCursorVersion, Source: query.Source, QueryHash: queryHash,
		Status: query.Status, EventType: query.EventType, From: from, To: to, Limit: limit,
		CreatedAt: row.CreatedAt.UTC().Format(time.RFC3339Nano), EventID: row.ID.String(),
	})
	if err != nil || len(payload) > adminOutboxMaxCursorBytes {
		return "", ErrAdminOutboxInvalidQuery
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeAdminOutboxCursor(token string, query AdminOutboxQuery, from, to, queryHash string) (*adminOutboxPosition, error) {
	if token == "" || len(token) > adminOutboxMaxCursorBytes {
		return nil, ErrAdminOutboxInvalidQuery
	}
	payload, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || base64.RawURLEncoding.EncodeToString(payload) != token || len(payload) > adminOutboxMaxCursorBytes {
		return nil, ErrAdminOutboxInvalidQuery
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var cursor adminOutboxCursor
	if err := decoder.Decode(&cursor); err != nil {
		return nil, ErrAdminOutboxInvalidQuery
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, ErrAdminOutboxInvalidQuery
	}
	canonical, err := json.Marshal(cursor)
	if err != nil || !bytes.Equal(canonical, payload) {
		return nil, ErrAdminOutboxInvalidQuery
	}
	if cursor.Version != adminOutboxCursorVersion || cursor.Source != query.Source || cursor.QueryHash != queryHash ||
		cursor.Status != query.Status || cursor.EventType != query.EventType || cursor.From != from || cursor.To != to || cursor.Limit != query.Limit {
		return nil, ErrAdminOutboxInvalidQuery
	}
	createdAt, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
	if err != nil || createdAt.UTC().Format(time.RFC3339Nano) != cursor.CreatedAt {
		return nil, ErrAdminOutboxInvalidQuery
	}
	eventID, err := uuid.Parse(cursor.EventID)
	if err != nil || eventID == uuid.Nil || eventID.String() != cursor.EventID {
		return nil, ErrAdminOutboxInvalidQuery
	}
	return &adminOutboxPosition{createdAt: createdAt.UTC(), eventID: eventID}, nil
}
