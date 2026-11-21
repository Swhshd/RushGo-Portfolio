package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidBusinessNotification = errors.New("invalid BusinessNotificationV1")

// BusinessNotificationV1 is the exact durable envelope written by Booking's
// transactional outbox. Unknown fields are rejected so ticket credentials,
// provider secrets, or arbitrary data cannot reach inbox or email templates.
type BusinessNotificationV1 struct {
	Version        int32     `json:"version"`
	NotificationID uuid.UUID `json:"notification_id"`
	Type           string    `json:"type"`
	BookingID      uuid.UUID `json:"booking_id"`
	UserID         uuid.UUID `json:"user_id"`
	EventID        uuid.UUID `json:"event_id"`
	Generation     int64     `json:"generation"`
	Currency       string    `json:"currency"`
	Total          int64     `json:"total"`
	Status         string    `json:"status"`
}

type InboxTemplate struct {
	Locale  string
	Subject string
	Title   string
	Body    string
}

func ParseBusinessNotificationV1(raw []byte) (*BusinessNotificationV1, [32]byte, error) {
	var zero [32]byte
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var notification BusinessNotificationV1
	if decoder.Decode(&notification) != nil {
		return nil, zero, ErrInvalidBusinessNotification
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, zero, ErrInvalidBusinessNotification
	}
	if notification.Version != 1 || notification.NotificationID == uuid.Nil ||
		notification.BookingID == uuid.Nil || notification.UserID == uuid.Nil || notification.EventID == uuid.Nil ||
		notification.Generation != 1 || notification.Total < 0 || !validCurrency(notification.Currency) {
		return nil, zero, ErrInvalidBusinessNotification
	}
	switch notification.Type {
	case "tickets_issued":
		if notification.Status != "issued" {
			return nil, zero, ErrInvalidBusinessNotification
		}
	case "refund_requested":
		if notification.Status != "REQUESTED" {
			return nil, zero, ErrInvalidBusinessNotification
		}
	case "refund_approved":
		if notification.Status != "APPROVED" {
			return nil, zero, ErrInvalidBusinessNotification
		}
	case "refund_rejected":
		if notification.Status != "REJECTED" {
			return nil, zero, ErrInvalidBusinessNotification
		}
	case "refund_succeeded":
		if notification.Status != "REFUNDED" {
			return nil, zero, ErrInvalidBusinessNotification
		}
	case "manual_attention":
		if notification.Status != "issuance_denied" && notification.Status != "NEEDS_ATTENTION" {
			return nil, zero, ErrInvalidBusinessNotification
		}
	default:
		return nil, zero, ErrInvalidBusinessNotification
	}
	canonical, err := json.Marshal(notification)
	if err != nil {
		return nil, zero, ErrInvalidBusinessNotification
	}
	return &notification, sha256.Sum256(canonical), nil
}

func validCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for _, char := range currency {
		if char < 'A' || char > 'Z' {
			return false
		}
	}
	return true
}

func RenderBusinessNotification(notification *BusinessNotificationV1, requestedLocale string) (InboxTemplate, error) {
	if notification == nil {
		return InboxTemplate{}, ErrInvalidBusinessNotification
	}
	locale := normalizedLocale(requestedLocale)
	amount := notification.Currency + " " + formatMinorAmount(notification.Total)
	if locale == "zh" {
		switch notification.Type {
		case "tickets_issued":
			return InboxTemplate{Locale: locale, Subject: "您的门票已开具", Title: "门票已准备就绪", Body: "订单 " + notification.BookingID.String() + " 的门票已开具。金额：" + amount + "。请在我的票务中查看订单和门票。"}, nil
		case "refund_requested":
			return InboxTemplate{Locale: locale, Subject: "已收到退款申请", Title: "退款申请已提交", Body: "订单 " + notification.BookingID.String() + " 的退款申请已提交，金额：" + amount + "。"}, nil
		case "refund_approved":
			return InboxTemplate{Locale: locale, Subject: "退款申请已批准", Title: "退款已批准", Body: "订单 " + notification.BookingID.String() + " 的退款申请已批准，金额：" + amount + "。"}, nil
		case "refund_rejected":
			return InboxTemplate{Locale: locale, Subject: "退款申请未获批准", Title: "退款申请未获批准", Body: "订单 " + notification.BookingID.String() + " 的退款申请未获批准。"}, nil
		case "refund_succeeded":
			return InboxTemplate{Locale: locale, Subject: "退款已完成", Title: "退款已完成", Body: "订单 " + notification.BookingID.String() + " 的退款已完成，金额：" + amount + "。"}, nil
		case "manual_attention":
			return InboxTemplate{Locale: locale, Subject: "订单需要进一步处理", Title: "订单正在处理中", Body: "订单 " + notification.BookingID.String() + " 需要进一步处理。请查看我的票务页面获取最新状态。"}, nil
		}
	}
	switch notification.Type {
	case "tickets_issued":
		return InboxTemplate{Locale: locale, Subject: "Your tickets are ready", Title: "Tickets are ready", Body: "Tickets for order " + notification.BookingID.String() + " have been issued. Total: " + amount + ". View the order in My Tickets."}, nil
	case "refund_requested":
		return InboxTemplate{Locale: locale, Subject: "Refund request received", Title: "Refund requested", Body: "Your refund request for order " + notification.BookingID.String() + " was received. Amount: " + amount + "."}, nil
	case "refund_approved":
		return InboxTemplate{Locale: locale, Subject: "Refund request approved", Title: "Refund approved", Body: "Your refund request for order " + notification.BookingID.String() + " was approved. Amount: " + amount + "."}, nil
	case "refund_rejected":
		return InboxTemplate{Locale: locale, Subject: "Refund request not approved", Title: "Refund not approved", Body: "Your refund request for order " + notification.BookingID.String() + " was not approved."}, nil
	case "refund_succeeded":
		return InboxTemplate{Locale: locale, Subject: "Refund completed", Title: "Refund completed", Body: "Your refund for order " + notification.BookingID.String() + " is complete. Amount: " + amount + "."}, nil
	case "manual_attention":
		return InboxTemplate{Locale: locale, Subject: "Your order needs attention", Title: "Your order is being reviewed", Body: "Order " + notification.BookingID.String() + " needs further attention. Check My Tickets for the latest status."}, nil
	default:
		return InboxTemplate{}, ErrInvalidBusinessNotification
	}
}

func normalizedLocale(locale string) string {
	locale = strings.ToLower(strings.TrimSpace(locale))
	if locale == "zh" || strings.HasPrefix(locale, "zh-") || strings.HasPrefix(locale, "zh_") {
		return "zh"
	}
	return "en"
}

func NormalizeLocale(locale string) string {
	return normalizedLocale(locale)
}

func formatMinorAmount(total int64) string {
	return formatInteger(total/100) + "." + twoDigits(total%100)
}

func formatInteger(value int64) string {
	if value < 1000 {
		return fmtInt(value)
	}
	return formatInteger(value/1000) + "," + threeDigits(value%1000)
}

func twoDigits(value int64) string {
	if value < 10 {
		return "0" + fmtInt(value)
	}
	return fmtInt(value)
}

func threeDigits(value int64) string {
	if value < 10 {
		return "00" + fmtInt(value)
	}
	if value < 100 {
		return "0" + fmtInt(value)
	}
	return fmtInt(value)
}

func fmtInt(value int64) string {
	if value == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for value > 0 {
		i--
		b[i] = byte('0' + value%10)
		value /= 10
	}
	return string(b[i:])
}

func NewBusinessInboxRecord(notification *BusinessNotificationV1, locale string, now time.Time) (BusinessInboxRecord, error) {
	template, err := RenderBusinessNotification(notification, locale)
	if err != nil {
		return BusinessInboxRecord{}, err
	}
	return BusinessInboxRecord{
		ID: notification.NotificationID, BookingID: notification.BookingID, EventID: notification.EventID,
		UserID: notification.UserID, Type: notification.Type, Locale: template.Locale,
		Title: template.Title, Body: template.Body, CreatedAt: now,
	}, nil
}

type BusinessInboxRecord struct {
	ID                  uuid.UUID
	BookingID           uuid.UUID
	EventID             uuid.UUID
	UserID              uuid.UUID
	SubscriptionID      uuid.UUID
	SubscriptionVersion int64
	Type                string
	Locale              string
	Title               string
	Body                string
	CreatedAt           time.Time
	ReadAt              *time.Time
}
