package service

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
)

func renderNotificationEmail(n *domain.Notification, frontendOrigin *url.URL) (domain.InboxTemplate, error) {
	if n == nil || frontendOrigin == nil {
		return domain.InboxTemplate{}, domain.ErrInvalidIntent
	}
	locale := domain.NormalizeLocale(n.Locale)
	switch n.Type {
	case "smtp_probe":
		return domain.InboxTemplate{Locale: locale, Subject: "RushGo SMTP delivery test", Title: "RushGo SMTP delivery test", Body: "This is an explicitly authorized configuration test. No order, account or payment was created."}, nil
	case "tickets_issued", "refund_requested", "refund_approved", "refund_rejected", "refund_succeeded", "manual_attention":
		raw, err := json.Marshal(n.Payload)
		if err != nil {
			return domain.InboxTemplate{}, domain.ErrInvalidBusinessNotification
		}
		business, _, err := domain.ParseBusinessNotificationV1(raw)
		if err != nil {
			return domain.InboxTemplate{}, err
		}
		template, err := domain.RenderBusinessNotification(business, locale)
		if err != nil {
			return domain.InboxTemplate{}, err
		}
		return appendOrderLink(template, frontendOrigin, business.BookingID)
	case "event_onsale", "event_restock":
		raw, err := json.Marshal(n.Payload)
		if err != nil {
			return domain.InboxTemplate{}, domain.ErrInvalidEventReminder
		}
		reminder, _, err := domain.ParseEventReminder(raw)
		if err != nil || reminder == nil || reminder.Action != "trigger" || !reminder.EmailOptIn || reminder.EmailConsentVersion != "event-alerts-v1" {
			return domain.InboxTemplate{}, domain.ErrInvalidEventReminder
		}
		template, err := domain.RenderEventReminder(reminder, locale)
		if err != nil {
			return domain.InboxTemplate{}, err
		}
		return appendEventLink(template, frontendOrigin, reminder.EventID)
	case "booking_confirmation", "booking_failure", "booking_cancellation", "welcome_email":
		template, err := renderLegacyEmail(n, locale)
		if err != nil {
			return domain.InboxTemplate{}, err
		}
		if n.Type == "welcome_email" {
			return appendAccountLink(template, frontendOrigin)
		}
		bookingID, ok := payloadUUID(n.Payload, "booking_id")
		if !ok {
			return domain.InboxTemplate{}, domain.ErrInvalidIntent
		}
		return appendOrderLink(template, frontendOrigin, bookingID)
	default:
		return domain.InboxTemplate{}, domain.ErrInvalidIntent
	}
}

func appendEventLink(template domain.InboxTemplate, frontendOrigin *url.URL, eventID uuid.UUID) (domain.InboxTemplate, error) {
	if frontendOrigin == nil || eventID == uuid.Nil {
		return domain.InboxTemplate{}, domain.ErrInvalidEventReminder
	}
	eventURL := (&url.URL{Scheme: frontendOrigin.Scheme, Host: frontendOrigin.Host, Path: "/events/" + eventID.String()}).String()
	if template.Locale == "zh" {
		template.Body += "\n查看活动：" + eventURL
	} else {
		template.Body += "\nView event: " + eventURL
	}
	return template, nil
}

func appendOrderLink(template domain.InboxTemplate, frontendOrigin *url.URL, bookingID uuid.UUID) (domain.InboxTemplate, error) {
	if frontendOrigin == nil || bookingID == uuid.Nil {
		return domain.InboxTemplate{}, domain.ErrInvalidIntent
	}
	orderURL := (&url.URL{Scheme: frontendOrigin.Scheme, Host: frontendOrigin.Host, Path: "/my-tickets/orders/" + bookingID.String()}).String()
	if template.Locale == "zh" {
		template.Body += "\n查看订单：" + orderURL
	} else {
		template.Body += "\nView order: " + orderURL
	}
	return template, nil
}

func appendAccountLink(template domain.InboxTemplate, frontendOrigin *url.URL) (domain.InboxTemplate, error) {
	if frontendOrigin == nil {
		return domain.InboxTemplate{}, domain.ErrInvalidIntent
	}
	accountURL := (&url.URL{Scheme: frontendOrigin.Scheme, Host: frontendOrigin.Host, Path: "/account"}).String()
	if template.Locale == "zh" {
		template.Body += "\n管理账户：" + accountURL
	} else {
		template.Body += "\nManage your account: " + accountURL
	}
	return template, nil
}

func renderLegacyEmail(n *domain.Notification, locale string) (domain.InboxTemplate, error) {
	if n.Type == "welcome_email" {
		if locale == "zh" {
			return domain.InboxTemplate{Locale: locale, Subject: "欢迎使用 RushGo", Title: "欢迎使用 RushGo", Body: "您的 RushGo 账户已准备就绪。"}, nil
		}
		return domain.InboxTemplate{Locale: locale, Subject: "Welcome to RushGo", Title: "Welcome to RushGo", Body: "Your RushGo account is ready."}, nil
	}
	bookingID, ok := payloadUUID(n.Payload, "booking_id")
	if !ok {
		return domain.InboxTemplate{}, domain.ErrInvalidIntent
	}
	switch n.Type {
	case "booking_confirmation":
		total, ok := payloadInt64(n.Payload["total"])
		if !ok || total < 0 {
			return domain.InboxTemplate{}, domain.ErrInvalidIntent
		}
		amount := ""
		if candidate, ok := n.Payload["currency"].(string); ok && validEmailCurrency(candidate) {
			amount = candidate + " " + minorAmount(total)
		}
		if locale == "zh" {
			body := "订单 " + bookingID.String() + " 已确认。"
			if amount != "" {
				body += "金额：" + amount + "。"
			}
			body += "请在我的票务中查看订单。"
			return domain.InboxTemplate{Locale: locale, Subject: "订单已确认", Title: "订单已确认", Body: body}, nil
		}
		body := "Order " + bookingID.String() + " is confirmed."
		if amount != "" {
			body += " Total: " + amount + "."
		}
		body += " View the order in My Tickets."
		return domain.InboxTemplate{Locale: locale, Subject: "Booking confirmed", Title: "Booking confirmed", Body: body}, nil
	case "booking_failure":
		if locale == "zh" {
			return domain.InboxTemplate{Locale: locale, Subject: "订单未能确认", Title: "订单未能确认", Body: "订单 " + bookingID.String() + " 未能确认。请在我的票务中查看状态。"}, nil
		}
		return domain.InboxTemplate{Locale: locale, Subject: "Booking could not be confirmed", Title: "Booking could not be confirmed", Body: "Order " + bookingID.String() + " could not be confirmed. Check My Tickets for its status."}, nil
	case "booking_cancellation":
		if locale == "zh" {
			return domain.InboxTemplate{Locale: locale, Subject: "订单已取消", Title: "订单已取消", Body: "订单 " + bookingID.String() + " 已取消。"}, nil
		}
		return domain.InboxTemplate{Locale: locale, Subject: "Booking cancelled", Title: "Booking cancelled", Body: "Order " + bookingID.String() + " has been cancelled."}, nil
	default:
		return domain.InboxTemplate{}, domain.ErrInvalidIntent
	}
}

func payloadUUID(payload map[string]interface{}, key string) (uuid.UUID, bool) {
	value, ok := payload[key].(string)
	if !ok {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(value)
	return id, err == nil && id != uuid.Nil
}

func payloadInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case json.Number:
		parsed, err := value.Int64()
		return parsed, err == nil
	case int64:
		return value, true
	case int:
		return int64(value), true
	case float64:
		if math.Trunc(value) != value || value > math.MaxInt64 || value < math.MinInt64 {
			return 0, false
		}
		return int64(value), true
	default:
		return 0, false
	}
}

func minorAmount(value int64) string {
	return fmt.Sprintf("%s.%02d", commaInteger(value/100), value%100)
}

func commaInteger(value int64) string {
	text := strconv.FormatInt(value, 10)
	if len(text) <= 3 {
		return text
	}
	negative := strings.HasPrefix(text, "-")
	start := 0
	if negative {
		start = 1
	}
	for i := len(text) - 3; i > start; i -= 3 {
		text = text[:i] + "," + text[i:]
	}
	return text
}

func validEmailCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, r := range value {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}
