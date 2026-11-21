package domain

import (
	"strings"
	"testing"
)

func TestBusinessNotificationV1ValidatesAndRendersBothLocales(t *testing.T) {
	raw := []byte(`{"version":1,"notification_id":"87df179f-43f8-49b2-89c3-74e4b61f6e3b","type":"tickets_issued","booking_id":"d25512e4-c46a-4eac-864f-1fd1da30d763","user_id":"9cd06b91-3ecb-4a49-90dd-37a13fcb138c","event_id":"fe591dda-0b95-4a07-9469-34e4a41a96ef","generation":1,"currency":"CNY","total":72500,"status":"issued"}`)
	notification, hash, err := ParseBusinessNotificationV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	if notification.NotificationID.String() != "87df179f-43f8-49b2-89c3-74e4b61f6e3b" || notification.Type != "tickets_issued" || len(hash) != 32 {
		t.Fatalf("parsed business identity = %+v hash=%d", notification, len(hash))
	}
	english, err := RenderBusinessNotification(notification, "en-US")
	if err != nil || english.Locale != "en" || !strings.Contains(strings.ToLower(english.Body), "tickets") {
		t.Fatalf("English template = %+v err=%v", english, err)
	}
	chinese, err := RenderBusinessNotification(notification, "zh-CN")
	if err != nil || chinese.Locale != "zh" || chinese.Title == english.Title || chinese.Body == english.Body {
		t.Fatalf("Chinese template = %+v err=%v", chinese, err)
	}
}

func TestBusinessNotificationV1RejectsUnknownOrUnsafeFieldsAndInvalidIdentity(t *testing.T) {
	valid := `{"version":1,"notification_id":"87df179f-43f8-49b2-89c3-74e4b61f6e3b","type":"tickets_issued","booking_id":"d25512e4-c46a-4eac-864f-1fd1da30d763","user_id":"9cd06b91-3ecb-4a49-90dd-37a13fcb138c","event_id":"fe591dda-0b95-4a07-9469-34e4a41a96ef","generation":1,"currency":"CNY","total":72500,"status":"issued"}`
	for name, raw := range map[string]string{
		"QR field":                   strings.TrimSuffix(valid, "}") + `,"qr_code":"secret"}`,
		"unknown field":              strings.TrimSuffix(valid, "}") + `,"email":"override@example.com"}`,
		"wrong version":              strings.Replace(valid, `"version":1`, `"version":2`, 1),
		"zero booking ID":            strings.Replace(valid, "d25512e4-c46a-4eac-864f-1fd1da30d763", "00000000-0000-0000-0000-000000000000", 1),
		"wrong lifecycle generation": strings.Replace(valid, `"generation":1`, `"generation":2`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseBusinessNotificationV1([]byte(raw)); err == nil {
				t.Fatal("invalid BusinessNotificationV1 payload was accepted")
			}
		})
	}
}

func TestLegacyIntentRequiresRealEmailRecipient(t *testing.T) {
	raw := []byte(`{"booking_id":"d25512e4-c46a-4eac-864f-1fd1da30d763","user_id":"9cd06b91-3ecb-4a49-90dd-37a13fcb138c","event_id":"fe591dda-0b95-4a07-9469-34e4a41a96ef","total":72500}`)
	if _, _, _, err := PrepareIntent("booking_confirmation", raw); err == nil {
		t.Fatal("intent without a resolved email recipient was accepted")
	}
	if _, _, _, err := PrepareIntent("booking_confirmation", []byte(strings.TrimSuffix(string(raw), "}")+`,"email":"person@example.com"}`)); err != nil {
		t.Fatalf("intent with a resolved email recipient failed: %v", err)
	}
}
