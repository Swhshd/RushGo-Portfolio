package domain

import "testing"

func TestLegacyIntentHashSeparatesRecipientSnapshotAndRetainsCurrency(t *testing.T) {
	const base = `{"booking_id":"d25512e4-c46a-4eac-864f-1fd1da30d763","user_id":"9cd06b91-3ecb-4a49-90dd-37a13fcb138c","event_id":"fe591dda-0b95-4a07-9469-34e4a41a96ef","total":72500,"currency":"USD"}`
	first := []byte(base[:len(base)-1] + `,"email":"first@example.test"}`)
	changedRecipient := []byte(base[:len(base)-1] + `,"email":"second@example.test"}`)
	key, notification, firstHash, err := PrepareIntent("booking_confirmation", first)
	if err != nil {
		t.Fatal(err)
	}
	otherKey, other, otherHash, err := PrepareIntent("booking_confirmation", changedRecipient)
	if err != nil {
		t.Fatal(err)
	}
	if key != otherKey || notification.ID != other.ID || firstHash != otherHash {
		t.Fatal("recipient or locale/envelope change altered semantic identity")
	}
	if notification.Recipient != "first@example.test" || other.Recipient != "second@example.test" {
		t.Fatal("recipient snapshot was not retained on each incoming intent")
	}
	if _, present := notification.Payload["email"]; present {
		t.Fatal("recipient snapshot leaked into immutable business payload")
	}
	if notification.Payload["currency"] != "USD" {
		t.Fatalf("currency not retained in legacy payload: %#v", notification.Payload)
	}

	changedTotal := []byte(`{"booking_id":"d25512e4-c46a-4eac-864f-1fd1da30d763","user_id":"9cd06b91-3ecb-4a49-90dd-37a13fcb138c","event_id":"fe591dda-0b95-4a07-9469-34e4a41a96ef","total":72501,"currency":"USD","email":"second@example.test"}`)
	_, _, changedHash, err := PrepareIntent("booking_confirmation", changedTotal)
	if err != nil {
		t.Fatal(err)
	}
	if changedHash == firstHash {
		t.Fatal("changed business amount retained the same semantic hash")
	}

	withoutCurrency := []byte(`{"booking_id":"d25512e4-c46a-4eac-864f-1fd1da30d763","email":"first@example.test","total":72500}`)
	_, missing, _, err := PrepareIntent("booking_confirmation", withoutCurrency)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := missing.Payload["currency"]; present {
		t.Fatal("missing legacy currency was guessed")
	}
	invalidCurrency := []byte(`{"booking_id":"d25512e4-c46a-4eac-864f-1fd1da30d763","email":"first@example.test","total":72500,"currency":"usd"}`)
	if _, _, _, err := PrepareIntent("booking_confirmation", invalidCurrency); err == nil {
		t.Fatal("noncanonical currency accepted")
	}
}
