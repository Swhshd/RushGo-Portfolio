package admission

import (
	"crypto/sha256"
	"github.com/google/uuid"
	"testing"
)

func identityFixture() Identity {
	actor := uuid.New()
	key := sha256.Sum256([]byte("scan-key"))
	return Identity{RequestID: RequestIDFor(actor, key), PaymentID: uuid.New(), BookingID: uuid.New(), TicketID: uuid.New(), EventID: uuid.New(), UserID: uuid.New(), ActorID: actor, Generation: 1, KeyDigest: key, Fingerprint: sha256.Sum256([]byte("event+credential"))}
}

func TestIdentityBindsAllFieldsWithoutChangingRetryIdentity(t *testing.T) {
	original := identityFixture()
	if err := original.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Identity){func(i *Identity) { i.TicketID = uuid.New() }, func(i *Identity) { i.PaymentID = uuid.New() }, func(i *Identity) { i.BookingID = uuid.New() }, func(i *Identity) { i.EventID = uuid.New() }, func(i *Identity) { i.UserID = uuid.New() }, func(i *Identity) { i.Fingerprint[0] ^= 1 }} {
		changed := original
		change(&changed)
		if changed.RequestID != original.RequestID || changed.Digest() == original.Digest() {
			t.Fatal("same key payload conflict lost")
		}
	}
	original.RequestID = uuid.New()
	if original.Validate() == nil {
		t.Fatal("request ID unbound from actor/key accepted")
	}
}

func TestPermitIdentityFenceAndTerminalState(t *testing.T) {
	id := identityFixture()
	p := Permit{Identity: id, Token: uuid.New(), Fence: 7, State: Pending}
	if err := p.ValidateFor(id); err != nil {
		t.Fatal(err)
	}
	other := id
	other.ActorID = uuid.New()
	if p.ValidateFor(other) == nil {
		t.Fatal("wrong identity accepted")
	}
	p.Fence = 0
	if p.ValidateFor(id) == nil {
		t.Fatal("zero fence accepted")
	}
	if Pending.Terminal() || !Committed.Terminal() || !Aborted.Terminal() || State("unknown").Valid() {
		t.Fatal("unknown/terminal classification")
	}
}
