// Package admission defines the internal Payment/Booking coordination identity.
// A permit orders internal preparation; it does not attest external atomicity.
package admission

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalid     = errors.New("invalid admission identity")
	ErrConflict    = errors.New("admission identity conflict")
	ErrUnavailable = errors.New("admission outcome unavailable")
	ErrBlocked     = errors.New("admission conflicts with financial preparation")
	ErrStale       = errors.New("admission token or fence is stale")
	ErrNotFound    = errors.New("admission decision not found")
)

type State string

const (
	Pending   State = "PENDING"
	Committed State = "COMMITTED"
	Aborted   State = "ABORTED"
)

func (s State) Valid() bool    { return s == Pending || s == Committed || s == Aborted }
func (s State) Terminal() bool { return s == Committed || s == Aborted }

type Identity struct {
	RequestID, PaymentID, BookingID, TicketID, EventID, UserID, ActorID uuid.UUID
	Generation                                                          int64
	KeyDigest, Fingerprint                                              [32]byte
}

func RequestIDFor(actor uuid.UUID, key [32]byte) uuid.UUID {
	data := append([]byte("rushgo:admission:v1:"), actor[:]...)
	data = append(data, key[:]...)
	return uuid.NewSHA1(uuid.NameSpaceURL, data)
}

func (i Identity) Validate() error {
	for _, id := range []uuid.UUID{i.RequestID, i.PaymentID, i.BookingID, i.TicketID, i.EventID, i.UserID, i.ActorID} {
		if id == uuid.Nil {
			return ErrInvalid
		}
	}
	if i.Generation != 1 || i.KeyDigest == [32]byte{} || i.Fingerprint == [32]byte{} || i.RequestID != RequestIDFor(i.ActorID, i.KeyDigest) {
		return ErrInvalid
	}
	return nil
}

func (i Identity) Digest() [32]byte {
	data := []byte("rushgo:admission-binding:v1:")
	for _, id := range []uuid.UUID{i.RequestID, i.PaymentID, i.BookingID, i.TicketID, i.EventID, i.UserID, i.ActorID} {
		data = append(data, id[:]...)
	}
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], uint64(i.Generation))
	data = append(data, generation[:]...)
	data = append(data, i.KeyDigest[:]...)
	data = append(data, i.Fingerprint[:]...)
	return sha256.Sum256(data)
}

type Permit struct {
	Identity                 Identity
	Token                    uuid.UUID
	Fence                    int64
	State                    State
	ObservedAt               time.Time
	Source                   string
	CreatedAt, NextAttemptAt time.Time
}

func (p Permit) ValidateFor(i Identity) error {
	if err := i.Validate(); err != nil {
		return err
	}
	if p.Identity != i {
		return ErrConflict
	}
	if p.Token == uuid.Nil || p.Fence <= 0 || !p.State.Valid() {
		return ErrStale
	}
	return nil
}

type Outcome struct {
	Identity   Identity
	Token      uuid.UUID
	Fence      int64
	State      State
	ResultCode string
}

type Coordinator interface {
	PrepareAdmission(context.Context, Identity) (Permit, error)
	GetAdmission(context.Context, Identity) (Permit, error)
	ReconcileAdmission(context.Context, Identity, uuid.UUID, int64) (Permit, error)
}

type OutcomeResolver interface {
	ResolveAdmission(context.Context, Identity, uuid.UUID, int64, bool) (Outcome, error)
}
