// Package refundtime defines the internal Event-owned refund time qualification contract.
package refundtime

import (
	"context"
	"encoding/hex"
	"github.com/google/uuid"
	"time"
)

type Binding struct {
	AttemptID, RequestID, BookingID, PaymentID, EventID, ApproverID uuid.UUID
	Generation                                                      int64
	Fingerprint                                                     string
}

func (b Binding) Valid() bool {
	raw, err := hex.DecodeString(b.Fingerprint)
	return b.AttemptID != uuid.Nil && b.RequestID != uuid.Nil && b.BookingID != uuid.Nil && b.PaymentID != uuid.Nil && b.EventID != uuid.Nil && b.ApproverID != uuid.Nil && b.Generation == 1 && err == nil && len(raw) == 32 && hex.EncodeToString(raw) == b.Fingerprint
}

type Permit struct {
	Binding               Binding
	State                 string
	StartsAt, QualifiedAt time.Time
	Version               int64
}

func (p Permit) QualifiedFor(b Binding) bool {
	return b.Valid() && p.Binding == b && p.State == "QUALIFIED" && p.Version > 0 && !p.QualifiedAt.IsZero() && p.QualifiedAt.Before(p.StartsAt)
}

type Authority interface {
	Acquire(context.Context, Binding) (Permit, error)
	Release(context.Context, Binding, string) (Permit, error)
}
