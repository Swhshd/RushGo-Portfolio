package admission

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RPCDeadline bounds a coordination round trip; expiry is UNKNOWN, never abort.
const RPCDeadline = 20 * time.Second

func IdentityToProto(i Identity) *admissionv1.Identity {
	return &admissionv1.Identity{RequestId: i.RequestID.String(), PaymentId: i.PaymentID.String(), BookingId: i.BookingID.String(), TicketId: i.TicketID.String(), EventId: i.EventID.String(), UserId: i.UserID.String(), ActorId: i.ActorID.String(), Generation: i.Generation, KeyDigest: append([]byte(nil), i.KeyDigest[:]...), Fingerprint: append([]byte(nil), i.Fingerprint[:]...)}
}

func IdentityFromProto(p *admissionv1.Identity) (Identity, error) {
	var i Identity
	if p == nil || len(p.KeyDigest) != 32 || len(p.Fingerprint) != 32 {
		return i, ErrInvalid
	}
	targets := []*uuid.UUID{&i.RequestID, &i.PaymentID, &i.BookingID, &i.TicketID, &i.EventID, &i.UserID, &i.ActorID}
	for n, s := range []string{p.RequestId, p.PaymentId, p.BookingId, p.TicketId, p.EventId, p.UserId, p.ActorId} {
		parsed, err := uuid.Parse(s)
		if err != nil {
			return Identity{}, ErrInvalid
		}
		*targets[n] = parsed
	}
	i.Generation = p.Generation
	copy(i.KeyDigest[:], p.KeyDigest)
	copy(i.Fingerprint[:], p.Fingerprint)
	return i, i.Validate()
}

func wireTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
func nativeTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func PermitToProto(p Permit) *admissionv1.Permit {
	return &admissionv1.Permit{Identity: IdentityToProto(p.Identity), Token: p.Token.String(), Fence: p.Fence, State: string(p.State), ObservedAtUnixNano: wireTime(p.ObservedAt), Source: p.Source, CreatedAtUnixNano: wireTime(p.CreatedAt)}
}
func permitFromProto(p *admissionv1.Permit, i Identity) (Permit, error) {
	if p == nil {
		return Permit{}, ErrUnavailable
	}
	identity, err := IdentityFromProto(p.Identity)
	if err != nil {
		return Permit{}, err
	}
	token, err := uuid.Parse(p.Token)
	if err != nil {
		return Permit{}, ErrStale
	}
	result := Permit{Identity: identity, Token: token, Fence: p.Fence, State: State(p.State), ObservedAt: nativeTime(p.ObservedAtUnixNano), Source: p.Source, CreatedAt: nativeTime(p.CreatedAtUnixNano)}
	return result, result.ValidateFor(i)
}

func OutcomeToProto(o Outcome) *admissionv1.Outcome {
	token := ""
	if o.Token != uuid.Nil {
		token = o.Token.String()
	}
	return &admissionv1.Outcome{Identity: IdentityToProto(o.Identity), Token: token, Fence: o.Fence, State: string(o.State), ResultCode: o.ResultCode}
}

func ResolutionFromProto(r *admissionv1.ResolutionRequest, allowProof bool) (Identity, uuid.UUID, int64, error) {
	if r == nil {
		return Identity{}, uuid.Nil, 0, ErrInvalid
	}
	i, err := IdentityFromProto(r.Identity)
	if err != nil {
		return i, uuid.Nil, 0, err
	}
	if allowProof && r.Token == "" && r.Fence == 0 && !r.AbortPending {
		return i, uuid.Nil, 0, nil
	}
	token, err := uuid.Parse(r.Token)
	if err != nil || token == uuid.Nil || r.Fence <= 0 {
		return i, uuid.Nil, 0, ErrStale
	}
	return i, token, r.Fence, nil
}

// RPCError deliberately omits underlying database/provider errors from wire data.
func RPCError(err error) error {
	if err == nil {
		return nil
	}
	code := codes.Unavailable
	message := "admission coordination unavailable"
	switch {
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
		message = "admission request canceled"
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
		message = "admission outcome unknown"
	case errors.Is(err, ErrInvalid):
		code = codes.InvalidArgument
		message = "invalid admission identity"
	case errors.Is(err, ErrConflict):
		code = codes.AlreadyExists
		message = "admission identity conflict"
	case errors.Is(err, ErrStale):
		code = codes.FailedPrecondition
		message = "stale admission token or fence"
	case errors.Is(err, ErrBlocked):
		code = codes.FailedPrecondition
		message = "admission blocked"
	case errors.Is(err, ErrNotFound):
		code = codes.NotFound
		message = "admission decision not found"
	}
	return status.Error(code, message)
}
func nativeError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return ErrInvalid
	case codes.AlreadyExists:
		return ErrConflict
	case codes.FailedPrecondition:
		return ErrBlocked
	case codes.NotFound:
		return ErrNotFound
	default:
		return ErrUnavailable
	}
}

type CoordinatorClient struct {
	client admissionv1.PaymentAdmissionCoordinatorClient
}

func NewCoordinatorClient(conn grpc.ClientConnInterface) *CoordinatorClient {
	return &CoordinatorClient{admissionv1.NewPaymentAdmissionCoordinatorClient(conn)}
}
func (c *CoordinatorClient) PrepareAdmission(ctx context.Context, i Identity) (Permit, error) {
	if err := i.Validate(); err != nil {
		return Permit{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, RPCDeadline)
	defer cancel()
	p, err := c.client.PrepareAdmission(ctx, IdentityToProto(i))
	if err != nil {
		return Permit{}, nativeError(err)
	}
	return permitFromProto(p, i)
}
func (c *CoordinatorClient) GetAdmission(ctx context.Context, i Identity) (Permit, error) {
	if err := i.Validate(); err != nil {
		return Permit{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, RPCDeadline)
	defer cancel()
	p, err := c.client.GetAdmission(ctx, IdentityToProto(i))
	if err != nil {
		return Permit{}, nativeError(err)
	}
	return permitFromProto(p, i)
}
func (c *CoordinatorClient) ReconcileAdmission(ctx context.Context, i Identity, token uuid.UUID, fence int64) (Permit, error) {
	if err := i.Validate(); err != nil {
		return Permit{}, err
	}
	if token == uuid.Nil || fence <= 0 {
		return Permit{}, ErrStale
	}
	ctx, cancel := context.WithTimeout(ctx, RPCDeadline)
	defer cancel()
	p, err := c.client.ReconcileAdmission(ctx, &admissionv1.ResolutionRequest{Identity: IdentityToProto(i), Token: token.String(), Fence: fence})
	if err != nil {
		return Permit{}, nativeError(err)
	}
	permit, err := permitFromProto(p, i)
	if err == nil && (permit.Token != token || permit.Fence != fence) {
		return Permit{}, ErrStale
	}
	return permit, err
}

type OutcomeClient struct {
	client admissionv1.BookingAdmissionAuthorityClient
}

func NewOutcomeClient(conn grpc.ClientConnInterface) *OutcomeClient {
	return &OutcomeClient{admissionv1.NewBookingAdmissionAuthorityClient(conn)}
}
func (c *OutcomeClient) ResolveAdmission(ctx context.Context, i Identity, token uuid.UUID, fence int64, abort bool) (Outcome, error) {
	if err := i.Validate(); err != nil {
		return Outcome{}, err
	}
	tokenText := ""
	if token != uuid.Nil {
		tokenText = token.String()
	}
	req := &admissionv1.ResolutionRequest{Identity: IdentityToProto(i), Token: tokenText, Fence: fence, AbortPending: abort}
	if _, _, _, err := ResolutionFromProto(req, true); err != nil {
		return Outcome{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, RPCDeadline)
	defer cancel()
	p, err := c.client.ResolveAdmission(ctx, req)
	if err != nil {
		return Outcome{}, nativeError(err)
	}
	if p == nil {
		return Outcome{}, ErrUnavailable
	}
	identity, err := IdentityFromProto(p.Identity)
	if err != nil {
		return Outcome{}, err
	}
	if identity != i {
		return Outcome{}, ErrConflict
	}
	result := Outcome{Identity: identity, Fence: p.Fence, State: State(p.State), ResultCode: p.ResultCode}
	if p.Token != "" {
		result.Token, err = uuid.Parse(p.Token)
		if err != nil {
			return Outcome{}, ErrStale
		}
	}
	if !result.State.Valid() {
		return Outcome{}, ErrUnavailable
	}
	if token != uuid.Nil && (result.Token != token || result.Fence != fence) {
		return Outcome{}, ErrStale
	}
	return result, nil
}
