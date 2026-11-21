package admission

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	admissionv1 "github.com/ticketbox/pkg/proto/admission/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWireIdentityRejectsMalformedBinding(t *testing.T) {
	i := Identity{PaymentID: uuid.New(), BookingID: uuid.New(), TicketID: uuid.New(), EventID: uuid.New(), UserID: uuid.New(), ActorID: uuid.New(), Generation: 1, KeyDigest: [32]byte{1}, Fingerprint: [32]byte{2}}
	i.RequestID = RequestIDFor(i.ActorID, i.KeyDigest)
	got, err := IdentityFromProto(IdentityToProto(i))
	if err != nil || got != i {
		t.Fatalf("round trip: %v", err)
	}
	for _, mutate := range []func(*admissionv1.Identity){
		func(p *admissionv1.Identity) { p.KeyDigest = p.KeyDigest[:31] },
		func(p *admissionv1.Identity) { p.PaymentId = "bad" },
		func(p *admissionv1.Identity) { p.RequestId = uuid.NewString() },
		func(p *admissionv1.Identity) { p.Generation = 2 },
	} {
		p := IdentityToProto(i)
		mutate(p)
		if _, err := IdentityFromProto(p); !errors.Is(err, ErrInvalid) {
			t.Fatalf("malformed accepted: %v", err)
		}
	}
	if _, err := IdentityFromProto(nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil accepted")
	}
}

type reconcileResponseClient struct {
	admissionv1.PaymentAdmissionCoordinatorClient
	response *admissionv1.Permit
}

func (c reconcileResponseClient) ReconcileAdmission(context.Context, *admissionv1.ResolutionRequest, ...grpc.CallOption) (*admissionv1.Permit, error) {
	return c.response, nil
}
func TestReconcileResponseCannotChangePermitProof(t *testing.T) {
	i := Identity{PaymentID: uuid.New(), BookingID: uuid.New(), TicketID: uuid.New(), EventID: uuid.New(), UserID: uuid.New(), ActorID: uuid.New(), Generation: 1, KeyDigest: [32]byte{1}, Fingerprint: [32]byte{2}}
	i.RequestID = RequestIDFor(i.ActorID, i.KeyDigest)
	token := uuid.New()
	for _, tc := range []struct {
		name  string
		token uuid.UUID
		fence int64
		want  error
	}{
		{"same", token, 4, nil}, {"different token", uuid.New(), 4, ErrStale}, {"different fence", token, 5, ErrStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := PermitToProto(Permit{Identity: i, Token: tc.token, Fence: tc.fence, State: Committed})
			client := &CoordinatorClient{client: reconcileResponseClient{response: p}}
			_, err := client.ReconcileAdmission(context.Background(), i, token, 4)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error %v want %v", err, tc.want)
			}
		})
	}
}

func TestRPCErrorClassificationDoesNotExposeUnderlyingData(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{ErrInvalid, codes.InvalidArgument}, {ErrConflict, codes.AlreadyExists}, {ErrBlocked, codes.FailedPrecondition}, {ErrStale, codes.FailedPrecondition},
		{ErrNotFound, codes.NotFound}, {ErrUnavailable, codes.Unavailable}, {context.DeadlineExceeded, codes.DeadlineExceeded},
		{errors.New("private database connection string"), codes.Unavailable},
	} {
		got := RPCError(tc.err)
		if status.Code(got) != tc.code {
			t.Fatalf("%v: %v", tc.err, got)
		}
		if status.Convert(got).Message() == "private database connection string" {
			t.Fatal("private data exposed")
		}
	}
}
