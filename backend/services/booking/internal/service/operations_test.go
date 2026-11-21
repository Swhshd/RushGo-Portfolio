package service

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

type operationsRepoStub struct {
	repository.BookingRepository
	repository.OperationsRepository
	row domain.OperationsBooking
}

func (r *operationsRepoStub) ListOperationsBookings(context.Context, domain.OperationsQuery) ([]domain.OperationsBooking, bool, error) {
	return []domain.OperationsBooking{r.row}, false, nil
}

type operationsPaymentStub struct {
	paymentv1.PaymentServiceClient
	reply *paymentv1.OperationsPayment
	err   error
}

func (p operationsPaymentStub) GetOperationsPayment(context.Context, *paymentv1.FinancialPaymentRequest, ...grpc.CallOption) (*paymentv1.OperationsPayment, error) {
	return p.reply, p.err
}
func TestOperationsFinancialEvidenceAndDependencyUnknown(t *testing.T) {
	id := uuid.New()
	for _, tc := range []struct {
		name, outcome, refund, source string
		err                           error
		kind                          string
	}{
		{name: "collected missing ticket", outcome: "collected", refund: "not_required", source: "available", kind: "paid_unissued"},
		{name: "unknown is issue", outcome: "unknown", refund: "not_required", source: "available", kind: "provider_unknown"},
		{name: "dependency unavailable", err: status.Error(codes.Unavailable, "down"), source: "unavailable"},
		{name: "not recorded", err: status.Error(codes.NotFound, "missing"), source: "not_recorded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &operationsRepoStub{row: domain.OperationsBooking{BookingID: id, EventID: uuid.New(), Generation: 1, ExpectedTickets: 1}}
			s := &BookingService{bookingRepo: repo, checkoutPayment: operationsPaymentStub{err: tc.err, reply: &paymentv1.OperationsPayment{Payment: &paymentv1.PaymentEntry{BookingId: id.String(), PaymentGeneration: 1, ProviderOutcome: tc.outcome, RefundState: tc.refund}, ObservedAt: timestamppb.New(time.Now())}}}
			rows, _, err := s.ListOperationsBookings(context.Background(), domain.OperationsQuery{})
			if err != nil || len(rows) != 1 || rows[0].PaymentSourceState != tc.source {
				t.Fatalf("result %+v %v", rows, err)
			}
			if tc.kind == "" {
				if len(rows[0].Issues) != 0 {
					t.Fatal("read failure fabricated business fact")
				}
			} else if len(rows[0].Issues) != 1 || rows[0].Issues[0].Kind != tc.kind {
				t.Fatalf("missing issue %+v", rows[0].Issues)
			}
		})
	}
}
