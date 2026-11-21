package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type financialReadRepository struct {
	repository.PaymentRepositoryInterface
	view *domain.FinancialView
	user *uuid.UUID
}

func (r *financialReadRepository) GetFinancial(context.Context, uuid.UUID) (*domain.FinancialView, error) {
	return r.view, nil
}
func (r *financialReadRepository) GetListPaymentsByCondition(_ context.Context, q *domain.GetPaymentsByCondition) ([]domain.Payment, error) {
	r.user = q.UserId
	return []domain.Payment{*r.view.Payment}, nil
}
func (r *financialReadRepository) UpdatePaymentStatus(context.Context, uuid.UUID, domain.PaymentStatus) error {
	return repository.ErrFinancialStateProtected
}
func (r *financialReadRepository) SoftDeletePayment(context.Context, uuid.UUID) error {
	return repository.ErrFinancialStateProtected
}

func TestFinancialRPCErrorMapsAccountingReadiness(t *testing.T) {
	cases := []struct {
		err  error
		code codes.Code
		msg  string
	}{
		{service.ErrAccountingNotReady, codes.FailedPrecondition, "accounting_not_ready"},
		{service.ErrAccountingUnavailable, codes.Unavailable, "accounting_readiness_unavailable"},
	}
	for _, tc := range cases {
		got := financialRPCError(tc.err)
		if status.Code(got) != tc.code || status.Convert(got).Message() != tc.msg {
			t.Fatalf("mapped %v to %v %q", tc.err, status.Code(got), status.Convert(got).Message())
		}
	}
}

func TestPaymentReadsExposeFinancialFactsAndProtectedRPCError(t *testing.T) {
	id, user := uuid.New(), uuid.New()
	created := time.Date(2026, 10, 11, 12, 34, 56, 0, time.UTC)
	r := &financialReadRepository{view: &domain.FinancialView{Payment: &domain.Payment{ID: id, UserId: user, Status: domain.PaymentSuccess, Price: 725, Currency: "usd", CreatedAt: created, UpdateAt: created}, Generation: 1, ProviderOutcome: "collected", ReconciliationState: "refund_required", RefundState: "pending", RefundAmountCents: 725, BookingDisposition: "EXPIRED"}}
	s := NewPaymentServer(service.NewPaymentService(r, zap.NewNop(), nil), zap.NewNop())
	ctx := context.Background()
	entry, err := s.GetPaymentById(ctx, &paymentv1.GetPaymentByIdReq{Id: id.String()})
	if err != nil || entry.ProviderOutcome != "collected" || entry.RefundState != "pending" || entry.BookingDisposition != "EXPIRED" || !entry.CreatedAt.AsTime().Equal(created) {
		t.Fatalf("financial read lost facts/time: %+v %v", entry, err)
	}
	list, err := s.GetPayments(ctx, &paymentv1.GetPaymentsReqeust{UserId: user.String()})
	if err != nil || len(list.Payments) != 1 || list.Payments[0].RefundAmountCents != 725 || r.user == nil || *r.user != user {
		t.Fatal("financial list missing scoped facts", err)
	}
	if _, err := s.GetPayments(ctx, &paymentv1.GetPaymentsReqeust{}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("unscoped financial list accepted", err)
	}
	if _, err := s.UpdatePaymentStatus(ctx, &paymentv1.UpdatePaymentStatusRequest{Id: id.String(), Status: "success"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("protected status mapped incorrectly", err)
	}
	if _, err := s.DeletePayment(ctx, &paymentv1.DeletePaymentRequest{Id: id.String()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("protected delete mapped incorrectly", err)
	}
}
