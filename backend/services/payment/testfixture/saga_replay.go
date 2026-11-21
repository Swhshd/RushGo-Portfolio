// Package testfixture provides production service wiring and trusted provider
// evidence injection only for disposable integration-test schemas.
package testfixture

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	paymentgrpc "github.com/ticketbox/payment/internal/grpc"
	"github.com/ticketbox/payment/internal/repository"
	"github.com/ticketbox/payment/internal/service"
	bookingv1 "github.com/ticketbox/pkg/proto/booking/v1"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"go.uber.org/zap"
)

func SagaReplayServer(pool *pgxpool.Pool, booking bookingv1.BookingServiceClient) paymentv1.PaymentServiceServer {
	logger := zap.NewNop()
	s := service.NewPaymentService(repository.NewPostgresPaymentRepository(pool), logger, nil)
	s.SetFinancialAuthority(booking, "acct_saga_fixture", false)
	return paymentgrpc.NewPaymentServer(s, logger)
}

func SeedCollection(ctx context.Context, pool *pgxpool.Pool, booking, user uuid.UUID, amount int64) (uuid.UUID, error) {
	r := repository.NewPostgresPaymentRepository(pool)
	id := uuid.New()
	identity := domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: 1, ProviderAccount: "acct_saga_fixture", AmountCents: amount, Currency: "usd"}
	frozen, _ := json.Marshal(domain.FrozenCreateParameters{Version: 1, Amount: amount, Currency: "usd", AutomaticPaymentMethods: domain.AutomaticPaymentMethods{Enabled: true, AllowRedirects: "always"}})
	var err error
	identity.Fingerprint, err = domain.FinancialFingerprint(identity, frozen)
	if err != nil {
		return uuid.Nil, err
	}
	_, _, err = r.CreateOrGet(ctx, domain.CreateOperation{Identity: identity, PaymentID: id, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen})
	if err != nil {
		return uuid.Nil, err
	}
	err = r.RecordObservation(ctx, domain.ProviderObservation{Account: "acct_saga_fixture", PaymentID: id, BookingID: booking, Generation: 1, IntentID: "pi_" + id.String(), AmountCents: amount, Currency: "usd", Status: "succeeded", Collected: true, ObservedAt: time.Now()})
	return id, err
}
