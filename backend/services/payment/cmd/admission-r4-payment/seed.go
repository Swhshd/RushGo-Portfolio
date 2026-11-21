//go:build admission_r4

package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
)

func initializeFinancialFixtures(ctx context.Context, pool *pgxpool.Pool, repo *repository.PostgresPaymentRepository, cfg bootstrapConfig) error {
	resumed, err := verifyStaticRestart(ctx, pool, cfg)
	if err != nil || resumed {
		return err
	}
	for _, fixture := range cfg.Fixtures {
		if err := seedFinancialFixture(ctx, pool, repo, cfg.RuntimeMode, fixture); err != nil {
			return err
		}
	}
	return nil
}

func seedFinancialFixture(ctx context.Context, pool *pgxpool.Pool, repo *repository.PostgresPaymentRepository, runtimeMode string, fixture fixtureConfig) error {
	paymentID, err := uuid.Parse(fixture.PaymentID)
	if err != nil {
		return errors.New("invalid Payment fixture id")
	}
	bookingID, err := uuid.Parse(fixture.BookingID)
	if err != nil {
		return errors.New("invalid Booking fixture id")
	}
	userID, err := uuid.Parse(fixture.UserID)
	if err != nil {
		return errors.New("invalid user fixture id")
	}
	frozen, err := json.Marshal(domain.FrozenCreateParameters{
		Version: 1, Amount: fixture.AmountCents, Currency: fixture.Currency,
		AutomaticPaymentMethods: domain.AutomaticPaymentMethods{Enabled: true, AllowRedirects: "always"},
	})
	if err != nil {
		return errors.New("cannot encode frozen Payment fixture parameters")
	}
	identity := domain.FinancialIdentity{
		BookingID: bookingID, UserID: userID, Generation: fixture.Generation,
		ProviderAccount: fixture.ProviderAccount, LiveMode: false,
		AmountCents: fixture.AmountCents, Currency: fixture.Currency,
	}
	identity.Fingerprint, err = domain.FinancialFingerprint(identity, frozen)
	if err != nil {
		return errors.New("Payment fixture fingerprint is invalid")
	}
	operation := domain.CreateOperation{
		Identity: identity, PaymentID: paymentID,
		Key:          "ticketbox:payment:" + paymentID.String() + ":g1:create",
		FrozenParams: frozen,
	}
	financial, _, err := repo.CreateOrGet(ctx, operation)
	if err != nil || financial == nil || financial.Payment == nil || financial.Payment.ID != paymentID {
		return errors.New("Payment fixture repository identity was rejected")
	}
	observedAt := time.Now().UTC()
	if err := repo.RecordObservation(ctx, domain.ProviderObservation{
		Account: fixture.ProviderAccount, LiveMode: false,
		EventID: "evt_r4_" + paymentID.String(), IntentID: fixture.PaymentIntentID,
		Status: "succeeded", PaymentID: paymentID, BookingID: bookingID,
		Generation: fixture.Generation, AmountCents: fixture.AmountCents,
		Currency: fixture.Currency, Collected: true, ObservedAt: observedAt,
	}); err != nil {
		return errors.New("Payment fixture collection observation was rejected")
	}
	if runtimeMode != "default_worker" {
		if _, err := pool.Exec(ctx, `UPDATE payments SET reconciliation_state='sale_verified' WHERE managed AND id=$1`, paymentID); err != nil {
			return errors.New("Payment fixture reconciliation state could not be initialized")
		}
	}
	verified, err := repo.GetFinancial(ctx, paymentID)
	if err != nil || verified == nil || verified.Payment == nil || verified.Payment.ID != paymentID ||
		verified.Payment.BookingId == nil || *verified.Payment.BookingId != bookingID ||
		verified.Payment.UserId != userID || verified.Generation != fixture.Generation ||
		!verified.Managed || verified.ProviderAccount != fixture.ProviderAccount || verified.LiveMode == nil || *verified.LiveMode ||
		verified.Payment.PaymentIntentId != fixture.PaymentIntentID || int64(verified.Payment.Price) != fixture.AmountCents ||
		verified.Payment.Currency != fixture.Currency || verified.ProviderOutcome != "collected" ||
		verified.ReconciliationState != expectedFixtureReconciliationState(runtimeMode) ||
		(runtimeMode == "default_worker" && verified.BookingDisposition != "") ||
		verified.CreateState != "create_ready" || verified.RefundState != "not_required" {
		return errors.New("seeded Payment fixture failed authoritative identity verification")
	}
	return nil
}

func expectedFixtureReconciliationState(runtimeMode string) string {
	if runtimeMode == "default_worker" {
		return "pending"
	}
	return "sale_verified"
}
