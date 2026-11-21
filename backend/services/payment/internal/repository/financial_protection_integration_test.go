package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/payment/internal/domain"
)

func TestProtectedFinancialFactsCannotBeUpdatedOrDeletedIntegration(t *testing.T) {
	p, _ := paymentFinancialFixture(t)
	r := NewPostgresPaymentRepository(p)
	ctx := context.Background()
	op := financialFixtureOperation()
	if _, _, err := r.CreateOrGet(ctx, op); err != nil {
		t.Fatal(err)
	}
	legacy := uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO payments(id,user_id,booking_id,status,price,currency) VALUES($1,$2,$3,'pending',725,'usd')`, legacy, op.Identity.UserID, uuid.New()); err != nil {
		t.Fatal(err)
	}
	newID := uuid.New()
	if err := r.CreatePayment(ctx, &domain.Payment{ID: newID, UserId: op.Identity.UserID, Status: domain.PaymentSuccess, Price: 725, Currency: "usd"}); err == nil {
		t.Error("unmanaged create fabricated collection")
	}
	var count int
	if err := p.QueryRow(ctx, `SELECT COUNT(*) FROM payments WHERE id=$1`, newID).Scan(&count); err != nil || count != 0 {
		t.Error("untrusted generic creation persisted", err)
	}
	for _, id := range []uuid.UUID{op.PaymentID, legacy} {
		t.Run(id.String(), func(t *testing.T) {
			var before string
			if err := p.QueryRow(ctx, `SELECT to_jsonb(p)::text FROM payments p WHERE id=$1`, id).Scan(&before); err != nil {
				t.Fatal(err)
			}
			for name, mutate := range map[string]func() error{
				"status": func() error { return r.UpdatePaymentStatus(ctx, id, domain.PaymentSuccess) },
				"update": func() error {
					return r.UpdatePayment(ctx, &domain.Payment{ID: id, Price: 100, Currency: "eur", Status: domain.PaymentSuccess})
				},
				"soft_delete": func() error { return r.SoftDeletePayment(ctx, id) },
				"delete":      func() error { return r.DeletePayment(ctx, id) },
			} {
				t.Run(name, func(t *testing.T) {
					if err := mutate(); err == nil {
						t.Error("financial facts mutation did not reject")
					}
					var after string
					if err := p.QueryRow(ctx, `SELECT to_jsonb(p)::text FROM payments p WHERE id=$1`, id).Scan(&after); err != nil || before != after {
						t.Error("financial facts changed/deleted", err)
					}
				})
			}
		})
	}
	o := domain.ProviderObservation{PaymentID: op.PaymentID, BookingID: op.Identity.BookingID, Generation: 1, Account: op.Identity.ProviderAccount, AmountCents: 725, Currency: "usd", IntentID: "pi_protected", Status: "succeeded", Collected: true, ObservedAt: time.Now()}
	if err := r.RecordObservation(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := r.UpdatePaymentStatusByPaymentIntentId(ctx, o.IntentID, domain.PaymentFail); err == nil {
		t.Error("generic intent status did not reject")
	}
	v, err := r.GetFinancial(ctx, op.PaymentID)
	if err != nil || v.ProviderOutcome != "collected" {
		t.Fatal("trusted financial facts not retained", err)
	}
	l, err := r.GetFinancial(ctx, legacy)
	if err != nil || l.Payment.PaymentIntentId != "" || l.ProviderOutcome != "unknown" {
		t.Fatal("historical nullable payment unreadable", err)
	}
}
