package stripe

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v84"
	"github.com/ticketbox/payment/internal/domain"
)

func (s *StripePaymentGateway) CreateIntent(ctx context.Context, op domain.CreateOperation) (*domain.ProviderObservation, string, error) {
	hash, err := domain.FinancialFingerprint(op.Identity, op.FrozenParams)
	if err != nil || hash != op.Identity.Fingerprint || op.Key != "ticketbox:payment:"+op.PaymentID.String()+":g1:create" || op.Identity.LiveMode {
		return nil, "", errors.New("invalid frozen provider identity")
	}
	var frozen domain.FrozenCreateParameters
	if err := json.Unmarshal(op.FrozenParams, &frozen); err != nil {
		return nil, "", err
	}
	params := &stripego.PaymentIntentCreateParams{Amount: stripego.Int64(frozen.Amount), Currency: stripego.String(frozen.Currency), AutomaticPaymentMethods: &stripego.PaymentIntentCreateAutomaticPaymentMethodsParams{Enabled: stripego.Bool(true), AllowRedirects: stripego.String("always")}, Metadata: map[string]string{"ticketbox_payment_id": op.PaymentID.String(), "ticketbox_booking_id": op.Identity.BookingID.String(), "ticketbox_generation": strconv.FormatInt(op.Identity.Generation, 10)}}
	if frozen.ReceiptEmail != "" {
		params.ReceiptEmail = stripego.String(frozen.ReceiptEmail)
	}
	params.SetIdempotencyKey(op.Key)
	pi, err := s.stripeClient.V1PaymentIntents.Create(ctx, params)
	if err != nil {
		return nil, "", errors.New("provider create outcome unknown")
	}
	o, err := intentObservation(pi, op.Identity.ProviderAccount, op.Identity.LiveMode)
	if err != nil || o.PaymentID != op.PaymentID || o.BookingID != op.Identity.BookingID || o.AmountCents != op.Identity.AmountCents || o.Currency != op.Identity.Currency || o.Generation != op.Identity.Generation {
		return nil, "", errors.New("provider identity mismatch")
	}
	return o, pi.ClientSecret, nil
}

func (s *StripePaymentGateway) RetrieveIntent(ctx context.Context, account string, live bool, id string) (*domain.ProviderObservation, string, error) {
	if live || id == "" {
		return nil, "", errors.New("provider identity invalid")
	}
	pi, err := s.stripeClient.V1PaymentIntents.Retrieve(ctx, id, nil)
	if err != nil {
		return nil, "", errors.New("provider retrieval unavailable")
	}
	o, err := intentObservation(pi, account, live)
	if err != nil {
		return nil, "", err
	}
	secret := ""
	if pi.Status != stripego.PaymentIntentStatusSucceeded && pi.Status != stripego.PaymentIntentStatusCanceled {
		secret = pi.ClientSecret
	}
	return o, secret, nil
}

func (s *StripePaymentGateway) FindIntent(ctx context.Context, op domain.CreateOperation) ([]domain.ProviderObservation, error) {
	if op.Identity.LiveMode {
		return nil, errors.New("live financial work blocked")
	}
	query := "metadata['ticketbox_payment_id']:'" + op.PaymentID.String() + "'"
	var result []domain.ProviderObservation
	for pi, err := range s.stripeClient.V1PaymentIntents.Search(ctx, &stripego.PaymentIntentSearchParams{SearchParams: stripego.SearchParams{Query: query, Limit: stripego.Int64(10)}}) {
		if err != nil {
			return nil, errors.New("provider correlation unavailable")
		}
		o, err := intentObservation(pi, op.Identity.ProviderAccount, op.Identity.LiveMode)
		if err != nil {
			return nil, err
		}
		if o.PaymentID != op.PaymentID || o.BookingID != op.Identity.BookingID || o.AmountCents != op.Identity.AmountCents || o.Currency != op.Identity.Currency || o.Generation != op.Identity.Generation {
			return nil, errors.New("provider correlation mismatch")
		}
		result = append(result, *o)
		if len(result) > 1 {
			break
		}
	}
	return result, nil
}

func intentObservation(pi *stripego.PaymentIntent, account string, live bool) (*domain.ProviderObservation, error) {
	if pi == nil || pi.ID == "" || pi.Livemode != live || account == "" {
		return nil, errors.New("provider scope mismatch")
	}
	payment, err := uuid.Parse(pi.Metadata["ticketbox_payment_id"])
	if err != nil || payment == uuid.Nil {
		return nil, errors.New("provider payment correlation missing")
	}
	booking, err := uuid.Parse(pi.Metadata["ticketbox_booking_id"])
	if err != nil || booking == uuid.Nil {
		return nil, errors.New("provider Booking correlation missing")
	}
	generation, err := strconv.ParseInt(pi.Metadata["ticketbox_generation"], 10, 64)
	if err != nil || generation != 1 {
		return nil, errors.New("provider generation correlation missing")
	}
	collected := pi.Status == stripego.PaymentIntentStatusSucceeded
	if collected && pi.AmountReceived != pi.Amount {
		return nil, errors.New("provider collected amount mismatch")
	}
	o := &domain.ProviderObservation{Account: account, LiveMode: live, PaymentID: payment, BookingID: booking, Generation: generation, IntentID: pi.ID, AmountCents: pi.Amount, Currency: string(pi.Currency), Status: string(pi.Status), Collected: collected, ObservedAt: time.Now()}
	o.SafeObject, _ = json.Marshal(map[string]any{"id": pi.ID, "status": pi.Status, "amount": pi.Amount, "amount_received": pi.AmountReceived, "currency": pi.Currency, "livemode": pi.Livemode, "metadata": map[string]string{"ticketbox_payment_id": payment.String(), "ticketbox_booking_id": booking.String(), "ticketbox_generation": "1"}})
	o.EvidenceHash = sha256.Sum256(o.SafeObject)
	return o, nil
}

// VerifyTestAccount is a credential-bound read before any managed writer is
// activated. Never log the key or provider response body.
func (s *StripePaymentGateway) VerifyTestAccount(ctx context.Context, key string, expectedAccount ...string) (string, error) {
	if !strings.HasPrefix(key, "sk_test_") && !strings.HasPrefix(key, "rk_test_") {
		return "", errors.New("STRIPE_LIVE_MODE_BLOCKED")
	}
	account, err := s.stripeClient.V1Accounts.Retrieve(ctx, nil)
	if err != nil || account == nil || account.ID == "" {
		return "", errors.New("Stripe TEST account not established")
	}
	if len(expectedAccount) > 1 || (len(expectedAccount) == 1 && expectedAccount[0] != "" && expectedAccount[0] != account.ID) {
		return "", errors.New("Stripe TEST account differs from deployment identity")
	}
	return account.ID, nil
}
