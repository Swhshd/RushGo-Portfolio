package recoverycomponent

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func testBool(value bool) *bool { return &value }

func validRefundFixture() (ProductSource, ProviderEvidence) {
	paymentID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	bookingID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	decisionID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+bookingID.String()+":g1:expire"))
	refundCommand := &PersistedRefundCommand{
		DecisionID:  decisionID,
		AmountCents: 725,
		Currency:    "usd",
		Reason:      "EXPIRED",
		Key:         "ticketbox:payment:" + paymentID.String() + ":g1:refund",
	}
	source := ProductSource{
		Managed:         true,
		PaymentID:       paymentID,
		BookingID:       bookingID,
		Generation:      1,
		ProviderAccount: "acct_component_fixture",
		LiveMode:        testBool(false),
		AmountCents:     725,
		Currency:        "usd",
		PaymentIntentID: "pi_component_fixture",
		PaymentStatus:   "success",
		ProviderOutcome: "collected",
		CreateState:     "create_ready",
		ProviderStatus:  "succeeded",
		RefundState:     "pending",
		CreateKey:       "ticketbox:payment:" + paymentID.String() + ":g1:create",
		RequestHash:     make([]byte, 32),
		RefundCommand:   refundCommand,
	}
	requested := time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)
	request := func(id, objectID, operation string) RequestObservation {
		return RequestObservation{
			ID:          id,
			Operation:   operation,
			AccountID:   "acct_component_fixture",
			LiveMode:    testBool(false),
			ObjectID:    objectID,
			RequestedAt: requested,
			ObservedAt:  requested.Add(time.Second),
		}
	}
	provider := ProviderEvidence{
		Class: EvidenceSynthetic,
		PaymentIntent: PaymentIntentFact{
			ID:                  "pi_component_fixture",
			Status:              "succeeded",
			AmountCents:         725,
			AmountReceivedCents: 725,
			Currency:            "usd",
			LatestChargeID:      "ch_component_fixture",
			LiveMode:            testBool(false),
			Request:             request("req_pi_fixture", "pi_component_fixture", "retrieve_payment_intent"),
			Metadata: map[string]string{
				"ticketbox_payment_id": paymentID.String(),
				"ticketbox_booking_id": bookingID.String(),
				"ticketbox_generation": "1",
			},
		},
		Charge: ChargeFact{
			ID:              "ch_component_fixture",
			PaymentIntentID: "pi_component_fixture",
			AmountCents:     725,
			Currency:        "usd",
			Paid:            testBool(true),
			Captured:        testBool(true),
			LiveMode:        testBool(false),
			Request:         request("req_charge_fixture", "ch_component_fixture", "retrieve_charge"),
		},
		Refund: RefundFact{
			ID:              "re_component_fixture",
			PaymentIntentID: "pi_component_fixture",
			ChargeID:        "ch_component_fixture",
			Status:          "succeeded",
			AmountCents:     725,
			Currency:        "usd",
			LiveMode:        testBool(false),
			Request:         request("req_refund_fixture", "re_component_fixture", "retrieve_refund"),
			Metadata: map[string]string{
				"ticketbox_payment_id": paymentID.String(),
				"ticketbox_booking_id": bookingID.String(),
				"ticketbox_generation": "1",
			},
		},
	}
	return source, provider
}

func hasFinding(findings []string, want string) bool {
	for _, finding := range findings {
		if finding == want {
			return true
		}
	}
	return false
}

func TestExactSingleFullRefundReportsMissingPersistedChargeAssociation(t *testing.T) {
	source, provider := validRefundFixture()
	got := ValidateSingleFullRefund(source, provider)

	if got.ProviderFacts != StatusPass {
		t.Fatalf("synthetic Provider facts status = %q, want %q; findings=%v", got.ProviderFacts, StatusPass, got.Findings)
	}
	if got.ProductCommand != StatusPass {
		t.Fatalf("persisted Payment/refund command status = %q, want %q; findings=%v", got.ProductCommand, StatusPass, got.Findings)
	}
	if got.Provenance != StatusUnknown || got.Overall != StatusUnknown {
		t.Fatalf("missing persisted Charge association must remain UNKNOWN, got provenance=%q overall=%q", got.Provenance, got.Overall)
	}
	if !hasFinding(got.Findings, FindingChargeAssociationNotPersisted) {
		t.Fatalf("missing explicit Charge provenance gap: %v", got.Findings)
	}
}

func TestPartialRefundCannotSatisfySingleFullRefund(t *testing.T) {
	source, provider := validRefundFixture()
	provider.Refund.AmountCents = 300
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusFail {
		t.Fatalf("partial refund status = %q, want FAIL; findings=%v", got.ProviderFacts, got.Findings)
	}
}

func TestPendingRefundCannotSatisfySucceededRefund(t *testing.T) {
	source, provider := validRefundFixture()
	provider.Refund.Status = "pending"
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusFail {
		t.Fatalf("pending refund status = %q, want FAIL; findings=%v", got.ProviderFacts, got.Findings)
	}
}

func TestMissingLivemodeIsUnknownAndLiveModeIsRejected(t *testing.T) {
	source, provider := validRefundFixture()
	provider.Refund.LiveMode = nil
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusUnknown {
		t.Fatalf("missing Refund livemode = %q, want UNKNOWN; findings=%v", got.ProviderFacts, got.Findings)
	}

	source, provider = validRefundFixture()
	provider.Refund.LiveMode = testBool(true)
	got = ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusFail {
		t.Fatalf("live Refund livemode = %q, want FAIL; findings=%v", got.ProviderFacts, got.Findings)
	}
}

func TestProviderRequestMustUsePersistedTestAccount(t *testing.T) {
	source, provider := validRefundFixture()
	provider.Charge.Request.AccountID = "acct_other_fixture"
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusFail {
		t.Fatalf("wrong Provider account status = %q, want FAIL; findings=%v", got.ProviderFacts, got.Findings)
	}
}

func TestPaymentIntentChargeRefundLinksMustMatch(t *testing.T) {
	source, provider := validRefundFixture()
	provider.Charge.PaymentIntentID = "pi_other_fixture"
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusFail {
		t.Fatalf("wrong Charge PaymentIntent link status = %q, want FAIL; findings=%v", got.ProviderFacts, got.Findings)
	}

	source, provider = validRefundFixture()
	provider.Refund.ChargeID = "ch_other_fixture"
	got = ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusFail {
		t.Fatalf("wrong Refund Charge link status = %q, want FAIL; findings=%v", got.ProviderFacts, got.Findings)
	}
}

func TestRefundDecisionMustBelongToPersistedBookingCommand(t *testing.T) {
	source, provider := validRefundFixture()
	otherBooking := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	source.RefundCommand.DecisionID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:booking:"+otherBooking.String()+":g1:expire"))
	got := ValidateSingleFullRefund(source, provider)
	if got.ProductCommand != StatusFail {
		t.Fatalf("wrong Booking decision status = %q, want FAIL; findings=%v", got.ProductCommand, got.Findings)
	}
}

func TestMetadataIsOnlyAnOptionalConsistencyCheck(t *testing.T) {
	source, provider := validRefundFixture()
	provider.PaymentIntent.Metadata = nil
	provider.Refund.Metadata = nil
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusPass || got.ProductCommand != StatusPass {
		t.Fatalf("metadata absence must not replace or block persisted identity checks: provider=%q command=%q findings=%v", got.ProviderFacts, got.ProductCommand, got.Findings)
	}

	source, provider = validRefundFixture()
	provider.Refund.Metadata["ticketbox_payment_id"] = uuid.NewString()
	got = ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusFail {
		t.Fatalf("conflicting metadata status = %q, want FAIL; findings=%v", got.ProviderFacts, got.Findings)
	}
}

func TestMissingRequestIdentityOrObservationTimeIsUnknown(t *testing.T) {
	source, provider := validRefundFixture()
	provider.Refund.Request.ID = ""
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusUnknown {
		t.Fatalf("missing request identity status = %q, want UNKNOWN; findings=%v", got.ProviderFacts, got.Findings)
	}

	source, provider = validRefundFixture()
	provider.Charge.Request.ObservedAt = time.Time{}
	got = ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusUnknown {
		t.Fatalf("missing observation time status = %q, want UNKNOWN; findings=%v", got.ProviderFacts, got.Findings)
	}
}

func TestNonSyntheticProviderEvidenceIsNotAccepted(t *testing.T) {
	source, provider := validRefundFixture()
	provider.Class = "REAL_PROVIDER"
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusUnknown || got.Overall == StatusPass {
		t.Fatalf("non-synthetic Provider evidence must not pass: provider=%q overall=%q findings=%v", got.ProviderFacts, got.Overall, got.Findings)
	}
}

func TestMissingPaymentLivemodeAndPersistedCommandRemainUnknown(t *testing.T) {
	source, provider := validRefundFixture()
	source.LiveMode = nil
	got := ValidateSingleFullRefund(source, provider)
	if got.ProductCommand != StatusUnknown {
		t.Fatalf("missing Payment livemode = %q, want UNKNOWN; findings=%v", got.ProductCommand, got.Findings)
	}

	source, provider = validRefundFixture()
	source.RefundCommand = nil
	got = ValidateSingleFullRefund(source, provider)
	if got.ProductCommand != StatusUnknown {
		t.Fatalf("missing persisted refund command = %q, want UNKNOWN; findings=%v", got.ProductCommand, got.Findings)
	}
}

func TestRefundCurrencyMustMatchPersistedFullAmountCurrency(t *testing.T) {
	source, provider := validRefundFixture()
	provider.Refund.Currency = "eur"
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusFail {
		t.Fatalf("wrong refund currency status = %q, want FAIL; findings=%v", got.ProviderFacts, got.Findings)
	}
}

func TestProviderRequestIdentityCannotBeReused(t *testing.T) {
	source, provider := validRefundFixture()
	provider.Refund.Request.ID = provider.Charge.Request.ID
	got := ValidateSingleFullRefund(source, provider)
	if got.ProviderFacts != StatusFail {
		t.Fatalf("reused request identity status = %q, want FAIL; findings=%v", got.ProviderFacts, got.Findings)
	}
}
