package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	stripego "github.com/stripe/stripe-go/v84"
)

const refundEvidencePageLimit = 100
const refundEvidenceMaxPages = 100
const refundEvidenceMaxObjects = 500

// RefundEvidenceObject is the allowlisted, per-object projection used by the
// candidate refund chain. It is not a persisted product type.
type RefundEvidenceObject struct {
	Object        string            `json:"object"`
	ID            string            `json:"id"`
	Status        string            `json:"status"`
	Amount        int64             `json:"amount"`
	Currency      string            `json:"currency"`
	PaymentIntent string            `json:"payment_intent"`
	Charge        string            `json:"charge"`
	Metadata      map[string]string `json:"metadata"`
	Livemode      *bool             `json:"livemode"`
}

type RefundEvidenceCharge struct {
	Object         string `json:"object"`
	ID             string `json:"id"`
	Status         string `json:"status"`
	Amount         int64  `json:"amount"`
	AmountCaptured int64  `json:"amount_captured"`
	AmountRefunded int64  `json:"amount_refunded"`
	Currency       string `json:"currency"`
	PaymentIntent  string `json:"payment_intent"`
	Paid           bool   `json:"paid"`
	Captured       bool   `json:"captured"`
	Livemode       *bool  `json:"livemode"`
}

type RefundEvidenceIntent struct {
	Object         string            `json:"object"`
	ID             string            `json:"id"`
	Status         string            `json:"status"`
	Amount         int64             `json:"amount"`
	AmountReceived int64             `json:"amount_received"`
	Currency       string            `json:"currency"`
	LatestCharge   string            `json:"latest_charge"`
	Metadata       map[string]string `json:"metadata"`
	Livemode       *bool             `json:"livemode"`
}

type RefundPageTrace struct {
	PageNumber     int      `json:"pageNumber"`
	RequestCursor  string   `json:"requestCursor"`
	ResponseLastID string   `json:"responseLastId"`
	Limit          int      `json:"limit"`
	HasMore        bool     `json:"hasMore"`
	ItemIDs        []string `json:"itemIds"`
	ObjectCount    int      `json:"objectCount"`
	QueryPath      string   `json:"queryPath"`
	StartingAfter  string   `json:"startingAfter"`
}

type RefundEvidenceCollection struct {
	Intent             RefundEvidenceIntent   `json:"intent"`
	Charge             RefundEvidenceCharge   `json:"charge"`
	Refunds            []RefundEvidenceObject `json:"refunds"`
	ChargePages        []RefundPageTrace      `json:"chargePages"`
	RefundPages        []RefundPageTrace      `json:"refundPages"`
	PaginationComplete bool                   `json:"paginationComplete"`
	HasMore            bool                   `json:"hasMore"`
	EndReason          string                 `json:"endReason"`
	ObservedAt         time.Time              `json:"observedAt"`
	Limitation         string                 `json:"limitation"`
}

type refundEvidencePage struct {
	stripego.APIResource
	Object  *string                  `json:"object"`
	HasMore *bool                    `json:"has_more"`
	Data    *[]*RefundEvidenceObject `json:"data"`
}

type chargeEvidencePage struct {
	stripego.APIResource
	Object  *string                  `json:"object"`
	HasMore *bool                    `json:"has_more"`
	Data    *[]*RefundEvidenceCharge `json:"data"`
}

type intentEvidenceResponse struct {
	stripego.APIResource
	Object         string            `json:"object"`
	ID             string            `json:"id"`
	Status         string            `json:"status"`
	Amount         int64             `json:"amount"`
	AmountReceived int64             `json:"amount_received"`
	Currency       string            `json:"currency"`
	LatestCharge   jsonString        `json:"latest_charge"`
	Metadata       map[string]string `json:"metadata"`
	Livemode       *bool             `json:"livemode"`
}

type jsonString string

func (s *jsonString) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	if data[0] == '"' {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		*s = jsonString(value)
		return nil
	}
	var expanded struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &expanded); err != nil {
		return err
	}
	*s = jsonString(expanded.ID)
	return nil
}

func (s *StripePaymentGateway) SetRefundEvidenceReadBackend(backend stripego.Backend, apiKey string) {
	if backend == nil || (!strings.HasPrefix(apiKey, "sk_test_") && !strings.HasPrefix(apiKey, "rk_test_")) {
		s.refundEvidenceBackend, s.refundEvidenceAPIKey = nil, ""
		return
	}
	s.refundEvidenceBackend, s.refundEvidenceAPIKey = backend, apiKey
}

// CollectRefundEvidence performs the candidate-only, read-only object walk.
// It never creates or mutates a provider object and is not wired into the
// production refund worker.
func (s *StripePaymentGateway) CollectRefundEvidence(ctx context.Context, account, paymentIntentID string) (*RefundEvidenceCollection, error) {
	if s.refundEvidenceBackend == nil || s.refundEvidenceAPIKey == "" || account == "" ||
		!strings.HasPrefix(paymentIntentID, "pi_") || ctx == nil {
		return nil, errors.New("refund evidence reader is not configured")
	}
	params := &stripego.Params{Context: ctx, StripeAccount: stripego.String(account)}
	var intent intentEvidenceResponse
	if err := s.refundEvidenceBackend.Call("GET", "/v1/payment_intents/"+paymentIntentID, s.refundEvidenceAPIKey, params, &intent); err != nil {
		return nil, errors.New("refund evidence payment intent unavailable")
	}
	if intent.ID != paymentIntentID || intent.Object != "payment_intent" ||
		intent.Status != "succeeded" || intent.Amount < 1 ||
		intent.AmountReceived != intent.Amount || intent.Currency == "" ||
		intent.Livemode == nil || *intent.Livemode {
		return nil, errors.New("refund evidence payment intent mismatch")
	}
	latestCharge := string(intent.LatestCharge)

	charges, chargePages, err := s.walkChargeEvidence(ctx, account, paymentIntentID)
	if err != nil {
		return nil, err
	}
	if len(charges) != 1 {
		return nil, errors.New("refund evidence requires exactly one charge")
	}
	charge := charges[0]
	if charge.Object != "charge" || charge.ID == "" || charge.PaymentIntent != paymentIntentID ||
		charge.Status != "succeeded" || !charge.Paid || !charge.Captured ||
		charge.Amount != intent.Amount || charge.AmountCaptured != intent.Amount ||
		charge.Currency != intent.Currency || charge.Livemode == nil || *charge.Livemode {
		return nil, errors.New("refund evidence charge mismatch")
	}
	if latestCharge != "" && charge.ID != latestCharge {
		return nil, errors.New("refund evidence latest charge mismatch")
	}
	refunds, refundPages, complete, hasMore, endReason, err := s.walkRefundEvidence(ctx, account, paymentIntentID)
	if err != nil {
		return nil, err
	}
	return &RefundEvidenceCollection{
		Intent: RefundEvidenceIntent{
			Object: intent.Object, ID: intent.ID, Status: intent.Status,
			Amount: intent.Amount, AmountReceived: intent.AmountReceived,
			Currency: intent.Currency, LatestCharge: latestCharge,
			Metadata: intent.Metadata, Livemode: intent.Livemode,
		},
		Charge: charge, Refunds: refunds,
		ChargePages: chargePages, RefundPages: refundPages,
		PaginationComplete: complete, HasMore: hasMore, EndReason: endReason,
		ObservedAt: time.Now().UTC(),
		Limitation: "READ_ONLY_PAGE_WALK_NOT_A_CROSS_PAGE_ATOMIC_SNAPSHOT",
	}, nil
}

func (s *StripePaymentGateway) walkChargeEvidence(ctx context.Context, account, paymentIntentID string) ([]RefundEvidenceCharge, []RefundPageTrace, error) {
	var charges []RefundEvidenceCharge
	var traces []RefundPageTrace
	cursor, seen := "", map[string]bool{}
	for page := 1; page <= refundEvidenceMaxPages; page++ {
		values := url.Values{"payment_intent": {paymentIntentID}, "limit": {strconv.Itoa(refundEvidencePageLimit)}}
		if cursor != "" {
			values.Set("starting_after", cursor)
		}
		params := &stripego.Params{Context: ctx, StripeAccount: stripego.String(account),
			Extra: &stripego.ExtraValues{Values: values}}
		var response chargeEvidencePage
		if err := s.refundEvidenceBackend.Call("GET", "/v1/charges", s.refundEvidenceAPIKey, params, &response); err != nil {
			return nil, nil, errors.New("refund evidence charge page unavailable")
		}
		if response.Object == nil || *response.Object != "list" || response.HasMore == nil ||
			response.Data == nil || *response.Data == nil || len(*response.Data) > refundEvidencePageLimit {
			return nil, nil, errors.New("refund evidence malformed charge page")
		}
		trace := RefundPageTrace{PageNumber: page, RequestCursor: cursor, Limit: refundEvidencePageLimit,
			HasMore: *response.HasMore, ObjectCount: len(*response.Data), QueryPath: "/v1/charges",
			StartingAfter: cursor}
		for _, charge := range *response.Data {
			if charge == nil || charge.ID == "" {
				return nil, nil, errors.New("refund evidence malformed charge")
			}
			trace.ItemIDs = append(trace.ItemIDs, charge.ID)
			charges = append(charges, *charge)
		}
		if len(charges) > refundEvidenceMaxObjects {
			return nil, nil, errors.New("refund evidence charge bound exceeded")
		}
		if len(*response.Data) > 0 {
			trace.ResponseLastID = (*response.Data)[len(*response.Data)-1].ID
		}
		traces = append(traces, trace)
		if !*response.HasMore {
			return charges, traces, nil
		}
		if len(*response.Data) == 0 {
			return nil, nil, errors.New("refund evidence empty charge page with has_more")
		}
		next := trace.ResponseLastID
		if next == "" || seen[next] {
			return nil, nil, errors.New("refund evidence charge cursor did not advance")
		}
		seen[next] = true
		cursor = next
	}
	return nil, nil, errors.New("refund evidence charge page bound exceeded")
}

func (s *StripePaymentGateway) walkRefundEvidence(ctx context.Context, account, paymentIntentID string) ([]RefundEvidenceObject, []RefundPageTrace, bool, bool, string, error) {
	var refunds []RefundEvidenceObject
	var traces []RefundPageTrace
	seenIDs := map[string]bool{}
	cursor, seenCursors := "", map[string]bool{}
	for page := 1; page <= refundEvidenceMaxPages; page++ {
		values := url.Values{"payment_intent": {paymentIntentID}, "limit": {strconv.Itoa(refundEvidencePageLimit)}}
		if cursor != "" {
			values.Set("starting_after", cursor)
		}
		params := &stripego.Params{Context: ctx, StripeAccount: stripego.String(account),
			Extra: &stripego.ExtraValues{Values: values}}
		var response refundEvidencePage
		if err := s.refundEvidenceBackend.Call("GET", "/v1/refunds", s.refundEvidenceAPIKey, params, &response); err != nil {
			return nil, traces, false, false, "PAGE_ERROR", errors.New("refund evidence page unavailable")
		}
		if response.Object == nil || *response.Object != "list" || response.HasMore == nil ||
			response.Data == nil || *response.Data == nil || len(*response.Data) > refundEvidencePageLimit {
			return nil, traces, false, false, "MALFORMED_PAGE", errors.New("refund evidence malformed page")
		}
		trace := RefundPageTrace{PageNumber: page, RequestCursor: cursor, Limit: refundEvidencePageLimit,
			HasMore: *response.HasMore, ObjectCount: len(*response.Data), QueryPath: "/v1/refunds",
			StartingAfter: cursor}
		for _, refund := range *response.Data {
			if refund == nil || refund.ID == "" || refund.Object != "refund" ||
				refund.PaymentIntent != paymentIntentID || refund.Amount < 1 ||
				refund.Currency == "" || refund.Status == "" {
				return nil, traces, false, false, "MALFORMED_OBJECT", errors.New("refund evidence malformed refund object")
			}
			if seenIDs[refund.ID] {
				return nil, traces, false, false, "DUPLICATE_ID", errors.New("refund evidence duplicate refund id")
			}
			seenIDs[refund.ID] = true
			trace.ItemIDs = append(trace.ItemIDs, refund.ID)
			refunds = append(refunds, *refund)
		}
		if len(refunds) > refundEvidenceMaxObjects {
			return nil, traces, false, false, "OBJECT_BOUND", errors.New("refund evidence object bound exceeded")
		}
		if len(*response.Data) > 0 {
			trace.ResponseLastID = (*response.Data)[len(*response.Data)-1].ID
		}
		traces = append(traces, trace)
		if !*response.HasMore {
			return refunds, traces, true, false, "HAS_MORE_FALSE", nil
		}
		if len(*response.Data) == 0 {
			return nil, traces, false, true, "EMPTY_PAGE_WITH_HAS_MORE", errors.New("refund evidence empty page with has_more")
		}
		next := trace.ResponseLastID
		if next == "" || seenCursors[next] {
			return nil, traces, false, true, "CURSOR_NOT_ADVANCING", errors.New("refund evidence cursor did not advance")
		}
		seenCursors[next] = true
		cursor = next
	}
	return nil, traces, false, true, "PAGE_BOUND", errors.New("refund evidence page bound exceeded")
}
