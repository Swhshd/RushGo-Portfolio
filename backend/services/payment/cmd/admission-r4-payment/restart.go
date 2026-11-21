//go:build admission_r4

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
)

// The local provider has no durable refund ledger. Only untouched static
// fixtures can be reconstructed. This check must precede EVERY seed/write.
// sourceCommit remains the base run binding for an explicitly labelled overlay;
// buildSourceCommit identifies the actual patched executable in control replies.
var buildSourceCommit = ""

func restartVerificationMode(args []string) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	if len(args) == 1 && args[0] == "--verify-static-restart" {
		return true, nil
	}
	return false, errors.New("unsupported Payment arguments; refusing startup")
}

func verifyStaticRestart(ctx context.Context, pool *pgxpool.Pool, cfg bootstrapConfig) (bool, error) {
	if cfg.RuntimeMode != "default_worker" {
		return false, nil
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, errors.New("static restart snapshot unavailable")
	}
	defer tx.Rollback(context.Background())
	var history bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM payment_refund_audit)
		OR EXISTS(SELECT 1 FROM payment_provider_events WHERE refund_id IS NOT NULL OR event_type ILIKE '%refund%')`).Scan(&history); err != nil {
		return false, errors.New("static restart refund history unavailable")
	}
	rows, err := tx.Query(ctx, "SELECT to_jsonb(p) FROM payments p ORDER BY id")
	if err != nil {
		return false, errors.New("static restart payment snapshot unavailable")
	}
	var records []map[string]any
	for rows.Next() {
		var raw []byte
		var record map[string]any
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return false, errors.New("static restart row unavailable")
		}
		if json.Unmarshal(raw, &record) != nil {
			rows.Close()
			return false, errors.New("static restart row invalid")
		}
		records = append(records, record)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, errors.New("static restart rows incomplete")
	}
	resumed, err := validateStaticRestart(records, history, cfg.Fixtures)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, errors.New("static restart snapshot completion failed")
	}
	return resumed, nil
}

func validateStaticRestart(records []map[string]any, history bool, fixtures []fixtureConfig) (bool, error) {
	if history {
		return false, errors.New("static restart refused: durable refund history")
	}
	if len(records) == 0 {
		return false, nil
	}
	if len(records) != len(fixtures) {
		return false, errors.New("static restart refused: incomplete or extra payment inventory")
	}
	byID := make(map[string]fixtureConfig, len(fixtures))
	for _, f := range fixtures {
		byID[f.PaymentID] = f
	}
	for _, r := range records {
		id, ok := r["id"].(string)
		f, found := byID[id]
		if !ok || !found {
			return false, errors.New("static restart refused: foreign payment identity")
		}
		delete(byID, id)
		prefix := "ticketbox:payment:" + id + ":g1:"
		expected := map[string]any{
			"id": f.PaymentID, "booking_id": f.BookingID, "user_id": f.UserID,
			"payment_generation": float64(f.Generation), "managed": true, "livemode": false,
			"provider_account": f.ProviderAccount, "payment_intent_id": f.PaymentIntentID,
			"price": float64(f.AmountCents), "currency": f.Currency, "status": "success",
			"provider_outcome": "collected", "provider_status": "succeeded", "create_state": "create_ready",
			"cancel_state": "not_requested", "refund_state": "not_required", "deleted_at": nil,
			"create_key": prefix + "create", "cancel_key": prefix + "cancel", "refund_key": prefix + "refund",
			"cancel_first_attempt_at": nil, "claim_token": nil, "lease_until": nil,
		}
		for key, want := range expected {
			got, present := r[key]
			if !present || !reflect.DeepEqual(got, want) {
				return false, fmt.Errorf("static restart refused: %s mismatch", key)
			}
		}
		for key, value := range r {
			if strings.HasPrefix(key, "refund_") && key != "refund_key" && key != "refund_state" && value != nil {
				return false, errors.New("static restart refused: refund metadata present")
			}
		}
		disposition := r["booking_disposition"]
		if disposition == nil {
			disposition = ""
		}
		state := r["reconciliation_state"]
		if !((state == "pending" && (disposition == "" || disposition == "CONFIRMED")) || (state == "sale_verified" && disposition == "CONFIRMED")) {
			return false, errors.New("static restart refused: reconciliation outside untouched static lifecycle")
		}
		if kind := r["operation_kind"]; kind != nil && kind != "reconcile" {
			return false, errors.New("static restart refused: unfinished financial operation")
		}
		frozen, _ := json.Marshal(domain.FrozenCreateParameters{Version: 1, Amount: f.AmountCents, Currency: f.Currency, AutomaticPaymentMethods: domain.AutomaticPaymentMethods{Enabled: true, AllowRedirects: "always"}})
		var expectedFrozen any
		_ = json.Unmarshal(frozen, &expectedFrozen)
		identity := domain.FinancialIdentity{BookingID: uuid.MustParse(f.BookingID), UserID: uuid.MustParse(f.UserID), Generation: f.Generation, ProviderAccount: f.ProviderAccount, AmountCents: f.AmountCents, Currency: f.Currency}
		hash, err := domain.FinancialFingerprint(identity, frozen)
		if err != nil || !reflect.DeepEqual(r["frozen_create"], expectedFrozen) || r["request_hash"] != "\\x"+hex.EncodeToString(hash[:]) {
			return false, errors.New("static restart refused: frozen identity mismatch")
		}
	}
	return true, nil
}
