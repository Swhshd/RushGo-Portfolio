//go:build admission_r4

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/payment/internal/domain"
	"github.com/ticketbox/payment/internal/repository"
	"sort"
	"strings"
	"time"
)

type providerPersistence struct {
	pool        *pgxpool.Pool
	run, source string
	hash        [32]byte
	revision    int64
}
type providerSnapshot struct {
	Version  int                        `json:"version"`
	Fixtures []persistedProviderFixture `json:"fixtures"`
}
type persistedProviderFixture struct {
	PaymentID, BookingID, UserID                                      uuid.UUID
	Generation                                                        int64
	IntentID, Account, Currency, RefundID                             string
	AmountCents                                                       int64
	RefundCalls, RefundEffects                                        int64
	Refunded, Dynamic, Created, Confirmed, ReceiptAccepted, Cancelled bool
	CreateOperation                                                   domain.CreateOperation
	FrozenReceiptJSON                                                 []byte
}

func providerConfigHash(cfg bootstrapConfig) [32]byte {
	fixtures := append([]fixtureConfig(nil), cfg.Fixtures...)
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].PaymentID < fixtures[j].PaymentID })
	body, _ := json.Marshal(struct {
		Run, Source string
		Fixtures    []fixtureConfig
		Dynamic     *dynamicPurchaseConfig
	}{cfg.RunID, cfg.SourceCommit, fixtures, cfg.DynamicPurchase})
	return sha256.Sum256(body)
}
func (p *localProvider) snapshotLocked() ([]byte, error) {
	snapshot := providerSnapshot{Version: 1, Fixtures: []persistedProviderFixture{}}
	for _, f := range p.byPayment {
		var receipt []byte
		if f.frozenReceipt != nil {
			var err error
			receipt, err = json.Marshal(f.frozenReceipt)
			if err != nil {
				return nil, err
			}
		}
		snapshot.Fixtures = append(snapshot.Fixtures, persistedProviderFixture{PaymentID: f.paymentID, BookingID: f.bookingID, UserID: f.userID, Generation: f.generation, IntentID: f.intentID, Account: f.account, Currency: f.currency, RefundID: f.refundID, AmountCents: f.amountCents, RefundCalls: f.refundCalls, RefundEffects: f.refundEffects, Refunded: f.refunded, Dynamic: f.dynamic, Created: f.created, Confirmed: f.confirmed, ReceiptAccepted: f.receiptAccepted, Cancelled: f.cancelled, CreateOperation: f.createOperation, FrozenReceiptJSON: receipt})
	}
	sort.Slice(snapshot.Fixtures, func(i, j int) bool {
		return snapshot.Fixtures[i].PaymentID.String() < snapshot.Fixtures[j].PaymentID.String()
	})
	return json.Marshal(snapshot)
}

// A lost commit acknowledgement poisons this process. No read may invent a
// successful or absent provider effect; restart reloads the durable authority.
func (p *localProvider) persistLocked() error {
	if p.persistError != nil {
		return p.persistError
	}
	if p.persistence == nil {
		return nil
	}
	body, err := p.snapshotLocked()
	if err != nil {
		p.persistError = err
		return err
	}
	d := p.persistence
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var next int64
	err = d.pool.QueryRow(ctx, `UPDATE simulated_provider_state SET state=$1,revision=revision+1,updated_at=clock_timestamp() WHERE run_id=$2 AND source_commit=$3 AND config_hash=$4 AND revision=$5 RETURNING revision`, body, d.run, d.source, d.hash[:], d.revision).Scan(&next)
	if err != nil {
		p.persistError = errors.New("simulated provider persistence uncertain; restart required")
		return p.persistError
	}
	d.revision = next
	return nil
}

func (p *localProvider) checkPersistenceLocked() error {
	if p.persistError != nil {
		return p.persistError
	}
	if p.persistence == nil {
		return nil
	}
	d := p.persistence
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var revision int64
	err := d.pool.QueryRow(ctx, `SELECT revision FROM simulated_provider_state WHERE run_id=$1 AND source_commit=$2 AND config_hash=$3`, d.run, d.source, d.hash[:]).Scan(&revision)
	if err != nil || revision != d.revision {
		p.persistError = errors.New("simulated provider state changed or unavailable; restart required")
	}
	return p.persistError
}

func loadDurableProvider(ctx context.Context, pool *pgxpool.Pool, cfg bootstrapConfig) (*localProvider, bool, error) {
	p, err := newLocalProvider(cfg)
	if err != nil {
		return nil, false, err
	}
	var run, source string
	var hash, body []byte
	var revision int64
	rows, err := pool.Query(ctx, `SELECT run_id::text,source_commit,config_hash,state,revision FROM simulated_provider_state LIMIT 2`)
	if err != nil {
		return nil, false, errors.New("simulated provider storage unavailable")
	}
	count := 0
	for rows.Next() {
		count++
		err = rows.Scan(&run, &source, &hash, &body, &revision)
		if err != nil {
			rows.Close()
			return nil, false, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, false, err
	}
	if count == 0 {
		return p, false, nil
	}
	wantHash := providerConfigHash(cfg)
	if count != 1 || run != cfg.RunID || source != cfg.SourceCommit || !bytes.Equal(hash, wantHash[:]) || revision < 1 {
		return nil, false, errors.New("simulated provider run or source identity mismatch")
	}
	var snapshot providerSnapshot
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&snapshot) != nil || snapshot.Version != 1 || len(snapshot.Fixtures) < len(cfg.Fixtures) || len(snapshot.Fixtures) > len(cfg.Fixtures)+1 {
		return nil, false, errors.New("simulated provider snapshot invalid")
	}
	seen := map[uuid.UUID]bool{}
	for _, v := range snapshot.Fixtures {
		if seen[v.PaymentID] || v.PaymentID == uuid.Nil {
			return nil, false, errors.New("duplicate simulated provider identity")
		}
		seen[v.PaymentID] = true
		f := p.byPayment[v.PaymentID]
		if v.Dynamic {
			if f != nil || p.dynamic != nil || !v.Created || !validDynamicOperation(v.CreateOperation, p.dynamicUser) || v.CreateOperation.PaymentID != v.PaymentID {
				return nil, false, errors.New("dynamic provider identity conflict")
			}
			i := v.CreateOperation.Identity
			f = &providerFixture{paymentID: v.PaymentID, bookingID: i.BookingID, userID: i.UserID, generation: i.Generation, account: i.ProviderAccount, amountCents: i.AmountCents, currency: i.Currency, intentID: "pi_r4_" + strings.ReplaceAll(v.PaymentID.String(), "-", ""), refundID: "re_r4_" + strings.ReplaceAll(v.PaymentID.String(), "-", ""), dynamic: true, createOperation: cloneCreateOperation(v.CreateOperation)}
			p.dynamic = f
			p.byPayment[f.paymentID] = f
			p.byIntent[f.intentID] = f
			p.byRefund[f.refundID] = f
		}
		if f == nil || v.BookingID != f.bookingID || v.UserID != f.userID || v.Generation != f.generation || v.IntentID != f.intentID || v.Account != f.account || v.Currency != f.currency || v.AmountCents != f.amountCents || v.RefundID != f.refundID || v.Dynamic != f.dynamic {
			return nil, false, errors.New("provider immutable identity mismatch")
		}
		if v.RefundEffects < 0 || v.RefundEffects > 1 || v.RefundCalls < v.RefundEffects || v.Refunded != (v.RefundEffects == 1) || v.ReceiptAccepted && !v.Confirmed || v.Cancelled && (!v.Dynamic || v.Confirmed) || v.Dynamic && v.Refunded && !v.ReceiptAccepted {
			return nil, false, errors.New("provider effect state conflict")
		}
		f.created, f.confirmed, f.receiptAccepted, f.cancelled = v.Created, v.Confirmed, v.ReceiptAccepted, v.Cancelled
		f.refunded, f.refundCalls, f.refundEffects = v.Refunded, v.RefundCalls, v.RefundEffects
		if len(v.FrozenReceiptJSON) > 0 {
			var receipt domain.ProviderReceipt
			if json.Unmarshal(v.FrozenReceiptJSON, &receipt) != nil {
				return nil, false, errors.New("provider receipt encoding invalid")
			}
			f.frozenReceipt = &receipt
		}
		if v.Dynamic && v.Confirmed && !validPersistedReceipt(f) {
			return nil, false, errors.New("provider confirmation receipt invalid")
		}
		if !v.Confirmed && len(v.FrozenReceiptJSON) > 0 {
			return nil, false, errors.New("unexpected provider receipt")
		}
	}
	if len(p.byPayment) != len(seen) {
		return nil, false, errors.New("provider fixtures missing")
	}
	p.persistence = &providerPersistence{pool: pool, run: run, source: source, hash: wantHash, revision: revision}
	if err = validateDurablePaymentInventory(ctx, pool, p); err != nil {
		return nil, false, err
	}
	return p, true, nil
}
func validPersistedReceipt(f *providerFixture) bool {
	if f.frozenReceipt == nil {
		return false
	}
	r := f.frozenReceipt
	o := r.Observation
	event := "evt_r4_" + strings.ReplaceAll(f.paymentID.String(), "-", "")
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticketbox:stripe:"+f.account+":test:"+event))
	return r.ID == id && r.EnvelopeID == id.String() && r.EventType == "payment_intent.succeeded" && r.CorrelationState == "pending" && r.RelayState == "pending" && r.ReceivedAt.Equal(o.ObservedAt) && !o.ObservedAt.IsZero() && o.PaymentID == f.paymentID && o.BookingID == f.bookingID && o.Generation == f.generation && o.Account == f.account && !o.LiveMode && o.IntentID == f.intentID && o.EventID == event && o.Status == "succeeded" && o.Collected && o.AmountCents == f.amountCents && o.Currency == f.currency && o.RefundID == "" && o.SucceededRefundCents == 0 && sha256.Sum256(o.SafeObject) == o.EvidenceHash
}
func validateDurablePaymentInventory(ctx context.Context, pool *pgxpool.Pool, p *localProvider) error {
	rows, err := pool.Query(ctx, `SELECT id,user_id,booking_id,managed,COALESCE(payment_generation,0),COALESCE(provider_account,''),COALESCE(livemode,true),price,currency,request_hash,frozen_create,COALESCE(payment_intent_id,''),COALESCE(provider_outcome,''),COALESCE(refund_state,'') FROM payments ORDER BY id LIMIT $1`, len(p.byPayment)+2)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := map[uuid.UUID]bool{}
	pending := 0
	for rows.Next() {
		var id, user, booking uuid.UUID
		var managed, live bool
		var generation, amount int64
		var account, currency, intent, outcome, refund string
		var hash, frozen []byte
		if err = rows.Scan(&id, &user, &booking, &managed, &generation, &account, &live, &amount, &currency, &hash, &frozen, &intent, &outcome, &refund); err != nil {
			return errors.New("provider payment inventory unreadable")
		}
		i := domain.FinancialIdentity{BookingID: booking, UserID: user, Generation: generation, ProviderAccount: account, LiveMode: live, AmountCents: amount, Currency: currency}
		fingerprint, e := domain.FinancialFingerprint(i, frozen)
		if e != nil || !managed || !bytes.Equal(hash, fingerprint[:]) {
			return errors.New("provider payment fingerprint mismatch")
		}
		i.Fingerprint = fingerprint
		f := p.byPayment[id]
		if f == nil {
			op := domain.CreateOperation{PaymentID: id, Identity: i, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen}
			if p.dynamic != nil || pending != 0 || !validDynamicOperation(op, p.dynamicUser) || intent != "" || outcome != "unknown" || refund != "not_required" {
				return errors.New("provider has foreign or lost dynamic payment state")
			}
			pending++
			continue
		}
		if seen[id] || i.BookingID != f.bookingID || i.UserID != f.userID || i.Generation != f.generation || i.ProviderAccount != f.account || i.LiveMode || i.AmountCents != f.amountCents || i.Currency != f.currency || (intent != "" && intent != f.intentID) {
			return errors.New("provider payment binding conflict")
		}
		seen[id] = true
		if f.dynamic && !sameDynamicOperation(f.createOperation, domain.CreateOperation{PaymentID: id, Identity: i, Key: "ticketbox:payment:" + id.String() + ":g1:create", FrozenParams: frozen}) {
			return errors.New("provider dynamic command changed")
		}
		if outcome == "collected" && f.dynamic && !f.confirmed || refund == "succeeded" && !f.refunded {
			return errors.New("provider persisted financial effects conflict")
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(seen) != len(p.byPayment) {
		return errors.New("provider payment inventory incomplete")
	}
	return nil
}

func initializeDurableProvider(ctx context.Context, pool *pgxpool.Pool, repo *repository.PostgresPaymentRepository, cfg bootstrapConfig) (*localProvider, error) {
	p, found, err := loadDurableProvider(ctx, pool, cfg)
	if err != nil {
		return nil, err
	}
	if !found {
		if cfg.DynamicPurchase != nil {
			if err = requireUnusedDynamicBuyer(ctx, pool, cfg.DynamicPurchase.UserID); err != nil {
				return nil, err
			}
		}
		// Only a genuinely fresh database or the existing untouched static
		// fixture verifier may establish the first synthetic authority snapshot.
		if err = initializeFinancialFixtures(ctx, pool, repo, cfg); err != nil {
			return nil, err
		}
		body, err := p.snapshotLocked()
		if err != nil {
			return nil, err
		}
		hash := providerConfigHash(cfg)
		_, err = pool.Exec(ctx, `INSERT INTO simulated_provider_state(run_id,source_commit,config_hash,revision,state) VALUES($1,$2,$3,1,$4)`, cfg.RunID, cfg.SourceCommit, hash[:], body)
		if err != nil {
			return nil, errors.New("provider bootstrap persistence uncertain; reload required")
		}
		p.persistence = &providerPersistence{pool: pool, run: cfg.RunID, source: cfg.SourceCommit, hash: hash, revision: 1}
	}
	if f := p.dynamic; f != nil && f.confirmed && !f.receiptAccepted {
		// Re-deliver the same durable provider receipt across a crash boundary;
		// this does not confirm or charge the simulated purchase a second time.
		if _, _, err = repo.AcceptProviderReceipt(ctx, cloneProviderReceipt(*f.frozenReceipt)); err != nil {
			return nil, err
		}
		f.receiptAccepted = true
		if err = p.persistLocked(); err != nil {
			return nil, err
		}
	}
	return p, nil
}
