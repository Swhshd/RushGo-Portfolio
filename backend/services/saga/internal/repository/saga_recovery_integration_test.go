package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sagapkg "github.com/ticketbox/pkg/saga"
	"github.com/ticketbox/saga/internal/domain"
)

func sagaRecoveryFixture(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("SAGA_TEST_DATABASE_URL")
	u, err := url.Parse(dsn)
	if err != nil || u.Host != "127.0.0.1:25439" || u.Path != "/ticketbox_saga_test" {
		t.Fatal("BLOCKED: isolated Saga fixture required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	schema := "saga_recovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	isolated := u.String()
	if out, err := exec.Command("migrate", "-path", filepath.ToSlash(filepath.Join("..", "..", "migrations")), "-database", isolated, "up").CombinedOutput(); err != nil {
		t.Fatalf("actual migrations failed: %s", strings.ReplaceAll(string(out), isolated, "[fixture]"))
	}
	config, err := pgxpool.ParseConfig(isolated)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["application_name"] = schema
	p, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p, isolated
}

func TestSagaNullableGraphAndStableStepIDsIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	ctx := context.Background()
	id, booking, step := uuid.New(), uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO sagas(id,booking_id,name,status,current_step_index) VALUES($1,$2,'ORDER_SAGA',NULL,NULL)`, id, booking); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO saga_steps(id,saga_id,name) VALUES($1,$2,'RESERVED_SEAT_STEP')`, step, id); err != nil {
		t.Fatal(err)
	}
	for _, load := range []func(context.Context, uuid.UUID) (*domain.Saga, error){r.GetSagaById, func(c context.Context, _ uuid.UUID) (*domain.Saga, error) { return r.GetSagaByBookingId(c, booking) }} {
		v, err := load(ctx, id)
		if err != nil {
			t.Errorf("legal NULL graph unreadable: %v", err)
			continue
		}
		if v.ID != id || len(v.Steps) != 1 || v.Steps[0].ID != step {
			t.Fatal("graph identity changed")
		}
		raw, err := json.Marshal(v.Steps[0])
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		if fields["ExecutedAt"] != nil || fields["CompenstatedAt"] != nil {
			t.Fatal("absent step times fabricated", string(raw))
		}
		if v.RecoveryState != "attention" || v.ErrorCategory != "missing_step_fields" || v.NullableStatus != nil || v.NullableCurrentStepIndex != nil || v.Steps[0].NullableOrder != nil || v.Steps[0].NullablePause != nil {
			t.Fatal("missing fields silently defaulted")
		}
		if err := r.UpsertBatchSagaSteps(ctx, v.Steps); err != nil {
			t.Fatal("nullable upsert failed", err)
		}
		if err := r.UpdateSaga(ctx, v, &v.Steps[0].ID); err != nil {
			t.Fatal("nullable update failed", err)
		}
		var absent bool
		if err := p.QueryRow(ctx, `SELECT s.status IS NULL AND s.current_step_index IS NULL AND st.status IS NULL AND st."order" IS NULL AND st.should_pause_for_payment IS NULL AND st.executed_at IS NULL AND st.compensated_at IS NULL FROM sagas s JOIN saga_steps st ON st.saga_id=s.id WHERE s.id=$1`, id).Scan(&absent); err != nil || !absent {
			t.Fatal("nullable write invented history", err)
		}
	}
	if err := r.UpsertBatchSagaSteps(ctx, nil); err != nil {
		t.Errorf("empty step batch must be a no-op: %v", err)
	}
	t.Run("stable_concurrent_graph", func(t *testing.T) {
		target, ok := any(r).(RecoveryRepository)
		if !ok {
			t.Fatal("durable stable graph capability absent")
		}
		input := recoveryInput()
		var wg sync.WaitGroup
		results := make(chan *domain.Saga, 16)
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v, _, err := target.CreateOrGetSaga(ctx, input, recoverySteps())
				if err != nil {
					t.Error(err)
					return
				}
				results <- v
			}()
		}
		wg.Wait()
		close(results)
		var first *domain.Saga
		count := 0
		for v := range results {
			count++
			if first == nil {
				first = v
			}
			if v.ID != first.ID || len(v.Steps) != 5 {
				t.Fatal("logical retry allocated another graph")
			}
			for i, st := range v.Steps {
				if st.ID != first.Steps[i].ID || st.ExecutedAt != nil || st.CompenstatedAt != nil {
					t.Fatal("retry fabricated execution or step identity")
				}
			}
		}
		if count != 16 {
			t.Fatal("missing graph results")
		}
		fresh := NewSagaRepository(p)
		v, err := fresh.GetSagaById(ctx, first.ID)
		if err != nil || v.Input == nil || v.Input.Fingerprint != input.Fingerprint {
			t.Fatal("restart input lost", err)
		}
		mismatch := input
		mismatch.AmountCents++
		mismatch, _, _ = domain.CanonicalInput(mismatch)
		if _, _, err := target.CreateOrGetSaga(ctx, mismatch, recoverySteps()); err == nil {
			t.Fatal("changed immutable input accepted")
		}
		if err := r.UpdateSaga(ctx, v, nil); err == nil {
			t.Fatal("legacy writer bypassed managed claim")
		}
		legacyID := uuid.New()
		if err := r.Create(ctx, &domain.Saga{ID: legacyID, BookingID: input.BookingID, Name: "ORDER_SAGA", Status: domain.SAGA_PENDING, Steps: recoveryStepsFor(legacyID)}); err == nil {
			t.Fatal("legacy create bypassed managed identity")
		}
		if err := r.UpsertBatchSagaSteps(ctx, recoveryStepsFor(first.ID)); err == nil {
			t.Fatal("random rebuild replaced managed steps")
		}
	})
	t.Run("classified_legacy_materialized_once", func(t *testing.T) {
		target, ok := any(r).(RecoveryRepository)
		if !ok {
			t.Fatal("durable stable graph capability absent")
		}
		in := recoveryInput()
		id := uuid.New()
		steps := recoveryStepsFor(id)
		now := time.Now().UTC()
		legacy := &domain.Saga{ID: id, BookingID: in.BookingID, Name: "ORDER_SAGA", Status: domain.SAGA_WAIT_FOR_PAYMENT, CurrentStepIndex: 1, Steps: steps[:2]}
		for i := range legacy.Steps {
			legacy.Steps[i].Status = domain.SAGA_STEP_COMPLETED
			legacy.Steps[i].ExecutedAt = &now
		}
		if err := r.Create(ctx, legacy); err != nil {
			t.Fatal(err)
		}
		// Proven original fixture snapshot is independent of the adoption request.
		_, original, err := domain.CanonicalInput(in)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := target.CreateOrGetSaga(ctx, in, recoverySteps()); err == nil {
			t.Fatal("unproven historical input adopted")
		}
		if _, err := p.Exec(ctx, `UPDATE sagas SET input_snapshot=$2,input_fingerprint=$3 WHERE id=$1`, id, original, in.Fingerprint[:]); err != nil {
			t.Fatal(err)
		}
		v, created, err := target.CreateOrGetSaga(ctx, in, recoverySteps())
		if err != nil || created || v.ID != id || len(v.Steps) != 5 || v.CurrentStepIndex != 2 || v.Steps[0].ID != steps[0].ID || v.Steps[1].ID != steps[1].ID {
			t.Fatal("legacy identity/evidence lost", err)
		}
		again, _, err := target.CreateOrGetSaga(ctx, in, recoverySteps())
		if err != nil {
			t.Fatal(err)
		}
		for i := range v.Steps {
			if again.Steps[i].ID != v.Steps[i].ID {
				t.Fatal("legacy tail rematerialized")
			}
		}
		// Null status/index/order/pause remain explicit non-executable history.
		_, _, err = target.CreateOrGetSaga(ctx, domain.SagaInput{BookingID: booking, UserID: in.UserID, EventID: in.EventID, Generation: 1, SeatIDs: in.SeatIDs, AmountCents: 725, Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: in.ReservationDeadline}, recoverySteps())
		if err == nil {
			t.Fatal("unknown legacy graph adopted")
		}
	})
}

func TestSagaRecoveryMigrationAndDuplicateHistoryIntegration(t *testing.T) {
	p, dsn := sagaRecoveryFixture(t)
	ctx := context.Background()
	run := func(args ...string) (string, error) {
		out, err := exec.Command("migrate", append([]string{"-path", "../../migrations", "-database", dsn}, args...)...).CombinedOutput()
		return strings.ReplaceAll(string(out), dsn, "[fixture]"), err
	}
	if out, err := run("goto", "202610120001"); err != nil {
		t.Fatal("select recovery migration under test", out)
	}
	if out, err := run("version"); err != nil || !strings.Contains(out, "202610120001") {
		t.Fatal("parsed recovery version wrong", out, err)
	}
	if out, err := run("down", "1"); err != nil {
		t.Fatal("empty down failed", out)
	}
	if out, err := run("version"); err != nil || !strings.Contains(out, "202610110001") {
		t.Fatal("parsed legacy version wrong", out)
	}
	if out, err := run("up"); err != nil {
		t.Fatal("reapply failed", out)
	}
	r := NewSagaRepository(p)
	in := recoveryInput()
	for i := 0; i < 2; i++ {
		if err := r.Create(ctx, &domain.Saga{ID: uuid.New(), BookingID: in.BookingID, Name: "ORDER_SAGA", Status: domain.SAGA_WAIT_FOR_PAYMENT, CurrentStepIndex: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := r.CreateOrGetSaga(ctx, in, recoverySteps()); err == nil {
		t.Fatal("duplicate legacy history adopted")
	}
	var count, managed, attention int
	if err := p.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE recovery_managed),count(*) FILTER(WHERE recovery_state='attention') FROM sagas WHERE booking_id=$1`, in.BookingID).Scan(&count, &managed, &attention); err != nil || count != 2 || managed != 0 || attention != 2 {
		t.Fatal("duplicate history deleted/classified", err)
	}
	if out, err := run("goto", "202610110001"); err == nil {
		t.Fatal("attention responsibility deleted by down", out)
	}
}

func TestSagaRealPreflightReportsUnknownWithoutMutationIntegration(t *testing.T) {
	p, dsn := sagaRecoveryFixture(t)
	ctx := context.Background()
	id, booking := uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO sagas(id,booking_id,name) VALUES($1,$2,'ORDER_SAGA')`, id, booking); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(dsn)
	schema := strings.Split(u.Query().Get("search_path"), ",")[0]
	manifest := map[string]any{"version": 1, "compose_project": os.Getenv("RELIABILITY_TEST_PROJECT"), "postgres_containers": map[string]any{"saga": map[string]any{"container": os.Getenv("RELIABILITY_TEST_PG_CONTAINER"), "database": "ticketbox_saga_test", "user": "reliability_fixture", "schema": schema}}}
	raw, _ := json.Marshal(manifest)
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("python", "../../../../scripts/reliability/preflight.py", "--manifest", path, "--read-only").CombinedOutput()
	if err == nil {
		t.Fatal("unknown Saga activation not blocked")
	}
	var report struct {
		Services map[string]struct {
			Counts   map[string]int
			Mutation bool
		}
	}
	if err := json.Unmarshal(out, &report); err != nil || report.Services["saga"].Counts["unproven_input_rows"] != 1 || report.Services["saga"].Mutation {
		t.Fatal("Saga preflight classification absent", string(out), err)
	}
	var retained bool
	if err := p.QueryRow(ctx, `SELECT status IS NULL AND current_step_index=0 AND input_snapshot IS NULL AND NOT recovery_managed AND recovery_state='legacy' FROM sagas WHERE id=$1`, id).Scan(&retained); err != nil || !retained {
		t.Fatal("preflight rewrote history", err)
	}
}

func TestSagaNullableLoadersRejectScanAndIterationErrors(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	ctx := context.Background()
	r := NewSagaRepository(p)
	id, booking := uuid.New(), uuid.New()
	if _, err := p.Exec(ctx, `INSERT INTO sagas(id,booking_id,name,status) VALUES($1,$2,'ORDER_SAGA','PENDING')`, id, booking); err != nil {
		t.Fatal(err)
	}
	// Place a view over test-only base rows to exercise an error after Query succeeds.
	if _, err := p.Exec(ctx, `ALTER TABLE saga_steps RENAME TO saga_steps_base; CREATE VIEW saga_steps AS SELECT id,saga_id,name,executed_at,compensated_at,status,"order",should_pause_for_payment,execute_state,compensate_state,execute_evidence,compensate_evidence FROM saga_steps_base WHERE 1/(CASE WHEN "order"=1 THEN 0 ELSE 1 END)>0`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := p.Exec(ctx, `INSERT INTO saga_steps_base(id,saga_id,name,status,"order",should_pause_for_payment) VALUES($1,$2,'fixture','PENDING',$3,FALSE)`, uuid.New(), id, i); err != nil {
			t.Fatal(err)
		}
	}
	if v, err := r.GetSagaById(ctx, id); err == nil || v != nil {
		t.Fatal("row iteration error discarded")
	}
	if _, err := p.Exec(ctx, `DROP VIEW saga_steps; CREATE VIEW saga_steps AS SELECT id::text||'bad' AS id,saga_id,name,executed_at,compensated_at,status,"order",should_pause_for_payment,execute_state,compensate_state,execute_evidence,compensate_evidence FROM saga_steps_base`); err != nil {
		t.Fatal(err)
	}
	if v, err := r.GetSagaByBookingId(ctx, booking); err == nil || v != nil {
		t.Fatal("row scan error discarded")
	}
}

func TestSagaClaimsRejectContradictoryGraphIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	ctx := context.Background()
	in := recoveryInput()
	v, _, err := r.CreateOrGetSaga(ctx, in, recoverySteps())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE sagas SET current_step_index=4 WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	claims, err := r.ClaimDueSagas(ctx, 1, time.Minute)
	if err != nil || len(claims) != 0 {
		t.Fatal("claim skipped required unfinished steps", err)
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil || got.RecoveryState != "attention" {
		t.Fatal("invalid graph responsibility not diagnosable", err)
	}
}

func TestSagaEarlyWakeSurvivesPaymentStepResultIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	ctx := context.Background()
	in := recoveryInput()
	v, _, err := r.CreateOrGetSaga(ctx, in, recoverySteps())
	if err != nil {
		t.Fatal(err)
	}
	claims, err := r.ClaimDueSagas(ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatal(err)
	}
	c := claims[0]
	for i := 0; i < 2; i++ {
		st := v.Steps[i]
		if ok, err := r.PersistStepIntent(ctx, c, st.ID, "forward"); err != nil || !ok {
			t.Fatal(err)
		}
		if i == 1 {
			payment := uuid.New()
			if err := r.WakeSaga(ctx, in.BookingID, 1, &payment, "payment_observed"); err != nil {
				t.Fatal(err)
			}
		}
		if ok, err := r.PersistStepResult(ctx, c, domain.StepResult{StepID: st.ID, Direction: "forward", State: "completed"}); err != nil || !ok {
			t.Fatal(err)
		}
	}
	resumed, err := r.ClaimDueSagas(ctx, 1, time.Minute)
	if err != nil || len(resumed) != 1 || resumed[0].Saga.CurrentStepIndex != 2 {
		t.Fatal("S1 result stranded earlier durable wake", err)
	}
}

func TestSagaExpiredLeaseAfterRowLockWaitRejectsResultIntegration(t *testing.T) {
	p, dsn := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	ctx := context.Background()
	v, _, err := r.CreateOrGetSaga(ctx, recoveryInput(), recoverySteps())
	if err != nil {
		t.Fatal(err)
	}
	claims, err := r.ClaimDueSagas(ctx, 1, time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatal(err)
	}
	c := claims[0]
	if ok, err := r.PersistStepIntent(ctx, c, v.Steps[0].ID, "forward"); err != nil || !ok {
		t.Fatal(err)
	}
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `SELECT id FROM sagas WHERE id=$1 FOR UPDATE`, v.ID); err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct {
		ok  bool
		err error
	}, 1)
	go func() {
		ok, err := r.PersistStepResult(ctx, c, domain.StepResult{StepID: v.Steps[0].ID, Direction: "forward", State: "completed"})
		finished <- struct {
			ok  bool
			err error
		}{ok, err}
	}()
	u, _ := url.Parse(dsn)
	app := strings.Split(u.Query().Get("search_path"), ",")[0]
	deadline := time.Now().Add(time.Second)
	for {
		var waiting bool
		if err := p.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')`, app).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("result did not reach lock barrier")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(time.Until(c.LeaseUntil.Add(50 * time.Millisecond)))
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-finished
	if result.err != nil || result.ok {
		t.Fatal("expired row-lock waiter accepted result", result.err)
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil || got.Steps[0].ExecutedAt != nil || got.CurrentStepIndex != 0 {
		t.Fatal("expired result changed progress", err)
	}
}

func TestSagaManagedNullIdentityAndDueRejectedIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	ctx := context.Background()
	v, _, err := r.CreateOrGetSaga(ctx, recoveryInput(), recoverySteps())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `UPDATE sagas SET lifecycle_generation=NULL WHERE id=$1`, v.ID); err == nil {
		t.Error("managed NULL generation bypassed uniqueness")
	}
	if _, err := p.Exec(ctx, `UPDATE sagas SET next_attempt_at=NULL WHERE id=$1`, v.ID); err == nil {
		t.Error("managed NULL due hid recovery")
	}
}

func TestSagaClaimsRejectRegressedAndFabricatedCompletionIntegration(t *testing.T) {
	for _, kind := range []string{"regressed_index", "fabricated_completion", "unfinished_compensation_tail", "regressed_compensated_index", "fabricated_compensated_status"} {
		t.Run(kind, func(t *testing.T) {
			p, _ := sagaRecoveryFixture(t)
			r := NewSagaRepository(p)
			ctx := context.Background()
			v, _, err := r.CreateOrGetSaga(ctx, recoveryInput(), recoverySteps())
			if err != nil {
				t.Fatal(err)
			}
			if kind == "regressed_index" {
				claims, _ := r.ClaimDueSagas(ctx, 1, time.Minute)
				c := claims[0]
				if ok, err := r.PersistStepIntent(ctx, c, v.Steps[0].ID, "forward"); err != nil || !ok {
					t.Fatal(err)
				}
				if ok, err := r.PersistStepResult(ctx, c, domain.StepResult{StepID: v.Steps[0].ID, Direction: "forward", State: "completed"}); err != nil || !ok {
					t.Fatal(err)
				}
				_, err = p.Exec(ctx, `UPDATE sagas SET current_step_index=0,claim_token=NULL,lease_until=NULL,next_attempt_at=NOW() WHERE id=$1`, v.ID)
			} else if kind == "fabricated_completion" {
				_, err = p.Exec(ctx, `UPDATE saga_steps SET status='COMPLETED' WHERE id=$1`, v.Steps[0].ID)
			} else {
				_, err = p.Exec(ctx, `UPDATE sagas SET direction='compensate',status='ROLLING_BACK',current_step_index=0 WHERE id=$1;`, v.ID)
				if err == nil {
					_, err = p.Exec(ctx, `UPDATE saga_steps SET execute_state='completed',executed_at=NOW(),status='COMPLETED' WHERE saga_id=$1 AND "order"<2`, v.ID)
				}
				if err == nil && kind != "unfinished_compensation_tail" {
					_, err = p.Exec(ctx, `UPDATE sagas SET current_step_index=1 WHERE id=$1`, v.ID)
					if err == nil {
						if kind == "regressed_compensated_index" {
							_, err = p.Exec(ctx, `UPDATE saga_steps SET compensate_state='completed',compensated_at=NOW(),status='COMPENSATED' WHERE id=$1`, v.Steps[1].ID)
						} else {
							_, err = p.Exec(ctx, `UPDATE saga_steps SET status='COMPENSATED' WHERE id=$1`, v.Steps[1].ID)
						}
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			claims, err := r.ClaimDueSagas(ctx, 1, time.Minute)
			if err != nil || len(claims) != 0 {
				t.Fatal("contradictory graph still claimable", err)
			}
			got, err := r.GetSagaById(ctx, v.ID)
			if err != nil || got.RecoveryState != "attention" {
				t.Fatal("contradiction not retained as attention", err)
			}
		})
	}
}

func recoveryInput() domain.SagaInput {
	bookingID := uuid.New()
	if supplied := strings.TrimSpace(os.Getenv("SAGA_COMPONENT_BOOKING_ID")); supplied != "" {
		parsed, err := uuid.Parse(supplied)
		if err != nil || parsed == uuid.Nil {
			panic("SAGA_COMPONENT_BOOKING_ID must be a non-nil UUID")
		}
		bookingID = parsed
	}
	in, _, err := domain.CanonicalInput(domain.SagaInput{BookingID: bookingID, UserID: uuid.New(), EventID: uuid.New(), Generation: 1, SeatIDs: []uuid.UUID{uuid.New(), uuid.New()}, AmountCents: 725, Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: time.Now().Add(time.Minute)})
	if err != nil {
		panic(err)
	}
	return in
}
func recoveryInputForCase(caseName string) domain.SagaInput {
	key := "SAGA_COMPONENT_BOOKING_ID_" + strings.ToUpper(caseName)
	if supplied := strings.TrimSpace(os.Getenv(key)); supplied != "" {
		parsed, err := uuid.Parse(supplied)
		if err != nil || parsed == uuid.Nil {
			panic(key + " must be a non-nil UUID")
		}
		in, _, err := domain.CanonicalInput(domain.SagaInput{BookingID: parsed, UserID: uuid.New(), EventID: uuid.New(), Generation: 1, SeatIDs: []uuid.UUID{uuid.New(), uuid.New()}, AmountCents: 725, Currency: "usd", BookingMode: "pessimistic", ReservationDeadline: time.Now().Add(time.Minute)})
		if err != nil {
			panic(err)
		}
		return in
	}
	return recoveryInput()
}

func recoverySteps() []domain.SagaStep { return recoveryStepsFor(uuid.Nil) }
func recoveryStepsFor(saga uuid.UUID) []domain.SagaStep {
	names := []sagapkg.SagaStepName{sagapkg.RESERVED_SEAT_STEP, sagapkg.CREATE_PAYMENT_INTENT_STEP, sagapkg.UPDATE_SEAT_BOOKED, sagapkg.UPDATE_BOOKING_STATUS_CONFIRMED, sagapkg.UPDATE_AVAILABLE_SEAT_NUMBER}
	var out []domain.SagaStep
	for i, name := range names {
		out = append(out, domain.SagaStep{ID: uuid.New(), SagaID: saga, Name: string(name), Status: domain.SAGA_STEP_PENDING, Order: int16(i), ShouldPauseForPayment: i == 1})
	}
	return out
}

func TestSagaPersistedClaimIntentsAndResultsIntegration(t *testing.T) {
	p, _ := sagaRecoveryFixture(t)
	r := NewSagaRepository(p)
	target, ok := any(r).(RecoveryRepository)
	if !ok {
		t.Fatal("durable claim capability absent")
	}
	ctx := context.Background()
	in := recoveryInput()
	v, _, err := target.CreateOrGetSaga(ctx, in, recoverySteps())
	if err != nil {
		t.Fatal(err)
	}
	claims, err := target.ClaimDueSagas(ctx, 4, 30*time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatal("claim missing", err)
	}
	c := claims[0]
	if claims, err := target.ClaimDueSagas(ctx, 4, time.Second); err != nil || len(claims) != 0 {
		t.Fatal("two owners hold live claim", err)
	}
	before := sha256.Sum256([]byte(v.Steps[0].ID.String()))
	if applied, err := target.PersistStepResult(ctx, c, domain.StepResult{StepID: v.Steps[0].ID, Direction: "forward", State: "completed"}); err == nil || applied {
		t.Fatal("result without durable intent accepted")
	}
	for i := 0; i < 2; i++ {
		step := v.Steps[i]
		if applied, err := target.PersistStepIntent(ctx, c, step.ID, "forward"); err != nil || !applied {
			t.Fatal("intent missing", err)
		}
		if applied, err := target.PersistStepResult(ctx, c, domain.StepResult{StepID: step.ID, Direction: "forward", State: "completed", Evidence: json.RawMessage(`{"verified":true}`)}); err != nil || !applied {
			t.Fatal("result missing", err)
		}
	}
	got, err := r.GetSagaById(ctx, v.ID)
	if err != nil || got.Status != domain.SAGA_WAIT_FOR_PAYMENT || got.CurrentStepIndex != 2 || got.Steps[0].ExecutedAt == nil || got.Steps[2].ExecutedAt != nil || sha256.Sum256([]byte(got.Steps[0].ID.String())) != before {
		t.Fatal("WAIT must retain original completed S1 and next index2", err)
	}
	payment := uuid.New()
	if err := target.WakeSaga(ctx, in.BookingID, 1, &payment, "payment_observed"); err != nil {
		t.Fatal(err)
	}
	next, err := target.ClaimDueSagas(ctx, 1, time.Minute)
	if err != nil || len(next) != 1 {
		t.Fatal("wakeup not durable", err)
	}
	if applied, err := target.PersistStepIntent(ctx, c, v.Steps[2].ID, "forward"); err != nil || applied {
		t.Fatal("stale owner wrote intent", err)
	}
	fresh := next[0]
	if fresh.Fence <= c.Fence || fresh.Token == c.Token {
		t.Fatal("claim not fenced")
	}
	if applied, err := target.PersistStepIntent(ctx, fresh, v.Steps[4].ID, "forward"); err == nil || applied {
		t.Fatal("out of order execution accepted")
	}
}
