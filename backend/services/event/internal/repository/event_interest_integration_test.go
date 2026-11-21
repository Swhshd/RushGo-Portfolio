package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/pkg/eventreminder"
)

func TestEventInterestFirstSaleFenceAndCategoryEmailRevocationIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	events := NewPostgresEventRepository(pool)
	interests := NewPostgresEventInterestRepository(pool)
	event := newAtomicEventFixture(1)
	if err := events.Create(ctx, event); err != nil {
		t.Fatalf("create event: %v", err)
	}
	start := time.Now().UTC().Add(30 * time.Minute)
	if _, err := pool.Exec(ctx, `UPDATE events SET sales_status='scheduled',sales_start_at=$2,sales_end_at=NULL,first_sale_opened_at=NULL WHERE id=$1`, event.ID, start); err != nil {
		t.Fatal(err)
	}
	if available, err := interests.CanCreateOnSaleInterest(ctx, event.ID); err != nil || !available {
		t.Fatalf("scheduled first-sale subscription availability=%v err=%v, want true", available, err)
	}

	owner, otherOwner := uuid.New(), uuid.New()
	for _, input := range []struct {
		owner uuid.UUID
		kind  string
	}{
		{owner: owner, kind: InterestOnSale},
		{owner: owner, kind: InterestRestock},
		{owner: otherOwner, kind: InterestOnSale},
	} {
		if _, err := interests.UpsertMyEventInterest(ctx, input.owner, event.ID, input.kind, true, eventreminder.ConsentVersion); err != nil {
			t.Fatalf("create %s subscription for owner %s: %v", input.kind, input.owner, err)
		}
	}

	updated, err := interests.DisableMyEventAlertEmails(ctx, owner, InterestOnSale)
	if err != nil || updated != 1 {
		t.Fatalf("disable on-sale emails: updated=%d err=%v, want 1", updated, err)
	}
	var optIn bool
	var consentID *uuid.UUID
	var revokedAt *time.Time
	var version int64
	if err := pool.QueryRow(ctx, `SELECT email_opt_in,email_consent_id,email_revoked_at,version FROM event_interests WHERE user_id=$1 AND event_id=$2 AND kind='onsale'`, owner, event.ID).
		Scan(&optIn, &consentID, &revokedAt, &version); err != nil {
		t.Fatal(err)
	}
	if optIn || consentID != nil || revokedAt == nil || version != 2 {
		t.Fatalf("revoked owner subscription state optIn=%v consent=%v revoked=%v version=%d", optIn, consentID, revokedAt, version)
	}
	if err := pool.QueryRow(ctx, `SELECT email_opt_in FROM event_interests WHERE user_id=$1 AND event_id=$2 AND kind='restock'`, owner, event.ID).Scan(&optIn); err != nil || !optIn {
		t.Fatalf("other category email consent changed: optIn=%v err=%v", optIn, err)
	}
	if err := pool.QueryRow(ctx, `SELECT email_opt_in FROM event_interests WHERE user_id=$1 AND event_id=$2 AND kind='onsale'`, otherOwner, event.ID).Scan(&optIn); err != nil || !optIn {
		t.Fatalf("other owner's email consent changed: optIn=%v err=%v", optIn, err)
	}

	var intents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2 AND action='consent_updated' AND subscription_version=2`, owner, event.ID).Scan(&intents); err != nil || intents != 1 {
		t.Fatalf("category consent outbox intents=%d err=%v, want 1", intents, err)
	}
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2 AND action='consent_updated'`, owner, event.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	envelope, err := eventreminder.Parse(raw)
	if err != nil || envelope.EmailOptIn || envelope.EmailConsentID != nil || envelope.SubscriptionVersion != 2 {
		t.Fatalf("category consent envelope=%+v err=%v", envelope, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE events SET sales_status='selling',sales_start_at=clock_timestamp()-interval '1 minute',first_sale_opened_at=clock_timestamp() WHERE id=$1`, event.ID); err != nil {
		t.Fatal(err)
	}
	if available, err := interests.CanCreateOnSaleInterest(ctx, event.ID); err != nil || available {
		t.Fatalf("already-open first-sale subscription availability=%v err=%v, want false", available, err)
	}
	if _, err := interests.UpsertMyEventInterest(ctx, uuid.New(), event.ID, InterestOnSale, false, ""); !errors.Is(err, ErrInterestAlreadyOnSale) {
		t.Fatalf("new first-sale subscription after sale opened error=%v, want ErrInterestAlreadyOnSale", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_interests WHERE event_id=$1 AND kind='onsale'`, event.ID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("post-sale subscription rows=%d err=%v, want unchanged two owners", count, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE events SET publication_status='draft',status='cancelled',date=clock_timestamp()-interval '1 day',
		sales_status='ended',sales_start_at=clock_timestamp()-interval '2 hours',sales_end_at=clock_timestamp()-interval '1 hour' WHERE id=$1`, event.ID); err != nil {
		t.Fatal(err)
	}
	var priorRestockStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM event_interests WHERE user_id=$1 AND event_id=$2 AND kind='restock'`, owner, event.ID).Scan(&priorRestockStatus); err != nil {
		t.Fatal(err)
	}
	revoked, err := interests.UpsertMyEventInterest(ctx, owner, event.ID, InterestRestock, false, "")
	if err != nil || revoked.EmailOptIn || revoked.Status != priorRestockStatus || revoked.Version != 2 || revoked.ActiveSinceVersion != 1 {
		t.Fatalf("ended/unpublished event email withdrawal=%+v err=%v", revoked, err)
	}
	var restockRevocations, restockTriggers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2
		AND notification_type='event_restock' AND action='consent_updated' AND subscription_version=2`, owner, event.ID).Scan(&restockRevocations); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2
		AND notification_type='event_restock' AND action='trigger'`, owner, event.ID).Scan(&restockTriggers); err != nil {
		t.Fatal(err)
	}
	if restockRevocations != 1 || restockTriggers != 0 {
		t.Fatalf("ended event consent outbox revocations=%d triggers=%d", restockRevocations, restockTriggers)
	}
	if _, err := interests.UpsertMyEventInterest(ctx, uuid.New(), event.ID, InterestRestock, false, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("new restock interest for unpublished event err=%v, want ErrNotFound", err)
	}
}

func TestCategoryEmailRevocationPagesAndHonorsConsentCutoffIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	owner := uuid.New()
	const consentedCount = 205
	if _, err := pool.Exec(ctx, `WITH seeded_events AS (
			INSERT INTO events(id,title,description,category,venue,location,date,status,publication_status,sales_status,created_at,updated_at)
			SELECT uuid_generate_v4(),'Category email fixture','fixture','music','hall','city',clock_timestamp()+interval '1 day',
				'active','published','selling',clock_timestamp(),clock_timestamp()
			FROM generate_series(1,$1)
			RETURNING id
		), numbered_events AS (
			SELECT id,row_number() OVER (ORDER BY id) AS position FROM seeded_events
		)
		INSERT INTO event_interests(
			id,user_id,event_id,kind,status,email_opt_in,email_consent_version,email_consent_id,email_consented_at,version,active_since_version)
		SELECT uuid_generate_v4(),$2,id,'restock','active',TRUE,$3,uuid_generate_v4(),
			CASE WHEN position=$1 THEN clock_timestamp()+interval '1 hour' ELSE clock_timestamp() END,1,1
		FROM numbered_events`, consentedCount+1, owner, eventreminder.ConsentVersion); err != nil {
		t.Fatalf("seed multi-page category consents: %v", err)
	}

	updated, err := NewPostgresEventInterestRepository(pool).DisableMyEventAlertEmails(ctx, owner, InterestRestock)
	if err != nil || updated != consentedCount {
		t.Fatalf("multi-page category revocation updated=%d err=%v, want %d", updated, err, consentedCount)
	}
	var remaining, outboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_interests WHERE user_id=$1 AND kind='restock' AND email_opt_in`, owner).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("multi-page remaining opted-in subscriptions=%d err=%v, want only the post-cutoff consent", remaining, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE user_id=$1 AND action='consent_updated'`, owner).Scan(&outboxCount); err != nil || outboxCount != consentedCount {
		t.Fatalf("multi-page category intents=%d err=%v, want %d", outboxCount, err, consentedCount)
	}
}

func TestEventInterestConsentIntentPayloadIsSafeJSON(t *testing.T) {
	message := eventreminder.Envelope{
		Version: 1, NotificationID: uuid.New(), SubscriptionID: uuid.New(), SubscriptionVersion: 2,
		Action: eventreminder.ActionConsentUpdated, Type: eventreminder.TypeOnSale,
		UserID: uuid.New(), EventID: uuid.New(), EventTitle: "Synthetic event", EventDate: time.Now().UTC(),
	}
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := eventreminder.Parse(data)
	if err != nil || parsed.EmailOptIn || parsed.EmailConsentID != nil {
		t.Fatalf("opt-out envelope parse=%+v err=%v", parsed, err)
	}
}

func TestEventReminderTransitionsAreDurableAndInventoryAuthoritativeIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	events := NewPostgresEventRepository(pool)
	interests := NewPostgresEventInterestRepository(pool)

	t.Run("scheduled_first_sale", func(t *testing.T) {
		event := newAtomicEventFixture(1)
		if err := events.Create(ctx, event); err != nil {
			t.Fatalf("create event: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE events SET sales_status='scheduled',sales_start_at=clock_timestamp()+interval '30 minutes',sales_end_at=NULL,
			first_sale_opened_at=NULL,sales_generation=1,sale_notification_generation=0 WHERE id=$1`, event.ID); err != nil {
			t.Fatal(err)
		}
		owner := uuid.New()
		if _, err := interests.UpsertMyEventInterest(ctx, owner, event.ID, InterestOnSale, true, eventreminder.ConsentVersion); err != nil {
			t.Fatalf("subscribe before scheduled first sale: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE events SET sales_start_at=clock_timestamp()-interval '1 second' WHERE id=$1`, event.ID); err != nil {
			t.Fatal(err)
		}
		collected, err := interests.CollectDueSalesTransitions(ctx, 100)
		if err != nil || collected != 1 {
			t.Fatalf("collect due first-sale transition: count=%d err=%v", collected, err)
		}
		collected, err = interests.CollectDueSalesTransitions(ctx, 100)
		if err != nil || collected != 0 {
			t.Fatalf("duplicate first-sale transition collection: count=%d err=%v", collected, err)
		}
		processReminderFanouts(t, ctx, interests, 2)
		assertDurableReminderTrigger(t, ctx, pool, owner, event.ID, eventreminder.TypeOnSale)
	})

	t.Run("resubscribe_activation_floor", func(t *testing.T) {
		event := newAtomicEventFixture(1)
		if err := events.Create(ctx, event); err != nil {
			t.Fatalf("create event: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE events SET sales_status='scheduled',sales_start_at=clock_timestamp()+interval '30 minutes',sales_end_at=NULL,
			first_sale_opened_at=NULL,sales_generation=1,sale_notification_generation=0 WHERE id=$1`, event.ID); err != nil {
			t.Fatal(err)
		}
		owner := uuid.New()
		initial, err := interests.UpsertMyEventInterest(ctx, owner, event.ID, InterestOnSale, false, "")
		if err != nil || initial.Version != 1 || initial.ActiveSinceVersion != 1 {
			t.Fatalf("initial subscription=%+v err=%v", initial, err)
		}
		withdrawn, err := interests.WithdrawMyEventInterest(ctx, owner, event.ID, InterestOnSale)
		if err != nil || withdrawn.Version != 2 || withdrawn.ActiveSinceVersion != 1 {
			t.Fatalf("withdrawn subscription=%+v err=%v", withdrawn, err)
		}
		resubscribed, err := interests.UpsertMyEventInterest(ctx, owner, event.ID, InterestOnSale, false, "")
		if err != nil || resubscribed.Version != 3 || resubscribed.ActiveSinceVersion != 3 {
			t.Fatalf("resubscribed subscription=%+v err=%v", resubscribed, err)
		}
		if _, err := pool.Exec(ctx, `UPDATE events SET sales_start_at=clock_timestamp()-interval '1 second' WHERE id=$1`, event.ID); err != nil {
			t.Fatal(err)
		}
		if collected, err := interests.CollectDueSalesTransitions(ctx, 100); err != nil || collected != 1 {
			t.Fatalf("collect first sale: count=%d err=%v", collected, err)
		}
		processReminderFanouts(t, ctx, interests, 2)
		var raw []byte
		if err := pool.QueryRow(ctx, `SELECT payload FROM event_reminder_outbox WHERE event_id=$1 AND action='trigger'`, event.ID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		trigger, err := eventreminder.Parse(raw)
		if err != nil || trigger.SubscriptionVersion != 4 || trigger.ActiveSinceVersion != 3 {
			t.Fatalf("post-resubscribe trigger=%+v err=%v, want subscription version 4 with activation floor 3", trigger, err)
		}
	})

	t.Run("reliable_zero_to_positive_restock", func(t *testing.T) {
		event := newAtomicEventFixture(1)
		if err := events.Create(ctx, event); err != nil {
			t.Fatalf("create event: %v", err)
		}
		owner := uuid.New()
		if _, err := interests.UpsertMyEventInterest(ctx, owner, event.ID, InterestRestock, true, eventreminder.ConsentVersion); err != nil {
			t.Fatalf("subscribe for restock: %v", err)
		}
		request := lifecycleRequest(event.ID, uuid.New(), []uuid.UUID{event.Seats[0].ID}, time.Now().UTC().Add(10*time.Minute))
		request.TierQuantities = map[uuid.UUID]int32{event.Tiers[0].ID: 1}
		seats := NewPostgresSeatRepository(pool)
		for _, action := range []string{"reserve", "book", "account", "close"} {
			request.Action = action
			request.OperationID = uuid.New()
			if _, err := seats.ApplyLifecycle(ctx, request); err != nil {
				t.Fatalf("apply %s inventory transition: %v", action, err)
			}
		}
		processReminderFanouts(t, ctx, interests, 2)
		assertDurableReminderTrigger(t, ctx, pool, owner, event.ID, eventreminder.TypeRestock)
	})
}

func TestReminderFanoutKeepsSeatCloseConstantAndCompletesInBoundedPagesIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	events := NewPostgresEventRepository(pool)
	interests := NewPostgresEventInterestRepository(pool)
	event := newAtomicEventFixture(1)
	if err := events.Create(ctx, event); err != nil {
		t.Fatalf("create event inventory: %v", err)
	}
	const subscriberCount = 250
	if _, err := pool.Exec(ctx, `INSERT INTO event_interests(id,user_id,event_id,kind,status,version,active_since_version,active_since_at)
		SELECT uuid_generate_v4(),uuid_generate_v4(),$1,'restock','active',1,1,clock_timestamp()
		FROM generate_series(1,$2)`, event.ID, subscriberCount); err != nil {
		t.Fatalf("seed synthetic restock subscribers: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE fanout_interest_update_counter(n BIGINT NOT NULL);
		INSERT INTO fanout_interest_update_counter(n) VALUES(0);
		CREATE FUNCTION count_fanout_interest_update() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN UPDATE fanout_interest_update_counter SET n=n+1; RETURN NEW; END $$;
		CREATE TRIGGER count_fanout_interest_updates AFTER UPDATE ON event_interests
		FOR EACH ROW EXECUTE FUNCTION count_fanout_interest_update()`); err != nil {
		t.Fatalf("install subscriber-write counter: %v", err)
	}

	request := lifecycleRequest(event.ID, uuid.New(), []uuid.UUID{event.Seats[0].ID}, time.Now().UTC().Add(10*time.Minute))
	request.TierQuantities = map[uuid.UUID]int32{event.Tiers[0].ID: 1}
	seats := NewPostgresSeatRepository(pool)
	for _, action := range []string{"reserve", "book", "account", "close"} {
		request.Action = action
		request.OperationID = uuid.New()
		if _, err := seats.ApplyLifecycle(ctx, request); err != nil {
			t.Fatalf("apply %s inventory transition: %v", action, err)
		}
	}
	var writes, fanouts, intents, unchanged int
	if err := pool.QueryRow(ctx, `SELECT n FROM fanout_interest_update_counter`).Scan(&writes); err != nil || writes != 0 {
		t.Fatalf("seat close wrote subscriber rows=%d err=%v, want 0 writes before background fanout", writes, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_fanouts WHERE event_id=$1 AND kind='restock'`, event.ID).Scan(&fanouts); err != nil || fanouts != 1 {
		t.Fatalf("seat close Event trigger rows=%d err=%v, want one", fanouts, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE event_id=$1 AND action='trigger'`, event.ID).Scan(&intents); err != nil || intents != 0 {
		t.Fatalf("seat close Notification intents=%d err=%v, want none before paged worker", intents, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_interests WHERE event_id=$1 AND kind='restock' AND status='active' AND version=1`, event.ID).Scan(&unchanged); err != nil || unchanged != subscriberCount {
		t.Fatalf("seat close changed active subscriptions=%d err=%v, want %d unchanged", unchanged, err, subscriberCount)
	}

	processed := 0
	for page := 0; page < 5; page++ {
		count, err := interests.ProcessPendingReminderFanoutPage(ctx, reminderFanoutBatchSize)
		if err != nil {
			t.Fatalf("process restock fanout page %d: %v", page+1, err)
		}
		processed += count
		if count == 0 {
			break
		}
	}
	if processed != subscriberCount {
		t.Fatalf("paged fanout processed %d subscribers, want %d", processed, subscriberCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE event_id=$1 AND notification_type='event_restock' AND action='trigger'`, event.ID).Scan(&intents); err != nil || intents != subscriberCount {
		t.Fatalf("restock outbox intents=%d err=%v, want exactly %d", intents, err, subscriberCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_interests WHERE event_id=$1 AND kind='restock' AND status='active' AND version=2`, event.ID).Scan(&unchanged); err != nil || unchanged != subscriberCount {
		t.Fatalf("fanout updated subscriptions=%d err=%v, want %d version-2 active rows", unchanged, err, subscriberCount)
	}
	if _, err := interests.ProcessPendingReminderFanoutPage(ctx, reminderFanoutBatchSize); err != nil {
		t.Fatalf("retry completed restock fanout: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE event_id=$1 AND notification_type='event_restock' AND action='trigger'`, event.ID).Scan(&intents); err != nil || intents != subscriberCount {
		t.Fatalf("restock outbox after retry=%d err=%v, want no duplicates (%d)", intents, err, subscriberCount)
	}
}

func TestReminderFanoutHonorsWithdrawalAndSubscriptionCutoffIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx := context.Background()
	events := NewPostgresEventRepository(pool)
	interests := NewPostgresEventInterestRepository(pool)
	seats := NewPostgresSeatRepository(pool)
	event := newAtomicEventFixture(1)
	if err := events.Create(ctx, event); err != nil {
		t.Fatalf("create event inventory: %v", err)
	}
	withdrawnOwner, retainedOwner, lateOwner := uuid.New(), uuid.New(), uuid.New()
	for _, owner := range []uuid.UUID{withdrawnOwner, retainedOwner} {
		if _, err := interests.UpsertMyEventInterest(ctx, owner, event.ID, InterestRestock, false, ""); err != nil {
			t.Fatalf("create pre-trigger restock subscription: %v", err)
		}
	}
	request := lifecycleRequest(event.ID, uuid.New(), []uuid.UUID{event.Seats[0].ID}, time.Now().UTC().Add(10*time.Minute))
	request.TierQuantities = map[uuid.UUID]int32{event.Tiers[0].ID: 1}
	for _, action := range []string{"reserve", "book", "account", "close"} {
		request.Action = action
		request.OperationID = uuid.New()
		if _, err := seats.ApplyLifecycle(ctx, request); err != nil {
			t.Fatalf("apply %s inventory transition: %v", action, err)
		}
	}
	if _, err := interests.WithdrawMyEventInterest(ctx, withdrawnOwner, event.ID, InterestRestock); err != nil {
		t.Fatalf("withdraw before background fanout: %v", err)
	}
	if _, err := interests.UpsertMyEventInterest(ctx, lateOwner, event.ID, InterestRestock, false, ""); err != nil {
		t.Fatalf("create post-trigger subscription: %v", err)
	}
	processReminderFanouts(t, ctx, interests, 3)
	for _, check := range []struct {
		owner uuid.UUID
		want  int
	}{{withdrawnOwner, 0}, {retainedOwner, 1}, {lateOwner, 0}} {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2
			AND notification_type='event_restock' AND action='trigger'`, check.owner, event.ID).Scan(&count); err != nil || count != check.want {
			t.Fatalf("owner %s trigger intents=%d err=%v, want %d", check.owner, count, err, check.want)
		}
	}
	var withdrawIntents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2
		AND notification_type='event_restock' AND action='withdrawn'`, withdrawnOwner, event.ID).Scan(&withdrawIntents); err != nil || withdrawIntents != 1 {
		t.Fatalf("withdrawal intents=%d err=%v, want one durable control", withdrawIntents, err)
	}
	var persistedID uuid.UUID
	var persistedPayload []byte
	if err := pool.QueryRow(ctx, `SELECT id,payload FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2
		AND notification_type='event_restock' AND action='withdrawn'`, withdrawnOwner, event.ID).Scan(&persistedID, &persistedPayload); err != nil {
		t.Fatalf("read persisted withdrawal intent: %v", err)
	}
	persistedEnvelope, err := eventreminder.Parse(persistedPayload)
	if err != nil || persistedEnvelope.Action != eventreminder.ActionWithdrawn || persistedEnvelope.NotificationID != persistedID {
		t.Fatalf("persisted withdrawal row/envelope identity row=%s envelope=%s action=%s err=%v", persistedID, persistedEnvelope.NotificationID, persistedEnvelope.Action, err)
	}
	withdrawalPublications := 0
	if _, err := interests.ProcessPendingReminderOutbox(ctx, 100, func(_ context.Context, reminder ReminderOutboxEvent) error {
		if reminder.Action != "withdrawn" {
			return nil
		}
		withdrawalPublications++
		envelope, err := eventreminder.Parse(reminder.Payload)
		if err != nil {
			return err
		}
		if envelope.Action != eventreminder.ActionWithdrawn || envelope.NotificationID != reminder.ID {
			return errors.New("withdrawal outbox publisher ID does not match envelope NotificationID")
		}
		return nil
	}); err != nil {
		t.Fatalf("publish durable event reminder outbox boundary: %v", err)
	}
	if withdrawalPublications != 1 {
		t.Fatalf("withdrawal reminder publication count=%d, want 1", withdrawalPublications)
	}
}

func TestReminderFanoutLocksEventBeforeMutationsIntegration(t *testing.T) {
	pool := eventSalesIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	events := NewPostgresEventRepository(pool)
	const appFanout = "reminder_fanout_lock_worker"
	const appMutator = "reminder_interest_lock_mutator"
	const barrierKey int64 = 93510022
	fanoutPool := poolWithApplicationName(t, pool, appFanout)
	mutatorPool := poolWithApplicationName(t, pool, appMutator)
	fanoutRepo := NewPostgresEventInterestRepository(fanoutPool)
	mutatorRepo := NewPostgresEventInterestRepository(mutatorPool)
	if _, err := pool.Exec(ctx, `CREATE TABLE reminder_lock_test_targets(event_id uuid PRIMARY KEY);
		CREATE FUNCTION hold_reminder_outbox_for_lock_test() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.action='trigger' AND EXISTS (SELECT 1 FROM reminder_lock_test_targets WHERE event_id=NEW.event_id) THEN
				PERFORM pg_advisory_xact_lock(93510022);
			END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER hold_reminder_outbox_for_lock_test BEFORE INSERT ON event_reminder_outbox
		FOR EACH ROW EXECUTE FUNCTION hold_reminder_outbox_for_lock_test()`); err != nil {
		t.Fatalf("install deterministic outbox barrier: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(context.Context, *PostgresEventInterestRepository, uuid.UUID, uuid.UUID) error
	}{
		{name: "upsert", mutate: func(ctx context.Context, repo *PostgresEventInterestRepository, owner, eventID uuid.UUID) error {
			_, err := repo.UpsertMyEventInterest(ctx, owner, eventID, InterestRestock, false, "")
			return err
		}},
		{name: "withdraw", mutate: func(ctx context.Context, repo *PostgresEventInterestRepository, owner, eventID uuid.UUID) error {
			_, err := repo.WithdrawMyEventInterest(ctx, owner, eventID, InterestRestock)
			return err
		}},
		{name: "category_email_off", mutate: func(ctx context.Context, repo *PostgresEventInterestRepository, owner, eventID uuid.UUID) error {
			_, err := repo.DisableMyEventAlertEmails(ctx, owner, InterestRestock)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := newAtomicEventFixture(1)
			if err := events.Create(ctx, event); err != nil {
				t.Fatalf("create Event: %v", err)
			}
			owner := uuid.New()
			if _, err := mutatorRepo.UpsertMyEventInterest(ctx, owner, event.ID, InterestRestock, true, eventreminder.ConsentVersion); err != nil {
				t.Fatalf("create restock subscription: %v", err)
			}
			triggerID := uuid.New()
			if _, err := pool.Exec(ctx, `INSERT INTO reminder_lock_test_targets(event_id) VALUES($1)`, event.ID); err != nil {
				t.Fatalf("register Event for outbox barrier: %v", err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO event_reminder_fanouts(id,event_id,kind,trigger_id,event_title,event_date,created_at)
				SELECT $2,id,'restock',$3,title,date,clock_timestamp() FROM events WHERE id=$1`, event.ID, uuid.New(), triggerID); err != nil {
				t.Fatalf("seed durable restock fanout: %v", err)
			}

			barrierConn, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatalf("acquire advisory lock connection: %v", err)
			}
			defer barrierConn.Release()
			if _, err := barrierConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, barrierKey); err != nil {
				t.Fatalf("hold deterministic outbox barrier: %v", err)
			}
			barrierHeld := true
			releaseBarrier := func() error {
				if !barrierHeld {
					return nil
				}
				barrierHeld = false
				_, err := barrierConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, barrierKey)
				return err
			}
			defer func() {
				if err := releaseBarrier(); err != nil {
					t.Errorf("release advisory barrier: %v", err)
				}
			}()

			fanoutDone := make(chan reminderFanoutTestResult, 1)
			go func() {
				count, err := fanoutRepo.ProcessPendingReminderFanoutPage(ctx, 10)
				fanoutDone <- reminderFanoutTestResult{count: count, err: err}
			}()
			fanoutQuery, err := waitForPostgresLockWait(ctx, pool, appFanout)
			if err != nil {
				_ = releaseBarrier()
				_ = receiveFanoutResult(t, fanoutDone)
				t.Fatalf("wait for fanout to reach outbox barrier: %v", err)
			}
			if !strings.Contains(strings.ToLower(fanoutQuery), "insert into event_reminder_outbox") {
				_ = releaseBarrier()
				_ = receiveFanoutResult(t, fanoutDone)
				t.Fatalf("fanout blocked outside outbox barrier: %q", fanoutQuery)
			}

			mutatorDone := make(chan error, 1)
			go func() { mutatorDone <- test.mutate(ctx, mutatorRepo, owner, event.ID) }()
			mutatorQuery, lockErr := waitForPostgresLockWait(ctx, pool, appMutator)
			orderCorrect := lockErr == nil && strings.Contains(strings.ToLower(mutatorQuery), "from events") && strings.Contains(strings.ToLower(mutatorQuery), "for update")
			releaseErr := releaseBarrier()
			fanoutResultValue := receiveFanoutResult(t, fanoutDone)
			mutatorErr := receiveRepositoryResult(t, mutatorDone)
			if releaseErr != nil {
				t.Fatalf("release advisory barrier: %v", releaseErr)
			}
			if !orderCorrect {
				t.Fatalf("%s did not wait on its Event row before subscription/outbox locks; query=%q waitErr=%v", test.name, mutatorQuery, lockErr)
			}
			if fanoutResultValue.err != nil || fanoutResultValue.count != 1 {
				t.Fatalf("fanout result count=%d err=%v, want one completed subscription", fanoutResultValue.count, fanoutResultValue.err)
			}
			if mutatorErr != nil {
				t.Fatalf("%s after fanout barrier: %v", test.name, mutatorErr)
			}
		})
	}
}

func poolWithApplicationName(t *testing.T, base *pgxpool.Pool, applicationName string) *pgxpool.Pool {
	t.Helper()
	config := base.Config().Copy()
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	config.MaxConns = 1
	result, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("create isolated pool %q: %v", applicationName, err)
	}
	t.Cleanup(result.Close)
	return result
}

func waitForPostgresLockWait(ctx context.Context, observer *pgxpool.Pool, applicationName string) (string, error) {
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var query string
		var waitEventType string
		err := observer.QueryRow(ctx, `SELECT query,COALESCE(wait_event_type,'') FROM pg_stat_activity
			WHERE application_name=$1 AND state='active' ORDER BY query_start DESC LIMIT 1`, applicationName).Scan(&query, &waitEventType)
		if err == nil && waitEventType == "Lock" {
			return query, nil
		}
		if err != nil && err != pgx.ErrNoRows {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return "", errors.New("timed out waiting for PostgreSQL lock state")
		case <-ticker.C:
		}
	}
}

type reminderFanoutTestResult struct {
	count int
	err   error
}

func receiveFanoutResult(t *testing.T, result <-chan reminderFanoutTestResult) reminderFanoutTestResult {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("fanout worker did not finish after releasing lock barrier")
		return reminderFanoutTestResult{}
	}
}

func receiveRepositoryResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("interest mutation did not finish after releasing lock barrier")
		return errors.New("interest mutation timed out")
	}
}

func TestLegacyFirstSaleGenerationsBackfillAndCollectIntegration(t *testing.T) {
	pool, migrationDSN := eventSalesIntegrationPoolAtVersion(t, 9)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	scheduledID, sellingFutureID, sellingOpenID := uuid.New(), uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO events(id,title,description,category,venue,location,date,status,publication_status,sales_status,sales_start_at,sales_end_at,created_at,updated_at)
		VALUES
		($1,'Legacy scheduled','fixture','music','hall','city',$4,'active','published','scheduled',$5,NULL,$6,$6),
		($2,'Legacy selling future','fixture','music','hall','city',$4,'active','published','selling',$5,NULL,$6,$6),
		($3,'Legacy already open','fixture','music','hall','city',$4,'active','published','selling',NULL,NULL,$6,$6)`,
		scheduledID, sellingFutureID, sellingOpenID, now.Add(24*time.Hour), now.Add(time.Hour), now)
	if err != nil {
		t.Fatalf("seed version-9 legacy event rows: %v", err)
	}
	output, err := exec.Command("migrate", "-path", "../../migrations", "-database", migrationDSN, "goto", "10").CombinedOutput()
	if err != nil {
		t.Fatalf("upgrade Event schema to reminder migration: %s", strings.ReplaceAll(string(output), migrationDSN, "[isolated DSN]"))
	}
	assertGeneration := func(eventID uuid.UUID, wantGeneration, wantNotified int64, wantOpened bool) {
		t.Helper()
		var generation, notified int64
		var openedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT sales_generation,sale_notification_generation,first_sale_opened_at FROM events WHERE id=$1`, eventID).Scan(&generation, &notified, &openedAt); err != nil {
			t.Fatal(err)
		}
		if generation != wantGeneration || notified != wantNotified || (openedAt != nil) != wantOpened {
			t.Fatalf("legacy sale identity %s generation=%d notified=%d opened=%v, want %d/%d/opened=%v", eventID, generation, notified, openedAt != nil, wantGeneration, wantNotified, wantOpened)
		}
	}
	assertGeneration(scheduledID, 1, 0, false)
	assertGeneration(sellingFutureID, 1, 0, false)
	assertGeneration(sellingOpenID, 1, 1, true)

	interests := NewPostgresEventInterestRepository(pool)
	ownerScheduled, ownerSelling := uuid.New(), uuid.New()
	for _, item := range []struct{ owner, event uuid.UUID }{{ownerScheduled, scheduledID}, {ownerSelling, sellingFutureID}} {
		if _, err := pool.Exec(ctx, `INSERT INTO event_interests(id,user_id,event_id,kind,status) VALUES($1,$2,$3,'onsale','active')`, uuid.New(), item.owner, item.event); err != nil {
			t.Fatalf("seed legacy sale subscriber: %v", err)
		}
	}
	if collected, err := interests.CollectDueSalesTransitions(ctx, 100); err != nil || collected != 0 {
		t.Fatalf("legacy future schedules fired early: collected=%d err=%v", collected, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE events SET sales_start_at=clock_timestamp()-interval '1 second' WHERE id=ANY($1)`, []uuid.UUID{scheduledID, sellingFutureID}); err != nil {
		t.Fatal(err)
	}
	if collected, err := interests.CollectDueSalesTransitions(ctx, 100); err != nil || collected != 2 {
		t.Fatalf("collect legacy scheduled and selling first-sale generations: collected=%d err=%v", collected, err)
	}
	processReminderFanouts(t, ctx, interests, 4)
	for _, item := range []struct{ owner, event uuid.UUID }{{ownerScheduled, scheduledID}, {ownerSelling, sellingFutureID}} {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2 AND notification_type='event_onsale' AND action='trigger'`, item.owner, item.event).Scan(&count); err != nil || count != 1 {
			t.Fatalf("legacy event %s first-sale intents=%d err=%v, want one", item.event, count, err)
		}
	}
}

func processReminderFanouts(t *testing.T, ctx context.Context, repo *PostgresEventInterestRepository, maxPages int) {
	t.Helper()
	for page := 0; page < maxPages; page++ {
		if _, err := repo.ProcessPendingReminderFanoutPage(ctx, 100); err != nil {
			t.Fatalf("process event reminder fanout page %d: %v", page+1, err)
		}
	}
}

func assertDurableReminderTrigger(t *testing.T, ctx context.Context, pool *pgxpool.Pool, owner, eventID uuid.UUID, reminderType string) {
	t.Helper()
	var payload []byte
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2 AND notification_type=$3 AND action='trigger'`, owner, eventID, reminderType).Scan(&count); err != nil || count != 1 {
		t.Fatalf("durable %s trigger count=%d err=%v, want exactly one", reminderType, count, err)
	}
	if err := pool.QueryRow(ctx, `SELECT payload FROM event_reminder_outbox WHERE user_id=$1 AND event_id=$2 AND notification_type=$3 AND action='trigger'`, owner, eventID, reminderType).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	envelope, err := eventreminder.Parse(payload)
	if err != nil || envelope.Action != eventreminder.ActionTrigger || envelope.Type != reminderType || envelope.UserID != owner || envelope.EventID != eventID || !envelope.EmailOptIn || envelope.EmailConsentID == nil {
		t.Fatalf("durable %s trigger payload=%+v err=%v", reminderType, envelope, err)
	}
}
