package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketbox/event/internal/domain"
	"github.com/ticketbox/pkg/eventreminder"
)

var (
	ErrInterestNotFound      = errors.New("event interest not found")
	ErrInterestAlreadyOnSale = errors.New("event is already on sale")
	ErrInterestNotAvailable  = errors.New("event interest is unavailable")
	ErrInvalidInterest       = errors.New("invalid event interest")
)

const (
	InterestFavorite        = "favorite"
	InterestOnSale          = "onsale"
	InterestRestock         = "restock"
	reminderFanoutBatchSize = 100
)

type EventInterest struct {
	ID                  uuid.UUID
	UserID              uuid.UUID
	EventID             uuid.UUID
	Kind                string
	Status              string
	EmailOptIn          bool
	EmailConsentVersion string
	EmailConsentID      *uuid.UUID
	EmailConsentedAt    *time.Time
	EmailRevokedAt      *time.Time
	Version             int64
	ActiveSinceVersion  int64
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type EventInterestPage struct {
	Items   []EventInterest
	HasMore bool
}

type EventInterestCounts struct {
	Favorites  int64
	OnSale     int64
	Restock    int64
	ObservedAt time.Time
}

type EventInterestRepository interface {
	ListMyEventInterests(context.Context, uuid.UUID, string, int32, int32) (EventInterestPage, error)
	GetMyEventInterests(context.Context, uuid.UUID, uuid.UUID) ([]EventInterest, error)
	UpsertMyEventInterest(context.Context, uuid.UUID, uuid.UUID, string, bool, string) (EventInterest, error)
	WithdrawMyEventInterest(context.Context, uuid.UUID, uuid.UUID, string) (EventInterest, error)
	DisableMyEventAlertEmails(context.Context, uuid.UUID, string) (int32, error)
	ReadEventInterestCounts(context.Context, uuid.UUID) (EventInterestCounts, error)
	RestockCollectionStatus(context.Context, uuid.UUID) (string, error)
	CanCreateOnSaleInterest(context.Context, uuid.UUID) (bool, error)
}

type PostgresEventInterestRepository struct{ pool *pgxpool.Pool }

func NewPostgresEventInterestRepository(pool *pgxpool.Pool) *PostgresEventInterestRepository {
	return &PostgresEventInterestRepository{pool: pool}
}

func (r *PostgresEventInterestRepository) ListMyEventInterests(ctx context.Context, userID uuid.UUID, kind string, limit, offset int32) (EventInterestPage, error) {
	if userID == uuid.Nil || limit < 1 || limit > 100 || offset < 0 || offset > 100000 || kind != "" && kind != InterestFavorite && kind != InterestOnSale && kind != InterestRestock {
		return EventInterestPage{}, ErrInvalidInterest
	}
	rows, err := r.pool.Query(ctx, `SELECT id,user_id,event_id,kind,status,email_opt_in,email_consent_version,email_consent_id,email_consented_at,email_revoked_at,version,active_since_version,created_at,updated_at
		FROM event_interests WHERE user_id=$1 AND ($2='' AND kind IN ('onsale','restock') OR kind=$2)
		AND status <> 'withdrawn' AND (kind <> 'favorite' OR status='active')
		ORDER BY created_at DESC,id DESC LIMIT $3 OFFSET $4`, userID, kind, limit+1, offset)
	if err != nil {
		return EventInterestPage{}, fmt.Errorf("list event interests: %w", err)
	}
	defer rows.Close()
	page := EventInterestPage{Items: make([]EventInterest, 0, limit)}
	for rows.Next() {
		item, err := scanEventInterest(rows)
		if err != nil {
			return EventInterestPage{}, err
		}
		if int32(len(page.Items)) == limit {
			page.HasMore = true
			break
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return EventInterestPage{}, err
	}
	return page, nil
}

func (r *PostgresEventInterestRepository) GetMyEventInterests(ctx context.Context, userID, eventID uuid.UUID) ([]EventInterest, error) {
	if userID == uuid.Nil || eventID == uuid.Nil {
		return nil, ErrInvalidInterest
	}
	rows, err := r.pool.Query(ctx, `SELECT id,user_id,event_id,kind,status,email_opt_in,email_consent_version,email_consent_id,email_consented_at,email_revoked_at,version,active_since_version,created_at,updated_at
		FROM event_interests WHERE user_id=$1 AND event_id=$2 ORDER BY kind`, userID, eventID)
	if err != nil {
		return nil, fmt.Errorf("read event interests: %w", err)
	}
	defer rows.Close()
	items := make([]EventInterest, 0, 3)
	for rows.Next() {
		item, err := scanEventInterest(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (r *PostgresEventInterestRepository) UpsertMyEventInterest(ctx context.Context, userID, eventID uuid.UUID, kind string, emailOptIn bool, consentVersion string) (EventInterest, error) {
	if userID == uuid.Nil || eventID == uuid.Nil || kind != InterestFavorite && kind != InterestOnSale && kind != InterestRestock ||
		kind == InterestFavorite && emailOptIn || emailOptIn && consentVersion != eventreminder.ConsentVersion || !emailOptIn && consentVersion != "" {
		return EventInterest{}, ErrInvalidInterest
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return EventInterest{}, fmt.Errorf("begin event interest: %w", err)
	}
	defer tx.Rollback(ctx)
	var event domain.Event
	var startAt, endAt *time.Time
	var firstSaleOpenedAt *time.Time
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT id,date,status,publication_status,sales_status,sales_start_at,sales_end_at,first_sale_opened_at,clock_timestamp()
		FROM events WHERE id=$1 FOR UPDATE`, eventID).Scan(&event.ID, &event.Date, &event.Status, &event.PublicationStatus, &event.SalesStatus, &startAt, &endAt, &firstSaleOpenedAt, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return EventInterest{}, ErrNotFound
	}
	if err != nil {
		return EventInterest{}, fmt.Errorf("lock event for interest: %w", err)
	}
	event.SalesStartAt, event.SalesEndAt = startAt, endAt
	prior := EventInterest{}
	priorErr := tx.QueryRow(ctx, `SELECT id,user_id,event_id,kind,status,email_opt_in,email_consent_version,email_consent_id,email_consented_at,email_revoked_at,version,active_since_version,created_at,updated_at
		FROM event_interests WHERE user_id=$1 AND event_id=$2 AND kind=$3 FOR UPDATE`, userID, eventID, kind).Scan(
		&prior.ID, &prior.UserID, &prior.EventID, &prior.Kind, &prior.Status, &prior.EmailOptIn, &prior.EmailConsentVersion, &prior.EmailConsentID, &prior.EmailConsentedAt, &prior.EmailRevokedAt, &prior.Version, &prior.ActiveSinceVersion, &prior.CreatedAt, &prior.UpdatedAt)
	if priorErr != nil && !errors.Is(priorErr, pgx.ErrNoRows) {
		return EventInterest{}, fmt.Errorf("lock existing event interest: %w", priorErr)
	}
	priorExists := priorErr == nil
	// Email withdrawal is an owner-only privacy action. It stays available for
	// an existing alert after an event is unpublished, cancelled, or ended.
	emailOptOutOnly := kind != InterestFavorite && priorExists && prior.Status != "withdrawn" && !emailOptIn
	if emailOptOutOnly && !prior.EmailOptIn {
		return prior, nil
	}
	if !domain.EventIsPublished(&event) && !emailOptOutOnly {
		return EventInterest{}, ErrNotFound
	}
	if kind != InterestFavorite && !emailOptOutOnly {
		eligible, reason := domain.EventSaleEligibility(&event, now)
		if kind == InterestOnSale && (!priorExists || prior.Status == "withdrawn") && (eligible || firstSaleOpenedAt != nil) {
			return EventInterest{}, ErrInterestAlreadyOnSale
		}
		if (reason == "event_cancelled" || reason == "sales_ended" || reason == "event_ended") && (!priorExists || prior.Status == "withdrawn") {
			return EventInterest{}, ErrInterestNotAvailable
		}
	}
	statusValue := "active"
	if emailOptOutOnly {
		statusValue = prior.Status
	} else if kind == InterestRestock {
		reliable, _, reliabilityErr := eventInventoryTrackingState(ctx, tx, eventID)
		if reliabilityErr != nil {
			return EventInterest{}, fmt.Errorf("check inventory tracking: %w", reliabilityErr)
		}
		if !reliable {
			statusValue = "collection_pending"
		}
	}
	item := EventInterest{}
	var consentedAt, revokedAt *time.Time
	var newConsentID any
	if emailOptIn {
		newConsentID = uuid.New()
	}
	err = tx.QueryRow(ctx, `INSERT INTO event_interests(id,user_id,event_id,kind,status,email_opt_in,email_consent_version,email_consent_id,email_consented_at,email_revoked_at,version,active_since_version,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,CASE WHEN $6 THEN clock_timestamp() ELSE NULL END,NULL,1,1,clock_timestamp(),clock_timestamp())
		ON CONFLICT(user_id,event_id,kind) DO UPDATE SET
		 status=CASE WHEN event_interests.status='triggered' AND event_interests.kind='onsale' THEN 'triggered' ELSE EXCLUDED.status END,
		 active_since_version=CASE WHEN event_interests.status='withdrawn' THEN event_interests.version+1 ELSE event_interests.active_since_version END,
		 active_since_at=CASE WHEN event_interests.status='withdrawn' THEN clock_timestamp() ELSE event_interests.active_since_at END,
		 email_opt_in=EXCLUDED.email_opt_in,
		 email_consent_version=CASE WHEN EXCLUDED.email_opt_in THEN EXCLUDED.email_consent_version ELSE event_interests.email_consent_version END,
		 email_consent_id=CASE WHEN EXCLUDED.email_opt_in AND NOT event_interests.email_opt_in THEN EXCLUDED.email_consent_id WHEN NOT EXCLUDED.email_opt_in THEN NULL ELSE event_interests.email_consent_id END,
		 email_consented_at=CASE WHEN EXCLUDED.email_opt_in AND NOT event_interests.email_opt_in THEN clock_timestamp() ELSE event_interests.email_consented_at END,
		 email_revoked_at=CASE WHEN event_interests.email_opt_in AND NOT EXCLUDED.email_opt_in THEN clock_timestamp() WHEN EXCLUDED.email_opt_in THEN NULL ELSE event_interests.email_revoked_at END,
		 version=event_interests.version+1,updated_at=clock_timestamp()
		RETURNING id,user_id,event_id,kind,status,email_opt_in,email_consent_version,email_consent_id,email_consented_at,email_revoked_at,version,active_since_version,created_at,updated_at`,
		uuid.New(), userID, eventID, kind, statusValue, emailOptIn, consentVersion, newConsentID).Scan(&item.ID, &item.UserID, &item.EventID, &item.Kind, &item.Status, &item.EmailOptIn, &item.EmailConsentVersion, &item.EmailConsentID, &consentedAt, &revokedAt, &item.Version, &item.ActiveSinceVersion, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		return EventInterest{}, fmt.Errorf("upsert event interest: %w", err)
	}
	item.EmailConsentedAt, item.EmailRevokedAt = consentedAt, revokedAt
	if kind != InterestFavorite && priorExists && (prior.EmailOptIn != item.EmailOptIn || prior.EmailConsentVersion != item.EmailConsentVersion || prior.Status == "withdrawn") {
		var title string
		var eventDate time.Time
		if err := tx.QueryRow(ctx, `SELECT title,date FROM events WHERE id=$1`, eventID).Scan(&title, &eventDate); err != nil {
			return EventInterest{}, err
		}
		action := eventreminder.ActionConsentUpdated
		if prior.Status == "withdrawn" {
			action = eventreminder.ActionResubscribed
		}
		message := eventreminder.Envelope{Version: 1, NotificationID: uuid.New(), SubscriptionID: item.ID, SubscriptionVersion: item.Version, ActiveSinceVersion: item.ActiveSinceVersion,
			Action: action, Type: reminderType(kind), UserID: userID, EventID: eventID, EventTitle: title, EventDate: eventDate.UTC(),
			EmailOptIn: item.EmailOptIn, EmailConsentID: item.EmailConsentID}
		if item.EmailOptIn {
			message.EmailConsentVersion = item.EmailConsentVersion
		}
		payload, marshalErr := json.Marshal(message)
		if marshalErr != nil {
			return EventInterest{}, marshalErr
		}
		if _, err := tx.Exec(ctx, `INSERT INTO event_reminder_outbox(id,subscription_id,event_id,user_id,notification_type,action,subscription_version,trigger_id,payload)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, message.NotificationID, item.ID, eventID, userID, message.Type, action, item.Version, uuid.New(), payload); err != nil {
			return EventInterest{}, fmt.Errorf("queue event reminder consent update: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return EventInterest{}, fmt.Errorf("commit event interest: %w", err)
	}
	return item, nil
}

func (r *PostgresEventInterestRepository) WithdrawMyEventInterest(ctx context.Context, userID, eventID uuid.UUID, kind string) (EventInterest, error) {
	if userID == uuid.Nil || eventID == uuid.Nil || kind != InterestFavorite && kind != InterestOnSale && kind != InterestRestock {
		return EventInterest{}, ErrInvalidInterest
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return EventInterest{}, fmt.Errorf("begin event interest withdrawal: %w", err)
	}
	defer tx.Rollback(ctx)
	var lockedEventID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM events WHERE id=$1 FOR UPDATE`, eventID).Scan(&lockedEventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return EventInterest{}, ErrInterestNotFound
	}
	if err != nil {
		return EventInterest{}, fmt.Errorf("lock event before interest withdrawal: %w", err)
	}
	var item EventInterest
	var consentedAt, revokedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT id,user_id,event_id,kind,status,email_opt_in,email_consent_version,email_consent_id,email_consented_at,email_revoked_at,version,active_since_version,created_at,updated_at
		FROM event_interests WHERE user_id=$1 AND event_id=$2 AND kind=$3 FOR UPDATE`, userID, eventID, kind).
		Scan(&item.ID, &item.UserID, &item.EventID, &item.Kind, &item.Status, &item.EmailOptIn, &item.EmailConsentVersion, &item.EmailConsentID, &consentedAt, &revokedAt, &item.Version, &item.ActiveSinceVersion, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return EventInterest{}, ErrInterestNotFound
	}
	if err != nil {
		return EventInterest{}, fmt.Errorf("lock event interest withdrawal: %w", err)
	}
	item.EmailConsentedAt, item.EmailRevokedAt = consentedAt, revokedAt
	if item.Status != "withdrawn" {
		item.Status = "withdrawn"
		item.Version++
		item.UpdatedAt = time.Now().UTC()
		item.EmailOptIn = false
		item.EmailConsentID = nil
		if revokedAt == nil && item.EmailRevokedAt == nil && item.EmailConsentedAt != nil {
			revoked := time.Now().UTC()
			item.EmailRevokedAt = &revoked
		}
		if _, err := tx.Exec(ctx, `UPDATE event_interests SET status='withdrawn',email_opt_in=FALSE,email_consent_id=NULL,
			email_revoked_at=CASE WHEN email_opt_in THEN clock_timestamp() ELSE email_revoked_at END,
			version=$2,updated_at=clock_timestamp() WHERE id=$1`, item.ID, item.Version); err != nil {
			return EventInterest{}, fmt.Errorf("withdraw event interest: %w", err)
		}
		if kind != InterestFavorite {
			if _, err := tx.Exec(ctx, `UPDATE event_reminder_outbox SET suppressed_at=clock_timestamp(),claim_token=NULL,lease_until=NULL
				WHERE subscription_id=$1 AND published_at IS NULL AND suppressed_at IS NULL`, item.ID); err != nil {
				return EventInterest{}, fmt.Errorf("suppress queued event reminders: %w", err)
			}
			var title string
			var eventDate time.Time
			if err := tx.QueryRow(ctx, `SELECT title,date FROM events WHERE id=$1`, eventID).Scan(&title, &eventDate); err != nil {
				return EventInterest{}, fmt.Errorf("read event for withdrawal intent: %w", err)
			}
			notificationID := uuid.New()
			payload, marshalErr := json.Marshal(eventreminder.Envelope{Version: 1, NotificationID: notificationID, SubscriptionID: item.ID, SubscriptionVersion: item.Version, ActiveSinceVersion: item.ActiveSinceVersion,
				Action: eventreminder.ActionWithdrawn, Type: reminderType(kind), UserID: userID, EventID: eventID, EventTitle: title, EventDate: eventDate.UTC()})
			if marshalErr != nil {
				return EventInterest{}, marshalErr
			}
			if _, err := tx.Exec(ctx, `INSERT INTO event_reminder_outbox(id,subscription_id,event_id,user_id,notification_type,action,subscription_version,trigger_id,payload)
				VALUES($1,$2,$3,$4,$5,'withdrawn',$6,$7,$8)`, notificationID, item.ID, eventID, userID, reminderType(kind), item.Version, uuid.New(), payload); err != nil {
				return EventInterest{}, fmt.Errorf("queue event reminder withdrawal: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return EventInterest{}, fmt.Errorf("commit event interest withdrawal: %w", err)
	}
	return item, nil
}

// DisableMyEventAlertEmails revokes the current user's email consent for every
// active alert of the requested category. Each revocation and its durable
// Notification intent commit atomically; future email delivery requires a new
// explicit opt-in.
func (r *PostgresEventInterestRepository) DisableMyEventAlertEmails(ctx context.Context, userID uuid.UUID, kind string) (int32, error) {
	if userID == uuid.Nil || kind != InterestOnSale && kind != InterestRestock {
		return 0, ErrInvalidInterest
	}
	var cutoff time.Time
	if err := r.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&cutoff); err != nil {
		return 0, fmt.Errorf("capture category email consent cutoff: %w", err)
	}
	var updated int32
	var cursorEventID, cursorInterestID uuid.UUID
	for {
		count, lastEventID, lastInterestID, err := r.disableMyEventAlertEmailBatch(ctx, userID, kind, cutoff, cursorEventID, cursorInterestID)
		if err != nil {
			return updated, err
		}
		if lastEventID == uuid.Nil {
			return updated, nil
		}
		updated += count
		cursorEventID, cursorInterestID = lastEventID, lastInterestID
	}
}

const categoryConsentBatchSize = 100

// disableMyEventAlertEmailBatch walks a bounded keyset page. It locks every
// affected Event row in UUID order before locking its subscriptions, matching
// Upsert/withdraw/fanout's lock order and avoiding an unbounded cross-event
// lock set.
func (r *PostgresEventInterestRepository) disableMyEventAlertEmailBatch(ctx context.Context, userID uuid.UUID, kind string, cutoff time.Time, cursorEventID, cursorInterestID uuid.UUID) (int32, uuid.UUID, uuid.UUID, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, uuid.Nil, uuid.Nil, fmt.Errorf("begin category email consent withdrawal: %w", err)
	}
	defer tx.Rollback(ctx)

	var cursorEventValue, cursorInterestValue any
	if cursorEventID != uuid.Nil {
		cursorEventValue, cursorInterestValue = cursorEventID, cursorInterestID
	}
	candidateRows, err := tx.Query(ctx, `SELECT event_id,id FROM event_interests
		WHERE user_id=$1 AND kind=$2 AND created_at<=$3 AND active_since_at<=$3
		AND ($4::uuid IS NULL OR (event_id,id)>($4::uuid,$5::uuid))
		ORDER BY event_id,id LIMIT $6`, userID, kind, cutoff, cursorEventValue, cursorInterestValue, categoryConsentBatchSize)
	if err != nil {
		return 0, uuid.Nil, uuid.Nil, fmt.Errorf("select category email consent page: %w", err)
	}
	type candidate struct{ eventID, interestID uuid.UUID }
	candidates := make([]candidate, 0, categoryConsentBatchSize)
	eventIDs := make([]uuid.UUID, 0, categoryConsentBatchSize)
	seenEvents := make(map[uuid.UUID]struct{}, categoryConsentBatchSize)
	for candidateRows.Next() {
		var item candidate
		if err := candidateRows.Scan(&item.eventID, &item.interestID); err != nil {
			candidateRows.Close()
			return 0, uuid.Nil, uuid.Nil, fmt.Errorf("read category email consent page: %w", err)
		}
		candidates = append(candidates, item)
		if _, ok := seenEvents[item.eventID]; !ok {
			seenEvents[item.eventID] = struct{}{}
			eventIDs = append(eventIDs, item.eventID)
		}
	}
	if err := candidateRows.Err(); err != nil {
		candidateRows.Close()
		return 0, uuid.Nil, uuid.Nil, fmt.Errorf("iterate category email consent page: %w", err)
	}
	candidateRows.Close()
	if len(candidates) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, uuid.Nil, uuid.Nil, fmt.Errorf("commit empty category email consent page: %w", err)
		}
		return 0, uuid.Nil, uuid.Nil, nil
	}
	// Event locks always precede subscription locks and outbox FK checks.
	eventRows, err := tx.Query(ctx, `SELECT id FROM events WHERE id=ANY($1) ORDER BY id FOR UPDATE`, eventIDs)
	if err != nil {
		return 0, uuid.Nil, uuid.Nil, fmt.Errorf("lock category consent Events: %w", err)
	}
	lockedEvents := 0
	for eventRows.Next() {
		var ignored uuid.UUID
		if err := eventRows.Scan(&ignored); err != nil {
			eventRows.Close()
			return 0, uuid.Nil, uuid.Nil, fmt.Errorf("read locked category consent Event: %w", err)
		}
		lockedEvents++
	}
	if err := eventRows.Err(); err != nil {
		eventRows.Close()
		return 0, uuid.Nil, uuid.Nil, fmt.Errorf("iterate locked category consent Events: %w", err)
	}
	eventRows.Close()
	if lockedEvents != len(eventIDs) {
		return 0, uuid.Nil, uuid.Nil, fmt.Errorf("category consent page referenced a missing Event")
	}

	interestIDs := make([]uuid.UUID, 0, len(candidates))
	for _, item := range candidates {
		interestIDs = append(interestIDs, item.interestID)
	}
	rows, err := tx.Query(ctx, `SELECT i.id,i.event_id,i.user_id,i.version,i.active_since_version,e.title,e.date,i.email_opt_in,i.email_consented_at
		FROM event_interests i JOIN events e ON e.id=i.event_id
		WHERE i.id=ANY($1) AND i.user_id=$2 AND i.kind=$3
		ORDER BY i.event_id,i.id FOR UPDATE OF i`, interestIDs, userID, kind)
	if err != nil {
		return 0, uuid.Nil, uuid.Nil, fmt.Errorf("lock category email subscriptions: %w", err)
	}
	type consent struct {
		id, eventID, owner   uuid.UUID
		version, activeSince int64
		title                string
		eventDate            time.Time
		emailOptIn           bool
		consentedAt          *time.Time
	}
	items := make([]consent, 0, len(candidates))
	var updated int32
	for rows.Next() {
		var item consent
		if err := rows.Scan(&item.id, &item.eventID, &item.owner, &item.version, &item.activeSince, &item.title, &item.eventDate, &item.emailOptIn, &item.consentedAt); err != nil {
			rows.Close()
			return 0, uuid.Nil, uuid.Nil, fmt.Errorf("read category email subscription: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, uuid.Nil, uuid.Nil, fmt.Errorf("iterate category email subscriptions: %w", err)
	}
	rows.Close()
	for _, item := range items {
		if !item.emailOptIn || item.consentedAt == nil || item.consentedAt.After(cutoff) {
			continue
		}
		version := item.version + 1
		tag, err := tx.Exec(ctx, `UPDATE event_interests SET email_opt_in=FALSE,email_consent_id=NULL,
			email_revoked_at=clock_timestamp(),version=$2,updated_at=clock_timestamp()
			WHERE id=$1 AND user_id=$3 AND version=$4 AND email_opt_in`, item.id, version, userID, item.version)
		if err != nil {
			return 0, uuid.Nil, uuid.Nil, fmt.Errorf("revoke category email consent: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return 0, uuid.Nil, uuid.Nil, fmt.Errorf("category email consent changed during revocation")
		}
		updated++
		message := eventreminder.Envelope{
			Version: 1, NotificationID: uuid.New(), SubscriptionID: item.id,
			SubscriptionVersion: version, Action: eventreminder.ActionConsentUpdated,
			ActiveSinceVersion: item.activeSince, Type: reminderType(kind), UserID: userID, EventID: item.eventID,
			EventTitle: item.title, EventDate: item.eventDate.UTC(),
		}
		payload, err := json.Marshal(message)
		if err != nil {
			return 0, uuid.Nil, uuid.Nil, fmt.Errorf("encode category consent update: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO event_reminder_outbox
			(id,subscription_id,event_id,user_id,notification_type,action,subscription_version,trigger_id,payload)
			VALUES($1,$2,$3,$4,$5,'consent_updated',$6,$7,$8)`, message.NotificationID, item.id,
			item.eventID, userID, message.Type, version, uuid.New(), payload); err != nil {
			return 0, uuid.Nil, uuid.Nil, fmt.Errorf("queue category consent update: %w", err)
		}
	}
	last := candidates[len(candidates)-1]
	if err := tx.Commit(ctx); err != nil {
		return 0, uuid.Nil, uuid.Nil, fmt.Errorf("commit category email consent page: %w", err)
	}
	return updated, last.eventID, last.interestID, nil
}

func (r *PostgresEventInterestRepository) ReadEventInterestCounts(ctx context.Context, eventID uuid.UUID) (EventInterestCounts, error) {
	if eventID == uuid.Nil {
		return EventInterestCounts{}, ErrInvalidInterest
	}
	var result EventInterestCounts
	err := r.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE kind='favorite' AND status='active'),
		count(*) FILTER (WHERE kind='onsale' AND status IN ('active','triggered')),
		count(*) FILTER (WHERE kind='restock' AND status IN ('active','collection_pending')),
		clock_timestamp() FROM event_interests WHERE event_id=$1`, eventID).Scan(&result.Favorites, &result.OnSale, &result.Restock, &result.ObservedAt)
	if err != nil {
		return EventInterestCounts{}, fmt.Errorf("read event interest counts: %w", err)
	}
	return result, nil
}

// RestockCollectionStatus reports whether the authoritative seat/accounting
// evidence is complete enough to produce restock alerts. It never repairs or
// changes inventory.
func (r *PostgresEventInterestRepository) RestockCollectionStatus(ctx context.Context, eventID uuid.UUID) (string, error) {
	if eventID == uuid.Nil {
		return "", ErrInvalidInterest
	}
	reliable, _, err := eventInventoryTrackingState(ctx, r.pool, eventID)
	if err != nil {
		return "", fmt.Errorf("read restock inventory evidence: %w", err)
	}
	if !reliable {
		return "collection_pending", nil
	}
	return "available", nil
}

func (r *PostgresEventInterestRepository) CanCreateOnSaleInterest(ctx context.Context, eventID uuid.UUID) (bool, error) {
	var eligible bool
	err := r.pool.QueryRow(ctx, `SELECT publication_status='published' AND status<>'cancelled' AND sales_status IN ('scheduled','selling')
		AND first_sale_opened_at IS NULL AND date>clock_timestamp()
		AND (sales_start_at IS NULL OR sales_start_at>clock_timestamp())
		AND (sales_end_at IS NULL OR sales_end_at>clock_timestamp())
		AND NOT (sales_status='selling' AND (sales_start_at IS NULL OR sales_start_at<=clock_timestamp()))
		FROM events WHERE id=$1`, eventID).Scan(&eligible)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	return eligible, err
}

func scanEventInterest(row pgx.Row) (EventInterest, error) {
	var item EventInterest
	err := row.Scan(&item.ID, &item.UserID, &item.EventID, &item.Kind, &item.Status, &item.EmailOptIn, &item.EmailConsentVersion, &item.EmailConsentID, &item.EmailConsentedAt, &item.EmailRevokedAt, &item.Version, &item.ActiveSinceVersion, &item.CreatedAt, &item.UpdatedAt)
	return item, err
}

func reminderType(kind string) string {
	if kind == InterestOnSale {
		return eventreminder.TypeOnSale
	}
	return eventreminder.TypeRestock
}

func saleGenerationTriggerID(eventID uuid.UUID, generation int64) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("rushgo:event-sale:%s:%d", eventID, generation)))
}

func eventInventoryTrackingState(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, eventID uuid.UUID) (bool, int64, error) {
	var reliable bool
	var available int64
	err := q.QueryRow(ctx, `SELECT COALESCE(bool_and(
		t.lifecycle_accounting_baseline='verified'
		AND t.total_quantity>=0 AND t.available_quantity BETWEEN 0 AND t.total_quantity
		AND (SELECT count(*) FROM seats s WHERE s.ticket_tier_id=t.id AND s.deleted_at IS NULL)=t.total_quantity
		AND NOT EXISTS(SELECT 1 FROM seats s LEFT JOIN booking_seat_lifecycles l ON l.booking_id=s.booking_id AND l.generation=s.lifecycle_generation
			WHERE s.ticket_tier_id=t.id AND s.deleted_at IS NULL AND s.status='booked'
			AND (s.lifecycle_generation IS NULL OR l.state IS DISTINCT FROM 'booked'))
		AND t.available_quantity=t.total_quantity-COALESCE((SELECT SUM((l.debits->>t.id::text)::bigint) FROM booking_seat_lifecycles l WHERE l.accounting_state='applied'),0)
	),false),COALESCE(sum(available_quantity),0)
	FROM ticket_tiers t WHERE t.event_id=$1`, eventID).Scan(&reliable, &available)
	return reliable, available, err
}

func enqueueOnSaleAlerts(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, generation int64, triggerID uuid.UUID) error {
	var title string
	var eventDate time.Time
	if err := tx.QueryRow(ctx, `SELECT title,date FROM events WHERE id=$1`, eventID).Scan(&title, &eventDate); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO event_reminder_fanouts(id,event_id,kind,trigger_id,event_title,event_date,created_at)
		VALUES($1,$2,'onsale',$3,$4,$5,clock_timestamp()) ON CONFLICT(event_id,kind,trigger_id) DO NOTHING`, uuid.New(), eventID, triggerID, title, eventDate)
	if err != nil {
		return fmt.Errorf("persist first-sale fanout trigger for generation %d: %w", generation, err)
	}
	return nil
}

func enqueueRestockAlerts(ctx context.Context, tx pgx.Tx, eventID, triggerID uuid.UUID) error {
	var title string
	var eventDate time.Time
	// Serialize the fanout's subscription cutoff with new subscriptions. The
	// operation takes one event lock regardless of subscriber count.
	if err := tx.QueryRow(ctx, `SELECT title,date FROM events WHERE id=$1 FOR UPDATE`, eventID).Scan(&title, &eventDate); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO event_reminder_fanouts(id,event_id,kind,trigger_id,event_title,event_date,created_at)
		VALUES($1,$2,'restock',$3,$4,$5,clock_timestamp()) ON CONFLICT(event_id,kind,trigger_id) DO NOTHING`, uuid.New(), eventID, triggerID, title, eventDate)
	if err != nil {
		return fmt.Errorf("persist restock fanout trigger: %w", err)
	}
	return nil
}

// ProcessPendingReminderFanoutPage expands one immutable Event trigger into a
// bounded number of per-user Notification outbox intents. Subscription state,
// version changes, and the durable cursor commit atomically, so withdrawal or
// email-consent changes cannot be bypassed by a retry.
func (r *PostgresEventInterestRepository) ProcessPendingReminderFanoutPage(ctx context.Context, batchSize int) (int, error) {
	if batchSize < 1 {
		batchSize = 1
	}
	if batchSize > reminderFanoutBatchSize {
		batchSize = reminderFanoutBatchSize
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin event reminder fanout page: %w", err)
	}
	defer tx.Rollback(ctx)

	var fanoutID, eventID, triggerID uuid.UUID
	var kind, title string
	var eventDate, createdAt time.Time
	var cursor pgtype.UUID
	// Read a candidate without taking its row lock. Event lifecycle and
	// interest mutation transactions lock Event first, so the fanout worker
	// must do the same before locking either the source row or subscriptions.
	err = tx.QueryRow(ctx, `SELECT id,event_id FROM event_reminder_fanouts WHERE completed_at IS NULL
		ORDER BY created_at,id LIMIT 1`).Scan(&fanoutID, &eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit empty event reminder fanout page: %w", err)
		}
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("select pending event reminder fanout candidate: %w", err)
	}
	var lockedEventID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM events WHERE id=$1 FOR UPDATE`, eventID).Scan(&lockedEventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("lock Event for reminder fanout: %w", pgx.ErrNoRows)
	}
	if err != nil {
		return 0, fmt.Errorf("lock Event before reminder fanout: %w", err)
	}
	err = tx.QueryRow(ctx, `SELECT event_id,kind,trigger_id,event_title,event_date,created_at,cursor_id
		FROM event_reminder_fanouts WHERE completed_at IS NULL
		AND id=$1 FOR UPDATE`, fanoutID).Scan(
		&eventID, &kind, &triggerID, &title, &eventDate, &createdAt, &cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit empty event reminder fanout page: %w", err)
		}
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("lock pending event reminder fanout after Event: %w", err)
	}
	if eventID != lockedEventID {
		return 0, fmt.Errorf("reminder fanout Event changed while acquiring locks")
	}
	var cursorValue any
	if cursor.Valid {
		cursorValue = uuid.UUID(cursor.Bytes)
	}
	rows, err := tx.Query(ctx, `SELECT id,user_id,version,active_since_version,status,email_opt_in,email_consent_version,email_consent_id
		FROM event_interests
		WHERE event_id=$1 AND kind=$2 AND status IN ('active','collection_pending')
		AND ($2<>'onsale' OR status='active') AND active_since_at<=$3
		AND ($4::uuid IS NULL OR id>$4::uuid)
		ORDER BY id LIMIT $5 FOR UPDATE`, eventID, kind, createdAt, cursorValue, batchSize)
	if err != nil {
		return 0, fmt.Errorf("select event reminder fanout page: %w", err)
	}
	type subscriber struct {
		id, user             uuid.UUID
		version, activeSince int64
		status               string
		emailOptIn           bool
		consentVersion       string
		consentID            *uuid.UUID
	}
	subscribers := make([]subscriber, 0, batchSize)
	for rows.Next() {
		var item subscriber
		if err := rows.Scan(&item.id, &item.user, &item.version, &item.activeSince, &item.status, &item.emailOptIn, &item.consentVersion, &item.consentID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan event reminder fanout page: %w", err)
		}
		subscribers = append(subscribers, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("read event reminder fanout page: %w", err)
	}
	rows.Close()
	if len(subscribers) == 0 {
		if _, err := tx.Exec(ctx, `UPDATE event_reminder_fanouts SET completed_at=clock_timestamp() WHERE id=$1 AND completed_at IS NULL`, fanoutID); err != nil {
			return 0, fmt.Errorf("complete empty event reminder fanout: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit completed event reminder fanout: %w", err)
		}
		return 0, nil
	}
	reminderType := eventreminder.TypeOnSale
	newStatus := "triggered"
	if kind == InterestRestock {
		reminderType = eventreminder.TypeRestock
		newStatus = "active" // persistent restock subscriptions can fire again.
	}
	var lastID uuid.UUID
	for _, subscriber := range subscribers {
		version := subscriber.version + 1
		message := eventreminder.Envelope{Version: 1, NotificationID: uuid.New(), SubscriptionID: subscriber.id,
			SubscriptionVersion: version, ActiveSinceVersion: subscriber.activeSince, Action: eventreminder.ActionTrigger, Type: reminderType,
			UserID: subscriber.user, EventID: eventID, EventTitle: title, EventDate: eventDate.UTC(), EmailOptIn: subscriber.emailOptIn, EmailConsentID: subscriber.consentID}
		if message.EmailOptIn {
			message.EmailConsentVersion = subscriber.consentVersion
		}
		payload, err := json.Marshal(message)
		if err != nil {
			return 0, fmt.Errorf("encode event reminder fanout intent: %w", err)
		}
		tag, err := tx.Exec(ctx, `UPDATE event_interests SET status=$3,version=$4,updated_at=clock_timestamp()
			WHERE id=$1 AND version=$2 AND status=$5`, subscriber.id, subscriber.version, newStatus, version, subscriber.status)
		if err != nil {
			return 0, fmt.Errorf("advance event reminder subscription: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return 0, fmt.Errorf("event reminder subscription changed during locked fanout")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO event_reminder_outbox(id,subscription_id,event_id,user_id,notification_type,action,subscription_version,trigger_id,payload)
			VALUES($1,$2,$3,$4,$5,'trigger',$6,$7,$8) ON CONFLICT(subscription_id,subscription_version,action,trigger_id) DO NOTHING`,
			message.NotificationID, subscriber.id, eventID, subscriber.user, message.Type, version, triggerID, payload); err != nil {
			return 0, fmt.Errorf("persist event reminder outbox intent: %w", err)
		}
		lastID = subscriber.id
	}
	if len(subscribers) < batchSize {
		if _, err := tx.Exec(ctx, `UPDATE event_reminder_fanouts SET cursor_id=$2,completed_at=clock_timestamp() WHERE id=$1 AND completed_at IS NULL`, fanoutID, lastID); err != nil {
			return 0, fmt.Errorf("complete event reminder fanout page: %w", err)
		}
	} else if _, err := tx.Exec(ctx, `UPDATE event_reminder_fanouts SET cursor_id=$2 WHERE id=$1 AND completed_at IS NULL`, fanoutID, lastID); err != nil {
		return 0, fmt.Errorf("advance event reminder fanout cursor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit event reminder fanout page: %w", err)
	}
	return len(subscribers), nil
}

// CollectDueSalesTransitions turns scheduled sales into selling when the
// persisted start time arrives. The event row lock serializes the collector
// with admin sales changes and bounds each pass to the requested batch size.
func (r *PostgresEventInterestRepository) CollectDueSalesTransitions(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin scheduled sale transition collection: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id,title,category,venue,location,date,status,publication_status,sales_status,sales_start_at,sales_end_at,
		sales_generation,sale_notification_generation,clock_timestamp()
		FROM events WHERE publication_status='published' AND sales_status IN ('scheduled','selling') AND sales_generation>sale_notification_generation
		AND ((sales_start_at IS NOT NULL AND sales_start_at<=clock_timestamp())
			OR sales_end_at<=clock_timestamp() OR date<=clock_timestamp())
		ORDER BY COALESCE(sales_start_at,date),id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, fmt.Errorf("select due sale transitions: %w", err)
	}
	type transition struct {
		event                domain.Event
		start, end           *time.Time
		generation, notified int64
		now                  time.Time
	}
	var due []transition
	for rows.Next() {
		var item transition
		if err := rows.Scan(&item.event.ID, &item.event.Title, &item.event.Category, &item.event.Venue, &item.event.Location, &item.event.Date, &item.event.Status,
			&item.event.PublicationStatus, &item.event.SalesStatus, &item.start, &item.end, &item.generation, &item.notified, &item.now); err != nil {
			rows.Close()
			return 0, err
		}
		item.event.SalesStartAt, item.event.SalesEndAt = item.start, item.end
		due = append(due, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, item := range due {
		eligible, reason := domain.EventSaleEligibility(&item.event, item.now)
		if eligible {
			if _, err := tx.Exec(ctx, `UPDATE events SET sales_status='selling',sale_notification_generation=$2,
				first_sale_opened_at=COALESCE(first_sale_opened_at,clock_timestamp()),updated_at=clock_timestamp() WHERE id=$1`, item.event.ID, item.generation); err != nil {
				return 0, fmt.Errorf("mark scheduled sale active: %w", err)
			}
			if err := enqueueOnSaleAlerts(ctx, tx, item.event.ID, item.generation, saleGenerationTriggerID(item.event.ID, item.generation)); err != nil {
				return 0, fmt.Errorf("enqueue scheduled on-sale reminders: %w", err)
			}
			continue
		}
		if reason == "sales_ended" || reason == "event_ended" {
			if _, err := tx.Exec(ctx, `UPDATE events SET sales_status='ended',sale_notification_generation=$2,updated_at=clock_timestamp() WHERE id=$1`, item.event.ID, item.generation); err != nil {
				return 0, fmt.Errorf("mark ended scheduled sale: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit scheduled sale transition intents: %w", err)
	}
	return len(due), nil
}

// ProcessPendingReminderOutbox leases a bounded batch, publishes it, and marks
// rows only after Kafka acknowledges them. Notification deduplicates by ID and
// subscription version, so an ACK loss is safely retried.
func (r *PostgresEventInterestRepository) ProcessPendingReminderOutbox(ctx context.Context, limit int, publish func(context.Context, ReminderOutboxEvent) error) (int, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}
	claimToken := uuid.New()
	rows, err := r.pool.Query(ctx, `WITH candidates AS (
		SELECT o.id FROM event_reminder_outbox o WHERE o.published_at IS NULL AND o.suppressed_at IS NULL
		AND (o.lease_until IS NULL OR o.lease_until<=clock_timestamp())
		AND NOT EXISTS(SELECT 1 FROM event_reminder_outbox prior WHERE prior.subscription_id=o.subscription_id AND prior.published_at IS NULL AND prior.suppressed_at IS NULL
			AND (prior.created_at,prior.id)<(o.created_at,o.id))
		ORDER BY o.created_at,o.id LIMIT $1 FOR UPDATE SKIP LOCKED
		) UPDATE event_reminder_outbox o SET claim_token=$2,lease_until=clock_timestamp()+interval '30 seconds'
		FROM candidates c WHERE o.id=c.id
		RETURNING o.id,o.subscription_id,o.event_id,o.user_id,o.notification_type,o.action,o.subscription_version,o.trigger_id,o.payload,o.created_at`, limit, claimToken)
	if err != nil {
		return 0, err
	}
	var events []ReminderOutboxEvent
	for rows.Next() {
		var event ReminderOutboxEvent
		if err := rows.Scan(&event.ID, &event.SubscriptionID, &event.EventID, &event.UserID, &event.Type, &event.Action, &event.SubscriptionVersion, &event.TriggerID, &event.Payload, &event.CreatedAt); err != nil {
			rows.Close()
			return 0, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, event := range events {
		if err := publish(ctx, event); err != nil {
			_, _ = r.pool.Exec(ctx, `UPDATE event_reminder_outbox SET claim_token=NULL,lease_until=NULL WHERE id=$1 AND claim_token=$2`, event.ID, claimToken)
			return 0, fmt.Errorf("publish event reminder %s: %w", event.ID, err)
		}
		if _, err := r.pool.Exec(ctx, `UPDATE event_reminder_outbox SET published_at=clock_timestamp(),claim_token=NULL,lease_until=NULL WHERE id=$1 AND claim_token=$2`, event.ID, claimToken); err != nil {
			return 0, err
		}
	}
	return len(events), nil
}

type ReminderOutboxEvent struct {
	ID, SubscriptionID, EventID, UserID, TriggerID uuid.UUID
	Type, Action                                   string
	SubscriptionVersion                            int64
	Payload                                        []byte
	CreatedAt                                      time.Time
}

var _ EventInterestRepository = (*PostgresEventInterestRepository)(nil)
