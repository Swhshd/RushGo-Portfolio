package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/pkg/eventreminder"
)

// AcceptEventReminder advances the per-subscription version fence and writes
// the in-app record/email intent in one transaction. The event bus envelope
// never includes a recipient address; a verified address is supplied only by
// Notification's User lookup after explicit consent was validated.
func (r *PostgresNotificationRepository) AcceptEventReminder(ctx context.Context, event *eventreminder.Envelope, hash [32]byte, rendered domain.InboxTemplate, recipient string, emailVerified bool) (bool, error) {
	if event == nil || event.NotificationID == uuid.Nil || event.SubscriptionID == uuid.Nil || event.UserID == uuid.Nil || event.EventID == uuid.Nil ||
		event.SubscriptionVersion < 1 || event.Action != eventreminder.ActionTrigger && event.Action != eventreminder.ActionWithdrawn && event.Action != eventreminder.ActionConsentUpdated && event.Action != eventreminder.ActionResubscribed ||
		event.Type != eventreminder.TypeOnSale && event.Type != eventreminder.TypeRestock ||
		event.EmailOptIn && (event.EmailConsentVersion != eventreminder.ConsentVersion || event.EmailConsentID == nil || *event.EmailConsentID == uuid.Nil) ||
		!event.EmailOptIn && (event.EmailConsentVersion != "" || event.EmailConsentID != nil) ||
		emailVerified && (!event.EmailOptIn || recipient == "") || rendered.Locale != "en" && rendered.Locale != "zh" {
		return false, domain.ErrInvalidEventReminder
	}
	canonical, err := json.Marshal(event)
	if err != nil {
		return false, domain.ErrInvalidEventReminder
	}
	_, actualHash, err := domain.ParseEventReminder(canonical)
	if err != nil || actualHash != hash {
		return false, domain.ErrInvalidEventReminder
	}
	if event.Action == eventreminder.ActionTrigger && (rendered.Title == "" || rendered.Body == "") {
		return false, domain.ErrInvalidEventReminder
	}

	ctx, cancel := context.WithTimeout(ctx, inboxQueryTimeout)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackNotification(tx)

	active := event.Action != eventreminder.ActionWithdrawn
	var consentID any
	if event.EmailConsentID != nil {
		consentID = *event.EmailConsentID
	}
	tag, err := tx.Exec(ctx, `INSERT INTO notification_event_reminder_state
		(subscription_id,user_id,event_id,type,subscription_version,active_since_version,notification_id,payload_hash,active,email_opt_in,email_consent_version,email_consent_id,updated_at)
		VALUES($1,$2,$3,$4,$5,$12,$6,$7,$8,$9,$10,$11,clock_timestamp())
		ON CONFLICT(subscription_id) DO UPDATE SET user_id=EXCLUDED.user_id,event_id=EXCLUDED.event_id,type=EXCLUDED.type,
		 subscription_version=EXCLUDED.subscription_version,active_since_version=EXCLUDED.active_since_version,
		 notification_id=EXCLUDED.notification_id,payload_hash=EXCLUDED.payload_hash,
		 active=EXCLUDED.active,email_opt_in=EXCLUDED.email_opt_in,email_consent_version=EXCLUDED.email_consent_version,email_consent_id=EXCLUDED.email_consent_id,updated_at=clock_timestamp()
		WHERE notification_event_reminder_state.user_id=EXCLUDED.user_id
		 AND notification_event_reminder_state.event_id=EXCLUDED.event_id
		 AND notification_event_reminder_state.type=EXCLUDED.type
		 AND notification_event_reminder_state.subscription_version < EXCLUDED.subscription_version`,
		event.SubscriptionID, event.UserID, event.EventID, event.Type, event.SubscriptionVersion, event.NotificationID, hash[:], active, event.EmailOptIn, event.EmailConsentVersion, consentID, event.ActiveSinceVersion)
	if err != nil {
		return false, fmt.Errorf("persist event reminder version fence: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var owner, eventID, notificationID uuid.UUID
		var typ string
		var version, activeSince int64
		var existingHash []byte
		var stateActive, stateEmailOptIn bool
		var stateConsentVersion string
		var stateConsentID *uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT user_id,event_id,type,subscription_version,active_since_version,notification_id,payload_hash,active,email_opt_in,email_consent_version,email_consent_id
			FROM notification_event_reminder_state WHERE subscription_id=$1 FOR UPDATE`, event.SubscriptionID).
			Scan(&owner, &eventID, &typ, &version, &activeSince, &notificationID, &existingHash, &stateActive, &stateEmailOptIn, &stateConsentVersion, &stateConsentID); err != nil {
			return false, fmt.Errorf("reload event reminder version fence: %w", err)
		}
		if owner != event.UserID || eventID != event.EventID || typ != event.Type {
			return false, ErrSemanticConflict
		}
		if version == event.SubscriptionVersion {
			if notificationID != event.NotificationID || !bytes.Equal(existingHash, hash[:]) {
				return false, ErrSemanticConflict
			}
			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("commit duplicate event reminder: %w", err)
			}
			return false, nil
		}
		if event.SubscriptionVersion < version && event.Action == eventreminder.ActionTrigger && stateActive && event.SubscriptionVersion >= activeSince {
			emailAllowed := emailVerified && stateEmailOptIn && event.EmailOptIn && stateConsentVersion == event.EmailConsentVersion &&
				stateConsentID != nil && event.EmailConsentID != nil && *stateConsentID == *event.EmailConsentID
			created, persistErr := persistEventReminderTrigger(ctx, tx, event, hash, rendered, recipient, emailAllowed)
			if persistErr != nil {
				return false, persistErr
			}
			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("commit out-of-order event reminder: %w", err)
			}
			return created, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit suppressed event reminder: %w", err)
		}
		return false, nil
	}

	// A withdrawal or email-consent revocation cancels work that has not been
	// claimed by the SMTP worker. DELIVERING/UNKNOWN is left intact because the
	// external SMTP outcome may already be irreversible or ambiguous.
	if event.Action != eventreminder.ActionTrigger {
		if _, err := tx.Exec(ctx, `UPDATE notifications SET status='CANCELLED',delivery_due_at=NULL,delivery_token=NULL,delivery_lease_until=NULL,
			last_error='consent_withdrawn' WHERE managed AND entity_id=$1 AND type=$2 AND status IN ('PENDING','RETRYABLE')`,
			event.SubscriptionID, event.Type); err != nil {
			return false, fmt.Errorf("cancel withdrawn event reminder emails: %w", err)
		}
	}

	if event.Action == eventreminder.ActionTrigger {
		if _, err := persistEventReminderTrigger(ctx, tx, event, hash, rendered, recipient, emailVerified); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit event reminder intent: %w", err)
	}
	return true, nil
}

func persistEventReminderTrigger(ctx context.Context, tx pgx.Tx, event *eventreminder.Envelope, hash [32]byte, rendered domain.InboxTemplate, recipient string, sendEmail bool) (bool, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO notification_inbox(notification_id,user_id,booking_id,event_id,subscription_id,subscription_version,type,locale,title,body,payload_hash,created_at)
		VALUES($1,$2,NULL,$3,$4,$5,$6,$7,$8,$9,$10,clock_timestamp()) ON CONFLICT(notification_id) DO NOTHING`,
		event.NotificationID, event.UserID, event.EventID, event.SubscriptionID, event.SubscriptionVersion, event.Type, rendered.Locale, rendered.Title, rendered.Body, hash[:])
	if err != nil {
		return false, fmt.Errorf("persist event reminder inbox: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var owner uuid.UUID
		var existingHash []byte
		if err := tx.QueryRow(ctx, `SELECT user_id,payload_hash FROM notification_inbox WHERE notification_id=$1 FOR UPDATE`, event.NotificationID).Scan(&owner, &existingHash); err != nil {
			return false, fmt.Errorf("reload duplicate event reminder inbox: %w", err)
		}
		if owner != event.UserID || !bytes.Equal(existingHash, hash[:]) {
			return false, ErrSemanticConflict
		}
		return false, nil
	}
	if sendEmail {
		payload, err := json.Marshal(event)
		if err != nil {
			return false, domain.ErrInvalidEventReminder
		}
		_, err = tx.Exec(ctx, `INSERT INTO notifications(id,type,recipient,channel,locale,payload,status,created_at,managed,entity_id,transition_generation,payload_hash,delivery_id,delivery_fence,delivery_due_at,delivery_attempts)
			VALUES($1,$2,$3,'email',$4,$5,'PENDING',clock_timestamp(),true,$6,$7,$8,$1,0,clock_timestamp(),0)`,
			event.NotificationID, event.Type, recipient, rendered.Locale, payload, event.SubscriptionID, event.SubscriptionVersion, hash[:])
		if err != nil {
			return false, fmt.Errorf("persist event reminder email: %w", err)
		}
	}
	return true, nil
}

func (r *PostgresNotificationRepository) CancelEventReminderEmail(ctx context.Context, subscriptionID uuid.UUID, typ string) error {
	if subscriptionID == uuid.Nil || typ != eventreminder.TypeOnSale && typ != eventreminder.TypeRestock {
		return domain.ErrInvalidEventReminder
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := r.pool.Exec(ctx, `UPDATE notifications SET status='CANCELLED',delivery_due_at=NULL,delivery_token=NULL,delivery_lease_until=NULL,
		last_error='consent_withdrawn' WHERE managed AND entity_id=$1 AND type=$2 AND status IN ('PENDING','RETRYABLE')`, subscriptionID, typ)
	return err
}

// EventReminderEmailEligible rechecks current consent immediately before the
// existing SMTP worker sends a claimed reminder. It fails closed if its
// immutable event payload or current subscription consent cannot be verified.
func (r *PostgresNotificationRepository) EventReminderEmailEligible(ctx context.Context, notification *domain.Notification) (bool, error) {
	if notification == nil || notification.ID == uuid.Nil || notification.Type != eventreminder.TypeOnSale && notification.Type != eventreminder.TypeRestock {
		return false, domain.ErrInvalidEventReminder
	}
	raw, err := json.Marshal(notification.Payload)
	if err != nil {
		return false, domain.ErrInvalidEventReminder
	}
	event, _, err := domain.ParseEventReminder(raw)
	if err != nil || event.Action != eventreminder.ActionTrigger || event.Type != notification.Type || !event.EmailOptIn || event.EmailConsentVersion != eventreminder.ConsentVersion || event.EmailConsentID == nil {
		return false, domain.ErrInvalidEventReminder
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var eligible bool
	err = r.pool.QueryRow(ctx, `SELECT active AND email_opt_in AND email_consent_version=$2 AND email_consent_id=$3 AND user_id=$4 AND event_id=$5 AND type=$6
		FROM notification_event_reminder_state WHERE subscription_id=$1`, event.SubscriptionID, event.EmailConsentVersion, *event.EmailConsentID, event.UserID, event.EventID, event.Type).Scan(&eligible)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return eligible, err
}

var _ EventReminderRepository = (*PostgresNotificationRepository)(nil)
var _ EventReminderDeliveryGate = (*PostgresNotificationRepository)(nil)
