package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ticketbox/notification/internal/domain"
)

func rollbackNotification(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (r *PostgresNotificationRepository) AcceptIntent(ctx context.Context, key domain.SemanticKey, n *domain.Notification, hash [32]byte) (*domain.Notification, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if n == nil {
		return nil, false, domain.ErrInvalidIntent
	}
	payload, err := payloadWithRecipient(n.Payload, n.Recipient)
	if err != nil {
		return nil, false, domain.ErrInvalidIntent
	}
	actual, canonical, computed, err := domain.PrepareIntent(n.Type, payload)
	if err != nil || actual != key || n.ID != canonical.ID || n.Recipient != canonical.Recipient || n.Channel != key.Channel || hash != computed {
		return nil, false, domain.ErrInvalidIntent
	}
	if n.Locale != "zh" {
		n.Locale = "en"
	}
	payload, err = json.Marshal(canonical.Payload)
	if err != nil {
		return nil, false, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer rollbackNotification(tx)
	// Serialize acceptance and legacy classification for this stable identity.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, n.ID.String()); err != nil {
		return nil, false, err
	}
	var ambiguous bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM notifications WHERE NOT managed AND type=$1 AND channel=$2 AND `+legacyEntitySQL+`=$4 AND id<>$5)`, key.Type, key.Channel, entityField(key.Type), strings.ReplaceAll(key.EntityID.String(), "-", ""), n.ID).Scan(&ambiguous); err != nil {
		return nil, false, err
	}
	if ambiguous {
		if _, err := tx.Exec(ctx, `UPDATE notifications SET last_error='legacy_semantic_history_attention' WHERE NOT managed AND type=$1 AND channel=$2 AND `+legacyEntitySQL+`=$4`, key.Type, key.Channel, entityField(key.Type), strings.ReplaceAll(key.EntityID.String(), "-", "")); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		return nil, false, ErrLegacyAttention
	}
	result, err := tx.Exec(ctx, `INSERT INTO notifications(id,type,recipient,channel,locale,payload,status,created_at,managed,entity_id,transition_generation,payload_hash,delivery_id,delivery_fence,delivery_due_at,delivery_attempts)
	 VALUES($1,$2,$3,$4,$5,$6,'PENDING',$7,true,$8,$9,$10,$1,0,clock_timestamp(),0) ON CONFLICT DO NOTHING`, n.ID, key.Type, n.Recipient, key.Channel, n.Locale, payload, n.CreatedAt, key.EntityID, key.Generation, hash[:])
	if err != nil {
		return nil, false, fmt.Errorf("accept notification intent: %w", err)
	}
	var existing domain.Notification
	var raw, storedHash []byte
	err = tx.QueryRow(ctx, `SELECT id,type,recipient,channel,locale,payload,status,sent_at,created_at,managed,payload_hash FROM notifications WHERE id=$1 FOR UPDATE`, n.ID).Scan(&existing.ID, &existing.Type, &existing.Recipient, &existing.Channel, &existing.Locale, &raw, &existing.Status, &existing.SentAt, &existing.CreatedAt, &existing.Managed, &storedHash)
	if err != nil {
		return nil, false, fmt.Errorf("reload notification intent: %w", err)
	}
	if err := json.Unmarshal(raw, &existing.Payload); err != nil {
		return nil, false, err
	}
	conflict := existing.Type != key.Type || existing.Channel != key.Channel
	if existing.Managed {
		existingInput, encodeErr := payloadWithRecipient(existing.Payload, existing.Recipient)
		var existingBusinessHash [32]byte
		var existingCanonical *domain.Notification
		if encodeErr == nil {
			_, existingCanonical, existingBusinessHash, err = domain.PrepareIntent(existing.Type, existingInput)
			encodeErr = err
		}
		legacyHash, legacyHashErr := legacyRecipientPayloadHash(existing.Recipient, existing.Payload)
		storedHashMatches := bytes.Equal(storedHash, existingBusinessHash[:]) ||
			legacyHashErr == nil && bytes.Equal(storedHash, legacyHash)
		conflict = conflict || encodeErr != nil || existingBusinessHash != hash ||
			existingCanonical == nil || existingCanonical.ID != n.ID || !storedHashMatches
	} else {
		existingInput, encodeErr := payloadWithRecipient(existing.Payload, existing.Recipient)
		var legacy *domain.Notification
		var legacyHash [32]byte
		if encodeErr == nil {
			_, legacy, legacyHash, err = domain.PrepareIntent(existing.Type, existingInput)
			encodeErr = err
		}
		conflict = conflict || encodeErr != nil || legacyHash != hash || legacy == nil || legacy.ID != n.ID
	}
	if conflict {
		if _, err := tx.Exec(ctx, `UPDATE notifications SET last_error='semantic_payload_conflict' WHERE id=$1`, existing.ID); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		return &existing, false, ErrSemanticConflict
	}
	if !existing.Managed && existing.Status != "SENT" {
		if _, err := tx.Exec(ctx, `UPDATE notifications SET last_error='legacy_semantic_history_attention' WHERE id=$1`, existing.ID); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		return &existing, false, ErrLegacyAttention
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("notification acceptance commit outcome unknown: %w", err)
	}
	return &existing, result.RowsAffected() == 1, nil
}

func payloadWithRecipient(payload map[string]any, recipient string) ([]byte, error) {
	if payload == nil || recipient == "" || len(recipient) > 255 {
		return nil, domain.ErrInvalidIntent
	}
	withRecipient := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		withRecipient[key] = value
	}
	withRecipient["email"] = recipient
	return json.Marshal(withRecipient)
}

// Historical semantic rows hashed both recipient and payload. Accept either
// that old digest or the new business-only digest while still recomputing and
// comparing immutable business facts from the persisted payload.
func legacyRecipientPayloadHash(recipient string, payload map[string]any) ([]byte, error) {
	encoded, err := json.Marshal(struct {
		Recipient string         `json:"recipient"`
		Payload   map[string]any `json:"payload"`
	}{recipient, payload})
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(encoded)
	return hash[:], nil
}

func entityField(typ string) string {
	if typ == "welcome_email" {
		return "user_id"
	}
	return "booking_id"
}

// Restrict normalization to valid UUID spellings before comparison. Invalid or
// zero legacy identities remain unclassified; no unsafe cast can abort a scan.
const legacyEntitySQL = `CASE WHEN jsonb_typeof(payload->$3)='string' AND payload->>$3 ~* '^(urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{32})$'
 THEN translate(regexp_replace(lower(payload->>$3),'^urn:uuid:','','i'),'-{}','') ELSE NULL END`
