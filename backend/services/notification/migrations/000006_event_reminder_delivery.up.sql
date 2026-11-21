ALTER TABLE notification_inbox
    ALTER COLUMN booking_id DROP NOT NULL,
    ADD COLUMN subscription_id UUID,
    ADD COLUMN subscription_version BIGINT;

ALTER TABLE notification_inbox DROP CONSTRAINT notification_inbox_type_check;
ALTER TABLE notification_inbox ADD CONSTRAINT notification_inbox_type_check CHECK (type IN (
    'tickets_issued','refund_requested','refund_approved','refund_rejected','refund_succeeded','manual_attention',
    'event_onsale','event_restock'));
ALTER TABLE notification_inbox ADD CONSTRAINT notification_inbox_subscription_identity CHECK (
    (type IN ('event_onsale','event_restock') AND booking_id IS NULL AND subscription_id IS NOT NULL AND subscription_version > 0)
    OR (type NOT IN ('event_onsale','event_restock') AND booking_id IS NOT NULL AND subscription_id IS NULL AND subscription_version IS NULL));

CREATE TABLE notification_event_reminder_state (
    subscription_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    event_id UUID NOT NULL,
    type VARCHAR(32) NOT NULL CHECK (type IN ('event_onsale','event_restock')),
    subscription_version BIGINT NOT NULL CHECK (subscription_version > 0),
    active_since_version BIGINT NOT NULL CHECK (active_since_version BETWEEN 1 AND subscription_version),
    notification_id UUID NOT NULL,
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash)=32),
    active BOOLEAN NOT NULL,
    email_opt_in BOOLEAN NOT NULL DEFAULT FALSE,
    email_consent_version VARCHAR(64) NOT NULL DEFAULT '',
    email_consent_id UUID NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT notification_event_reminder_email_consent CHECK (
        (NOT email_opt_in AND email_consent_version='' AND email_consent_id IS NULL)
        OR (email_opt_in AND email_consent_version='event-alerts-v1' AND email_consent_id IS NOT NULL))
);

ALTER TABLE notifications DROP CONSTRAINT notification_managed_identity;
ALTER TABLE notifications ADD CONSTRAINT notification_managed_identity CHECK (
 NOT managed OR (entity_id IS NOT NULL AND entity_id<>'00000000-0000-0000-0000-000000000000'::uuid
 AND transition_generation IS NOT NULL
 AND ((type IN ('event_onsale','event_restock') AND transition_generation>0) OR (type NOT IN ('event_onsale','event_restock') AND transition_generation=1))
 AND payload_hash IS NOT NULL AND octet_length(payload_hash)=32
 AND delivery_id IS NOT NULL AND delivery_id=id
 AND delivery_fence IS NOT NULL AND delivery_fence>=0
 AND delivery_attempts IS NOT NULL AND delivery_attempts>=0
 AND channel='email' AND type IN ('booking_confirmation','booking_failure','booking_cancellation','welcome_email',
   'tickets_issued','refund_requested','refund_approved','refund_rejected','refund_succeeded','manual_attention',
   'event_onsale','event_restock')
 AND status IN ('PENDING','DELIVERING','SENT','RETRYABLE','UNKNOWN','ATTENTION','CANCELLED')
 AND (status IN ('SENT','ATTENTION','CANCELLED') OR delivery_due_at IS NOT NULL)));

CREATE INDEX notification_event_reminder_state_event ON notification_event_reminder_state(event_id,type,active);
