ALTER TABLE notifications DROP CONSTRAINT notification_managed_identity;
ALTER TABLE notifications ADD COLUMN locale VARCHAR(2) NOT NULL DEFAULT 'en' CHECK (locale IN ('en','zh'));
ALTER TABLE notifications ADD CONSTRAINT notification_managed_identity CHECK (
 NOT managed OR (entity_id IS NOT NULL AND entity_id<>'00000000-0000-0000-0000-000000000000'::uuid
 AND transition_generation IS NOT NULL AND transition_generation=1
 AND payload_hash IS NOT NULL AND octet_length(payload_hash)=32
 AND delivery_id IS NOT NULL AND delivery_id=id
 AND delivery_fence IS NOT NULL AND delivery_fence>=0
 AND delivery_attempts IS NOT NULL AND delivery_attempts>=0
 AND channel='email' AND type IN ('booking_confirmation','booking_failure','booking_cancellation','welcome_email',
   'tickets_issued','refund_requested','refund_approved','refund_rejected','refund_succeeded','manual_attention')
 AND status IN ('PENDING','DELIVERING','SENT','RETRYABLE','UNKNOWN','ATTENTION')
 AND (status IN ('SENT','ATTENTION') OR delivery_due_at IS NOT NULL)));

CREATE TABLE notification_inbox (
    notification_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    booking_id UUID NOT NULL,
    event_id UUID NOT NULL,
    type VARCHAR(40) NOT NULL CHECK (type IN (
        'tickets_issued','refund_requested','refund_approved','refund_rejected','refund_succeeded','manual_attention')),
    locale VARCHAR(2) NOT NULL CHECK (locale IN ('en','zh')),
    title TEXT NOT NULL CHECK (length(title) BETWEEN 1 AND 160),
    body TEXT NOT NULL CHECK (length(body) BETWEEN 1 AND 2000),
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash)=32),
    created_at TIMESTAMPTZ NOT NULL,
    read_at TIMESTAMPTZ
);

CREATE INDEX notification_inbox_owner_page
    ON notification_inbox(user_id, created_at DESC, notification_id DESC);
CREATE INDEX notification_inbox_unread_owner
    ON notification_inbox(user_id, created_at DESC, notification_id DESC)
    WHERE read_at IS NULL;
