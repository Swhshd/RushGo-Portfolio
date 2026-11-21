DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM notification_inbox WHERE type IN ('event_onsale','event_restock'))
       OR EXISTS (SELECT 1 FROM notifications WHERE type IN ('event_onsale','event_restock'))
       OR EXISTS (SELECT 1 FROM notification_event_reminder_state) THEN
        RAISE EXCEPTION 'cannot roll back event reminders while reminder data exists';
    END IF;
END $$;

DROP INDEX notification_event_reminder_state_event;
DROP TABLE notification_event_reminder_state;
ALTER TABLE notifications DROP CONSTRAINT notification_managed_identity;
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
ALTER TABLE notification_inbox DROP CONSTRAINT notification_inbox_subscription_identity;
ALTER TABLE notification_inbox DROP CONSTRAINT notification_inbox_type_check;
ALTER TABLE notification_inbox ADD CONSTRAINT notification_inbox_type_check CHECK (type IN (
    'tickets_issued','refund_requested','refund_approved','refund_rejected','refund_succeeded','manual_attention'));
ALTER TABLE notification_inbox DROP COLUMN subscription_version, DROP COLUMN subscription_id;
ALTER TABLE notification_inbox ALTER COLUMN booking_id SET NOT NULL;
