DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM notification_inbox)
       OR EXISTS (SELECT 1 FROM notifications WHERE managed AND type IN (
           'tickets_issued','refund_requested','refund_approved','refund_rejected','refund_succeeded','manual_attention')) THEN
        RAISE EXCEPTION 'cannot remove Notification inbox while business notifications are retained';
    END IF;
END $$;

DROP TABLE notification_inbox;

ALTER TABLE notifications DROP CONSTRAINT notification_managed_identity;
ALTER TABLE notifications ADD CONSTRAINT notification_managed_identity CHECK (
 NOT managed OR (entity_id IS NOT NULL AND entity_id<>'00000000-0000-0000-0000-000000000000'::uuid
 AND transition_generation IS NOT NULL AND transition_generation=1
 AND payload_hash IS NOT NULL AND octet_length(payload_hash)=32
 AND delivery_id IS NOT NULL AND delivery_id=id
 AND delivery_fence IS NOT NULL AND delivery_fence>=0
 AND delivery_attempts IS NOT NULL AND delivery_attempts>=0
 AND channel='email' AND type IN ('booking_confirmation','booking_failure','booking_cancellation','welcome_email')
 AND status IN ('PENDING','DELIVERING','SENT','RETRYABLE','UNKNOWN','ATTENTION')
 AND (status IN ('SENT','ATTENTION') OR delivery_due_at IS NOT NULL)));
ALTER TABLE notifications DROP COLUMN locale;
