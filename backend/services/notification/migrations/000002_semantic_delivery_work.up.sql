ALTER TABLE notifications
 ADD COLUMN managed boolean NOT NULL DEFAULT false,
 ADD COLUMN entity_id uuid,
 ADD COLUMN transition_generation bigint,
 ADD COLUMN payload_hash bytea,
 ADD COLUMN delivery_id uuid,
 ADD COLUMN delivery_token uuid,
 ADD COLUMN delivery_fence bigint,
 ADD COLUMN delivery_lease_until timestamptz,
 ADD COLUMN delivery_due_at timestamptz,
 ADD COLUMN delivery_attempts integer,
 ADD COLUMN last_error text,
 ADD COLUMN provider_id text;

ALTER TABLE notifications ADD CONSTRAINT notification_managed_identity CHECK (
 NOT managed OR (entity_id IS NOT NULL AND entity_id<>'00000000-0000-0000-0000-000000000000'::uuid
 AND transition_generation IS NOT NULL AND transition_generation=1
 AND payload_hash IS NOT NULL AND octet_length(payload_hash)=32
 AND delivery_id IS NOT NULL AND delivery_id=id
 AND delivery_fence IS NOT NULL AND delivery_fence>=0
 AND delivery_attempts IS NOT NULL AND delivery_attempts>=0
 AND delivery_due_at IS NOT NULL AND channel='email'
 AND type IN ('booking_confirmation','booking_failure','booking_cancellation','welcome_email')
 AND status IN ('PENDING','DELIVERING','SENT','RETRYABLE','UNKNOWN','ATTENTION')));
CREATE UNIQUE INDEX notification_managed_semantic ON notifications(type,entity_id,transition_generation,channel) WHERE managed;
CREATE INDEX notification_delivery_due ON notifications(delivery_due_at,id) WHERE managed AND status IN ('PENDING','DELIVERING','RETRYABLE','UNKNOWN');
