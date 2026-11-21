DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM notifications WHERE managed OR last_error IS NOT NULL) THEN
  RAISE EXCEPTION 'retained notification responsibility or diagnostic prevents downgrade';
 END IF;
END $$;
DROP INDEX notification_delivery_due;
DROP INDEX notification_managed_semantic;
ALTER TABLE notifications DROP CONSTRAINT notification_managed_identity,
 DROP COLUMN managed,DROP COLUMN entity_id,DROP COLUMN transition_generation,DROP COLUMN payload_hash,
 DROP COLUMN delivery_id,DROP COLUMN delivery_token,DROP COLUMN delivery_fence,DROP COLUMN delivery_lease_until,
 DROP COLUMN delivery_due_at,DROP COLUMN delivery_attempts,DROP COLUMN last_error,DROP COLUMN provider_id;
