DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM outbound_events WHERE config_id IS NOT NULL) THEN
 RAISE EXCEPTION 'retained Scheduler outbound responsibility prevents downgrade';
 END IF;
END $$;
DROP INDEX scheduler_outbound_due;
DROP INDEX scheduler_outbound_version;
ALTER TABLE outbound_events DROP CONSTRAINT scheduler_outbound_identity,
 DROP COLUMN config_id,DROP COLUMN config_version,DROP COLUMN claim_token,DROP COLUMN claim_fence,
 DROP COLUMN lease_until,DROP COLUMN due_at,DROP COLUMN attempts,DROP COLUMN last_error;
