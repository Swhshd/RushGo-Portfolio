DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM sagas WHERE recovery_managed OR recovery_state='attention') THEN
  RAISE EXCEPTION 'cannot remove retained Saga recovery responsibility';
 END IF;
END $$;
DROP INDEX saga_step_original_order;
ALTER TABLE saga_steps DROP CONSTRAINT saga_step_recovery_valid,
 DROP COLUMN recovery_managed,DROP COLUMN execute_state,DROP COLUMN compensate_state,
 DROP COLUMN execute_evidence,DROP COLUMN compensate_evidence,DROP COLUMN error_category;
DROP INDEX saga_due_recovery;
DROP INDEX saga_logical_generation;
ALTER TABLE sagas DROP CONSTRAINT saga_recovery_valid,DROP CONSTRAINT saga_claim_pair,
 DROP CONSTRAINT saga_ready_due,
 DROP COLUMN recovery_managed,DROP COLUMN lifecycle_generation,DROP COLUMN input_snapshot,
 DROP COLUMN input_fingerprint,DROP COLUMN recovery_state,DROP COLUMN direction,
 DROP COLUMN next_attempt_at,DROP COLUMN claim_token,DROP COLUMN claim_fence,DROP COLUMN lease_until,
 DROP COLUMN attempts,DROP COLUMN error_category,DROP COLUMN payment_id,DROP COLUMN wake_reason,
 DROP COLUMN wake_version,DROP COLUMN consumed_wake_version,
 DROP COLUMN last_obligation_id;
