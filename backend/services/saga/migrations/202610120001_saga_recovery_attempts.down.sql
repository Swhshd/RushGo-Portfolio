DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM sagas WHERE recovery_managed OR recovery_state='attention') THEN
    RAISE EXCEPTION 'cannot remove retained Saga recovery responsibility';
  END IF;
END $$;
ALTER TABLE sagas
  DROP CONSTRAINT saga_failure_attempts_nonnegative,
  DROP COLUMN failure_attempts;
