ALTER TABLE sagas
  ADD COLUMN failure_attempts INTEGER NOT NULL DEFAULT 0;

ALTER TABLE sagas
  ADD CONSTRAINT saga_failure_attempts_nonnegative CHECK (failure_attempts >= 0);
