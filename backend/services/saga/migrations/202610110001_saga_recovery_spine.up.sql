ALTER TABLE sagas
 ADD COLUMN recovery_managed BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN lifecycle_generation BIGINT,
 ADD COLUMN input_snapshot JSONB,
 ADD COLUMN input_fingerprint BYTEA,
 ADD COLUMN recovery_state TEXT NOT NULL DEFAULT 'legacy',
 ADD COLUMN direction TEXT,
 ADD COLUMN next_attempt_at TIMESTAMPTZ,
 ADD COLUMN claim_token UUID,
 ADD COLUMN claim_fence BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN lease_until TIMESTAMPTZ,
 ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0,
 ADD COLUMN error_category TEXT,
 ADD COLUMN payment_id UUID,
 ADD COLUMN wake_reason TEXT,
 ADD COLUMN wake_version BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN consumed_wake_version BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN last_obligation_id UUID,
 ADD CONSTRAINT saga_recovery_valid CHECK (NOT recovery_managed OR
   (lifecycle_generation IS NOT NULL AND lifecycle_generation=1 AND input_snapshot IS NOT NULL AND input_fingerprint IS NOT NULL
    AND octet_length(input_fingerprint)=32 AND status IS NOT NULL AND current_step_index IS NOT NULL
    AND current_step_index BETWEEN -1 AND 5 AND direction IS NOT NULL AND direction IN ('forward','compensate')
    AND recovery_state IN ('ready','attention'))),
 ADD CONSTRAINT saga_claim_pair CHECK ((claim_token IS NULL)=(lease_until IS NULL));
ALTER TABLE sagas ADD CONSTRAINT saga_ready_due CHECK (NOT recovery_managed OR recovery_state<>'ready' OR status IN ('COMPLETED','ROLLED_BACK') OR next_attempt_at IS NOT NULL);

CREATE UNIQUE INDEX saga_logical_generation ON sagas(booking_id,lifecycle_generation) WHERE recovery_managed;
CREATE INDEX saga_due_recovery ON sagas(next_attempt_at,id) WHERE recovery_managed AND recovery_state='ready';

ALTER TABLE saga_steps
 ADD COLUMN recovery_managed BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN execute_state TEXT NOT NULL DEFAULT 'pending',
 ADD COLUMN compensate_state TEXT NOT NULL DEFAULT 'pending',
 ADD COLUMN execute_evidence JSONB,
 ADD COLUMN compensate_evidence JSONB,
 ADD COLUMN error_category TEXT,
 ADD CONSTRAINT saga_step_recovery_valid CHECK (NOT recovery_managed OR
   (status IS NOT NULL AND "order" IS NOT NULL AND "order" BETWEEN 0 AND 4 AND should_pause_for_payment IS NOT NULL
    AND execute_state IN ('pending','intended','completed','retryable','attention')
    AND compensate_state IN ('pending','intended','completed','retryable','attention')));
CREATE UNIQUE INDEX saga_step_original_order ON saga_steps(saga_id,"order") WHERE recovery_managed;
