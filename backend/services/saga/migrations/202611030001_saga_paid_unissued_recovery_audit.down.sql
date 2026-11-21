-- Recovery audits are durable operational history; archive them explicitly before removal.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM saga_paid_unissued_recovery_audit) THEN
        RAISE EXCEPTION 'retained paid-unissued recovery audits require explicit archival; refusing destructive down';
    END IF;
END $$;
DROP TABLE saga_paid_unissued_recovery_audit;
