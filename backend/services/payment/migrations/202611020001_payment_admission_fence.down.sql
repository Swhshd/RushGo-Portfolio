BEGIN;
SET LOCAL lock_timeout = '2s';
-- Keep activation/grant exclusion until the rollback has committed.
LOCK TABLE payment_admission_mode, payment_admission_permits IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM payment_admission_mode WHERE active) THEN
        RAISE EXCEPTION 'activated payment admission mode cannot be rolled back';
    END IF;
    IF EXISTS (SELECT 1 FROM payment_admission_permits) THEN
        RAISE EXCEPTION 'retained payment admission permits require explicit archival; refusing destructive down';
    END IF;
END;
$$;

DROP TRIGGER payment_refund_prepared_admission_guard ON payment_refund_audit;
DROP FUNCTION payment_refund_audit_admission_guard();
DROP TRIGGER payment_admission_permit_immutable ON payment_admission_permits;
DROP TRIGGER payment_admission_permit_no_truncate ON payment_admission_permits;
DROP FUNCTION payment_admission_permit_guard();
DROP TABLE payment_admission_permits;
DROP SEQUENCE payment_admission_fence_seq;
DROP TRIGGER payment_admission_mode_latch ON payment_admission_mode;
DROP TRIGGER payment_admission_mode_no_truncate ON payment_admission_mode;
DROP FUNCTION payment_admission_no_truncate();
DROP FUNCTION payment_admission_mode_monotonic();
DROP TABLE payment_admission_mode;
COMMIT;
