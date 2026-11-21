DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM booking_refund_approval_attempts) THEN
        RAISE EXCEPTION 'cannot remove durable Booking refund approval attempts';
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS booking_refund_request_attempt_guard_trg ON booking_refund_requests;
DROP FUNCTION IF EXISTS booking_refund_request_attempt_guard();
DROP TRIGGER IF EXISTS booking_refund_decision_attempt_guard_trg ON booking_refund_decisions;
DROP FUNCTION IF EXISTS booking_refund_decision_attempt_guard();
DROP TRIGGER IF EXISTS booking_refund_approval_attempt_no_truncate_trg ON booking_refund_approval_attempts;
DROP TRIGGER IF EXISTS booking_refund_approval_attempt_guard_trg ON booking_refund_approval_attempts;
DROP FUNCTION IF EXISTS booking_refund_approval_attempt_guard();

ALTER TABLE booking_refund_decisions
    DROP CONSTRAINT IF EXISTS booking_refund_decision_qualification_pair_check,
    DROP CONSTRAINT IF EXISTS booking_refund_decision_qualification_attempt_fk,
    DROP COLUMN IF EXISTS event_qualified_at,
    DROP COLUMN IF EXISTS qualification_attempt_id;
ALTER TABLE booking_refund_requests
    DROP CONSTRAINT IF EXISTS booking_refund_request_qualification_pair_check,
    DROP CONSTRAINT IF EXISTS booking_refund_request_qualification_attempt_fk,
    DROP COLUMN IF EXISTS event_qualified_at,
    DROP COLUMN IF EXISTS qualification_attempt_id;

DROP INDEX IF EXISTS booking_refund_approval_attempt_recovery;
DROP INDEX IF EXISTS booking_refund_one_approval_attempt_per_request;
DROP TABLE IF EXISTS booking_refund_approval_attempts;
