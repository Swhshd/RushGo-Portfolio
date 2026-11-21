BEGIN;
SET LOCAL lock_timeout = '2s';
-- Serialize the eligibility check and all DDL with activation and guarded writes.
LOCK TABLE booking_admission_mode, ticket_admission_intents IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM booking_admission_mode WHERE singleton = TRUE AND active)
       OR EXISTS (SELECT 1 FROM ticket_admission_intents)
       OR EXISTS (SELECT 1 FROM tickets WHERE admission_request_id IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot remove Booking admission fence after activation or intent creation' USING ERRCODE = '55000';
    END IF;
END;
$$;

DROP TRIGGER booking_guard_ticket_checkin_admission_trg ON tickets;
DROP FUNCTION booking_guard_ticket_checkin_admission();
DROP TRIGGER booking_lock_admission_mode_for_ticket_update_trg ON tickets;
DROP FUNCTION booking_lock_admission_mode_for_ticket_update();
DROP INDEX tickets_admission_request_idx;
ALTER TABLE tickets DROP COLUMN admission_request_id;
DROP TRIGGER booking_ticket_admission_intent_no_truncate_trg ON ticket_admission_intents;
DROP TRIGGER booking_ticket_admission_intent_guard_trg ON ticket_admission_intents;
DROP FUNCTION booking_ticket_admission_intent_guard();
DROP INDEX ticket_admission_intents_booking_state_idx;
DROP INDEX ticket_admission_intents_payment_state_idx;
DROP TABLE ticket_admission_intents;
DROP TRIGGER booking_admission_mode_no_truncate_trg ON booking_admission_mode;
DROP TRIGGER booking_admission_mode_no_delete_trg ON booking_admission_mode;
DROP FUNCTION booking_admission_mode_no_delete();
DROP TRIGGER booking_admission_mode_monotonic_trg ON booking_admission_mode;
DROP FUNCTION booking_admission_mode_monotonic();
DROP TABLE booking_admission_mode;
COMMIT;
