DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM booking_refund_requests)
       OR EXISTS (SELECT 1 FROM booking_refund_decisions)
       OR EXISTS (SELECT 1 FROM event_refund_approver_audit)
       OR EXISTS (SELECT 1 FROM event_refund_approvers)
       OR EXISTS (SELECT 1 FROM booking_refund_idempotency) THEN
        RAISE EXCEPTION 'Booking refund history is durable; use a forward repair instead of downgrading';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION booking_guard_ticket_checkin_admission() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    mode_active BOOLEAN;
    intent_found BOOLEAN;
BEGIN
    IF OLD.status <> 'ISSUED' OR NEW.status <> 'CHECKED_IN' THEN
        RETURN NEW;
    END IF;
    SELECT active INTO mode_active FROM booking_admission_mode WHERE singleton = TRUE FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Booking admission mode is unavailable' USING ERRCODE = '55000';
    END IF;
    IF NOT mode_active THEN
        RETURN NEW;
    END IF;
    IF OLD.admission_request_id IS NOT NULL OR NEW.admission_request_id IS NULL THEN
        RAISE EXCEPTION 'active Booking admission requires a new request link' USING ERRCODE = '23514';
    END IF;
    SELECT TRUE INTO intent_found
    FROM ticket_admission_intents i JOIN bookings b ON b.id=i.booking_id
    WHERE i.request_id=NEW.admission_request_id AND i.ticket_id=NEW.id AND i.booking_id=NEW.booking_id
      AND i.payment_id=NEW.payment_id AND i.event_id=NEW.event_id AND i.user_id=b.user_id AND i.event_id=b.event_id
      AND i.generation=NEW.lifecycle_generation AND i.state='PENDING' AND i.permit_token IS NOT NULL AND i.permit_fence>0
    FOR UPDATE OF i;
    IF NOT COALESCE(intent_found,FALSE) THEN
        RAISE EXCEPTION 'active Booking admission requires a matching pending permit' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS booking_refund_block_immutable_trg ON bookings;
DROP FUNCTION IF EXISTS booking_refund_block_immutable();
DROP TRIGGER IF EXISTS booking_refund_decisions_no_truncate_trg ON booking_refund_decisions;
DROP TRIGGER IF EXISTS booking_refund_decisions_immutable_trg ON booking_refund_decisions;
DROP FUNCTION IF EXISTS booking_refund_decision_immutable();
ALTER TABLE booking_refund_requests DROP CONSTRAINT IF EXISTS booking_refund_request_decision_fk;
ALTER TABLE bookings DROP COLUMN IF EXISTS refund_decision_id;
DROP TABLE IF EXISTS booking_refund_idempotency;
DROP TABLE IF EXISTS booking_refund_decisions;
DROP TABLE IF EXISTS booking_refund_requests;
DROP TABLE IF EXISTS event_refund_approver_audit;
DROP TABLE IF EXISTS event_refund_approvers;
DROP FUNCTION IF EXISTS booking_refund_append_only();
DROP FUNCTION IF EXISTS booking_refund_request_guard();
