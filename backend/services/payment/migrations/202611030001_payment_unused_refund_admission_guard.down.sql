DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM payments WHERE refund_reason = 'CONFIRMED_UNUSED_TICKET') THEN
        RAISE EXCEPTION 'cannot remove confirmed-unused Admission guard after a refund obligation exists; use a forward fix';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION payment_refund_audit_admission_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    admission_active BOOLEAN;
    locked_payment UUID;
BEGIN
    IF NEW.record_kind <> 'PREPARED' THEN
        RETURN NEW;
    END IF;

    SELECT active INTO admission_active
      FROM payment_admission_mode
     WHERE singleton = TRUE
     FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'payment admission mode latch is missing';
    END IF;
    IF NOT admission_active THEN
        RETURN NEW;
    END IF;

    SELECT id INTO locked_payment FROM payments WHERE id = NEW.payment_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'PREPARED refund has no Payment row';
    END IF;
    IF EXISTS (
        SELECT 1 FROM payment_admission_permits
         WHERE payment_id = NEW.payment_id AND state = 'PENDING'
    ) THEN
        RAISE EXCEPTION 'refund preparation waits for pending payment admission';
    END IF;
    RETURN NEW;
END;
$$;
