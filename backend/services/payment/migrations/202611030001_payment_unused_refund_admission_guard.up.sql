-- The new approved confirmed-ticket refund reason must never prepare a
-- Provider request after any Payment-owned admission was committed. Legacy
-- refund reasons retain their existing PENDING-only behavior.
CREATE OR REPLACE FUNCTION payment_refund_audit_admission_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    admission_active BOOLEAN;
    locked_payment UUID;
    refund_reason TEXT;
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

    SELECT id, payments.refund_reason INTO locked_payment, refund_reason
      FROM payments
     WHERE id = NEW.payment_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'PREPARED refund has no Payment row';
    END IF;

    IF refund_reason = 'CONFIRMED_UNUSED_TICKET' AND EXISTS (
        SELECT 1 FROM payment_admission_permits
         WHERE payment_id = NEW.payment_id AND state = 'COMMITTED'
    ) THEN
        RAISE EXCEPTION 'unused-ticket refund preparation conflicts with committed payment admission';
    END IF;

    IF NOT admission_active THEN
        RETURN NEW;
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

COMMENT ON FUNCTION payment_refund_audit_admission_guard() IS
    'PREPARED refund trigger: preserves PENDING waits and blocks confirmed-unused refunds after any COMMITTED admission.';
