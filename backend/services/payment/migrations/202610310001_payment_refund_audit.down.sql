DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM payment_refund_audit) THEN
        RAISE EXCEPTION 'retained refund provenance audits require explicit archival; refusing destructive down';
    END IF;
END $$;
DROP TABLE payment_refund_audit;
DROP FUNCTION payment_refund_audit_reject_mutation();
