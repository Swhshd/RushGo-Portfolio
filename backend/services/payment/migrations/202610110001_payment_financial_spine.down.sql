DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM payments WHERE managed) THEN
        RAISE EXCEPTION 'refusing to remove managed financial identities and responsibility; forward repair required';
    END IF;
END $$;

DROP INDEX payments_managed_due_work;
DROP INDEX payments_managed_provider_intent;
DROP INDEX payments_managed_logical_identity;
ALTER TABLE payments DROP CONSTRAINT payments_managed_identity_check;
ALTER TABLE payments
    DROP COLUMN managed, DROP COLUMN payment_generation, DROP COLUMN provider_account, DROP COLUMN livemode,
    DROP COLUMN request_hash, DROP COLUMN frozen_create, DROP COLUMN create_key, DROP COLUMN create_state,
    DROP COLUMN create_first_attempt_at, DROP COLUMN provider_outcome, DROP COLUMN provider_status,
    DROP COLUMN provider_observed_at, DROP COLUMN booking_disposition, DROP COLUMN cancel_key,
    DROP COLUMN cancel_state, DROP COLUMN cancel_first_attempt_at, DROP COLUMN reconciliation_state,
    DROP COLUMN refund_state, DROP COLUMN refund_reason, DROP COLUMN refund_decision_id,
    DROP COLUMN refund_amount_cents, DROP COLUMN refund_currency, DROP COLUMN refund_key,
    DROP COLUMN refund_provider_id, DROP COLUMN refund_provider_status, DROP COLUMN refund_requested_at,
    DROP COLUMN refund_first_attempt_at, DROP COLUMN refund_completed_at, DROP COLUMN operation_kind,
    DROP COLUMN claim_token, DROP COLUMN claim_fence, DROP COLUMN lease_until, DROP COLUMN due_at,
    DROP COLUMN attempts, DROP COLUMN last_error;
