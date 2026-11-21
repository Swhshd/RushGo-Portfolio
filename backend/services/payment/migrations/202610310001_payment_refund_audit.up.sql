CREATE TABLE payment_refund_audit (
    record_id UUID PRIMARY KEY,
    schema_version SMALLINT NOT NULL CHECK (schema_version=1),
    payment_id UUID,
    booking_id UUID,
    generation BIGINT,
    command_digest BYTEA CHECK (command_digest IS NULL OR octet_length(command_digest)=32),
    observation_digest BYTEA CHECK (observation_digest IS NULL OR octet_length(observation_digest)=32),
    application_attempt_id UUID,
    record_kind TEXT NOT NULL CHECK (record_kind IN ('PREPARED','RESPONSE','VERIFIED_EVENT')),
    refund_decision_id UUID,
    provider TEXT,
    intended_account TEXT,
    intended_livemode BOOLEAN,
    scope_source TEXT,
    target_amount_cents BIGINT CHECK (target_amount_cents IS NULL OR target_amount_cents>=0),
    currency TEXT,
    payment_intent_id TEXT,
    idempotency_key_digest BYTEA CHECK (idempotency_key_digest IS NULL OR octet_length(idempotency_key_digest)=32),
    claim_token_digest BYTEA CHECK (claim_token_digest IS NULL OR octet_length(claim_token_digest)=32),
    claim_fence BIGINT CHECK (claim_fence IS NULL OR claim_fence>=0),
    provider_request_id TEXT,
    provider_refund_id TEXT,
    provider_event_id TEXT,
    observed_at TIMESTAMPTZ,
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload)='object' AND payload ? 'version' AND payload->>'version'='1'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (record_kind<>'PREPARED' OR (
        payment_id IS NOT NULL AND booking_id IS NOT NULL AND generation IS NOT NULL AND generation>0 AND
        command_digest IS NOT NULL AND application_attempt_id IS NOT NULL AND refund_decision_id IS NOT NULL AND
        observation_digest IS NULL AND
        provider IS NOT NULL AND intended_account IS NOT NULL AND intended_livemode IS NOT NULL AND
        scope_source IS NOT NULL AND target_amount_cents IS NOT NULL AND target_amount_cents>0 AND currency IS NOT NULL AND
        payment_intent_id IS NOT NULL AND idempotency_key_digest IS NOT NULL AND
        claim_token_digest IS NOT NULL AND claim_fence IS NOT NULL AND claim_fence>0 AND
        payload ?& ARRAY['version','stage'] AND payload-ARRAY['version','stage']='{}'::jsonb
    )),
    CHECK (record_kind<>'RESPONSE' OR (
        payment_id IS NOT NULL AND booking_id IS NOT NULL AND generation IS NOT NULL AND generation>0 AND
        command_digest IS NOT NULL AND application_attempt_id IS NOT NULL AND refund_decision_id IS NOT NULL AND
        observation_digest IS NULL AND
        provider IS NOT NULL AND intended_account IS NOT NULL AND intended_livemode IS NOT NULL AND
        scope_source IS NOT NULL AND target_amount_cents IS NOT NULL AND target_amount_cents>0 AND currency IS NOT NULL AND
        payment_intent_id IS NOT NULL AND idempotency_key_digest IS NOT NULL AND
        claim_token_digest IS NOT NULL AND claim_fence IS NOT NULL AND claim_fence>0 AND
        payload ?& ARRAY['version','classification','transport_observation_state','observed_transport_calls','transport_observations_truncated'] AND
        payload-ARRAY['version','classification','error_category','financial_finish_outcome','provider_request_id','refund_id','charge_id',
            'payment_intent_id','provider_status','currency','amount_cents','observed_provider_account','observed_live_mode',
            'summary_source','observed_at','transport_observation_state','observed_transport_calls','transport_observations_truncated',
            'transport_observations','conflicting_fields']='{}'::jsonb
    )),
    CHECK (record_kind<>'VERIFIED_EVENT' OR (
        observation_digest IS NOT NULL AND
        payload ?& ARRAY['version','receipt_id','event_id','link_classification'] AND
        payload-ARRAY['version','receipt_id','event_id','event_type','object_type','object_id','provider_request_id','idempotency_key_digest',
            'observed_provider_account','observed_live_mode','refund_id','charge_id','payment_intent_id','provider_status','currency',
            'amount_cents','link_classification','link_source','event_created_at','observed_at']='{}'::jsonb
    ))
);

CREATE INDEX payment_refund_audit_payment_generation_recorded
    ON payment_refund_audit(payment_id,generation,recorded_at DESC,record_id);
CREATE INDEX payment_refund_audit_booking_generation_recorded
    ON payment_refund_audit(booking_id,generation,recorded_at DESC,record_id);
CREATE INDEX payment_refund_audit_command_recorded
    ON payment_refund_audit(command_digest,recorded_at DESC,record_id)
    WHERE command_digest IS NOT NULL;
CREATE INDEX payment_refund_audit_attempt_recorded
    ON payment_refund_audit(application_attempt_id,recorded_at DESC,record_id)
    WHERE application_attempt_id IS NOT NULL;
CREATE INDEX payment_refund_audit_request_recorded
    ON payment_refund_audit(provider_request_id,recorded_at DESC,record_id)
    WHERE provider_request_id IS NOT NULL;
CREATE INDEX payment_refund_audit_refund_recorded
    ON payment_refund_audit(provider_refund_id,recorded_at DESC,record_id)
    WHERE provider_refund_id IS NOT NULL;
CREATE INDEX payment_refund_audit_event_recorded
    ON payment_refund_audit(provider_event_id,recorded_at DESC,record_id)
    WHERE provider_event_id IS NOT NULL;
CREATE INDEX payment_refund_audit_event_observation
    ON payment_refund_audit(provider_event_id,observation_digest,recorded_at DESC)
    WHERE provider_event_id IS NOT NULL AND observation_digest IS NOT NULL;
CREATE INDEX payment_refund_audit_kind_recorded
    ON payment_refund_audit(record_kind,recorded_at DESC,record_id);

CREATE FUNCTION payment_refund_audit_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'payment_refund_audit is append-only';
END;
$$;

CREATE TRIGGER payment_refund_audit_immutable
    BEFORE UPDATE OR DELETE ON payment_refund_audit
    FOR EACH ROW EXECUTE FUNCTION payment_refund_audit_reject_mutation();
