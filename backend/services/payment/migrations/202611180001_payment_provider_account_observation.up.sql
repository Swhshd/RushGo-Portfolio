CREATE TABLE payment_provider_account_observation (
    observation_id UUID PRIMARY KEY,
    schema_version SMALLINT NOT NULL CHECK (schema_version=1),
    attempt_id UUID NOT NULL,
    deployment_id TEXT NOT NULL,
    credential_source TEXT NOT NULL,
    credential_sha256 BYTEA NOT NULL CHECK (octet_length(credential_sha256)=32),
    endpoint TEXT NOT NULL CHECK (endpoint='https://api.stripe.com'),
    stripe_api_version TEXT NOT NULL,
    stripe_go_version TEXT NOT NULL,
    account_id TEXT NOT NULL CHECK (account_id ~ '^acct_[A-Za-z0-9]+$'),
    account_object TEXT NOT NULL CHECK (account_object='account'),
    account_livemode BOOLEAN NOT NULL CHECK (account_livemode=false),
    http_status SMALLINT NOT NULL CHECK (http_status=200),
    request_id TEXT NOT NULL CHECK (request_id ~ '^req_[A-Za-z0-9]+$'),
    expected_account TEXT NOT NULL CHECK (expected_account ~ '^acct_[A-Za-z0-9]+$'),
    result_class TEXT NOT NULL CHECK (result_class IN ('MATCH','MISMATCH')),
    observed_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    observation_digest BYTEA NOT NULL CHECK (octet_length(observation_digest)=32),
    UNIQUE (attempt_id, observation_id),
    UNIQUE (observation_digest),
    CHECK (result_class<>'MATCH' OR account_id=expected_account)
);

CREATE INDEX payment_provider_account_observation_attempt
    ON payment_provider_account_observation(attempt_id, recorded_at DESC, observation_id);
CREATE INDEX payment_provider_account_observation_deployment
    ON payment_provider_account_observation(deployment_id, recorded_at DESC, observation_id);
CREATE INDEX payment_provider_account_observation_account
    ON payment_provider_account_observation(account_id, observed_at DESC, observation_id);
CREATE INDEX payment_provider_account_observation_request
    ON payment_provider_account_observation(request_id, recorded_at DESC);

CREATE FUNCTION payment_provider_account_observation_reject_mutation()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'payment_provider_account_observation is append-only';
END;
$$;

CREATE TRIGGER payment_provider_account_observation_immutable
    BEFORE UPDATE OR DELETE ON payment_provider_account_observation
    FOR EACH ROW EXECUTE FUNCTION payment_provider_account_observation_reject_mutation();

CREATE TRIGGER payment_provider_account_observation_no_truncate
    BEFORE TRUNCATE ON payment_provider_account_observation
    FOR EACH STATEMENT EXECUTE FUNCTION payment_provider_account_observation_reject_mutation();
