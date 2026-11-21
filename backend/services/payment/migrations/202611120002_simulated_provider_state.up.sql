-- Only the admission_r4 build may use this local, synthetic provider ledger.
-- Production adapters never consult it for financial authorization.
CREATE TABLE simulated_provider_state (
 run_id UUID PRIMARY KEY,
 source_commit TEXT NOT NULL CHECK(source_commit ~ '^[0-9a-f]{40}$'),
 config_hash BYTEA NOT NULL CHECK(octet_length(config_hash)=32),
 revision BIGINT NOT NULL CHECK(revision>0),
 state JSONB NOT NULL CHECK(jsonb_typeof(state)='object'),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
