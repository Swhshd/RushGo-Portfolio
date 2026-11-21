CREATE TABLE ticket_operation_idempotency (
    actor_user_id UUID NOT NULL,
    idempotency_key_digest BYTEA NOT NULL CHECK (octet_length(idempotency_key_digest)=32),
    operation TEXT NOT NULL CHECK (operation IN ('ROTATE_CREDENTIAL','CHECK_IN')),
    request_fingerprint BYTEA NOT NULL CHECK (octet_length(request_fingerprint)=32),
    ticket_id UUID NOT NULL,
    event_id UUID,
    result_code TEXT,
    credential_version BIGINT,
    credential_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (actor_user_id,idempotency_key_digest),
    CONSTRAINT ticket_operation_idempotency_ticket_fk FOREIGN KEY (ticket_id) REFERENCES tickets(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ticket_operation_idempotency_event_ck CHECK (
        (operation='ROTATE_CREDENTIAL' AND event_id IS NULL)
        OR (operation='CHECK_IN' AND event_id IS NOT NULL)
    ),
    CONSTRAINT ticket_operation_idempotency_result_ck CHECK (
        (completed_at IS NULL AND result_code IS NULL AND credential_version IS NULL AND credential_expires_at IS NULL)
        OR (
            completed_at IS NOT NULL
            AND result_code IS NOT NULL
            AND (
                (operation='ROTATE_CREDENTIAL' AND result_code='ROTATED' AND credential_version IS NOT NULL AND credential_version>0 AND credential_expires_at IS NOT NULL)
                OR (operation='CHECK_IN' AND result_code IN ('accepted','denied','already_checked_in') AND credential_version IS NULL AND credential_expires_at IS NULL)
            )
        )
    )
);

CREATE INDEX ticket_operation_idempotency_ticket_idx
    ON ticket_operation_idempotency(ticket_id,operation,created_at DESC);
