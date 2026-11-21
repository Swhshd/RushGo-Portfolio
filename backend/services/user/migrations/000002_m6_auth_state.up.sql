ALTER TABLE users ADD COLUMN email_canonical VARCHAR(255) COLLATE "C";
ALTER TABLE users ADD COLUMN email_verified_at TIMESTAMPTZ;

CREATE TABLE email_verification_guards (
    email_canonical VARCHAR(255) COLLATE "C" NOT NULL,
    purpose VARCHAR(32) NOT NULL CHECK (purpose = 'registration'),
    last_generation BIGINT NOT NULL CHECK (last_generation > 0),
    next_resend_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (email_canonical, purpose)
);
CREATE TABLE email_verification_requests (
    id UUID PRIMARY KEY,
    email_canonical VARCHAR(255) COLLATE "C" NOT NULL,
    purpose VARCHAR(32) NOT NULL CHECK (purpose = 'registration'),
    generation BIGINT NOT NULL CHECK (generation > 0),
    credential_hash VARCHAR(255) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    next_resend_at TIMESTAMPTZ NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 5),
    max_attempts INT NOT NULL DEFAULT 5 CHECK (max_attempts = 5),
    delivery_state VARCHAR(16) NOT NULL CHECK (delivery_state IN ('PREPARED','ACCEPTED','FAILED','UNKNOWN')),
    consumed_at TIMESTAMPTZ,
    invalidated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (email_canonical,purpose) REFERENCES email_verification_guards(email_canonical,purpose),
    CONSTRAINT ux_email_verification_generation UNIQUE(email_canonical,purpose,generation)
);
CREATE INDEX idx_email_verification_retention ON email_verification_requests(expires_at,id);

CREATE TABLE password_reset_guards (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    last_generation BIGINT NOT NULL CHECK (last_generation > 0),
    next_resend_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE password_reset_requests (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email_canonical VARCHAR(255) COLLATE "C" NOT NULL,
    token_hash CHAR(64) COLLATE "C" NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    next_resend_at TIMESTAMPTZ NOT NULL,
    delivery_state VARCHAR(16) NOT NULL CHECK (delivery_state IN ('PREPARED','ACCEPTED','FAILED','UNKNOWN')),
    consumed_at TIMESTAMPTZ,
    invalidated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ux_password_reset_token_hash UNIQUE(token_hash),
    CONSTRAINT ux_password_reset_generation UNIQUE(user_id,generation)
);
CREATE INDEX idx_password_reset_retention ON password_reset_requests(expires_at,id);
