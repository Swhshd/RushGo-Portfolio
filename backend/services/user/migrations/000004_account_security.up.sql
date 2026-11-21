ALTER TABLE users ADD COLUMN locale TEXT NOT NULL DEFAULT 'en' CHECK (locale IN ('en','zh'));
ALTER TABLE users ADD COLUMN credential_version BIGINT NOT NULL DEFAULT 0 CHECK (credential_version >= 0);

CREATE TABLE account_change_attempts (
 user_id UUID PRIMARY KEY REFERENCES users(id),
 window_started_at TIMESTAMPTZ NOT NULL,
 attempts INTEGER NOT NULL CHECK (attempts >= 0)
);
CREATE TABLE account_email_changes (
 id UUID PRIMARY KEY,
 user_id UUID NOT NULL REFERENCES users(id),
 email_canonical VARCHAR(255) NOT NULL,
 credential_hash TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 delivery_state TEXT NOT NULL DEFAULT 'PREPARED' CHECK (delivery_state IN ('PREPARED','ACCEPTED','FAILED','UNKNOWN')),
 expires_at TIMESTAMPTZ NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 consumed_at TIMESTAMPTZ,
 invalidated_at TIMESTAMPTZ
);
CREATE INDEX account_email_changes_owner ON account_email_changes(user_id,created_at DESC);
CREATE TABLE account_security_audit (
 id UUID PRIMARY KEY,
 user_id UUID NOT NULL,
 action TEXT NOT NULL CHECK (action IN ('password_changed','email_change_requested','email_changed')),
 occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
