CREATE TABLE event_interests (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    event_id UUID NOT NULL REFERENCES events(id),
    kind VARCHAR(16) NOT NULL CHECK (kind IN ('favorite','onsale','restock')),
    status VARCHAR(24) NOT NULL CHECK (status IN ('active','withdrawn','triggered','collection_pending')),
    email_opt_in BOOLEAN NOT NULL DEFAULT FALSE,
    email_consent_version VARCHAR(64) NOT NULL DEFAULT '',
    email_consent_id UUID NULL,
    email_consented_at TIMESTAMPTZ NULL,
    email_revoked_at TIMESTAMPTZ NULL,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    active_since_version BIGINT NOT NULL DEFAULT 1 CHECK (active_since_version BETWEEN 1 AND version),
    active_since_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT event_interest_email_consent_check CHECK (
        (NOT email_opt_in OR (kind <> 'favorite' AND length(email_consent_version) BETWEEN 1 AND 64
            AND email_consented_at IS NOT NULL AND email_revoked_at IS NULL AND email_consent_id IS NOT NULL))
        AND (kind <> 'favorite' OR NOT email_opt_in)
        AND (email_opt_in OR email_consent_id IS NULL)
    ),
    CONSTRAINT event_interest_favorite_status_check CHECK (
        kind <> 'favorite' OR status IN ('active','withdrawn')
    ),
    CONSTRAINT event_interest_onsale_status_check CHECK (
        kind <> 'onsale' OR status IN ('active','withdrawn','triggered')
    ),
    CONSTRAINT event_interest_restock_status_check CHECK (
        kind <> 'restock' OR status IN ('active','withdrawn','collection_pending')
    ),
    CONSTRAINT event_interest_owner_event_kind_unique UNIQUE (user_id,event_id,kind)
);

CREATE INDEX event_interests_active_user_page
    ON event_interests(user_id,kind,created_at DESC,id DESC)
    WHERE status='active';
CREATE INDEX event_interests_active_event_kind
    ON event_interests(event_id,kind,user_id)
    WHERE status='active';
CREATE INDEX event_interests_reminder_fanout_page
    ON event_interests(event_id,kind,id)
    INCLUDE (active_since_at, user_id, version, active_since_version, email_opt_in, email_consent_version, email_consent_id)
    WHERE status IN ('active','collection_pending');

ALTER TABLE events
    ADD COLUMN sales_generation BIGINT NOT NULL DEFAULT 0 CHECK (sales_generation >= 0),
    ADD COLUMN sale_notification_generation BIGINT NOT NULL DEFAULT 0 CHECK (sale_notification_generation >= 0 AND sale_notification_generation <= sales_generation),
    ADD COLUMN first_sale_opened_at TIMESTAMPTZ NULL;

UPDATE events SET first_sale_opened_at=COALESCE(updated_at,created_at,clock_timestamp())
 WHERE publication_status='published' AND sales_status='selling'
   AND (sales_start_at IS NULL OR sales_start_at<=clock_timestamp())
   AND (sales_end_at IS NULL OR sales_end_at>clock_timestamp()) AND date>clock_timestamp();

-- Seed one unnotified generation for pre-reminder sales that have a defined
-- opening time. Preserve already-open selling rows as historical, already
-- notified first sales.
UPDATE events SET sales_generation=1,
    sale_notification_generation=CASE WHEN first_sale_opened_at IS NOT NULL THEN 1 ELSE 0 END
 WHERE publication_status='published' AND sales_status IN ('scheduled','selling')
   AND date>clock_timestamp()
   AND (sales_end_at IS NULL OR sales_end_at>clock_timestamp())
   AND (sales_start_at IS NOT NULL OR first_sale_opened_at IS NOT NULL);

-- Initiating transactions write exactly one immutable event-level row. A
-- separate worker fans out eligible subscriptions in bounded pages and writes
-- the per-user Notification outbox intents atomically with its cursor.
CREATE TABLE event_reminder_fanouts (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events(id),
    kind VARCHAR(16) NOT NULL CHECK (kind IN ('onsale','restock')),
    trigger_id UUID NOT NULL,
    event_title VARCHAR(500) NOT NULL,
    event_date TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    cursor_id UUID NULL,
    completed_at TIMESTAMPTZ NULL,
    CONSTRAINT event_reminder_fanout_source_unique UNIQUE (event_id,kind,trigger_id)
);

CREATE INDEX event_reminder_fanouts_pending
    ON event_reminder_fanouts(created_at,id)
    WHERE completed_at IS NULL;

CREATE TABLE event_reminder_outbox (
    id UUID PRIMARY KEY,
    subscription_id UUID NOT NULL REFERENCES event_interests(id),
    event_id UUID NOT NULL REFERENCES events(id),
    user_id UUID NOT NULL,
    notification_type VARCHAR(32) NOT NULL CHECK (notification_type IN ('event_onsale','event_restock')),
    action VARCHAR(24) NOT NULL CHECK (action IN ('trigger','withdrawn','consent_updated','resubscribed')),
    subscription_version BIGINT NOT NULL CHECK (subscription_version > 0),
    trigger_id UUID NOT NULL,
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ NULL,
    claim_token UUID NULL,
    lease_until TIMESTAMPTZ NULL,
    suppressed_at TIMESTAMPTZ NULL,
    CONSTRAINT event_reminder_outbox_idempotency UNIQUE (subscription_id,subscription_version,action,trigger_id)
);

CREATE INDEX event_reminder_outbox_pending
    ON event_reminder_outbox(created_at,id)
    WHERE published_at IS NULL AND suppressed_at IS NULL;
