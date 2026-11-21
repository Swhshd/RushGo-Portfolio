ALTER TABLE events
    ADD COLUMN publication_status VARCHAR(20) NOT NULL DEFAULT 'draft',
    ADD COLUMN sales_status VARCHAR(20) NOT NULL DEFAULT 'scheduled',
    ADD COLUMN sales_start_at TIMESTAMPTZ NULL,
    ADD COLUMN sales_end_at TIMESTAMPTZ NULL;

-- Preserve legacy active/cancelled/ended visibility. Unknown old states are
-- kept private and paused; only the known active state remains open for sales.
UPDATE events
SET publication_status = CASE
        WHEN lower(status) IN ('draft', 'private') THEN 'draft'
        WHEN lower(status) IN ('active', 'cancelled', 'canceled', 'ended', 'closed') THEN 'published'
        ELSE 'draft'
    END,
    sales_status = CASE
        WHEN lower(status) = 'active' THEN 'selling'
        WHEN lower(status) IN ('cancelled', 'canceled') THEN 'cancelled'
        WHEN lower(status) IN ('ended', 'closed') THEN 'ended'
        ELSE 'paused'
    END;

ALTER TABLE events
    ADD CONSTRAINT events_publication_status_check
        CHECK (publication_status IN ('draft', 'published')),
    ADD CONSTRAINT events_sales_status_check
        CHECK (sales_status IN ('scheduled', 'selling', 'paused', 'ended', 'cancelled')),
    ADD CONSTRAINT events_sales_window_check
        CHECK (sales_start_at IS NULL OR sales_end_at IS NULL OR sales_start_at < sales_end_at);

CREATE INDEX idx_events_publication_date
    ON events (date ASC)
    WHERE publication_status = 'published';

CREATE TABLE event_creation_requests (
    actor_id UUID NOT NULL,
    request_id VARCHAR(128) NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128),
    request_hash BYTEA NOT NULL CHECK (octet_length(request_hash) = 32),
    event_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (actor_id, request_id)
);

ALTER TABLE ticket_tiers
    ADD CONSTRAINT ticket_tier_price_bounds
        CHECK (price_cents BETWEEN 1 AND 2147483647) NOT VALID,
    ADD CONSTRAINT ticket_tier_total_quantity_bounds
        CHECK (total_quantity BETWEEN 1 AND 5000) NOT VALID;
