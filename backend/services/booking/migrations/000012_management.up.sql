CREATE TABLE presentation_feature_flags (
 key TEXT PRIMARY KEY CHECK (key IN ('discovery_advanced_filters')),
 enabled BOOLEAN NOT NULL DEFAULT FALSE,
 version BIGINT NOT NULL DEFAULT 0 CHECK(version>=0),
 updated_at TIMESTAMPTZ,
 actor_id UUID
);
INSERT INTO presentation_feature_flags(key) VALUES('discovery_advanced_filters');
CREATE TABLE presentation_feature_audit (
 id UUID PRIMARY KEY,
 key TEXT NOT NULL REFERENCES presentation_feature_flags(key),
 before_enabled BOOLEAN NOT NULL,
 after_enabled BOOLEAN NOT NULL,
 version BIGINT NOT NULL,
 actor_id UUID NOT NULL,
 changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(key,version)
);
CREATE INDEX presentation_feature_audit_recent ON presentation_feature_audit(key,version DESC);
