CREATE TABLE events (
    id                TEXT PRIMARY KEY,
    occurred_at       TIMESTAMPTZ NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL,
    source            TEXT NOT NULL,
    namespace         TEXT NOT NULL DEFAULT 'default',
    entity_kind       TEXT NOT NULL,
    entity_name       TEXT NOT NULL,
    type              TEXT NOT NULL,
    severity          TEXT NOT NULL,
    title             TEXT NOT NULL,
    payload           JSONB,
    trace_id          TEXT,
    correlation_key   TEXT
);

-- The index that makes RCA fast. Every query is
-- "events in this time window" so ingested_at leads.
CREATE INDEX idx_events_time     ON events (ingested_at DESC);
CREATE INDEX idx_events_entity   ON events (entity_kind, entity_name, ingested_at DESC);
CREATE INDEX idx_events_corr     ON events (correlation_key);
CREATE INDEX idx_events_severity ON events (severity, ingested_at DESC)
                                 WHERE severity IN ('warning','critical');
CREATE INDEX idx_events_payload  ON events USING GIN (payload);
