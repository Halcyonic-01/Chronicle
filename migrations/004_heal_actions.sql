CREATE TABLE heal_actions (
    id          TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    rule        TEXT NOT NULL,
    cause_type  TEXT NOT NULL DEFAULT '',
    namespace   TEXT NOT NULL DEFAULT 'default',
    target      TEXT NOT NULL DEFAULT '',
    confidence  DOUBLE PRECISION NOT NULL DEFAULT 0,
    reasoning   JSONB NOT NULL DEFAULT '[]'::jsonb,
    status      TEXT NOT NULL,
    result      TEXT NOT NULL DEFAULT '',
    error       TEXT,
    dry_run     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT heal_actions_one_per_incident UNIQUE (incident_id)
);

CREATE INDEX idx_heal_actions_incident ON heal_actions (incident_id, created_at DESC);
CREATE INDEX idx_heal_actions_rule_time ON heal_actions (rule, created_at DESC);
