ALTER TABLE heal_actions
    ADD COLUMN IF NOT EXISTS action_type TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS approval TEXT NOT NULL DEFAULT 'not_required',
    ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS decided_by TEXT,
    ADD COLUMN IF NOT EXISTS decision_reason TEXT,
    ADD COLUMN IF NOT EXISTS decided_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_heal_actions_approval
    ON heal_actions (approval, created_at DESC);
