-- Did the symptom the action was for actually go away? Judged after an action
-- succeeds, separately from `outcome` (the labeller's verdict on whether the
-- proposal was right), which never credits Chronicle's own actions as evidence.
--   symptom_check: pending | succeeded | failed | unknown (NULL: not applicable)
ALTER TABLE heal_actions
    ADD COLUMN IF NOT EXISTS symptom_check       TEXT,
    ADD COLUMN IF NOT EXISTS symptom_detail      TEXT,
    ADD COLUMN IF NOT EXISTS symptom_started_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS symptom_improved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS symptom_due_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS symptom_checked_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_heal_actions_symptom_pending
    ON heal_actions (symptom_due_at) WHERE symptom_check = 'pending';
