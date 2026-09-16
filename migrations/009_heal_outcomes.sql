-- Shadow mode's value is measuring accuracy before granting autonomy, but the
-- audit store recorded only what Chronicle decided, never whether it was right.
-- Without these the observation period ends with no evidence to set a threshold
-- from.
ALTER TABLE heal_actions ADD COLUMN IF NOT EXISTS outcome        TEXT;
ALTER TABLE heal_actions ADD COLUMN IF NOT EXISTS outcome_at     TIMESTAMPTZ;
ALTER TABLE heal_actions ADD COLUMN IF NOT EXISTS outcome_detail TEXT;

-- The labelling pass looks for scored decisions not yet judged.
CREATE INDEX IF NOT EXISTS idx_heal_actions_unlabelled
    ON heal_actions (created_at)
    WHERE outcome IS NULL AND confidence > 0;
