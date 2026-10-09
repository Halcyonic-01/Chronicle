-- Outcome labels written before the labeller judged recovery by entity and
-- order (issues 16-20) were namespace-wide and are not reliable evidence. They
-- are kept, not deleted: each is copied to heal_outcome_history, then the live
-- label is cleared so the current labeller re-judges the decision.
--
-- Idempotent: a label carries outcome_labeller once the current labeller wrote
-- it, and only unstamped (legacy) labels are moved. One transaction, so a
-- failure cannot leave a label copied but not cleared.
BEGIN;
-- Every pod runs the migrations at start-up. Without this lock two of them copy
-- the same labels into the history before either clears them.
SELECT pg_advisory_xact_lock(7429010);

ALTER TABLE heal_actions ADD COLUMN IF NOT EXISTS outcome_labeller TEXT;

CREATE TABLE IF NOT EXISTS heal_outcome_history (
    id            BIGSERIAL PRIMARY KEY,
    action_id     TEXT        NOT NULL,
    outcome       TEXT        NOT NULL,
    outcome_at    TIMESTAMPTZ,
    outcome_detail TEXT,
    labeller      TEXT        NOT NULL,
    superseded_by TEXT        NOT NULL,
    superseded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_heal_outcome_history_action ON heal_outcome_history (action_id);

INSERT INTO heal_outcome_history (action_id, outcome, outcome_at, outcome_detail, labeller, superseded_by)
SELECT id, outcome, outcome_at, outcome_detail, 'v1-namespace-wide', 'v2-entity-ordered'
FROM heal_actions
WHERE outcome IS NOT NULL AND outcome_labeller IS NULL
ON CONFLICT DO NOTHING;

UPDATE heal_actions
SET outcome = NULL, outcome_at = NULL, outcome_detail = NULL
WHERE outcome IS NOT NULL AND outcome_labeller IS NULL;

COMMIT;
