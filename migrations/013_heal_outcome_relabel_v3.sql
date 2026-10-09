-- The labeller now ends each decision's window at its own incident's held
-- recovery and tolerates the one-second resolution of Kubernetes timestamps
-- (v2 judged against the next fault's events and missed fixes dated just before
-- the decision). Labels v2 wrote are kept in heal_outcome_history, then cleared
-- so v3 re-judges them. Same shape as 012: idempotent, one transaction, and only
-- labels stamped v2 are moved, so a v3 label is never touched.
BEGIN;
-- Every pod runs the migrations at start-up. Without this lock two of them copy
-- the same labels into the history before either clears them.
SELECT pg_advisory_xact_lock(7429010);

INSERT INTO heal_outcome_history (action_id, outcome, outcome_at, outcome_detail, labeller, superseded_by)
SELECT id, outcome, outcome_at, outcome_detail, outcome_labeller, 'v3-incident-bounded'
FROM heal_actions
WHERE outcome IS NOT NULL AND outcome_labeller = 'v2-entity-ordered'
ON CONFLICT DO NOTHING;

UPDATE heal_actions
SET outcome = NULL, outcome_at = NULL, outcome_detail = NULL, outcome_labeller = NULL
WHERE outcome IS NOT NULL AND outcome_labeller = 'v2-entity-ordered';

COMMIT;
