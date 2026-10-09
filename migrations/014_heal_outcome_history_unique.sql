-- A decision has at most one label per labeller version, so the history holds at
-- most one row per (decision, labeller). Two pods running migration 013 together
-- once copied the same labels twice; this removes only those exact duplicate
-- copies (same content, higher id) and then makes the rule structural. No
-- distinct label is touched.
BEGIN;
SELECT pg_advisory_xact_lock(7429010);

DELETE FROM heal_outcome_history a
USING heal_outcome_history b
WHERE a.action_id = b.action_id AND a.labeller = b.labeller AND a.id > b.id;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_heal_outcome_history_label
    ON heal_outcome_history (action_id, labeller);

COMMIT;
