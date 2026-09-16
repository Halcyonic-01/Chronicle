-- Decisions were grouped by symptom, so one outage analysed forty times looked
-- like forty independent confirmations and could clear an evidence bar on its
-- own. The cause event identifies the outage a decision is really about.
ALTER TABLE heal_actions ADD COLUMN IF NOT EXISTS cause_event_id TEXT;

CREATE INDEX IF NOT EXISTS idx_heal_actions_cause
    ON heal_actions (cause_event_id)
    WHERE cause_event_id IS NOT NULL;

-- Decisions recorded before this column exists still carry their cause's type,
-- namespace and target. Reconstructing the identity keeps their judgements
-- usable instead of discarding evidence that was already earned.
UPDATE heal_actions a
SET cause_event_id = (
    SELECT e.id FROM events e
    WHERE e.type = a.cause_type
      AND e.namespace = a.namespace
      AND e.entity_name = a.target
      AND e.occurred_at <= a.created_at
    ORDER BY e.occurred_at DESC
    LIMIT 1)
WHERE a.cause_event_id IS NULL
  AND a.confidence > 0
  AND COALESCE(a.cause_type,'') <> '';
