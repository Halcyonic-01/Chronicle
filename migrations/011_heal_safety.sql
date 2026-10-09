-- Healing safety hardening. Every statement is idempotent: migrations run on
-- each start.

ALTER TABLE heal_actions ADD COLUMN IF NOT EXISTS workload   TEXT NOT NULL DEFAULT '';
ALTER TABLE heal_actions ADD COLUMN IF NOT EXISTS proposed   BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE heal_actions ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE heal_actions ADD COLUMN IF NOT EXISTS related    JSONB NOT NULL DEFAULT '[]'::jsonb;

-- A Pod action changes its owner, so allowlists and cooldowns key on that.
UPDATE heal_actions
SET workload = CASE
    WHEN action_type IN ('restart_pod','bump_memory') AND COALESCE(payload->>'owner','') <> '' THEN payload->>'owner'
    ELSE target END
WHERE workload = '';

-- Decisions the engine would have acted on, whatever happened to them later.
UPDATE heal_actions SET proposed = true
WHERE NOT proposed
  AND (status IN ('would_run','approved','executing','succeeded','failed')
       OR approval IN ('approved','denied'));

-- Blocked and refused decisions were stored as awaiting approval, and the
-- approve endpoint accepted them.
UPDATE heal_actions SET approval = 'not_required'
WHERE approval = 'pending' AND status <> 'would_run';

-- A denial was recorded as blocked, indistinguishable from a safety gate.
UPDATE heal_actions SET status = 'denied'
WHERE approval = 'denied' AND status = 'blocked';

-- Approvals from before expiry existed describe the past; never act on them.
UPDATE heal_actions
SET status = 'expired', approval = 'expired',
    result = result || ' (expired: recorded before approvals could expire)'
WHERE expires_at IS NULL
  AND status IN ('would_run','approved')
  AND approval IN ('pending','approved');

-- The observation start was min(created_at), which moved forward whenever old
-- rows were pruned. It is now written once and never pruned.
CREATE TABLE IF NOT EXISTS heal_settings (
    key TEXT PRIMARY KEY,
    at  TIMESTAMPTZ NOT NULL
);
INSERT INTO heal_settings (key, at)
SELECT 'observation_started_at', min(created_at) FROM heal_actions
HAVING min(created_at) IS NOT NULL
ON CONFLICT (key) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_heal_actions_proposed_cause
    ON heal_actions (cause_event_id) WHERE proposed;
CREATE INDEX IF NOT EXISTS idx_heal_actions_started
    ON heal_actions (started_at) WHERE started_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_heal_actions_queue
    ON heal_actions (status, expires_at) WHERE status IN ('would_run','approved','executing');
