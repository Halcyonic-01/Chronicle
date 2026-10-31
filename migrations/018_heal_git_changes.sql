-- A Git-based remediation (git_revert) is a pull request, not a write to the
-- cluster. These record it on the decision: where it is, and how far it got
-- (open -> merged -> synced by the GitOps controller, or closed unmerged).
ALTER TABLE heal_actions
    ADD COLUMN IF NOT EXISTS git_url       TEXT,
    ADD COLUMN IF NOT EXISTS git_number    INT,
    ADD COLUMN IF NOT EXISTS git_branch    TEXT,
    ADD COLUMN IF NOT EXISTS git_commit    TEXT,
    ADD COLUMN IF NOT EXISTS git_state     TEXT,
    ADD COLUMN IF NOT EXISTS git_detail    TEXT,
    ADD COLUMN IF NOT EXISTS git_merged_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS git_synced_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_heal_actions_git_pending
    ON heal_actions (git_state) WHERE git_state IN ('open', 'merged');
