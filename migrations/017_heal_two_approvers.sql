-- Some actions need more than one person. approvals_required is fixed when the
-- decision is proposed; each approval is a row, one per distinct identity, so
-- the same person cannot approve twice and the audit shows every approver.
-- Rows from before this migration need one approval, which decided_by records.
ALTER TABLE heal_actions
    ADD COLUMN IF NOT EXISTS approvals_required INT NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS heal_action_approvals (
    action_id   TEXT        NOT NULL REFERENCES heal_actions (id) ON DELETE CASCADE,
    issuer      TEXT        NOT NULL,
    subject     TEXT        NOT NULL,
    display     TEXT        NOT NULL DEFAULT '',
    email       TEXT        NOT NULL DEFAULT '',
    groups      TEXT[]      NOT NULL DEFAULT '{}',
    reason      TEXT        NOT NULL DEFAULT '',
    approved_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (action_id, issuer, subject)
);
