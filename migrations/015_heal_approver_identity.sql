-- Who approved or denied, as the identity provider vouched for them. decided_by
-- stays the display name; the issuer and subject are what identifies a person
-- even if their email later changes hands. Rows decided before this migration
-- (or by the shared token) have neither.
ALTER TABLE heal_actions
    ADD COLUMN IF NOT EXISTS decided_by_issuer  TEXT,
    ADD COLUMN IF NOT EXISTS decided_by_subject TEXT;
