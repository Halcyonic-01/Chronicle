CREATE TABLE snapshots (
    id        TEXT PRIMARY KEY,
    taken_at  TIMESTAMPTZ NOT NULL,
    -- zstd-compressed JSON. ~2 MB raw becomes ~150 KB.
    data      BYTEA NOT NULL,
    obj_count INT NOT NULL
);

CREATE INDEX idx_snapshots_time ON snapshots (taken_at DESC);

-- Retention: a nightly job runs these to keep storage flat.
-- Keep every snapshot for 7 days, hourly for 30, daily for a year.
-- DELETE FROM snapshots
--   WHERE taken_at < now() - interval '7 days'
--   AND id NOT IN (
--     SELECT DISTINCT ON (date_trunc('hour', taken_at)) id
--     FROM snapshots ORDER BY date_trunc('hour', taken_at), taken_at
--   );
