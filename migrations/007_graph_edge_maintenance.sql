-- The graph sync now diffs against the open edge set instead of closing and
-- rewriting every edge on each pass. That makes two access patterns hot:
--   1. loading every open edge to diff against (GraphStore.Sync)
--   2. deleting edge versions that closed long ago (GraphStore.Prune)
CREATE INDEX IF NOT EXISTS idx_edges_open
    ON graph_edges (from_key, to_key, kind) WHERE valid_to IS NULL;

CREATE INDEX IF NOT EXISTS idx_edges_closed
    ON graph_edges (valid_to) WHERE valid_to IS NOT NULL;

-- Deployments that ran the previous close-everything-and-reinsert sync
-- accumulated one closed edge version per 30-second pass. Those rows are
-- identical apart from their validity interval, so the table can be very large
-- while carrying almost no history.
--
-- Chronicle's Prune now removes closed versions older than the retention
-- window on its own, so this backlog clears itself within a year. To reclaim
-- the space immediately, run the statement below by hand during a maintenance
-- window. It is deliberately NOT part of the automatic migration: it rewrites
-- a large table and discards edge history older than the cutoff you choose.
--
--   DELETE FROM graph_edges
--   WHERE valid_to IS NOT NULL AND valid_to < now() - interval '7 days';
--   VACUUM (ANALYZE) graph_edges;
