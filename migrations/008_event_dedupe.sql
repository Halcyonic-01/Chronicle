-- Collectors re-observe facts. An informer relists on restart, a poller whose
-- cursor lives only in memory rewinds to the beginning, and both then emit
-- events they have already emitted. Each re-emission minted a fresh ULID, so
-- the existing ON CONFLICT (id) guard -- which exists for Kafka's at-least-once
-- delivery -- never fired, and the same Kubernetes warning was stored once per
-- restart. RCA then offered one copy as the cause of another.
--
-- Identity for an observation is what it says about what, and when it happened.
-- A genuine recurrence carries a later occurred_at and is still a new row.

-- Existing duplicates must go before the index can be built. Keep the first
-- copy ingested; the rest are re-observations of it and carry nothing new.
DELETE FROM events a
USING events b
WHERE a.occurred_at = b.occurred_at
  AND a.source      = b.source
  AND a.namespace   = b.namespace
  AND a.entity_kind = b.entity_kind
  AND a.entity_name = b.entity_name
  AND a.type        = b.type
  AND a.title       = b.title
  AND (a.ingested_at > b.ingested_at
       OR (a.ingested_at = b.ingested_at AND a.id > b.id));

-- The title is bounded rather than hashed. A Kubernetes event message can be
-- long enough on its own to overrun the btree row limit for a multi-column text
-- index, but the hash functions that would compress it are not usable here:
-- md5 is refused outright by a server built in FIPS mode, and sha256 needs
-- convert_to, which is STABLE rather than IMMUTABLE and so cannot appear in an
-- index expression at all. A bounded prefix plus the full length separates
-- messages that share an opening but differ later, and needs nothing of the
-- server beyond IMMUTABLE built-ins.
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedupe ON events (
    occurred_at, source, namespace, entity_kind, entity_name, type,
    left(title, 200), length(title)
);
