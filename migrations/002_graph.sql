CREATE TABLE graph_edges (
    from_key    TEXT NOT NULL, -- "default/Service/redis"
    to_key      TEXT NOT NULL,
    kind        TEXT NOT NULL,
    weight      DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    source      TEXT NOT NULL,
    valid_from  TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to    TIMESTAMPTZ,   -- NULL = still true right now
    PRIMARY KEY (from_key, to_key, kind, valid_from)
);

CREATE INDEX idx_edges_from ON graph_edges (from_key) WHERE valid_to IS NULL;
CREATE INDEX idx_edges_to   ON graph_edges (to_key) WHERE valid_to IS NULL;
