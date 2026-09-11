package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

type GraphStore struct {
	pool *pgxpool.Pool
}

func NewGraphStore(pool *pgxpool.Pool) *GraphStore {
	return &GraphStore{pool: pool}
}

// Sync updates the graph in Postgres. It implements a temporal table:
// missing edges are "closed" (valid_to = now), new edges are inserted.
func (s *GraphStore) Sync(ctx context.Context, edges []graph.Edge) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// In a real implementation, you would do a diff against current active edges:
	// 1. Close edges that exist in DB but not in `edges`
	// 2. Insert edges that exist in `edges` but not in DB
	// 3. Update weights for edges where weight changed significantly

	// For simplicity in Phase 2, we will just do a bulk insert of new edges
	// and close everything else (in practice, this requires a temporary staging table).

	// Close all currently valid edges
	_, err = tx.Exec(ctx, `UPDATE graph_edges SET valid_to = now() WHERE valid_to IS NULL`)
	if err != nil {
		return err
	}

	// Insert current edges
	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"graph_edges"},
		[]string{"from_key", "to_key", "kind", "weight", "source"},
		pgx.CopyFromSlice(len(edges), func(i int) ([]any, error) {
			e := edges[i]
			return []any{e.From.Key(), e.To.Key(), e.Kind, e.Weight, e.Source}, nil
		}),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// Current returns the active dependency edges used by the operations UI.
func (s *GraphStore) Current(ctx context.Context) ([]graph.Edge, error) {
	return s.at(ctx, time.Now().UTC())
}

// At returns the graph that was active at the requested instant using the
// temporal validity interval, without reconstructing a Kubernetes snapshot.
func (s *GraphStore) At(ctx context.Context, at time.Time) ([]graph.Edge, error) {
	return s.at(ctx, at)
}

func (s *GraphStore) at(ctx context.Context, at time.Time) ([]graph.Edge, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT from_key, to_key, kind, weight, source
		FROM graph_edges
		WHERE valid_from <= $1 AND (valid_to IS NULL OR valid_to > $1)
		ORDER BY from_key, to_key`, at)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var edges []graph.Edge
	for rows.Next() {
		var fromKey, toKey string
		var e graph.Edge
		if err := rows.Scan(&fromKey, &toKey, &e.Kind, &e.Weight, &e.Source); err != nil {
			return nil, err
		}
		if e.From, err = graph.ParseKey(fromKey); err != nil {
			return nil, err
		}
		if e.To, err = graph.ParseKey(toKey); err != nil {
			return nil, err
		}
		edges = append(edges, e)
	}
	return edges, rows.Err()
}
