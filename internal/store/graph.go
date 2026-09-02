package store

import (
	"context"

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
