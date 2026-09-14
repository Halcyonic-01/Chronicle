package store

import (
	"context"
	"math"
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

// weightChangeThreshold is the relative weight movement that justifies writing
// a new edge version. Mesh weights are request rates that jitter constantly, so
// without this every sync would rewrite the entire graph.
const weightChangeThreshold = 0.2

// Sync updates the graph in Postgres as a true temporal table: it diffs the
// desired edge set against the edges currently open, closes only what
// disappeared or changed materially, and inserts only what is new. Rewriting
// every edge on every sync would grow this table by the full graph size every
// 30 seconds and destroy the validity intervals the replay and RCA paths read.
func (s *GraphStore) Sync(ctx context.Context, edges []graph.Edge) error {
	desired := make(map[string]graph.Edge, len(edges))
	for _, e := range edges {
		desired[edgeIdentity(e)] = e
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	open, err := openEdges(ctx, tx)
	if err != nil {
		return err
	}

	var (
		closeFrom, closeTo, closeKind []string
		insert                        []graph.Edge
	)
	for identity, current := range open {
		wanted, stillPresent := desired[identity]
		if stillPresent && !edgeChanged(current, wanted) {
			continue
		}
		closeFrom = append(closeFrom, current.From.Key())
		closeTo = append(closeTo, current.To.Key())
		closeKind = append(closeKind, current.Kind)
		if stillPresent {
			insert = append(insert, wanted)
		}
	}
	for identity, wanted := range desired {
		if _, exists := open[identity]; !exists {
			insert = append(insert, wanted)
		}
	}

	if len(closeFrom) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE graph_edges SET valid_to = now()
			WHERE valid_to IS NULL
			  AND (from_key, to_key, kind) IN (
			      SELECT * FROM unnest($1::text[], $2::text[], $3::text[])
			  )`, closeFrom, closeTo, closeKind); err != nil {
			return err
		}
	}

	if len(insert) > 0 {
		if _, err := tx.CopyFrom(ctx,
			pgx.Identifier{"graph_edges"},
			[]string{"from_key", "to_key", "kind", "weight", "source"},
			pgx.CopyFromSlice(len(insert), func(i int) ([]any, error) {
				e := insert[i]
				return []any{e.From.Key(), e.To.Key(), e.Kind, e.Weight, e.Source}, nil
			}),
		); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// Prune drops edge versions that closed before the cutoff. It mirrors the
// snapshot retention policy so historical replay and graph queries stay
// answerable for the same period.
func (s *GraphStore) Prune(ctx context.Context, before time.Time) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM graph_edges WHERE valid_to IS NOT NULL AND valid_to < $1`, before)
	return err
}

func edgeIdentity(e graph.Edge) string {
	return e.From.Key() + "|" + e.To.Key() + "|" + e.Kind
}

// edgeChanged reports whether an open edge differs enough from the desired one
// to deserve a new validity interval.
func edgeChanged(current, wanted graph.Edge) bool {
	if current.Source != wanted.Source {
		return true
	}
	difference := math.Abs(current.Weight - wanted.Weight)
	if difference < 1e-9 {
		return false
	}
	largest := math.Max(math.Abs(current.Weight), math.Abs(wanted.Weight))
	if largest == 0 {
		return false
	}
	return difference/largest > weightChangeThreshold
}

func openEdges(ctx context.Context, tx pgx.Tx) (map[string]graph.Edge, error) {
	rows, err := tx.Query(ctx, `
		SELECT from_key, to_key, kind, weight, source
		FROM graph_edges WHERE valid_to IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	open := make(map[string]graph.Edge)
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
		open[edgeIdentity(e)] = e
	}
	return open, rows.Err()
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

func (s *GraphStore) UpstreamAt(ctx context.Context, at time.Time, start string, maxDepth int) (map[string]int, error) {
	edges, err := s.At(ctx, at)
	if err != nil {
		return nil, err
	}
	g := graph.New()
	g.SetEdges(edges)
	return g.Upstream(start, maxDepth), nil
}

func (s *GraphStore) DownstreamAt(ctx context.Context, at time.Time, start string, maxDepth int) (map[string]int, error) {
	edges, err := s.At(ctx, at)
	if err != nil {
		return nil, err
	}
	g := graph.New()
	g.SetEdges(edges)
	return g.Downstream(start, maxDepth), nil
}

func (s *GraphStore) ImpactAt(ctx context.Context, at time.Time, start string, maxDepth int) (map[string]int, error) {
	edges, err := s.At(ctx, at)
	if err != nil {
		return nil, err
	}
	g := graph.New()
	g.SetEdges(edges)
	return g.Impact(start, maxDepth), nil
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
