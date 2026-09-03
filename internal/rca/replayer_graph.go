package rca

import (
	"context"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/replay"
)

type ReplayerGraphSource struct {
	Replayer *replay.Replayer
}

func (s *ReplayerGraphSource) UpstreamAt(ctx context.Context, t time.Time, start string, maxDepth int) (map[string]int, error) {
	snap, err := s.Replayer.At(ctx, t)
	if err != nil {
		return nil, err
	}

	// Create a temporary graph to traverse
	g := graph.New()
	g.SetEdges(snap.Edges)

	return g.Upstream(start, maxDepth), nil
}
