package rca

import (
	"context"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/graph"
	"github.com/Halcyonic-01/Chronicle/internal/replay"
)

type ReplayerGraphSource struct {
	Replayer   *replay.Replayer
	Historical interface {
		At(context.Context, time.Time) ([]graph.Edge, error)
	}
}

func (s *ReplayerGraphSource) edgesAt(ctx context.Context, t time.Time) ([]graph.Edge, error) {
	if s.Historical != nil {
		return s.Historical.At(ctx, t)
	}
	snap, err := s.Replayer.At(ctx, t)
	if err != nil {
		return nil, err
	}
	return snap.Edges, nil
}

func (s *ReplayerGraphSource) UpstreamAt(ctx context.Context, t time.Time, start string, maxDepth int) (map[string]int, error) {
	edges, err := s.edgesAt(ctx, t)
	if err != nil {
		return nil, err
	}
	g := graph.New()
	g.SetEdges(edges)
	return g.Upstream(start, maxDepth), nil
}

func (s *ReplayerGraphSource) DownstreamAt(ctx context.Context, t time.Time, start string, maxDepth int) (map[string]int, error) {
	edges, err := s.edgesAt(ctx, t)
	if err != nil {
		return nil, err
	}
	g := graph.New()
	g.SetEdges(edges)
	return g.Downstream(start, maxDepth), nil
}

func (s *ReplayerGraphSource) ImpactAt(ctx context.Context, t time.Time, start string, maxDepth int) (map[string]int, error) {
	edges, err := s.edgesAt(ctx, t)
	if err != nil {
		return nil, err
	}
	g := graph.New()
	g.SetEdges(edges)
	return g.Impact(start, maxDepth), nil
}

func (s *ReplayerGraphSource) EdgesAt(ctx context.Context, t time.Time) ([]graph.Edge, error) {
	return s.edgesAt(ctx, t)
}
