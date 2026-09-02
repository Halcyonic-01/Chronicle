package graph

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

const meshQuery = `
sum by (src_deploy, dst_deploy) (
  rate(request_total{direction="outbound"}[5m])
)
`

type MeshBuilder struct {
	prom v1.API
}

func NewMeshBuilder(client api.Client) *MeshBuilder {
	return &MeshBuilder{prom: v1.NewAPI(client)}
}

// RuntimeEdges fetches exact traffic volumes between deployments from the service mesh.
func (b *MeshBuilder) RuntimeEdges(ctx context.Context) ([]Edge, error) {
	result, _, err := b.prom.Query(ctx, meshQuery, time.Now())
	if err != nil {
		return nil, err
	}

	var edges []Edge
	vec, ok := result.(model.Vector)
	if !ok {
		return nil, nil
	}

	for _, s := range vec {
		src := string(s.Metric["src_deploy"])
		dst := string(s.Metric["dst_deploy"])

		if src == "" || dst == "" || src == dst {
			continue
		}

		edges = append(edges, Edge{
			From: Node{Kind: "Deployment", Name: src, Namespace: "default"}, // simplified namespace
			To:   Node{Kind: "Deployment", Name: dst, Namespace: "default"},
			Kind: "calls",
			// Weight = observed request rate. A service called 1000x/s is a far stronger
			// dependency than one called once an hour.
			Weight: float64(s.Value),
			Source: "mesh",
		})
	}
	return edges, nil
}
