package graph

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// meshQuery reads what Linkerd's proxies saw: HTTP requests, and TCP
// connections for everything else (a database, a cache). A dependency named
// only in code, like the worker's postgres, exists here and nowhere else.
const meshQuery = `
sum by (deployment, namespace, dst_deployment, dst_namespace) (
  rate(request_total{direction="outbound"}[5m])
)
or
sum by (deployment, namespace, dst_deployment, dst_namespace) (
  rate(tcp_open_total{direction="outbound", peer="dst"}[5m])
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
	vec, ok := result.(model.Vector)
	if !ok {
		return nil, nil
	}
	return meshEdges(vec), nil
}

// meshEdges turns observed traffic into calls edges. Linkerd's labels come
// first; the others are other meshes' names for the same thing.
func meshEdges(vec model.Vector) []Edge {
	var edges []Edge
	seen := map[string]bool{}
	for _, s := range vec {
		src := firstLabel(s.Metric, "deployment", "src_deploy", "source_workload")
		dst := firstLabel(s.Metric, "dst_deployment", "dst_deploy", "destination_workload")
		srcNamespace := firstLabel(s.Metric, "namespace", "src_namespace", "source_workload_namespace")
		dstNamespace := firstLabel(s.Metric, "dst_namespace", "destination_workload_namespace", "namespace")
		if srcNamespace == "" {
			srcNamespace = "default"
		}
		if dstNamespace == "" {
			dstNamespace = "default"
		}
		key := srcNamespace + "/" + src + ">" + dstNamespace + "/" + dst
		if src == "" || dst == "" || (src == dst && srcNamespace == dstNamespace) || seen[key] {
			continue
		}
		seen[key] = true
		edges = append(edges, Edge{
			From: Node{Kind: "Deployment", Name: src, Namespace: srcNamespace},
			To:   Node{Kind: "Deployment", Name: dst, Namespace: dstNamespace},
			Kind: "calls",
			// Weight = observed request or connection rate.
			Weight: float64(s.Value),
			Source: "mesh",
		})
	}
	return edges
}

func firstLabel(metric model.Metric, names ...string) string {
	for _, name := range names {
		if value := string(metric[model.LabelName(name)]); value != "" {
			return value
		}
	}
	return ""
}
