package replay

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/graph"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Snapshot is a complete picture of the cluster at one instant.
// Keyed by "namespace/Kind/name" — same key format as the dependency graph.
type Snapshot struct {
	ID      string                 `json:"id"`
	TakenAt time.Time              `json:"taken_at"`
	Objects map[string]ObjectState `json:"objects"`
	Edges   []graph.Edge           `json:"edges"`
	// Sampled metric values so replay shows real numbers, not just structure.
	Metrics map[string]float64 `json:"metrics"`
}

// ObjectState is a minimal, JSON-serializable summary of any K8s object.
type ObjectState struct {
	Kind       string            `json:"kind"`
	Name       string            `json:"name"`
	Namespace  string            `json:"namespace"`
	Image      string            `json:"image,omitempty"`
	Replicas   int32             `json:"replicas,omitempty"`
	ReadyCount int32             `json:"ready_count"`
	Phase      string            `json:"phase"` // Running | Pending | Failed
	Restarts   int32             `json:"restarts"`
	Labels     map[string]string `json:"labels,omitempty"`
	// Resource limits — explain OOM kills days later.
	MemLimit int64 `json:"mem_limit,omitempty"`
	CPULimit int64 `json:"cpu_limit,omitempty"`
	// Hash of the object spec — detect config drift without storing the whole spec.
	SpecHash string `json:"spec_hash"`
}

var _ = sync.Mutex{} // just to keep import

// podToState converts a live Pod object into our compact ObjectState.
func podToState(p v1.Pod) ObjectState {
	var restarts int32
	var readyCount int32
	var image string
	var memLimit, cpuLimit int64

	for _, cs := range p.Status.ContainerStatuses {
		restarts += cs.RestartCount
		if cs.Ready {
			readyCount++
		}
	}
	if len(p.Spec.Containers) > 0 {
		image = p.Spec.Containers[0].Image
		if lim := p.Spec.Containers[0].Resources.Limits; lim != nil {
			if mem, ok := lim[v1.ResourceMemory]; ok {
				memLimit = mem.Value()
			}
			if cpu, ok := lim[v1.ResourceCPU]; ok {
				cpuLimit = cpu.MilliValue()
			}
		}
	}

	specHash := fmt.Sprintf("%x", sha256.Sum256([]byte(
		fmt.Sprintf("%v", p.Spec),
	)))[:16]

	return ObjectState{
		Kind:       "Pod",
		Name:       p.Name,
		Namespace:  p.Namespace,
		Image:      image,
		ReadyCount: readyCount,
		Phase:      string(p.Status.Phase),
		Restarts:   restarts,
		Labels:     p.Labels,
		MemLimit:   memLimit,
		CPULimit:   cpuLimit,
		SpecHash:   specHash,
	}
}

// deployToState converts a Deployment into our ObjectState.
func deployToState(d interface{ GetName() string }, name, ns string, replicas, ready int32, image string) ObjectState {
	_ = resource.Quantity{} // keep import
	return ObjectState{
		Kind:       "Deployment",
		Name:       name,
		Namespace:  ns,
		Image:      image,
		Replicas:   replicas,
		ReadyCount: ready,
		Phase:      "Running",
	}
}
