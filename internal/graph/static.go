package graph

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// BuildServiceEdges builds 'routes_to' edges from Services to Pods.
// Service -> Pod: match the service's label selector against pod labels.
func BuildServiceEdges(svcs []corev1.Service, pods []corev1.Pod) []Edge {
	var edges []Edge
	for _, svc := range svcs {
		if len(svc.Spec.Selector) == 0 {
			continue // headless / external
		}
		sel := labels.SelectorFromSet(svc.Spec.Selector)

		for _, pod := range pods {
			if pod.Namespace != svc.Namespace {
				continue
			}
			if !sel.Matches(labels.Set(pod.Labels)) {
				continue
			}

			edges = append(edges, Edge{
				From:   Node{Kind: "Service", Name: svc.Name, Namespace: svc.Namespace},
				To:     Node{Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace},
				Kind:   "routes_to",
				Weight: 1.0,
				Source: "static",
			})
		}
	}
	return edges
}

// InferCallEdges builds 'calls' edges from Pods to Services inferred via Env vars.
func InferCallEdges(pod corev1.Pod, knownSvcs map[string]bool) []Edge {
	var edges []Edge
	for _, c := range pod.Spec.Containers {
		for _, env := range c.Env {
			// Looking for values like redis://redis.default.svc.cluster.local:6379 or http://api:8080
			for svcName := range knownSvcs {
				if strings.Contains(env.Value, svcName) {
					edges = append(edges, Edge{
						From:   Node{Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace},
						To:     Node{Kind: "Service", Name: svcName, Namespace: pod.Namespace},
						Kind:   "calls",
						Weight: 0.7, // inferred, so lower confidence
						Source: "static",
					})
				}
			}
		}
	}
	return edges
}
