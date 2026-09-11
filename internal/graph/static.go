package graph

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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
			for serviceKey := range knownSvcs {
				parts := strings.SplitN(serviceKey, "/", 2)
				svcNamespace, svcName := pod.Namespace, serviceKey
				if len(parts) == 2 {
					svcNamespace, svcName = parts[0], parts[1]
				}
				if svcNamespace != pod.Namespace {
					continue
				}
				if strings.Contains(env.Value, svcName) {
					edges = append(edges, Edge{
						From:   Node{Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace},
						To:     Node{Kind: "Service", Name: svcName, Namespace: svcNamespace},
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

// BuildOwnerEdges builds 'owns' edges from Deployments to Pods.
func BuildOwnerEdges(pods []corev1.Pod) []Edge {
	var edges []Edge
	for _, pod := range pods {
		for _, ref := range pod.OwnerReferences {
			if ref.Kind == "ReplicaSet" {
				name := ref.Name
				if idx := strings.LastIndex(name, "-"); idx > 0 {
					name = name[:idx]
				}
				edges = append(edges, Edge{
					From:   Node{Kind: "Deployment", Name: name, Namespace: pod.Namespace},
					To:     Node{Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace},
					Kind:   "owns",
					Weight: 1.0,
					Source: "static",
				})
			}
		}
	}
	return edges
}

// BuildReferenceEdges captures pod dependencies that are otherwise invisible
// in service topology: mounted/configured ConfigMaps, Secrets, PVCs, and the
// node hosting each pod.
func BuildReferenceEdges(pods []corev1.Pod) []Edge {
	var edges []Edge
	for _, pod := range pods {
		from := Node{Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace}
		add := func(kind, name, relation string) {
			if name != "" {
				edges = append(edges, Edge{From: from, To: Node{Kind: kind, Name: name, Namespace: pod.Namespace}, Kind: relation, Weight: 1, Source: "static"})
			}
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.ConfigMap != nil {
				add("ConfigMap", volume.ConfigMap.Name, "uses")
			}
			if volume.Secret != nil {
				add("Secret", volume.Secret.SecretName, "uses")
			}
			if volume.PersistentVolumeClaim != nil {
				add("PersistentVolumeClaim", volume.PersistentVolumeClaim.ClaimName, "mounts")
			}
		}
		containers := append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
		for _, container := range containers {
			for _, ref := range container.EnvFrom {
				if ref.ConfigMapRef != nil {
					add("ConfigMap", ref.ConfigMapRef.Name, "uses")
				}
				if ref.SecretRef != nil {
					add("Secret", ref.SecretRef.Name, "uses")
				}
			}
			for _, env := range container.Env {
				if env.ValueFrom == nil {
					continue
				}
				if env.ValueFrom.ConfigMapKeyRef != nil {
					add("ConfigMap", env.ValueFrom.ConfigMapKeyRef.Name, "uses")
				}
				if env.ValueFrom.SecretKeyRef != nil {
					add("Secret", env.ValueFrom.SecretKeyRef.Name, "uses")
				}
			}
		}
		if pod.Spec.NodeName != "" {
			edges = append(edges, Edge{From: from, To: Node{Kind: "Node", Name: pod.Spec.NodeName}, Kind: "runs_on", Weight: 1, Source: "static"})
		}
	}
	return dedupeEdges(edges)
}

// BuildIngressEdges maps each Ingress backend to its Service in the same namespace.
func BuildIngressEdges(ingresses []networkingv1.Ingress) []Edge {
	var edges []Edge
	for _, ingress := range ingresses {
		from := Node{Kind: "Ingress", Name: ingress.Name, Namespace: ingress.Namespace}
		add := func(name string) {
			if name != "" {
				edges = append(edges, Edge{From: from, To: Node{Kind: "Service", Name: name, Namespace: ingress.Namespace}, Kind: "routes_to", Weight: 1, Source: "static"})
			}
		}
		if ingress.Spec.DefaultBackend != nil && ingress.Spec.DefaultBackend.Service != nil {
			add(ingress.Spec.DefaultBackend.Service.Name)
		}
		for _, rule := range ingress.Spec.Rules {
			if rule.HTTP == nil {
				continue
			}
			for _, path := range rule.HTTP.Paths {
				if path.Backend.Service != nil {
					add(path.Backend.Service.Name)
				}
			}
		}
	}
	return dedupeEdges(edges)
}

func dedupeEdges(edges []Edge) []Edge {
	seen := make(map[string]struct{}, len(edges))
	result := make([]Edge, 0, len(edges))
	for _, edge := range edges {
		key := edge.From.Key() + "|" + edge.To.Key() + "|" + edge.Kind
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, edge)
	}
	return result
}
