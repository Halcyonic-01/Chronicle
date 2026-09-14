package graph

import (
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
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

// ArgoNamespace is where Argo CD Application nodes live in the graph. The
// collector and the edge builder must agree on it, or an Application event
// names a node that does not exist and the causal filter discards it.
const ArgoNamespace = "argocd"

// sidecarContainers are injected proxies. Their environment names the mesh
// control plane, which every meshed pod in the cluster depends on equally —
// so those edges say nothing about how the application is wired together.
var sidecarContainers = map[string]bool{"linkerd-proxy": true, "istio-proxy": true, "envoy": true}

// EdgeCallsInfra is a dependency on shared infrastructure rather than on
// another application component. Causality still follows it — if the mesh
// control plane fails, the pods do too — but blast radius ignores it, because
// "reachable through the thing everything is plugged into" is not impact.
const EdgeCallsInfra = "calls_infra"

// argoInstanceLabels are the labels Argo CD stamps on the workloads it manages.
var argoInstanceLabels = []string{"argocd.argoproj.io/instance", "app.kubernetes.io/instance"}

// hostNames pulls the host out of an environment value so a service is matched
// on identity rather than on appearing somewhere in the string. Accepts
// "redis://redis.default.svc.cluster.local:6379", "http://api:8080", "api:8080"
// and a bare "api".
func hostNames(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 2048 {
		return nil
	}
	if i := strings.Index(value, "://"); i >= 0 {
		value = value[i+3:]
	}
	if i := strings.IndexAny(value, "/?#"); i >= 0 {
		value = value[:i]
	}
	if i := strings.LastIndex(value, "@"); i >= 0 {
		value = value[i+1:]
	}
	if i := strings.LastIndex(value, ":"); i >= 0 {
		if _, err := strconv.Atoi(value[i+1:]); err == nil {
			value = value[:i]
		}
	}
	if value == "" || strings.ContainsAny(value, " \t\"'") {
		return nil
	}
	return strings.Split(value, ".")
}

// InferCallEdges builds 'calls' edges from Pods to Services named by their
// environment. Since causality now runs along these edges, a false match
// invents a false cause — so the service name must be the host's first label
// ("api" from "http://api:8080"), not merely a substring of the value.
func InferCallEdges(pod corev1.Pod, knownSvcs map[string]bool) []Edge {
	var edges []Edge
	containers := append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
	for _, c := range containers {
		for _, env := range c.Env {
			labels := hostNames(env.Value)
			if len(labels) == 0 {
				continue
			}
			host := labels[0]
			// A cross-namespace address spells out the namespace: svc.namespace...
			namespace := pod.Namespace
			if len(labels) > 1 {
				namespace = labels[1]
			}
			if !knownSvcs[namespace+"/"+host] {
				continue
			}
			kind, weight := "calls", 0.7 // inferred, so lower confidence
			if sidecarContainers[c.Name] {
				kind, weight = EdgeCallsInfra, 0.3
			}
			edges = append(edges, Edge{
				From:   Node{Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace},
				To:     Node{Kind: "Service", Name: host, Namespace: namespace},
				Kind:   kind,
				Weight: weight,
				Source: "static",
			})
		}
	}
	return dedupeEdges(edges)
}

// BuildArgoEdges links an Argo CD Application to the Deployments it manages,
// using the instance label Argo stamps on them. Without these edges an Argo
// sync event has no node in the graph, so it can never be ranked as a cause of
// anything it deployed.
func BuildArgoEdges(deployments []appsv1.Deployment) []Edge {
	var edges []Edge
	for _, deployment := range deployments {
		for _, label := range argoInstanceLabels {
			instance := deployment.Labels[label]
			if instance == "" {
				continue
			}
			edges = append(edges, Edge{
				From:   Node{Kind: "Application", Name: instance, Namespace: ArgoNamespace},
				To:     Node{Kind: "Deployment", Name: deployment.Name, Namespace: deployment.Namespace},
				Kind:   "owns",
				Weight: 1,
				Source: "static",
			})
			break
		}
	}
	return dedupeEdges(edges)
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
