#!/usr/bin/env bash
set -e

echo "Setting up Chronicle Phase 0 environment..."

# 1. Create the kind cluster
if ! kind get clusters | grep -q "^chronicle$"; then
    echo "Creating kind cluster..."
    kind create cluster --config deploy/kind/kind-config.yaml
else
    echo "Cluster 'chronicle' already exists."
fi

# Ensure context is set
kubectl cluster-info --context kind-chronicle

# 2. Install monitoring stack (Prometheus, Grafana, Alertmanager)
echo "Installing Prometheus and Grafana..."
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
    --namespace monitoring --create-namespace \
    --set prometheus.service.type=NodePort \
    --set prometheus.service.nodePort=30090 \
    --set grafana.service.type=NodePort \
    --set grafana.service.nodePort=30080

# 3. Install Loki (for logs)
echo "Installing Loki..."
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
helm upgrade --install loki grafana/loki-stack --namespace monitoring \
    --set loki.isDefault=false


# 4. Install Kafka and PostgreSQL for Chronicle storage
echo "Installing Kafka and PostgreSQL..."
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm upgrade --install kafka bitnami/kafka \
    --namespace chronicle --create-namespace \
    --set controller.replicaCount=1 \
    --set broker.replicaCount=1 \
    --set listeners.client.protocol=PLAINTEXT

helm upgrade --install postgres bitnami/postgresql \
    --namespace chronicle \
    --set auth.postgresPassword=postgres \
    --set primary.persistence.enabled=false


echo "Setup complete! Dashboard and Prometheus will be available shortly on:"
echo "Grafana: http://localhost:8080 (admin/prom-operator)"
echo "Prometheus: http://localhost:9090"
