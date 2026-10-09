.PHONY: rca-benchmark rca-benchmark-baseline setup teardown migrate build-chronicle deploy-chronicle deploy-ui test-phase4 test-phase5-kind chaos-list chaos-plan chaos-run chaos-report kyverno-test kyverno-live-test

POSTGRES_URL ?= postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable

setup:
	@chmod +x scripts/setup.sh
	@./scripts/setup.sh

teardown:
	@kind delete cluster --name chronicle

deploy-victim:
	@echo "Building victim image..."
	@docker build -t chronicle-victim:latest -f victim/Dockerfile .
	@echo "Loading image into kind..."
	@kind load docker-image chronicle-victim:latest --name chronicle
	@echo "Deploying victim application..."
	@kubectl apply -f deploy/victim/

migrate:
	@for migration in migrations/*.sql; do \
		echo "Applying $$migration"; \
		psql "$(POSTGRES_URL)" -v ON_ERROR_STOP=1 -f "$$migration"; \
	done

build-chronicle:
	@echo "Building Chronicle image..."
	@docker build -t chronicle:latest -f Dockerfile .
	@kind load docker-image chronicle:latest --name chronicle

deploy-chronicle: build-chronicle
	@chmod +x scripts/deploy-chronicle.sh
	@./scripts/deploy-chronicle.sh

deploy-ui:
	@chmod +x scripts/deploy-ui.sh
	@./scripts/deploy-ui.sh

test-phase4:
	@chmod +x scripts/test-phase4-redis.sh
	@./scripts/test-phase4-redis.sh

test-phase5-kind:
	@chmod +x scripts/test-phase5-kind.sh
	@./scripts/test-phase5-kind.sh

# RCA accuracy on the controlled benchmark (not production accuracy).
# See internal/rca/benchdata for what the incidents are and what the numbers mean.
rca-benchmark:
	@RCA_BENCH_PROFILE=improved go test ./internal/rca ./internal/heal -run 'TestRCABenchmark|TestRCAHoldout|TestHealingBenchmark' -v -count=1 | grep -vE '^(=== RUN|--- PASS|PASS|ok)'

# The same incidents with only the events the collectors emitted before the
# Service, Node, ConfigMap and HPA collectors existed.
rca-benchmark-baseline:
	@RCA_BENCH_PROFILE=baseline go test ./internal/rca ./internal/heal -run 'TestRCABenchmark|TestRCAHoldout|TestHealingBenchmark' -v -count=1 | grep -vE '^(=== RUN|--- PASS|PASS|ok)'

# Chaos experiments: real faults injected into the local kind cluster, one at a
# time, and Chronicle's live analysis scored like the benchmarks. Needs the
# Chronicle API on :8181 (port-forward) and Prometheus on :9090. Only kind-*
# contexts are allowed. See internal/chaos/README.md.
#   make chaos-run CHAOS_ARGS="-only redis-scaled-to-zero,redis-hang -trials 2"
CHAOS_ARGS ?=
chaos-list:
	@go run ./cmd/chaos list

chaos-plan:
	@go run ./cmd/chaos plan $(CHAOS_ARGS)

chaos-run:
	@go run ./cmd/chaos run $(CHAOS_ARGS)

chaos-report:
	@test -n "$(CHAOS_LOG)" || (echo "usage: make chaos-report CHAOS_LOG=chaos-results/run-....jsonl" && exit 2)
	@go run ./cmd/chaos report $(CHAOS_LOG)

# Healing guardrail policy tests (needs the kyverno CLI; nothing touches a
# cluster). Fails on skipped or invalid results, which `kyverno test` passes.
kyverno-test:
	@./scripts/test-kyverno.sh

# The same guardrails against a REAL Kyverno admission controller, in a
# disposable kind cluster named kyverno-test* (created and installed if absent;
# KEEP=1 keeps it). Needs docker, kind, kubectl, helm; downloads the Kyverno
# chart and images. Never touches the Chronicle cluster.
kyverno-live-test:
	@./scripts/test-kyverno-live.sh
