.PHONY: rca-benchmark rca-benchmark-baseline setup teardown migrate build-chronicle deploy-chronicle deploy-ui test-phase4 test-phase5-kind

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
