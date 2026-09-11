.PHONY: setup teardown migrate build-chronicle deploy-chronicle test-phase4

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

test-phase4:
	@chmod +x scripts/test-phase4-redis.sh
	@./scripts/test-phase4-redis.sh
