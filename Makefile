.PHONY: setup teardown

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

