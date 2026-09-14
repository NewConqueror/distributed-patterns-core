.PHONY: help build run test-k6-naive docker-up docker-down db-init db-reset clean

APP_NAME = flashsale-engine
BIN_DIR = bin

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Compile application binary
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/server ./cmd/server
	@echo "Build complete: $(BIN_DIR)/server"

run: ## Run application locally
	go run ./cmd/server

docker-up: ## Start PostgreSQL and Redis containers
	docker compose up -d

docker-down: ## Stop and remove containers
	docker compose down

db-init: ## Initialize database schema and seed data
	psql -h localhost -U postgres -d flashsale -f scripts/sql/init.sql

db-reset: ## Reset product stock to 100 and clear orders
	@curl -s -X POST http://localhost:8080/api/v1/products/1/reset \
		-H "Content-Type: application/json" \
		-d '{"stock": 100}' | jq . || echo "Failed to reset stock. Ensure server is running on :8080."

test-k6-naive: ## Run k6 load test for naive endpoint (100 VUs, 2000 reqs in 2s)
	@if command -v k6 >/dev/null 2>&1; then \
		k6 run scripts/k6/naive_test.js; \
	elif [ -f ./k6 ]; then \
		./k6 run scripts/k6/naive_test.js; \
	else \
		echo "k6 not found. Please install k6 or run using ./k6"; exit 1; \
	fi

test-k6-pessimistic: ## Run k6 load test for pessimistic locking endpoint (100 VUs, 2000 reqs in 2s)
	@if command -v k6 >/dev/null 2>&1; then \
		k6 run scripts/k6/pessimistic_test.js; \
	elif [ -f ./k6 ]; then \
		./k6 run scripts/k6/pessimistic_test.js; \
	else \
		echo "k6 not found. Please install k6 or run using ./k6"; exit 1; \
	fi

test-k6-optimistic: ## Run k6 load test for optimistic locking endpoint (100 VUs, 2000 reqs in 2s)
	@if command -v k6 >/dev/null 2>&1; then \
		k6 run scripts/k6/optimistic_test.js; \
	elif [ -f ./k6 ]; then \
		./k6 run scripts/k6/optimistic_test.js; \
	else \
		echo "k6 not found. Please install k6 or run using ./k6"; exit 1; \
	fi

clean: ## Clean built binaries and temporary files
	rm -rf $(BIN_DIR)
