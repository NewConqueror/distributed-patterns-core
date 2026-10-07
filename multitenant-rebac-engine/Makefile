.PHONY: up down test test-integration test-benchmark test-concurrency seed proto run load-cold load-warm load-thundering clean

# Start PostgreSQL & Redis Docker Containers
up:
	docker-compose up -d

# Stop Containers
down:
	docker-compose down

# Check container logs
logs:
	docker-compose logs -f

# Generate Protocol Buffers and gRPC code
proto:
	PATH=$$PATH:$$HOME/.local/bin protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/rebac/v1/rebac.proto

# Seed 1000 Tenants and 500k+ Tuples
seed:
	go run cmd/seed/main.go -tenants=1000

# Run all unit tests with race detection
test:
	go test -v -race ./internal/...

# Run integration tests (Requires Docker containers running)
test-integration:
	DB_PORT=5433 go test -v -race ./test/integration/...

# Run Concurrency & Exploit Test Suite (Showcase)
test-concurrency:
	DB_PORT=5433 go test -v -race -run 'TestRaceCondition|TestThunderingHerd' ./test/integration/...

# Run ReBAC benchmark tests comparing CTE vs In-Memory
test-benchmark:
	DB_PORT=5433 go test -v -bench=. -benchmem ./test/integration/...

# Run the API and gRPC server
run:
	go run cmd/api/main.go

# k6 Load Tests
load-cold:
	k6 run test/load/scenario1_cold_cache.js

load-warm:
	k6 run test/load/scenario2_warm_cache.js

load-thundering:
	k6 run test/load/scenario3_thundering_herd.js
