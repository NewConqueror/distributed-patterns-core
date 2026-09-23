.PHONY: up down test test-integration test-benchmark proto run clean

# Start PostgreSQL Docker Container
up:
	docker-compose up -d

# Stop PostgreSQL Docker Container
down:
	docker-compose down

# Check container logs
logs:
	docker-compose logs -f postgres

# Generate Protocol Buffers and gRPC code
proto:
	PATH=$$PATH:$$HOME/.local/bin protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/rebac/v1/rebac.proto

# Run all unit tests
test:
	go test -v -race ./internal/...

# Run integration tests (Requires PostgreSQL container on port 5433)
test-integration:
	DB_PORT=5433 go test -v -race ./test/integration/...

# Run ReBAC benchmark tests comparing CTE vs In-Memory
test-benchmark:
	DB_PORT=5433 go test -v -bench=. -benchmem ./test/integration/...

# Run the API and gRPC server
run:
	go run cmd/api/main.go
