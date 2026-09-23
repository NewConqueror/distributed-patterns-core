.PHONY: up down test test-integration run clean

# Start PostgreSQL Docker Container
up:
	docker-compose up -d

# Stop PostgreSQL Docker Container
down:
	docker-compose down

# Check container logs
logs:
	docker-compose logs -f postgres

# Run all unit tests
test:
	go test -v -race ./internal/...

# Run integration tests (Requires Docker container running on port 5433)
test-integration:
	DB_PORT=5433 go test -v -race ./test/integration/...

# Run the API server
run:
	go run cmd/api/main.go
