.PHONY: all proto build test benchmark clean run-server run-sim loadtest k6 help

PROTOC ?= protoc
PROTO_DIR = proto
GEN_DIR = gen/go

all: proto build

proto:
	@echo "==> Compiling Protocol Buffers..."
	@mkdir -p $(GEN_DIR)
	$(PROTOC) --proto_path=$(PROTO_DIR) \
		--go_out=. --go_opt=module=geopulse \
		--go-grpc_out=. --go-grpc_opt=module=geopulse \
		$(PROTO_DIR)/telemetry/v1/telemetry.proto
	@echo "==> Protobuf code generation completed successfully."

build:
	@echo "==> Building GeoPulse Server, Simulator, and Loadtester binaries..."
	@mkdir -p bin
	go build -v -o bin/geopulse-server cmd/server/main.go
	go build -v -o bin/geopulse-sim cmd/simulator/main.go
	go build -v -o bin/geopulse-loadtester cmd/loadtester/main.go
	@echo "==> Binaries generated in bin/"

run-server:
	go run cmd/server/main.go

run-sim:
	go run cmd/simulator/main.go -rate 10 -duration 10s

loadtest:
	@echo "==> Running 1,000 concurrent streaming agents load test (10,000 pings/sec)..."
	go run cmd/loadtester/main.go -clients 1000 -rate 10 -duration 10s

k6:
	@echo "==> Running k6 stress and latency benchmarks..."
	k6 run test/k6/loadtest.js

test:
	@echo "==> Running all unit & integration tests..."
	go test -v ./...

benchmark:
	@echo "==> Running Protobuf vs JSON benchmarks (throughput & memory)..."
	go test -v -bench=. -benchmem -run=^$$ ./test/...

clean:
	@echo "==> Cleaning generated artifacts..."
	rm -rf bin/ $(GEN_DIR)

help:
	@echo "GeoPulse Build System:"
	@echo "  make proto       - Compile .proto definitions into Go gRPC & protobuf code"
	@echo "  make build       - Compile the server, simulator, and loadtester binaries to bin/"
	@echo "  make test        - Run all unit and integration tests"
	@echo "  make benchmark   - Run serialization benchmarks (speed, allocs, wire size)"
	@echo "  make run-server  - Run gRPC ingestion server locally"
	@echo "  make run-sim     - Run mock client simulator (10 pings/sec for 10s)"
	@echo "  make loadtest    - Run 1,000 concurrent virtual vehicles load test"
	@echo "  make k6          - Run k6 performance test script"
	@echo "  make clean       - Remove build outputs and generated protobuf files"
