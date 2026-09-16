.PHONY: all proto build test benchmark clean help

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
	@echo "==> Building GeoPulse binary..."
	go build -v -o bin/geopulse main.go

test:
	@echo "==> Running unit and comparison tests..."
	go test -v ./...

benchmark:
	@echo "==> Running Protobuf vs JSON benchmarks (throughput & memory)..."
	go test -v -bench=. -benchmem -run=^$$ ./test/...

clean:
	@echo "==> Cleaning generated artifacts..."
	rm -rf bin/ $(GEN_DIR)

help:
	@echo "GeoPulse Makefile Commands:"
	@echo "  make proto      - Compile .proto definitions into Go gRPC & protobuf code"
	@echo "  make build      - Compile the Go project binary to bin/geopulse"
	@echo "  make test       - Execute all unit tests including payload comparison"
	@echo "  make benchmark  - Run serialization benchmarks (speed, allocs, wire size)"
	@echo "  make clean      - Remove build outputs and generated protobuf files"
