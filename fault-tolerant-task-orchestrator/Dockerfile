# Multi-stage Dockerfile for Distributed Fault-Tolerant Task Orchestrator

# Stage 1: Build
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binary with stripped debug symbols
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o orchestrator ./cmd/server

# Stage 2: Final Minimal Runtime
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies for healthchecks
RUN apk add --no-cache ca-certificates tzdata curl

# Copy binary from builder
COPY --from=builder /app/orchestrator /app/orchestrator

# Non-root user for security
RUN adduser -D -u 10001 orchestrator && \
    chown -R orchestrator:orchestrator /app

USER orchestrator

# Expose metrics/health HTTP port
EXPOSE 9090

# Default entrypoint
ENTRYPOINT ["/app/orchestrator"]
