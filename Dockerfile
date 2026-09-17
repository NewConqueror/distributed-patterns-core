# =====================================================================
# GeoPulse Multi-Stage Container Dockerfile
# =====================================================================

# Stage 1: Build environment
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Install build dependencies
RUN apk add --no-cache git make protobuf protobuf-dev

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source tree
COPY . .

# Build statically linked binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/geopulse-server cmd/server/main.go && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/geopulse-sim cmd/simulator/main.go && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/geopulse-loadtester cmd/loadtester/main.go

# Stage 2: Minimal, secure runtime image
FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S geopulse && adduser -S geopulse -G geopulse -u 10001

WORKDIR /app

# Copy compiled binaries from builder stage
COPY --from=builder /bin/geopulse-server /app/geopulse-server
COPY --from=builder /bin/geopulse-sim /app/geopulse-sim
COPY --from=builder /bin/geopulse-loadtester /app/geopulse-loadtester

USER geopulse:geopulse

EXPOSE 50051 2112

ENTRYPOINT ["/app/geopulse-server"]
CMD ["-port", "50051", "-metrics-port", "2112"]
