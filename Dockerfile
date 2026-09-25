# -------------------------------------------------------------
# Build Stage
# -------------------------------------------------------------
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and compile
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/sync-worker ./cmd/sync-worker

# -------------------------------------------------------------
# Runtime Stage (Minimal Alpine)
# -------------------------------------------------------------
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/sync-worker /app/sync-worker

ENTRYPOINT ["/app/sync-worker"]
