# Multi-stage build for ultra-lightweight containers
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /gateway ./cmd/gateway
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /mock_backend ./cmd/mock_backend

FROM alpine:3.19
RUN apk --no-cache add ca-certificates curl

WORKDIR /app
COPY --from=builder /gateway /app/gateway
COPY --from=builder /mock_backend /app/mock_backend
COPY configs/rules.yaml /app/configs/rules.yaml

EXPOSE 8080 8081 8082 8083
ENTRYPOINT ["/app/gateway"]
