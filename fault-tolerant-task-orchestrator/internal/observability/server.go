package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// ServerConfig configures the observability and health HTTP server.
type ServerConfig struct {
	Addr         string        // e.g., ":9090"
	ReadTimeout  time.Duration // e.g., 5s
	WriteTimeout time.Duration // e.g., 10s
	Logger       *slog.Logger
}

// HealthStatus represents the payload returned by health endpoints.
type HealthStatus struct {
	Status    string            `json:"status"`
	Timestamp string            `json:"timestamp"`
	Checks    map[string]string `json:"checks,omitempty"`
}

// Server serves Prometheus /metrics, /healthz (liveness), and /ready (readiness).
type Server struct {
	cfg      ServerConfig
	metrics  *Metrics
	redisCli *redis.Client
	server   *http.Server
	listener net.Listener
	isLive   atomic.Bool
	logger   *slog.Logger
}

// NewServer initializes the HTTP observability server.
func NewServer(cfg ServerConfig, metrics *Metrics, redisCli *redis.Client) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Addr == "" {
		cfg.Addr = ":9090"
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}

	s := &Server{
		cfg:      cfg,
		metrics:  metrics,
		redisCli: redisCli,
		logger:   cfg.Logger,
	}
	s.isLive.Store(true)

	mux := http.NewServeMux()

	// 1. Prometheus /metrics endpoint
	if metrics != nil && metrics.Registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		}))
	}

	// 2. Liveness check (/healthz and /live)
	mux.HandleFunc("/healthz", s.handleLiveness)
	mux.HandleFunc("/live", s.handleLiveness)

	// 3. Readiness check (/ready)
	mux.HandleFunc("/ready", s.handleReadiness)

	s.server = &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return s
}

// handleLiveness verifies that the binary process is running and alive.
func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	if !s.isLive.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(HealthStatus{
			Status:    "unhealthy",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(HealthStatus{
		Status:    "healthy",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
}

// handleReadiness verifies that external dependencies (e.g. Redis) are reachable.
func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	checks := make(map[string]string)
	allReady := true

	if s.redisCli != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer cancel()

		if err := s.redisCli.Ping(ctx).Err(); err != nil {
			checks["redis"] = fmt.Sprintf("down: %v", err)
			allReady = false
		} else {
			checks["redis"] = "up"
		}
	} else {
		checks["redis"] = "unconfigured (running local in-memory)"
	}

	if !allReady {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(HealthStatus{
			Status:    "not_ready",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Checks:    checks,
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(HealthStatus{
		Status:    "ready",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Checks:    checks,
	})
}

// Start listens on the configured address.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.cfg.Addr, err)
	}
	s.listener = ln
	s.logger.Info("Observability HTTP server listening", "addr", ln.Addr().String())

	go func() {
		if err := s.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("Observability HTTP server error", "err", err)
		}
	}()

	return nil
}

// Addr returns the actual listening address (useful when configured with :0 for testing).
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.cfg.Addr
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.isLive.Store(false)
	s.logger.Info("Shutting down observability HTTP server...")
	return s.server.Shutdown(ctx)
}
