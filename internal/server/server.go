package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"geopulse/internal/buffer"
	"geopulse/internal/ingestion"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Config encapsulates server startup parameters.
type Config struct {
	Port           int
	BufferCapacity int
	NumWorkers     int
	DropStrategy   buffer.DropStrategy
}

// Run boots up the gRPC ingestion server with worker pool and graceful shutdown.
func Run(cfg Config, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}

	logger.Info("Starting GeoPulse Telemetry Engine",
		"port", cfg.Port,
		"buffer_capacity", cfg.BufferCapacity,
		"workers", cfg.NumWorkers,
	)

	ingestionCfg := ingestion.Config{
		BufferCapacity: cfg.BufferCapacity,
		DropStrategy:   cfg.DropStrategy,
		NumWorkers:     cfg.NumWorkers,
	}
	server := ingestion.NewTelemetryServer(ingestionCfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	server.Start(ctx)

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  5 * time.Minute,
			Timeout:               1 * time.Second,
		}),
	)

	telemetryv1.RegisterTelemetryServiceServer(grpcServer, server)

	addr := fmt.Sprintf(":%d", cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		cancel()
		server.Stop()
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	go func() {
		logger.Info("gRPC Telemetry Ingestion Server ready", "listen_addr", addr)
		if err := grpcServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			logger.Error("gRPC server error", "err", err)
		}
	}()

	// Periodic metrics reporter
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				m := server.Metrics().Snapshot()
				b := server.Buffer().Stats()
				logger.Info("Engine Telemetry Metrics",
					"active_streams", m.ActiveStreams,
					"total_received", m.TotalReceived,
					"total_processed", m.TotalProcessed,
					"dropped_oldest", m.TotalDroppedOldest,
					"buffer_depth", b.CurrentDepth,
				)
			}
		}
	}()

	// Graceful shutdown handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutdown signal received, draining pipelines...")

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logger.Info("gRPC server stopped cleanly")
	case <-time.After(5 * time.Second):
		logger.Warn("Graceful shutdown timed out, forcing stop")
		grpcServer.Stop()
	}

	cancel()
	server.Stop()

	finalMetrics := server.Metrics().Snapshot()
	finalBuffer := server.Buffer().Stats()
	logger.Info("GeoPulse Shutdown Completed",
		"total_received", finalMetrics.TotalReceived,
		"total_processed", finalMetrics.TotalProcessed,
		"dropped_oldest", finalMetrics.TotalDroppedOldest,
		"final_buffer_depth", finalBuffer.CurrentDepth,
	)

	return nil
}
