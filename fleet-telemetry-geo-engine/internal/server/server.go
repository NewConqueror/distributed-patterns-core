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
	"geopulse/internal/metrics"
	"geopulse/internal/persistence"
	"geopulse/internal/spatial"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Config encapsulates server startup parameters.
type Config struct {
	Port           int
	BufferCapacity int
	NumWorkers     int
	DropStrategy   buffer.DropStrategy

	// Prometheus Metrics HTTP port (default: 2112)
	MetricsPort int

	// Redis Hot Path & Geospatial settings
	EnableRedis bool
	RedisCfg    spatial.RedisConfig

	// PostgreSQL Cold Path Batch Persistence settings
	EnablePostgres bool
	PostgresCfg    persistence.PostgresConfig
	BatchCfg       persistence.BatchConfig
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
		"redis_enabled", cfg.EnableRedis,
		"postgres_enabled", cfg.EnablePostgres,
	)

	ingestionCfg := ingestion.Config{
		BufferCapacity: cfg.BufferCapacity,
		DropStrategy:   cfg.DropStrategy,
		NumWorkers:     cfg.NumWorkers,
	}
	server := ingestion.NewTelemetryServer(ingestionCfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. Start Prometheus HTTP metrics server if configured
	var metricsServer *metrics.Server
	if cfg.MetricsPort > 0 {
		metricsAddr := fmt.Sprintf(":%d", cfg.MetricsPort)
		metricsServer = metrics.StartMetricsServer(metricsAddr, logger)
		defer func() {
			sCtx, sCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer sCancel()
			_ = metricsServer.Stop(sCtx)
		}()
	}

	// 1. Initialize and bind Redis Hot Path if enabled
	var sweeper *spatial.Sweeper
	if cfg.EnableRedis {
		redisClient, err := spatial.NewRedisClient(cfg.RedisCfg, logger)
		if err != nil {
			server.Stop()
			return fmt.Errorf("failed initializing Redis spatial layer: %w", err)
		}
		defer func() {
			_ = redisClient.Close()
		}()

		geoStore := spatial.NewGeoStore(redisClient, cfg.RedisCfg, nil, logger)
		sweeper = spatial.NewSweeper(redisClient, cfg.RedisCfg, logger)

		// Hook into worker pool for hot-path spatial indexing & pubsub fan-out
		server.WorkerPool().RegisterHook(geoStore.ProcessPing)

		// Hook into TelemetryServer for radius queries
		server.SetSpatialQuerier(geoStore)

		// Start background sweeper for ghost driver cleanup
		sweeper.Start(ctx)
		defer sweeper.Stop()
	}

	// 2. Initialize and bind PostgreSQL Cold Path Batch Engine if enabled
	var batchEngine *persistence.BatchEngine
	if cfg.EnablePostgres {
		repo, err := persistence.NewPostgresRepository(cfg.PostgresCfg, cfg.BatchCfg, logger)
		if err != nil {
			server.Stop()
			return fmt.Errorf("failed initializing PostgreSQL repository: %w", err)
		}

		if err := repo.InitSchema(ctx); err != nil {
			repo.Close()
			server.Stop()
			return fmt.Errorf("failed initializing database schema: %w", err)
		}

		batchEngine = persistence.NewBatchEngine(repo, cfg.BatchCfg, logger)

		// Hook into worker pool for asynchronous decoupled batch persistence
		server.WorkerPool().RegisterHook(batchEngine.ProcessPing)

		batchEngine.Start(ctx)
		defer batchEngine.Stop()
	}

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
				pruned := int64(0)
				if sweeper != nil {
					pruned = sweeper.TotalPruned()
				}
				var bm persistence.Metrics
				if batchEngine != nil {
					bm = batchEngine.Metrics()
				}
				logger.Info("Engine Telemetry Metrics",
					"active_streams", m.ActiveStreams,
					"total_received", m.TotalReceived,
					"total_processed", m.TotalProcessed,
					"dropped_oldest", m.TotalDroppedOldest,
					"buffer_depth", b.CurrentDepth,
					"stale_pruned", pruned,
					"db_flushed", bm.TotalFlushed,
					"db_batches", bm.TotalBatches,
					"db_errors", bm.TotalErrors,
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
