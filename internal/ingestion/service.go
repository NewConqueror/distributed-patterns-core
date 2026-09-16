package ingestion

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"geopulse/internal/buffer"
)

// TelemetryServer implements telemetryv1.TelemetryServiceServer.
type TelemetryServer struct {
	telemetryv1.UnimplementedTelemetryServiceServer

	buffer     *buffer.RingBuffer
	workerPool *WorkerPool
	metrics    *Metrics
	logger     *slog.Logger
}

// Config holds configuration parameters for the ingestion server.
type Config struct {
	BufferCapacity int
	DropStrategy   buffer.DropStrategy
	NumWorkers     int
}

// DefaultConfig returns recommended configuration for standard telemetry ingestion workloads.
func DefaultConfig() Config {
	return Config{
		BufferCapacity: 10000,
		DropStrategy:   buffer.DropOldest,
		NumWorkers:     4,
	}
}

// NewTelemetryServer initializes a TelemetryServer along with its backpressure buffer and worker pool.
func NewTelemetryServer(cfg Config, logger *slog.Logger) *TelemetryServer {
	if logger == nil {
		logger = slog.Default()
	}

	metrics := NewMetrics()
	rb := buffer.NewRingBuffer(cfg.BufferCapacity, cfg.DropStrategy)
	wp := NewWorkerPool(cfg.NumWorkers, rb, metrics, logger)

	return &TelemetryServer{
		buffer:     rb,
		workerPool: wp,
		metrics:    metrics,
		logger:     logger,
	}
}

// Start launches background workers.
func (s *TelemetryServer) Start(ctx context.Context) {
	s.workerPool.Start(ctx)
}

// Stop gracefully shuts down the worker pool and cleans up resources.
func (s *TelemetryServer) Stop() {
	s.workerPool.Stop()
}

// WorkerPool returns the server's worker pool (to register hooks for Phase 3 & 4).
func (s *TelemetryServer) WorkerPool() *WorkerPool {
	return s.workerPool
}

// Buffer returns the underlying ring buffer.
func (s *TelemetryServer) Buffer() *buffer.RingBuffer {
	return s.buffer
}

// Metrics returns the engine observability metrics.
func (s *TelemetryServer) Metrics() *Metrics {
	return s.metrics
}

// StreamLocation handles high-throughput client streaming of driver location pings.
func (s *TelemetryServer) StreamLocation(stream telemetryv1.TelemetryService_StreamLocationServer) error {
	ctx := stream.Context()
	streamID := s.metrics.NextStreamID()
	s.metrics.ActiveStreams.Add(1)
	defer s.metrics.ActiveStreams.Add(-1)

	s.logger.Info("gRPC client stream connected", "stream_id", streamID)

	var (
		processedCount int64 = 0
		driverID       string
	)

	// Stream receive loop
	for {
		// Context cancellation / disconnect check
		select {
		case <-ctx.Done():
			s.logger.Warn("gRPC client stream context cancelled",
				"stream_id", streamID,
				"driver_id", driverID,
				"reason", ctx.Err(),
			)
			return ctx.Err()
		default:
		}

		ping, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			s.logger.Info("gRPC client completed stream (EOF)",
				"stream_id", streamID,
				"driver_id", driverID,
				"total_received", processedCount,
			)
			return stream.SendAndClose(&telemetryv1.StreamAck{
				Success:        true,
				Message:        "Telemetry stream completed successfully",
				ProcessedCount: processedCount,
				ServerTime:     time.Now().UnixMilli(),
			})
		}
		if err != nil {
			s.logger.Error("gRPC stream read error",
				"stream_id", streamID,
				"driver_id", driverID,
				"err", err,
			)
			return err
		}

		if driverID == "" && ping.GetDriverId() != "" {
			driverID = ping.GetDriverId()
			s.logger.Debug("Identified driver stream", "stream_id", streamID, "driver_id", driverID)
		}

		s.metrics.TotalReceived.Add(1)
		processedCount++

		// Push to backpressure ring buffer with drop policy
		dropped, droppedOldest := s.buffer.Push(ping)
		if dropped {
			if droppedOldest {
				s.metrics.TotalDroppedOldest.Add(1)
			} else {
				s.metrics.TotalDroppedNewest.Add(1)
			}
		}
	}
}

// GetNearbyDrivers is a stubbed query handler to be fully powered by Redis Geospatial in Phase 3.
func (s *TelemetryServer) GetNearbyDrivers(ctx context.Context, req *telemetryv1.RadiusQuery) (*telemetryv1.DriverList, error) {
	s.logger.Info("Received GetNearbyDrivers request (stub)",
		"lat", req.Latitude,
		"lon", req.Longitude,
		"radius_km", req.RadiusKm,
	)

	return &telemetryv1.DriverList{
		Drivers:         []*telemetryv1.NearbyDriver{},
		SearchTimestamp: time.Now().UnixMilli(),
	}, nil
}
