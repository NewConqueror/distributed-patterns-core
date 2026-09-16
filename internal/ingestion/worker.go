package ingestion

import (
	"context"
	"log/slog"
	"sync"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"geopulse/internal/buffer"
)

// ProcessHook is an optional callback invoked by workers when a ping is dequeued.
// This allows cleanly plugging in Redis Hot Path (Phase 3) and PostGIS Batching (Phase 4).
type ProcessHook func(ctx context.Context, ping *telemetryv1.LocationPing) error

// WorkerPool manages background consumer goroutines that pull telemetry pings
// from the ingestion ring buffer and process them concurrently.
type WorkerPool struct {
	numWorkers int
	buffer     *buffer.RingBuffer
	metrics    *Metrics
	logger     *slog.Logger
	hooks      []ProcessHook

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWorkerPool creates a new telemetry processing worker pool.
func NewWorkerPool(numWorkers int, rb *buffer.RingBuffer, metrics *Metrics, logger *slog.Logger) *WorkerPool {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &WorkerPool{
		numWorkers: numWorkers,
		buffer:     rb,
		metrics:    metrics,
		logger:     logger,
		hooks:      make([]ProcessHook, 0),
	}
}

// RegisterHook adds a processing hook (for Redis geospatial caching or DB batching).
func (wp *WorkerPool) RegisterHook(hook ProcessHook) {
	wp.hooks = append(wp.hooks, hook)
}

// Start spawns the worker goroutines with structured lifecycle management.
func (wp *WorkerPool) Start(parentCtx context.Context) {
	wp.ctx, wp.cancel = context.WithCancel(parentCtx)

	wp.logger.Info("Starting ingestion worker pool", "workers", wp.numWorkers)
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.runWorker(i)
	}
}

// Stop initiates graceful shutdown of all workers and waits for in-flight jobs to complete.
func (wp *WorkerPool) Stop() {
	if wp.cancel != nil {
		wp.cancel()
	}
	wp.buffer.Close()
	wp.wg.Wait()
	wp.logger.Info("Ingestion worker pool stopped cleanly")
}

func (wp *WorkerPool) runWorker(id int) {
	defer wp.wg.Done()

	for {
		item, err := wp.buffer.Pop(wp.ctx)
		if err != nil {
			// Buffer closed or context canceled
			return
		}

		wp.processPing(id, item)
	}
}

func (wp *WorkerPool) processPing(workerID int, ping *telemetryv1.LocationPing) {
	// Defensive check: validate GPS coordinates range
	if ping.Latitude < -90.0 || ping.Latitude > 90.0 ||
		ping.Longitude < -180.0 || ping.Longitude > 180.0 {
		wp.logger.Warn("Invalid GPS coordinates dropped",
			"worker", workerID,
			"driver_id", ping.DriverId,
			"lat", ping.Latitude,
			"lon", ping.Longitude,
		)
		return
	}

	// Execute registered pipelines (Redis Hot Path / DB Batch Buffer)
	for _, hook := range wp.hooks {
		if err := hook(wp.ctx, ping); err != nil {
			wp.logger.Error("Error executing processing hook",
				"worker", workerID,
				"driver_id", ping.DriverId,
				"err", err,
			)
		}
	}

	wp.metrics.TotalProcessed.Add(1)
}
