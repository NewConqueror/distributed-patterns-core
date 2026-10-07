package persistence

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"geopulse/internal/metrics"
)

// Metrics exposes observable execution statistics for the batch persistence pipeline.
type Metrics struct {
	TotalPushed  int64
	TotalFlushed int64
	TotalBatches int64
	TotalErrors  int64
	CurrentDepth int
}

// BatchEngine orchestrates the in-memory batch accumulation and asynchronous
// dual-trigger flushing (size threshold and time ticker) into PostgreSQL.
type BatchEngine struct {
	repo   Repository
	buffer *BatchBuffer
	cfg    BatchConfig
	logger *slog.Logger

	flushSignal chan struct{}
	workQueue   chan []*TelemetryRecord

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	totalPushed  atomic.Int64
	totalFlushed atomic.Int64
	totalBatches atomic.Int64
	totalErrors  atomic.Int64
}

// NewBatchEngine instantiates the batch engine with buffered accumulation and worker channels.
func NewBatchEngine(repo Repository, cfg BatchConfig, logger *slog.Logger) *BatchEngine {
	if logger == nil {
		logger = slog.Default()
	}

	return &BatchEngine{
		repo:        repo,
		buffer:      NewBatchBuffer(cfg.BatchSize),
		cfg:         cfg,
		logger:      logger,
		flushSignal: make(chan struct{}, 1),
		workQueue:   make(chan []*TelemetryRecord, cfg.QueueCapacity),
	}
}

// ProcessPing implements the ingestion.ProcessHook contract, enqueueing incoming
// telemetry pings into the in-memory batch buffer without touching the database.
func (e *BatchEngine) ProcessPing(ctx context.Context, ping *telemetryv1.LocationPing) error {
	e.totalPushed.Add(1)

	_, thresholdMet := e.buffer.Push(ping)
	if thresholdMet {
		// Non-blocking trigger signal to coordinator
		select {
		case e.flushSignal <- struct{}{}:
		default:
		}
	}

	return nil
}

// Start launches the dual-trigger coordinator and asynchronous database flusher worker.
func (e *BatchEngine) Start(parentCtx context.Context) {
	e.ctx, e.cancel = context.WithCancel(parentCtx)

	e.logger.Info("Starting Batch Persistence Engine",
		"batch_size", e.cfg.BatchSize,
		"flush_interval", e.cfg.FlushInterval,
		"bulk_mode", e.cfg.BulkMode,
	)

	// 1. Worker to perform database write operations
	e.wg.Add(1)
	go e.flusherWorker()

	// 2. Coordinator to trigger on size or timer
	e.wg.Add(1)
	go e.coordinator()
}

// coordinator monitors the flush timer and size signals, draining the buffer and queueing batches.
func (e *BatchEngine) coordinator() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return

		case <-e.flushSignal:
			e.drainAndQueue("size_trigger")

		case <-ticker.C:
			e.drainAndQueue("timer_trigger")
		}
	}
}

// drainAndQueue detaches records from the buffer and pushes to the flusher worker queue.
func (e *BatchEngine) drainAndQueue(reason string) {
	batch := e.buffer.Drain()
	if len(batch) == 0 {
		return
	}

	e.logger.Debug("Triggered batch flush", "reason", reason, "count", len(batch))

	select {
	case e.workQueue <- batch:
	case <-e.ctx.Done():
		// Emergency direct write on shutdown if queue is full
		_ = e.repo.BulkInsert(context.Background(), batch)
	}
}

// flusherWorker consumes detached batches and flushes them to PostgreSQL.
func (e *BatchEngine) flusherWorker() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			// Process remaining queued batches before exiting
			for len(e.workQueue) > 0 {
				batch := <-e.workQueue
				e.writeBatch(context.Background(), batch)
			}
			return

		case batch, ok := <-e.workQueue:
			if !ok {
				return
			}
			e.writeBatch(e.ctx, batch)
		}
	}
}

func (e *BatchEngine) writeBatch(ctx context.Context, batch []*TelemetryRecord) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()
	err := e.repo.BulkInsert(ctx, batch)
	duration := time.Since(start).Seconds()
	metrics.Default().BatchFlushDuration.Observe(duration)

	if err != nil {
		e.totalErrors.Add(1)
		metrics.Default().BatchFlushErrors.Inc()
		e.logger.Error("Batch persistence failed", "err", err, "records", len(batch))
		return
	}

	e.totalBatches.Add(1)
	e.totalFlushed.Add(int64(len(batch)))
	metrics.Default().BatchFlushRecords.Add(float64(len(batch)))
}

// Stop initiates graceful shutdown, flushing any remaining in-memory records to PostgreSQL.
func (e *BatchEngine) Stop() {
	if e.cancel != nil {
		e.cancel()
	}

	e.wg.Wait()

	// Drain any last lingering records from the buffer directly
	finalBatch := e.buffer.Drain()
	if len(finalBatch) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.repo.BulkInsert(ctx, finalBatch); err != nil {
			e.totalErrors.Add(1)
			e.logger.Error("Failed flushing final batch on shutdown", "err", err)
		} else {
			e.totalBatches.Add(1)
			e.totalFlushed.Add(int64(len(finalBatch)))
		}
	}

	e.repo.Close()
	e.logger.Info("Batch Persistence Engine stopped cleanly",
		"total_pushed", e.totalPushed.Load(),
		"total_flushed", e.totalFlushed.Load(),
		"total_batches", e.totalBatches.Load(),
		"total_errors", e.totalErrors.Load(),
	)
}

// Metrics returns a snapshot of batch persistence statistics.
func (e *BatchEngine) Metrics() Metrics {
	return Metrics{
		TotalPushed:  e.totalPushed.Load(),
		TotalFlushed: e.totalFlushed.Load(),
		TotalBatches: e.totalBatches.Load(),
		TotalErrors:  e.totalErrors.Load(),
		CurrentDepth: e.buffer.Size(),
	}
}
