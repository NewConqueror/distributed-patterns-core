package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/pool"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

type EmailPayload struct {
	Recipient string `json:"recipient"`
	Subject   string `json:"subject"`
}

func main() {
	// 1. Logger Setup
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("Starting Distributed Fault-Tolerant Task Orchestrator (Phase 1: In-Memory Concurrency)")

	// 2. OS Signal Listening using signal.NotifyContext
	// Catches SIGINT (Ctrl+C) and SIGTERM (Kill)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Statistics counters
	var (
		completedCount atomic.Int64
		failedCount    atomic.Int64
		dlqCount       atomic.Int64
	)

	// Callback hook to monitor job outcomes
	onComplete := func(job *task.Job, err error) {
		switch job.State() {
		case task.StateCompleted:
			completedCount.Add(1)
		case task.StateDLQ:
			dlqCount.Add(1)
		case task.StateFailed:
			failedCount.Add(1)
		}
	}

	// 3. Worker Pool Configuration & Initialization
	cfg := pool.Config{
		WorkersCount:    runtime.NumCPU(),
		QueueCapacity:   50,
		DefaultTimeout:  5 * time.Second,
		ShutdownTimeout: 15 * time.Second,
		Logger:          logger,
	}
	workerPool := pool.NewPool(cfg, onComplete)

	// 4. Register Task Handlers for different job types
	registerHandlers(workerPool, logger)

	// 5. Start Worker Pool
	workerPool.Start()

	// 6. Simulate Incoming Jobs in a separate goroutine
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		jobTypes := []string{"email", "temporary_fail", "fatal_corrupt", "panic_task", "quick_calc"}

		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				logger.Info("Job producer stopping due to shutdown signal")
				return
			default:
				jType := jobTypes[rand.Intn(len(jobTypes))]
				jobID := fmt.Sprintf("job-%04d", i)

				var payload []byte
				if jType == "email" {
					payload, _ = json.Marshal(EmailPayload{
						Recipient: fmt.Sprintf("user%d@example.com", i),
						Subject:   "Task Processed Notification",
					})
				}

				maxRetries := 2
				newJob := task.NewJob(jobID, jType, payload, maxRetries)

				// Backpressure submission
				submitCtx, submitCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				err := workerPool.SubmitWithTimeout(submitCtx, newJob)
				submitCancel()

				if err != nil {
					if errors.Is(err, pool.ErrQueueFull) {
						logger.Warn("Queue is saturated, applying backpressure", "job_id", jobID)
					} else if errors.Is(err, context.Canceled) || errors.Is(err, pool.ErrPoolClosed) {
						return
					}
				}

				time.Sleep(150 * time.Millisecond)
			}
		}
	}()

	// 7. Wait for OS termination signal
	logger.Info("Application is running. Press Ctrl+C or send SIGTERM to trigger graceful shutdown.")
	<-ctx.Done()

	logger.Info("Shutdown signal received! Beginning graceful shutdown cascade...")
	stop() // Stop catching further signals to allow forced termination on repeated Ctrl+C

	// Wait for producer to stop dispatching
	<-producerDone

	// 8. Graceful Shutdown of Worker Pool (wait up to 15s for active jobs)
	shutdownStart := time.Now()
	if err := workerPool.Shutdown(15 * time.Second); err != nil {
		logger.Error("Graceful shutdown encountered an issue", "err", err)
	} else {
		logger.Info("Worker pool gracefully drained all active and queued jobs", "duration", time.Since(shutdownStart))
	}

	// 9. Report Final Stats
	logger.Info("--- Final Orchestrator Statistics ---",
		"completed", completedCount.Load(),
		"retried_failed", failedCount.Load(),
		"dlq_terminal", dlqCount.Load(),
	)
}

func registerHandlers(p *pool.Pool, logger *slog.Logger) {
	// Standard success job
	p.RegisterHandler("email", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		select {
		case <-time.After(100 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	// Simple calculation job
	p.RegisterHandler("quick_calc", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		select {
		case <-time.After(50 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	// Flaky job: fails with TemporaryError
	p.RegisterHandler("temporary_fail", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		select {
		case <-time.After(80 * time.Millisecond):
			return task.NewTemporaryError(errors.New("third-party upstream rate limited"))
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	// Fatal error job: should never retry, immediately routes to DLQ
	p.RegisterHandler("fatal_corrupt", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		return task.NewFatalError(errors.New("unrecoverable schema deserialization failure"))
	}))

	// Panicking job: triggers recover() in worker, worker survives
	p.RegisterHandler("panic_task", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		panic("unexpected nil pointer dereference simulation")
	}))
}
