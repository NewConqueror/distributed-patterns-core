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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/distributed"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/observability"
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

	logger.Info("Starting Distributed Fault-Tolerant Task Orchestrator (Phase 3: Observability & Health)")

	// 2. OS Signal Listening using signal.NotifyContext
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 3. Setup Observability: Prometheus Metrics & OpenTelemetry Tracing
	promReg := prometheus.DefaultRegisterer.(*prometheus.Registry)
	metrics := observability.NewMetrics(promReg)

	// Tracing provider
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracingMgr := observability.NewTracingManager(tp)

	// 4. Redis Client Initialization
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:            redisAddr,
		DialTimeout:     1 * time.Second,
		MaxRetries:      1,
		DisableIndentity: true,
	})
	defer rdb.Close()

	// 5. Start Observability HTTP Server (/metrics, /healthz, /ready)
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9090"
	}
	obsServer := observability.NewServer(observability.ServerConfig{
		Addr:   ":" + metricsPort,
		Logger: logger,
	}, metrics, rdb)

	if err := obsServer.Start(); err != nil {
		logger.Error("Failed to start observability HTTP server", "err", err)
	} else {
		logger.Info("Observability endpoints active",
			"metrics", fmt.Sprintf("http://localhost:%s/metrics", metricsPort),
			"healthz", fmt.Sprintf("http://localhost:%s/healthz", metricsPort),
			"ready", fmt.Sprintf("http://localhost:%s/ready", metricsPort),
		)
	}

	// Statistics counters
	var (
		completedCount atomic.Int64
		failedCount    atomic.Int64
		dlqCount       atomic.Int64
	)

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

	// 6. Local Worker Pool Initialization (with Metrics & Tracing instrumentation)
	poolCfg := pool.Config{
		WorkersCount:    runtime.NumCPU(),
		QueueCapacity:   50,
		DefaultTimeout:  5 * time.Second,
		ShutdownTimeout: 15 * time.Second,
		Logger:          logger,
		Metrics:         metrics,
		Tracer:          tracingMgr,
	}
	workerPool := pool.NewPool(poolCfg, onComplete)
	registerHandlers(workerPool, logger)
	workerPool.Start()

	// 7. Redis Connection Check for Phase 2 Distributed Mode
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 1*time.Second)
	pingErr := rdb.Ping(pingCtx).Err()
	pingCancel()

	var workerNode *distributed.WorkerNode
	var distQueue *distributed.Queue

	if pingErr == nil {
		logger.Info("Connected to Redis! Running in Phase 2 Distributed Cluster Mode", "addr", redisAddr)

		distCfg := distributed.DefaultConfig()
		distCfg.Logger = logger

		nodeID := fmt.Sprintf("worker-node-%d-%d", os.Getpid(), rand.Intn(1000))
		distQueue = distributed.NewQueue(rdb, distCfg)
		workerNode = distributed.NewWorkerNode(rdb, distCfg, nodeID, workerPool)

		if startErr := workerNode.Start(); startErr != nil {
			logger.Error("Failed to start distributed WorkerNode, continuing in local mode", "err", startErr)
		} else {
			defer workerNode.Stop()
		}
	} else {
		logger.Warn("Redis not detected. Running in Phase 1 In-Memory Mode (Start Redis on 127.0.0.1:6379 to activate Phase 2)",
			"redis_addr", redisAddr,
			"ping_err", pingErr,
		)
	}

	// 8. Simulate Incoming Jobs with Distributed Tracing (Producer Trace Context Propagation)
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		jobTypes := []string{"email", "temporary_fail", "fatal_corrupt", "panic_task", "quick_calc"}
		producerTracer := tp.Tracer("job-producer")

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
						Subject:   "Task Notification",
					})
				}

				newJob := task.NewJob(jobID, jType, payload, 2)

				// Create Producer Span and inject trace context (TraceID & SpanID) into job.Metadata
				pCtx, pSpan := producerTracer.Start(ctx, fmt.Sprintf("producer:enqueue:%s", jType))
				tracingMgr.InjectTraceContext(pCtx, newJob)
				pSpan.End()

				// If Redis distributed mode is active, publish to Redis Stream (XADD)
				if workerNode != nil && distQueue != nil {
					_, enqueueErr := distQueue.Enqueue(ctx, newJob)
					if enqueueErr != nil && !errors.Is(enqueueErr, context.Canceled) {
						logger.Error("Failed to enqueue job to Redis Stream", "job_id", jobID, "err", enqueueErr)
					}
				} else {
					// Otherwise, submit directly to local worker pool
					submitCtx, submitCancel := context.WithTimeout(ctx, 500*time.Millisecond)
					submitErr := workerPool.SubmitWithTimeout(submitCtx, newJob)
					submitCancel()

					if submitErr != nil {
						if errors.Is(submitErr, pool.ErrQueueFull) {
							logger.Warn("Local queue saturated, applying backpressure", "job_id", jobID)
						} else if errors.Is(submitErr, context.Canceled) || errors.Is(submitErr, pool.ErrPoolClosed) {
							return
						}
					}
				}

				time.Sleep(200 * time.Millisecond)
			}
		}
	}()

	// 9. Wait for OS termination signal
	logger.Info("Application is running. Press Ctrl+C or send SIGTERM to trigger graceful shutdown.")
	<-ctx.Done()

	logger.Info("Shutdown signal received! Beginning graceful shutdown cascade...")
	stop()

	// Wait for producer to stop
	<-producerDone

	// 10. Stop Distributed WorkerNode if active
	if workerNode != nil {
		workerNode.Stop()
	}

	// 11. Gracefully Drain In-Memory Worker Pool
	shutdownStart := time.Now()
	if err := workerPool.Shutdown(15 * time.Second); err != nil {
		logger.Error("Graceful shutdown encountered an issue", "err", err)
	} else {
		logger.Info("Worker pool gracefully drained all active and queued jobs", "duration", time.Since(shutdownStart))
	}

	// 12. Shutdown Observability HTTP Server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	if err := obsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("Observability server shutdown error", "err", err)
	}

	// 13. Report Final Stats
	logger.Info("--- Final Orchestrator Statistics ---",
		"completed", completedCount.Load(),
		"retried_failed", failedCount.Load(),
		"dlq_terminal", dlqCount.Load(),
	)
}

func registerHandlers(p *pool.Pool, logger *slog.Logger) {
	p.RegisterHandler("email", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		select {
		case <-time.After(100 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	p.RegisterHandler("quick_calc", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		select {
		case <-time.After(50 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	p.RegisterHandler("temporary_fail", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		select {
		case <-time.After(80 * time.Millisecond):
			return task.NewTemporaryError(errors.New("third-party upstream rate limited"))
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	p.RegisterHandler("fatal_corrupt", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		return task.NewFatalError(errors.New("unrecoverable schema deserialization failure"))
	}))

	p.RegisterHandler("panic_task", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		panic("unexpected nil pointer dereference simulation")
	}))
}
