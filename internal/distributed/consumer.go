package distributed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/pool"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

// WorkerNode represents a distributed consumer node that reads from Redis Streams,
// handles leases/heartbeats, delegates execution to the in-memory pool, and coordinates ACK/Retry/DLQ.
type WorkerNode struct {
	client       *redis.Client
	queue        *Queue
	leaseMgr     *LeaseManager
	retryMgr     *RetryManager
	reaper       *Reaper
	localPool    *pool.Pool
	cfg          Config
	consumerName string
	logger       *slog.Logger

	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	isStopping bool
	mu         sync.Mutex
}

// NewWorkerNode initializes a new distributed worker node.
func NewWorkerNode(
	client *redis.Client,
	cfg Config,
	consumerName string,
	localPool *pool.Pool,
) *WorkerNode {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	queue := NewQueue(client, cfg)
	leaseMgr := NewLeaseManager(client, cfg)
	retryMgr := NewRetryManager(client, queue, cfg)

	ctx, cancel := context.WithCancel(context.Background())

	node := &WorkerNode{
		client:       client,
		queue:        queue,
		leaseMgr:     leaseMgr,
		retryMgr:     retryMgr,
		localPool:    localPool,
		cfg:          cfg,
		consumerName: consumerName,
		logger:       cfg.Logger,
		ctx:          ctx,
		cancel:       cancel,
	}

	// Setup Reaper with callback to re-process claimed jobs
	node.reaper = NewReaper(client, queue, leaseMgr, retryMgr, cfg, consumerName, func(ctx context.Context, job *task.Job, msgID string) error {
		node.dispatchJob(job, msgID)
		return nil
	})

	return node
}

// Start launches the consumer loop, delayed tasks scheduler, and the orphan job reaper.
func (w *WorkerNode) Start() error {
	// 1. Ensure Stream and Consumer Group exist
	if err := w.queue.EnsureConsumerGroup(w.ctx); err != nil {
		return fmt.Errorf("failed to ensure consumer group: %w", err)
	}

	// 2. Start delayed tasks poller
	w.retryMgr.StartPoller(w.ctx, 500*time.Millisecond)

	// 3. Start orphan jobs reaper
	w.reaper.Start(w.ctx)

	// 4. Start consumer stream reader loop
	w.wg.Add(1)
	go w.readStreamLoop()

	w.logger.Info("Distributed WorkerNode started successfully",
		"node_id", w.consumerName,
		"stream", w.cfg.StreamName,
		"group", w.cfg.GroupName,
	)
	return nil
}

// readStreamLoop continuously fetches new messages using XREADGROUP.
func (w *WorkerNode) readStreamLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			// Read new messages (">" means messages never delivered to any other consumer)
			streams, err := w.client.XReadGroup(w.ctx, &redis.XReadGroupArgs{
				Group:    w.cfg.GroupName,
				Consumer: w.consumerName,
				Streams:  []string{w.cfg.StreamName, ">"},
				Count:    10,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if errors.Is(err, redis.Nil) || errors.Is(w.ctx.Err(), context.Canceled) {
					continue
				}
				w.logger.Error("Error reading from Redis Stream", "err", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					w.processIncomingMessage(msg)
				}
			}
		}
	}
}

// processIncomingMessage deserializes the stream entry and hands it to the local pool.
func (w *WorkerNode) processIncomingMessage(msg redis.XMessage) {
	job, err := deserializeJobFromStream(msg.Values)
	if err != nil {
		w.logger.Error("Failed to deserialize job from stream, moving to DLQ immediately",
			"msg_id", msg.ID,
			"err", err,
		)
		_, _ = w.queue.EnqueueDLQ(w.ctx, task.NewJob("unknown", "corrupt", nil, 0), msg.ID, err.Error())
		return
	}

	w.dispatchJob(job, msg.ID)
}

// dispatchJob acquires lease, attaches heartbeat, executes in local pool, and handles ACK/Retry/DLQ.
func (w *WorkerNode) dispatchJob(job *task.Job, msgID string) {
	// 1. Acquire Lease Lock
	acquired, err := w.leaseMgr.AcquireLock(w.ctx, job.ID, w.consumerName, w.cfg.LockTTL)
	if err != nil || !acquired {
		w.logger.Warn("Could not acquire lease lock for job, skipping execution",
			"job_id", job.ID,
			"consumer", w.consumerName,
			"err", err,
		)
		return
	}

	// 2. Start background Heartbeat to renew TTL while job is executing
	stopHeartbeat, lockLostCh := w.leaseMgr.StartHeartbeat(w.ctx, job.ID, w.consumerName, w.cfg.LockTTL, w.cfg.HeartbeatInterval)

	// 3. Monitor for unexpected lock loss in a separate goroutine
	jobCtx, jobCancel := context.WithCancel(w.ctx)
	go func() {
		select {
		case <-lockLostCh:
			w.logger.Error("Job cancelled because lease lock was lost", "job_id", job.ID)
			jobCancel()
		case <-jobCtx.Done():
		}
	}()

	// 4. Submit to local WorkerPool (which has panic recovery and concurrency management)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer jobCancel()
		defer stopHeartbeat()
		defer func() {
			_ = w.leaseMgr.ReleaseLock(context.Background(), job.ID, w.consumerName)
		}()

		// Submit with timeout backpressure
		err := w.localPool.SubmitWithTimeout(w.ctx, job)
		if err != nil {
			w.logger.Error("Failed to submit job to local worker pool", "job_id", job.ID, "err", err)
			_ = w.retryMgr.HandleFailure(w.ctx, job, msgID, err)
			return
		}

		// Wait until the job finishes execution in the pool
		w.waitForJobCompletion(jobCtx, job, msgID)
	}()
}

// waitForJobCompletion monitors job state transitions and executes ACK / Retry / DLQ.
func (w *WorkerNode) waitForJobCompletion(ctx context.Context, job *task.Job, msgID string) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Warn("Context terminated while waiting for job completion", "job_id", job.ID)
			return
		case <-ticker.C:
			switch job.State() {
			case task.StateCompleted:
				// Job finished successfully! Acknowledge in Redis Stream (clears PEL)
				if err := w.queue.Ack(w.ctx, msgID); err != nil {
					w.logger.Error("Failed to XACK completed job", "job_id", job.ID, "msg_id", msgID, "err", err)
				} else {
					w.logger.Debug("Job completed and XACK sent", "job_id", job.ID, "msg_id", msgID)
				}
				return

			case task.StateDLQ:
				// Job went to DLQ locally; replicate to distributed DLQ stream and ACK original
				_, _ = w.queue.EnqueueDLQ(w.ctx, job, msgID, "job routed to DLQ in worker pool")
				return

			case task.StateFailed:
				// Job failed and needs retry handling
				_ = w.retryMgr.HandleFailure(w.ctx, job, msgID, fmt.Errorf("job failed with retry"))
				return
			}
		}
	}
}

// Stop initiates graceful shutdown of the distributed worker node.
func (w *WorkerNode) Stop() {
	w.mu.Lock()
	if w.isStopping {
		w.mu.Unlock()
		return
	}
	w.isStopping = true
	w.mu.Unlock()

	w.logger.Info("Stopping distributed WorkerNode...")
	w.cancel()
	w.wg.Wait()
	w.logger.Info("Distributed WorkerNode stopped cleanly")
}
