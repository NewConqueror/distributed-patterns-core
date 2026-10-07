package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

var (
	// Lua script to atomically pop ready jobs from ZSET and XADD them back to the active Stream.
	// Guarantees zero duplicate enqueues across multiple distributed nodes.
	reEnqueueReadyJobsScript = redis.NewScript(`
		local delayed_key = KEYS[1]
		local stream_key = KEYS[2]
		local max_score = ARGV[1]
		local limit = tonumber(ARGV[2])

		local items = redis.call('zrangebyscore', delayed_key, '-inf', max_score, 'LIMIT', 0, limit)
		local count = 0

		for _, raw_item in ipairs(items) do
			if redis.call('zrem', delayed_key, raw_item) == 1 then
				redis.call('xadd', stream_key, '*', 'data', raw_item)
				count = count + 1
			end
		end

		return count
	`)
)

// RetryManager handles exponential backoff computation and delayed retry scheduling.
type RetryManager struct {
	client *redis.Client
	queue  *Queue
	cfg    Config
	logger *slog.Logger
}

// NewRetryManager creates a new RetryManager instance.
func NewRetryManager(client *redis.Client, queue *Queue, cfg Config) *RetryManager {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &RetryManager{
		client: client,
		queue:  queue,
		cfg:    cfg,
		logger: cfg.Logger,
	}
}

// CalculateBackoff implements: WaitTime = Base * 2^RetryCount + RandomJitter
func (r *RetryManager) CalculateBackoff(retryCount int) time.Duration {
	base := r.cfg.BaseRetryBackoff
	if base <= 0 {
		base = 1 * time.Second
	}
	maxBackoff := r.cfg.MaxRetryBackoff
	if maxBackoff <= 0 {
		maxBackoff = 60 * time.Second
	}

	// Calculate exponential multiplier: 2^retryCount
	multiplier := math.Pow(2, float64(retryCount))
	backoffDuration := time.Duration(float64(base) * multiplier)

	// Add Random Jitter (0ms to base/2)
	jitterLimit := int64(base / 2)
	if jitterLimit > 0 {
		jitter := time.Duration(rand.Int63n(jitterLimit))
		backoffDuration += jitter
	}

	// Apply maximum cap
	if backoffDuration > maxBackoff {
		backoffDuration = maxBackoff
	}

	return backoffDuration
}

// HandleFailure determines whether to schedule a delayed retry or move the job to DLQ.
func (r *RetryManager) HandleFailure(ctx context.Context, job *task.Job, originalMsgID string, jobErr error) error {
	// If the error is fatal or no retries left, move directly to DLQ
	if task.IsFatal(jobErr) {
		r.logger.Warn("Fatal error encountered, directing to DLQ immediately",
			"job_id", job.ID,
			"err", jobErr,
		)
		_, err := r.queue.EnqueueDLQ(ctx, job, originalMsgID, fmt.Sprintf("fatal error: %v", jobErr))
		return err
	}

	if !job.CanRetry() {
		r.logger.Warn("Job retries exhausted, directing to DLQ",
			"job_id", job.ID,
			"retry_count", job.RetryCount,
			"max_retries", job.MaxRetries,
		)
		_, err := r.queue.EnqueueDLQ(ctx, job, originalMsgID, fmt.Sprintf("retries exhausted: %v", jobErr))
		return err
	}

	// Increment retry count and transition states
	job.IncrementRetry()
	_ = job.TransitionTo(task.StateFailed)
	_ = job.TransitionTo(task.StatePending)

	// Calculate exponential backoff
	backoff := r.CalculateBackoff(job.RetryCount)
	runAt := time.Now().Add(backoff)

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to serialize job for delayed retry: %w", err)
	}

	// Save to Redis Sorted Set with score = Unix Milliseconds
	score := float64(runAt.UnixMilli())
	err = r.client.ZAdd(ctx, r.cfg.DelayedZSetName, redis.Z{
		Score:  score,
		Member: string(data),
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to add job to delayed ZSET: %w", err)
	}

	// Acknowledge the original message from the active stream so it leaves the PEL
	if originalMsgID != "" {
		if ackErr := r.queue.Ack(ctx, originalMsgID); ackErr != nil {
			r.logger.Warn("Failed to ack message after scheduling delayed retry", "msg_id", originalMsgID, "err", ackErr)
		}
	}

	r.logger.Info("Job scheduled for delayed retry",
		"job_id", job.ID,
		"retry_count", job.RetryCount,
		"max_retries", job.MaxRetries,
		"backoff", backoff,
		"run_at", runAt.Format(time.RFC3339),
	)
	return nil
}

// StartPoller runs a background loop that re-enqueues mature delayed jobs into the active Stream.
func (r *RetryManager) StartPoller(ctx context.Context, pollInterval time.Duration) {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}

	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.pollAndReEnqueue(ctx)
			}
		}
	}()
}

func (r *RetryManager) pollAndReEnqueue(ctx context.Context) {
	nowMs := fmt.Sprintf("%d", time.Now().UnixMilli())
	batchSize := 20

	res, err := reEnqueueReadyJobsScript.Run(ctx, r.client,
		[]string{r.cfg.DelayedZSetName, r.cfg.StreamName},
		nowMs,
		batchSize,
	).Int()

	if err != nil && !errors.Is(err, redis.Nil) && !errors.Is(ctx.Err(), context.Canceled) {
		r.logger.Error("Failed to poll delayed tasks from ZSET", "err", err)
		return
	}

	if res > 0 {
		r.logger.Info("Re-enqueued ready delayed jobs into active Stream", "count", res)
	}
}
