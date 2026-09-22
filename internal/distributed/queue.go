package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

var (
	ErrNilJob = errors.New("job cannot be nil")
)

// Config defines the configuration for distributed Redis stream operations.
type Config struct {
	StreamName        string        // Redis Stream key for active tasks (e.g., "tasks:stream")
	GroupName         string        // Redis Consumer Group name (e.g., "tasks:group")
	DLQStreamName     string        // Redis Stream key for dead-letter tasks (e.g., "tasks:dlq")
	DelayedZSetName   string        // Redis Sorted Set key for retries (e.g., "tasks:delayed")
	LockPrefix        string        // Redis key prefix for job lease locks (e.g., "job:lock:")
	LockTTL           time.Duration // Lease duration for job execution (e.g., 10s)
	HeartbeatInterval time.Duration // Interval to refresh lease TTL (e.g., 3s)
	ReaperInterval    time.Duration // Interval to check for orphan jobs (e.g., 15s)
	MinIdleTime       time.Duration // Minimum idle time before an unacked job can be claimed (e.g., 30s)
	BaseRetryBackoff  time.Duration // Base duration for exponential backoff (e.g., 1s)
	MaxRetryBackoff   time.Duration // Cap for exponential backoff (e.g., 60s)
	Logger            *slog.Logger
}

// DefaultConfig returns sensible defaults for distributed queue orchestration.
func DefaultConfig() Config {
	return Config{
		StreamName:        "tasks:stream",
		GroupName:         "tasks:group",
		DLQStreamName:     "tasks:dlq",
		DelayedZSetName:   "tasks:delayed",
		LockPrefix:        "job:lock:",
		LockTTL:           10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		ReaperInterval:    15 * time.Second,
		MinIdleTime:       30 * time.Second,
		BaseRetryBackoff:  1 * time.Second,
		MaxRetryBackoff:   60 * time.Second,
		Logger:            slog.Default(),
	}
}

// Queue handles Redis Stream publishing, group registration, and acknowledgements.
type Queue struct {
	client *redis.Client
	cfg    Config
	logger *slog.Logger
}

// NewQueue creates a new Queue instance.
func NewQueue(client *redis.Client, cfg Config) *Queue {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Queue{
		client: client,
		cfg:    cfg,
		logger: cfg.Logger,
	}
}

// EnsureConsumerGroup creates the stream and consumer group if they do not exist.
// Idempotent: ignores BUSYGROUP errors if the group is already registered.
func (q *Queue) EnsureConsumerGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.cfg.StreamName, q.cfg.GroupName, "$").Err()
	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			// Consumer group already exists, safe to ignore
			return nil
		}
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	q.logger.Info("Redis Consumer Group created",
		"stream", q.cfg.StreamName,
		"group", q.cfg.GroupName,
	)
	return nil
}

// Enqueue serializes a Job and appends it to the Redis Stream via XADD.
func (q *Queue) Enqueue(ctx context.Context, job *task.Job) (string, error) {
	if job == nil {
		return "", ErrNilJob
	}

	data, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("failed to serialize job: %w", err)
	}

	args := &redis.XAddArgs{
		Stream: q.cfg.StreamName,
		Values: map[string]interface{}{
			"id":   job.ID,
			"type": job.Type,
			"data": string(data),
		},
	}

	msgID, err := q.client.XAdd(ctx, args).Result()
	if err != nil {
		return "", fmt.Errorf("failed to XADD job to stream %s: %w", q.cfg.StreamName, err)
	}

	q.logger.Debug("Job enqueued to Redis Stream", "job_id", job.ID, "msg_id", msgID)
	return msgID, nil
}

// Ack acknowledges a message in the consumer group via XACK, removing it from PEL.
func (q *Queue) Ack(ctx context.Context, messageID string) error {
	err := q.client.XAck(ctx, q.cfg.StreamName, q.cfg.GroupName, messageID).Err()
	if err != nil {
		return fmt.Errorf("failed to XACK message %s: %w", messageID, err)
	}
	return nil
}

// EnqueueDLQ moves an unrecoverable or exhausted job to the DLQ stream via XADD and acknowledges it in the main stream.
func (q *Queue) EnqueueDLQ(ctx context.Context, job *task.Job, messageID string, reason string) (string, error) {
	if job == nil {
		return "", ErrNilJob
	}

	if job.State() == task.StatePending {
		_ = job.TransitionTo(task.StateRunning)
	}
	if job.State() == task.StateRunning {
		_ = job.TransitionTo(task.StateFailed)
	}
	_ = job.TransitionTo(task.StateDLQ)

	data, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("failed to serialize DLQ job: %w", err)
	}

	args := &redis.XAddArgs{
		Stream: q.cfg.DLQStreamName,
		Values: map[string]interface{}{
			"id":         job.ID,
			"type":       job.Type,
			"reason":     reason,
			"data":       string(data),
			"failed_at":  time.Now().UTC().Format(time.RFC3339),
			"retry_cnt":  job.RetryCount,
			"max_retries": job.MaxRetries,
		},
	}

	dlqMsgID, err := q.client.XAdd(ctx, args).Result()
	if err != nil {
		return "", fmt.Errorf("failed to write job to DLQ stream %s: %w", q.cfg.DLQStreamName, err)
	}

	// Once safely deposited in DLQ, acknowledge the original message from the active stream
	if messageID != "" {
		if ackErr := q.Ack(ctx, messageID); ackErr != nil {
			q.logger.Warn("Failed to ack message after moving to DLQ", "msg_id", messageID, "err", ackErr)
		}
	}

	q.logger.Warn("Job routed to Dead-Letter Queue (DLQ)",
		"job_id", job.ID,
		"reason", reason,
		"dlq_msg_id", dlqMsgID,
	)
	return dlqMsgID, nil
}
