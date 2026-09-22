package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

// ClaimHandler is a callback invoked when an orphan job is claimed by the reaper.
type ClaimHandler func(ctx context.Context, job *task.Job, msgID string) error

// Reaper periodically inspects the Pending Entries List (PEL) for orphan/stuck jobs.
type Reaper struct {
	client       *redis.Client
	queue        *Queue
	leaseMgr     *LeaseManager
	retryMgr     *RetryManager
	cfg          Config
	consumerName string
	onClaim      ClaimHandler
	logger       *slog.Logger
}

// NewReaper initializes a new orphan job scanner and claimer.
func NewReaper(client *redis.Client, queue *Queue, leaseMgr *LeaseManager, retryMgr *RetryManager, cfg Config, consumerName string, onClaim ClaimHandler) *Reaper {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Reaper{
		client:       client,
		queue:        queue,
		leaseMgr:     leaseMgr,
		retryMgr:     retryMgr,
		cfg:          cfg,
		consumerName: consumerName,
		onClaim:      onClaim,
		logger:       cfg.Logger,
	}
}

// Start launches the background reaper loop.
func (r *Reaper) Start(ctx context.Context) {
	interval := r.cfg.ReaperInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.ScanAndClaim(ctx)
			}
		}
	}()

	r.logger.Info("Reaper goroutine started", "interval", interval, "min_idle", r.cfg.MinIdleTime)
}

// ScanAndClaim inspects the consumer group's PEL and claims eligible orphan jobs.
func (r *Reaper) ScanAndClaim(ctx context.Context) {
	// 1. Fetch pending entries from PEL
	pendingArgs := &redis.XPendingExtArgs{
		Stream: r.cfg.StreamName,
		Group:  r.cfg.GroupName,
		Start:  "-",
		End:    "+",
		Count:  50,
	}

	entries, err := r.client.XPendingExt(ctx, pendingArgs).Result()
	if err != nil {
		if !isGroupNotFoundError(err) {
			r.logger.Error("Reaper failed to inspect PEL", "err", err)
		}
		return
	}

	if len(entries) == 0 {
		return
	}

	r.logger.Debug("Reaper inspecting PEL entries", "count", len(entries))

	for _, pending := range entries {
		// If message is stuck in PEL with high retry/delivery count, route to DLQ (Poison Pill protection)
		if pending.RetryCount > 5 {
			r.logger.Warn("Message exceeded maximum claim attempts, routing to DLQ as poison pill",
				"msg_id", pending.ID,
				"deliveries", pending.RetryCount,
			)
			r.routePoisonPillToDLQ(ctx, pending.ID)
			continue
		}

		// Check if idle time exceeds threshold
		isIdleExceeded := pending.Idle >= r.cfg.MinIdleTime

		// Check if the lock key is still active
		jobID := r.extractJobID(ctx, pending.ID)
		var isLockMissing bool
		if jobID != "" {
			held, _, lockErr := r.leaseMgr.IsLockHeld(ctx, jobID)
			if lockErr == nil && !held {
				// Lock is gone! If it's been idle at least 3 seconds, it's considered orphaned
				if pending.Idle >= 3*time.Second {
					isLockMissing = true
				}
			}
		}

		if isIdleExceeded || isLockMissing {
			r.logger.Info("Detected orphan job in PEL",
				"msg_id", pending.ID,
				"consumer", pending.Consumer,
				"idle", pending.Idle,
				"lock_missing", isLockMissing,
			)
			r.claimJob(ctx, pending.ID)
		}
	}
}

func (r *Reaper) claimJob(ctx context.Context, msgID string) {
	// XCLAIM transfers ownership of message and increments its delivery counter
	claimed, err := r.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   r.cfg.StreamName,
		Group:    r.cfg.GroupName,
		Consumer: r.consumerName,
		MinIdle:  time.Millisecond, // Since we already verified eligibility
		Messages: []string{msgID},
	}).Result()

	if err != nil {
		r.logger.Error("Failed to XCLAIM orphan job", "msg_id", msgID, "err", err)
		return
	}

	if len(claimed) == 0 {
		return
	}

	for _, msg := range claimed {
		r.logger.Info("Successfully claimed orphan job",
			"msg_id", msg.ID,
			"new_consumer", r.consumerName,
		)

		job, err := deserializeJobFromStream(msg.Values)
		if err != nil {
			r.logger.Error("Failed to deserialize claimed job", "msg_id", msg.ID, "err", err)
			continue
		}

		if r.onClaim != nil {
			if err := r.onClaim(ctx, job, msg.ID); err != nil {
				r.logger.Error("Error handling claimed job callback", "job_id", job.ID, "err", err)
			}
		}
	}
}

func (r *Reaper) extractJobID(ctx context.Context, msgID string) string {
	res, err := r.client.XRangeN(ctx, r.cfg.StreamName, msgID, msgID, 1).Result()
	if err != nil || len(res) == 0 {
		return ""
	}
	if idVal, ok := res[0].Values["id"]; ok {
		return fmt.Sprintf("%v", idVal)
	}
	return ""
}

func (r *Reaper) routePoisonPillToDLQ(ctx context.Context, msgID string) {
	res, err := r.client.XRangeN(ctx, r.cfg.StreamName, msgID, msgID, 1).Result()
	if err != nil || len(res) == 0 {
		_ = r.queue.Ack(ctx, msgID)
		return
	}

	job, err := deserializeJobFromStream(res[0].Values)
	if err != nil {
		_ = r.queue.Ack(ctx, msgID)
		return
	}

	_, _ = r.queue.EnqueueDLQ(ctx, job, msgID, "delivery counter exceeded in PEL (poison pill)")
}

func deserializeJobFromStream(values map[string]interface{}) (*task.Job, error) {
	dataStr, ok := values["data"].(string)
	if !ok {
		return nil, fmt.Errorf("missing 'data' field in stream entry")
	}

	var job task.Job
	if err := json.Unmarshal([]byte(dataStr), &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job JSON: %w", err)
	}
	return &job, nil
}

func isGroupNotFoundError(err error) bool {
	return err != nil && (err.Error() == "NOGROUP No such key 'tasks:stream' or consumer group 'tasks:group' in XPENDING")
}
