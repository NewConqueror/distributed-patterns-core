package distributed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrLockNotAcquired = errors.New("failed to acquire lease lock: key already held")
	ErrLockLost        = errors.New("lease lock lost during execution")

	// Lua script to renew TTL only if the lock still belongs to the calling worker
	renewLockScript = redis.NewScript(`
		if redis.call('get', KEYS[1]) == ARGV[1] then
			return redis.call('pexpire', KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	// Lua script to release lock only if the lock still belongs to the calling worker
	releaseLockScript = redis.NewScript(`
		if redis.call('get', KEYS[1]) == ARGV[1] then
			return redis.call('del', KEYS[1])
		else
			return 0
		end
	`)
)

// LeaseManager coordinates distributed locks and heartbeats for in-progress tasks.
type LeaseManager struct {
	client *redis.Client
	cfg    Config
	logger *slog.Logger
}

// NewLeaseManager creates a new LeaseManager instance.
func NewLeaseManager(client *redis.Client, cfg Config) *LeaseManager {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &LeaseManager{
		client: client,
		cfg:    cfg,
		logger: cfg.Logger,
	}
}

func (m *LeaseManager) lockKey(jobID string) string {
	return fmt.Sprintf("%s%s", m.cfg.LockPrefix, jobID)
}

// AcquireLock attempts to gain a temporary lease on a job via SET NX EX.
func (m *LeaseManager) AcquireLock(ctx context.Context, jobID, workerID string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = m.cfg.LockTTL
	}

	key := m.lockKey(jobID)
	ok, err := m.client.SetNX(ctx, key, workerID, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock for %s: %w", key, err)
	}
	return ok, nil
}

// IsLockHeld checks if a lock key exists for a job and returns the owner worker ID if held.
func (m *LeaseManager) IsLockHeld(ctx context.Context, jobID string) (bool, string, error) {
	key := m.lockKey(jobID)
	val, err := m.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, val, nil
}

// ReleaseLock deletes the lock key safely using Lua only if it matches workerID.
func (m *LeaseManager) ReleaseLock(ctx context.Context, jobID, workerID string) error {
	key := m.lockKey(jobID)
	res, err := releaseLockScript.Run(ctx, m.client, []string{key}, workerID).Result()
	if err != nil {
		return fmt.Errorf("failed to release lock for %s: %w", key, err)
	}

	// res == 1 if deleted, 0 if not matching or already expired
	if res == int64(0) {
		m.logger.Debug("Lock was already expired or belonged to another worker during release",
			"key", key,
			"worker_id", workerID,
		)
	}
	return nil
}

// StartHeartbeat initiates a background goroutine that extends the lease TTL every heartbeatInterval.
// It returns a cancel/stop function and a channel that signals if the lock is unexpectedly lost.
func (m *LeaseManager) StartHeartbeat(ctx context.Context, jobID, workerID string, ttl, interval time.Duration) (stop func(), lostChan <-chan struct{}) {
	if ttl <= 0 {
		ttl = m.cfg.LockTTL
	}
	if interval <= 0 {
		interval = m.cfg.HeartbeatInterval
	}

	key := m.lockKey(jobID)
	lostCh := make(chan struct{})
	stopCh := make(chan struct{})
	var stopOnce sync.Once

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		ttlMs := ttl.Milliseconds()

		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-ticker.C:
				// Renew lease TTL via Lua
				res, err := renewLockScript.Run(ctx, m.client, []string{key}, workerID, ttlMs).Result()
				if err != nil {
					m.logger.Warn("Heartbeat error renewing lease",
						"key", key,
						"worker_id", workerID,
						"err", err,
					)
					continue
				}

				if res == int64(0) {
					// Lock was lost! (Expired or taken over by another worker)
					m.logger.Error("Heartbeat detected lease lock loss!",
						"key", key,
						"worker_id", workerID,
					)
					close(lostCh)
					return
				}

				m.logger.Debug("Heartbeat renewed lease lock",
					"job_id", jobID,
					"worker_id", workerID,
					"ttl", ttl,
				)
			}
		}
	}()

	stop = func() {
		stopOnce.Do(func() {
			close(stopCh)
		})
	}

	return stop, lostCh
}
