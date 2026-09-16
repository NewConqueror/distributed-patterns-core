package spatial

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Sweeper periodically prunes stale (inactive) drivers from the active geospatial
// index and heartbeat sorted sets to prevent "ghost" vehicles from lingering on the map.
type Sweeper struct {
	client *redis.Client
	cfg    RedisConfig
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	totalPruned atomic.Int64
}

// NewSweeper creates a new stale driver cleaner instance.
func NewSweeper(client *redis.Client, cfg RedisConfig, logger *slog.Logger) *Sweeper {
	if logger == nil {
		logger = slog.Default()
	}

	return &Sweeper{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

// TotalPruned returns the cumulative number of stale driver entries evicted.
func (s *Sweeper) TotalPruned() int64 {
	return s.totalPruned.Load()
}

// Start launches the background sweeping loop.
func (s *Sweeper) Start(parentCtx context.Context) {
	s.ctx, s.cancel = context.WithCancel(parentCtx)

	s.logger.Info("Starting Stale Data Sweeper",
		"interval", s.cfg.SweepInterval,
		"threshold", s.cfg.StaleThreshold,
	)

	s.wg.Add(1)
	go s.run()
}

// Stop terminates the sweeper loop and waits for any in-flight sweep to finish.
func (s *Sweeper) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("Stale Data Sweeper stopped cleanly", "total_pruned", s.totalPruned.Load())
}

func (s *Sweeper) run() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			count, err := s.SweepOnce(s.ctx)
			if err != nil && s.ctx.Err() == nil {
				s.logger.Error("Error executing stale driver sweep", "err", err)
			} else if count > 0 {
				s.logger.Info("Evicted inactive ghost drivers",
					"pruned_count", count,
					"threshold", s.cfg.StaleThreshold,
				)
			}
		}
	}
}

// SweepOnce performs a single eviction cycle:
// 1. Queries ZRANGEBYSCORE drivers:heartbeat -inf <cutoff>
// 2. Removes stale members from drivers:active and drivers:heartbeat
// 3. Deletes associated metadata keys
func (s *Sweeper) SweepOnce(ctx context.Context) (int, error) {
	cutoffMs := time.Now().Add(-s.cfg.StaleThreshold).UnixMilli()
	cutoffStr := strconv.FormatInt(cutoffMs, 10)

	// Identify inactive drivers whose latest heartbeat is older than cutoff
	staleIDs, err := s.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     s.cfg.HeartbeatKey,
		Start:   "-inf",
		Stop:    cutoffStr,
		ByScore: true,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("failed querying stale heartbeats: %w", err)
	}

	if len(staleIDs) == 0 {
		return 0, nil
	}

	// Evict identified stale drivers in a single atomic pipeline
	pipe := s.client.Pipeline()

	// Convert []string to []interface{} for ZRem
	members := make([]interface{}, len(staleIDs))
	for i, id := range staleIDs {
		members[i] = id
		pipe.Del(ctx, s.cfg.MetaKeyPrefix+id)
	}

	pipe.ZRem(ctx, s.cfg.ActiveKey, members...)
	pipe.ZRemRangeByScore(ctx, s.cfg.HeartbeatKey, "-inf", cutoffStr)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed executing eviction pipeline: %w", err)
	}

	s.totalPruned.Add(int64(len(staleIDs)))
	return len(staleIDs), nil
}
