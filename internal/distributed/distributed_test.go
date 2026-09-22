package distributed

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/pool"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client, Config) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	cfg := Config{
		StreamName:        "test:tasks:stream",
		GroupName:         "test:tasks:group",
		DLQStreamName:     "test:tasks:dlq",
		DelayedZSetName:   "test:tasks:delayed",
		LockPrefix:        "test:job:lock:",
		LockTTL:           2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		ReaperInterval:    500 * time.Millisecond,
		MinIdleTime:       500 * time.Millisecond,
		BaseRetryBackoff:  100 * time.Millisecond,
		MaxRetryBackoff:   2 * time.Second,
	}

	return s, client, cfg
}

func TestQueue_Lifecycle(t *testing.T) {
	s, client, cfg := setupTestRedis(t)
	defer s.Close()
	defer client.Close()

	ctx := context.Background()
	queue := NewQueue(client, cfg)

	// 1. Group creation (idempotent)
	if err := queue.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("EnsureConsumerGroup failed: %v", err)
	}
	if err := queue.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("Second EnsureConsumerGroup (idempotency check) failed: %v", err)
	}

	// 2. Enqueue Job
	job := task.NewJob("job-001", "send_email", []byte(`{"to":"alice@example.com"}`), 3)
	msgID, err := queue.Enqueue(ctx, job)
	if err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}
	if msgID == "" {
		t.Fatalf("expected non-empty message ID")
	}

	// 3. Ack Job
	if err := queue.Ack(ctx, msgID); err != nil {
		t.Fatalf("failed to ack message: %v", err)
	}

	// 4. Enqueue to DLQ
	dlqJob := task.NewJob("job-fatal", "corrupt_data", nil, 0)
	dlqMsgID, err := queue.EnqueueDLQ(ctx, dlqJob, msgID, "schema validation failed")
	if err != nil {
		t.Fatalf("failed to enqueue to DLQ: %v", err)
	}
	if dlqMsgID == "" {
		t.Fatalf("expected non-empty DLQ message ID")
	}
	if dlqJob.State() != task.StateDLQ {
		t.Fatalf("expected job state to be DLQ, got %s", dlqJob.State())
	}
}

func TestLeaseManager_LockAndHeartbeat(t *testing.T) {
	s, client, cfg := setupTestRedis(t)
	defer s.Close()
	defer client.Close()

	ctx := context.Background()
	leaseMgr := NewLeaseManager(client, cfg)

	jobID := "job-lock-1"
	worker1 := "worker-node-1"
	worker2 := "worker-node-2"

	// 1. Worker 1 acquires lock
	acquired, err := leaseMgr.AcquireLock(ctx, jobID, worker1, 1*time.Second)
	if err != nil || !acquired {
		t.Fatalf("expected worker 1 to acquire lock, err: %v, acquired: %v", err, acquired)
	}

	// 2. Worker 2 attempts to acquire same lock (should fail)
	acquired2, err := leaseMgr.AcquireLock(ctx, jobID, worker2, 1*time.Second)
	if err != nil {
		t.Fatalf("unexpected error on second lock attempt: %v", err)
	}
	if acquired2 {
		t.Fatalf("worker 2 should NOT have acquired lock while held by worker 1")
	}

	// 3. Heartbeat extension
	stopHeartbeat, lostCh := leaseMgr.StartHeartbeat(ctx, jobID, worker1, 1*time.Second, 100*time.Millisecond)

	time.Sleep(350 * time.Millisecond)

	select {
	case <-lostCh:
		t.Fatalf("heartbeat signaled lock loss unexpectedly")
	default:
	}

	stopHeartbeat()

	// 4. Safe Release: Worker 2 cannot release Worker 1's lock
	_ = leaseMgr.ReleaseLock(ctx, jobID, worker2)
	held, owner, _ := leaseMgr.IsLockHeld(ctx, jobID)
	if !held || owner != worker1 {
		t.Fatalf("worker 2 should not be able to delete worker 1's lock")
	}

	// Worker 1 releases its own lock
	if err := leaseMgr.ReleaseLock(ctx, jobID, worker1); err != nil {
		t.Fatalf("worker 1 failed to release lock: %v", err)
	}

	heldAfter, _, _ := leaseMgr.IsLockHeld(ctx, jobID)
	if heldAfter {
		t.Fatalf("lock should be released")
	}
}

func TestRetryManager_BackoffAndScheduling(t *testing.T) {
	s, client, cfg := setupTestRedis(t)
	defer s.Close()
	defer client.Close()

	ctx := context.Background()
	queue := NewQueue(client, cfg)
	retryMgr := NewRetryManager(client, queue, cfg)

	// 1. Backoff formula test: Base * 2^retry + jitter
	b0 := retryMgr.CalculateBackoff(0)
	if b0 < 100*time.Millisecond || b0 > 200*time.Millisecond {
		t.Fatalf("backoff for retry 0 out of bounds: %v", b0)
	}

	b1 := retryMgr.CalculateBackoff(1)
	if b1 < 200*time.Millisecond || b1 > 300*time.Millisecond {
		t.Fatalf("backoff for retry 1 out of bounds: %v", b1)
	}

	// 2. Schedule retry to ZSET
	job := task.NewJob("retry-job-1", "flaky", []byte("payload"), 2)
	err := retryMgr.HandleFailure(ctx, job, "stream-msg-1", task.NewTemporaryError(errors.New("network timeout")))
	if err != nil {
		t.Fatalf("HandleFailure failed: %v", err)
	}

	if job.RetryCount != 1 {
		t.Fatalf("expected RetryCount to be 1, got %d", job.RetryCount)
	}

	// Check job in ZSET
	count, err := client.ZCard(ctx, cfg.DelayedZSetName).Result()
	if err != nil || count != 1 {
		t.Fatalf("expected 1 item in delayed ZSET, got %d (err: %v)", count, err)
	}

	// Sleep briefly for exponential backoff duration (100ms - 250ms) to elapse in real time
	time.Sleep(350 * time.Millisecond)

	// 3. Trigger Poller re-enqueue
	retryMgr.pollAndReEnqueue(ctx)

	// Verify ZSET is drained
	countAfter, _ := client.ZCard(ctx, cfg.DelayedZSetName).Result()
	if countAfter != 0 {
		t.Fatalf("expected 0 items in delayed ZSET after poller, got %d", countAfter)
	}
}

func TestWorkerNode_EndToEnd(t *testing.T) {
	s, client, cfg := setupTestRedis(t)
	defer s.Close()
	defer client.Close()

	ctx := context.Background()

	var processedCount atomic.Int32
	poolCfg := pool.Config{
		WorkersCount:  2,
		QueueCapacity: 10,
	}
	localPool := pool.NewPool(poolCfg, func(j *task.Job, err error) {
		if err == nil {
			processedCount.Add(1)
		}
	})
	localPool.RegisterHandler("compute", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		return nil
	}))
	localPool.Start()
	defer func() { _ = localPool.Shutdown(2 * time.Second) }()

	// Start distributed WorkerNode
	node := NewWorkerNode(client, cfg, "worker-node-alpha", localPool)
	if err := node.Start(); err != nil {
		t.Fatalf("failed to start WorkerNode: %v", err)
	}
	defer node.Stop()

	// Enqueue 3 jobs into the Redis Stream
	queue := NewQueue(client, cfg)
	for i := 1; i <= 3; i++ {
		job := task.NewJob(fmt.Sprintf("e2e-job-%d", i), "compute", []byte(`{"val":42}`), 1)
		_, err := queue.Enqueue(ctx, job)
		if err != nil {
			t.Fatalf("failed to enqueue job %d: %v", i, err)
		}
	}

	// Wait for worker node to process messages
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if processedCount.Load() == 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if processedCount.Load() != 3 {
		t.Fatalf("expected 3 jobs processed, got %d", processedCount.Load())
	}
}

func TestReaper_ClaimOrphanTask(t *testing.T) {
	s, client, cfg := setupTestRedis(t)
	defer s.Close()
	defer client.Close()

	ctx := context.Background()
	queue := NewQueue(client, cfg)
	leaseMgr := NewLeaseManager(client, cfg)
	retryMgr := NewRetryManager(client, queue, cfg)

	if err := queue.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	// 1. Enqueue job
	job := task.NewJob("orphan-job-1", "long_running", []byte("data"), 3)
	msgID, err := queue.Enqueue(ctx, job)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// 2. Dead worker reads the message (lands in PEL for 'crashed-worker')
	crashedStreams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    cfg.GroupName,
		Consumer: "crashed-worker",
		Streams:  []string{cfg.StreamName, ">"},
		Count:    1,
	}).Result()
	if err != nil || len(crashedStreams) == 0 || len(crashedStreams[0].Messages) == 0 {
		t.Fatalf("failed reading message for crashed worker: %v", err)
	}

	// Wait for idle time threshold (cfg.MinIdleTime is 500ms)
	time.Sleep(550 * time.Millisecond)

	// 3. Reaper scans PEL and claims orphan message
	var claimedJobID string
	reaper := NewReaper(client, queue, leaseMgr, retryMgr, cfg, "reaper-worker", func(ctx context.Context, claimedJob *task.Job, claimedMsgID string) error {
		claimedJobID = claimedJob.ID
		return nil
	})

	reaper.ScanAndClaim(ctx)

	if claimedJobID != "orphan-job-1" {
		t.Fatalf("expected reaper to claim orphan-job-1, got: %s", claimedJobID)
	}

	// 4. Verify message ownership in PEL has transitioned to reaper-worker
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: cfg.StreamName,
		Group:  cfg.GroupName,
		Start:  msgID,
		End:    msgID,
		Count:  1,
	}).Result()

	if err != nil || len(pending) == 0 {
		t.Fatalf("failed to query pending message in PEL: %v", err)
	}

	if pending[0].Consumer != "reaper-worker" {
		t.Fatalf("expected consumer in PEL to be reaper-worker, got %s", pending[0].Consumer)
	}
	if pending[0].RetryCount < 2 {
		t.Fatalf("expected delivery counter to increment to at least 2, got %d", pending[0].RetryCount)
	}
}
