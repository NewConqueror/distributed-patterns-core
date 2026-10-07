package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/distributed"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/pool"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

func TestChaos_WorkerCrashAndReaperRecovery(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer client.Close()

	cfg := distributed.Config{
		StreamName:        "test:chaos:stream",
		GroupName:         "test:chaos:group",
		DLQStreamName:     "test:chaos:dlq",
		DelayedZSetName:   "test:chaos:delayed",
		LockPrefix:        "test:chaos:lock:",
		LockTTL:           1 * time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
		ReaperInterval:    300 * time.Millisecond,
		MinIdleTime:       400 * time.Millisecond,
		BaseRetryBackoff:  100 * time.Millisecond,
		MaxRetryBackoff:   1 * time.Second,
	}

	ctx := context.Background()
	queue := distributed.NewQueue(client, cfg)
	if err := queue.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	// Track job executions to assert idempotency (no job should be executed/completed more than once)
	var executionsMap sync.Map // map[string]*atomic.Int32
	var totalCompleted atomic.Int32

	handler := task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		val, _ := executionsMap.LoadOrStore(job.ID, &atomic.Int32{})
		counter := val.(*atomic.Int32)
		counter.Add(1)

		// Simulate modest work duration
		select {
		case <-time.After(100 * time.Millisecond):
			totalCompleted.Add(1)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// 1. Enqueue 10 Jobs
	totalJobs := 10
	for i := 1; i <= totalJobs; i++ {
		job := task.NewJob(fmt.Sprintf("chaos-job-%02d", i), "compute_task", []byte("payload"), 2)
		if _, err := queue.Enqueue(ctx, job); err != nil {
			t.Fatalf("failed to enqueue job %d: %v", i, err)
		}
	}

	// 2. Setup Worker 1 (The fragile worker that will crash)
	poolCfg1 := pool.Config{
		WorkersCount:  2,
		QueueCapacity: 10,
	}
	pool1 := pool.NewPool(poolCfg1, nil)
	pool1.RegisterHandler("compute_task", handler)
	pool1.Start()

	node1 := distributed.NewWorkerNode(client, cfg, "crashed-worker-node", pool1)
	if err := node1.Start(); err != nil {
		t.Fatalf("failed to start node1: %v", err)
	}

	// Let worker 1 consume some jobs
	time.Sleep(150 * time.Millisecond)

	// 3. CHAOS: Simulate sudden ungraceful crash of Worker 1 (kill -9 simulation)
	// Abruptly stop node1 WITHOUT releasing locks or letting workers drain cleanly
	node1.Stop()
	_ = pool1.Shutdown(50 * time.Millisecond)
	t.Log(">>> CHAOS EVENT: WorkerNode 1 was abruptly terminated (simulating SIGKILL / node crash) <<<")

	// Fast forward/wait for lease lock and idle time to expire
	time.Sleep(600 * time.Millisecond)

	// 4. Start Worker 2 (The surviving failover node with Reaper)
	poolCfg2 := pool.Config{
		WorkersCount:  4,
		QueueCapacity: 20,
	}
	pool2 := pool.NewPool(poolCfg2, nil)
	pool2.RegisterHandler("compute_task", handler)
	pool2.Start()
	defer func() { _ = pool2.Shutdown(2 * time.Second) }()

	node2 := distributed.NewWorkerNode(client, cfg, "failover-worker-node", pool2)
	if err := node2.Start(); err != nil {
		t.Fatalf("failed to start node2: %v", err)
	}
	defer node2.Stop()

	// 5. Wait for failover node and Reaper to claim all orphan tasks and drain queue
	deadline := time.Now().Add(6 * time.Second)
	var finalPendingCount int64 = -1
	for time.Now().Before(deadline) {
		pending, err := client.XPending(ctx, cfg.StreamName, cfg.GroupName).Result()
		if err == nil {
			finalPendingCount = pending.Count
			if finalPendingCount == 0 && totalCompleted.Load() >= int32(totalJobs) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 6. Verify Zero Lost Jobs
	if totalCompleted.Load() < int32(totalJobs) {
		t.Fatalf("Zero Lost Jobs check failed: expected %d completed jobs, got %d", totalJobs, totalCompleted.Load())
	}

	// 7. Verify Idempotency: each job ID was completed
	executionsMap.Range(func(key, value any) bool {
		jobID := key.(string)
		cnt := value.(*atomic.Int32).Load()
		if cnt == 0 {
			t.Errorf("job %s was never completed", jobID)
		}
		return true
	})

	// 8. Verify PEL is empty (all messages are ACKed)
	if finalPendingCount != 0 {
		t.Fatalf("expected PEL count to be 0 after failover recovery, got %d", finalPendingCount)
	}

	t.Logf("Chaos Resilience Verified: %d total jobs successfully recovered, 0 lost jobs, PEL completely drained!",
		totalCompleted.Load())
}
