package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/distributed"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/pool"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

func TestPoisonPill_ResilienceAndDLQIsolation(t *testing.T) {
	// Setup miniredis
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
		MaxRetryBackoff:   1 * time.Second,
	}

	ctx := context.Background()

	// 1. Setup Worker Pool
	var normalJobsCompleted atomic.Int32
	poolCfg := pool.Config{
		WorkersCount:  2,
		QueueCapacity: 20,
	}
	localPool := pool.NewPool(poolCfg, func(j *task.Job, err error) {
		if j.Type == "normal_task" && err == nil {
			normalJobsCompleted.Add(1)
		}
	})

	// Register Handlers
	localPool.RegisterHandler("normal_task", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		return nil
	}))

	localPool.RegisterHandler("panic_poison", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		// Deliberate nil pointer dereference panic simulation
		var nilPtr *string
		*nilPtr = "this will panic instantly"
		return nil
	}))

	localPool.RegisterHandler("fatal_poison", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		return task.NewFatalError(errors.New("corrupt payload format: cannot deserialize"))
	}))

	localPool.Start()
	defer func() { _ = localPool.Shutdown(2 * time.Second) }()

	// 2. Setup Distributed WorkerNode
	node := distributed.NewWorkerNode(client, cfg, "worker-node-1", localPool)
	if err := node.Start(); err != nil {
		t.Fatalf("failed to start worker node: %v", err)
	}
	defer node.Stop()

	queue := distributed.NewQueue(client, cfg)

	// 3. Enqueue Poison Pill 1: Malformed raw JSON string directly into stream
	_, err = client.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.StreamName,
		Values: map[string]interface{}{
			"id":   "corrupted-raw-1",
			"type": "malformed",
			"data": "{invalid-json-data: not closed",
		},
	}).Result()
	if err != nil {
		t.Fatalf("failed to insert raw corrupt job: %v", err)
	}

	// 4. Enqueue Poison Pill 2: Handler that panics with nil pointer
	panicJob := task.NewJob("poison-panic-1", "panic_poison", []byte("payload"), 0)
	if _, err := queue.Enqueue(ctx, panicJob); err != nil {
		t.Fatalf("failed to enqueue panic job: %v", err)
	}

	// 5. Enqueue Poison Pill 3: Handler that returns FatalError
	fatalJob := task.NewJob("poison-fatal-1", "fatal_poison", []byte("payload"), 3)
	if _, err := queue.Enqueue(ctx, fatalJob); err != nil {
		t.Fatalf("failed to enqueue fatal job: %v", err)
	}

	// 6. Enqueue Normal Valid Jobs (to prove worker didn't die and continues working)
	totalNormal := 5
	for i := 1; i <= totalNormal; i++ {
		normalJob := task.NewJob(fmt.Sprintf("normal-job-%d", i), "normal_task", []byte("ok"), 1)
		if _, err := queue.Enqueue(ctx, normalJob); err != nil {
			t.Fatalf("failed to enqueue normal job %d: %v", i, err)
		}
	}

	// 7. Wait for processing to stabilize
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if normalJobsCompleted.Load() == int32(totalNormal) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify all normal jobs completed
	if normalJobsCompleted.Load() != int32(totalNormal) {
		t.Fatalf("expected %d normal jobs completed, got %d (worker may have crashed from poison pill)",
			totalNormal, normalJobsCompleted.Load())
	}

	// 8. Verify DLQ Stream contains the poison pills
	dlqEntries, err := client.XRange(ctx, cfg.DLQStreamName, "-", "+").Result()
	if err != nil {
		t.Fatalf("failed to read DLQ stream: %v", err)
	}

	if len(dlqEntries) < 3 {
		t.Fatalf("expected at least 3 poison pill messages in DLQ stream, got %d", len(dlqEntries))
	}

	t.Logf("Poison Pill Isolation verified: %d poison tasks safely moved to DLQ, %d normal tasks successfully processed!",
		len(dlqEntries), normalJobsCompleted.Load())
}
