package tests

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/distributed"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/pool"
	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

// BenchmarkLocalWorkerPool measures the pure in-memory throughput (jobs/sec) of WorkerPool.
func BenchmarkLocalWorkerPool(b *testing.B) {
	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	poolCfg := pool.Config{
		WorkersCount:  8,
		QueueCapacity: 50000,
		Logger:        silentLogger,
	}

	p := pool.NewPool(poolCfg, nil)
	p.RegisterHandler("bench_noop", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		return nil
	}))
	p.Start()
	defer func() { _ = p.Shutdown(2 * time.Second) }()

	job := task.NewJob("bench-job", "bench_noop", []byte("payload"), 0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = p.Submit(job)
	}
}

// BenchmarkDistributedStreamEnqueue measures the throughput of publishing jobs into Redis Streams (XADD).
func BenchmarkDistributedStreamEnqueue(b *testing.B) {
	s, err := miniredis.Run()
	if err != nil {
		b.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer client.Close()

	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := distributed.DefaultConfig()
	cfg.Logger = silentLogger

	queue := distributed.NewQueue(client, cfg)
	ctx := context.Background()
	_ = queue.EnsureConsumerGroup(ctx)

	job := task.NewJob("bench-job-dist", "compute", []byte(`{"value":12345}`), 1)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := queue.Enqueue(ctx, job)
		if err != nil {
			b.Fatalf("enqueue error: %v", err)
		}
	}
}
