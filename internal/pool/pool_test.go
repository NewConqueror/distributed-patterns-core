package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

func TestPool_BasicExecution(t *testing.T) {
	cfg := Config{
		WorkersCount:  3,
		QueueCapacity: 10,
	}

	completedJobs := sync.Map{}

	p := NewPool(cfg, func(job *task.Job, err error) {
		if err == nil {
			completedJobs.Store(job.ID, true)
		}
	})

	p.RegisterHandler("echo", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		return nil
	}))

	p.Start()

	total := 10
	for i := 0; i < total; i++ {
		job := task.NewJob(fmt.Sprintf("job-%d", i), "echo", []byte("payload"), 0)
		if err := p.Submit(job); err != nil {
			t.Fatalf("failed to submit job: %v", err)
		}
	}

	if err := p.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	count := 0
	completedJobs.Range(func(key, value any) bool {
		count++
		return true
	})

	if count != total {
		t.Fatalf("expected %d completed jobs, got %d", total, count)
	}
}

func TestPool_QueueFullRejection(t *testing.T) {
	cfg := Config{
		WorkersCount:  1,
		QueueCapacity: 2,
	}

	p := NewPool(cfg, nil)

	// Block the single worker so the queue fills up
	var startOnce sync.Once
	started := make(chan struct{})
	block := make(chan struct{})
	p.RegisterHandler("slow", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		startOnce.Do(func() {
			close(started)
		})
		<-block
		return nil
	}))

	p.Start()

	// 1. First job is consumed by worker and blocks
	job1 := task.NewJob("job-1", "slow", nil, 0)
	if err := p.Submit(job1); err != nil {
		t.Fatalf("job 1 submission failed: %v", err)
	}
	<-started

	// 2. Fill queue capacity (capacity = 2)
	job2 := task.NewJob("job-2", "slow", nil, 0)
	if err := p.Submit(job2); err != nil {
		t.Fatalf("job 2 submission failed: %v", err)
	}

	job3 := task.NewJob("job-3", "slow", nil, 0)
	if err := p.Submit(job3); err != nil {
		t.Fatalf("job 3 submission failed: %v", err)
	}

	// 3. Queue is now full (1 in worker, 2 in channel buffer). Next Submit must return ErrQueueFull
	job4 := task.NewJob("job-4", "slow", nil, 0)
	err := p.Submit(job4)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got: %v", err)
	}

	close(block)
	_ = p.Shutdown(5 * time.Second)
}

func TestPool_PanicRecovery(t *testing.T) {
	cfg := Config{
		WorkersCount:  1, // Only 1 worker to ensure worker doesn't die!
		QueueCapacity: 5,
	}

	var panickedJobID string
	var recoveredPanic error
	var completedSecondJob atomic.Bool

	p := NewPool(cfg, func(job *task.Job, err error) {
		if errors.Is(err, ErrJobPanic) {
			panickedJobID = job.ID
			recoveredPanic = err
		}
	})

	p.RegisterHandler("panic_job", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		panic("deliberate explosion in handler")
	}))

	p.RegisterHandler("normal_job", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		completedSecondJob.Store(true)
		return nil
	}))

	p.Start()

	// 1. Submit panicking job
	panicJob := task.NewJob("panic-1", "panic_job", nil, 0)
	if err := p.Submit(panicJob); err != nil {
		t.Fatalf("failed to submit panic job: %v", err)
	}

	// Wait briefly for panic to occur
	time.Sleep(100 * time.Millisecond)

	// 2. Submit normal job to the SAME worker to prove worker survived
	normalJob := task.NewJob("normal-1", "normal_job", nil, 0)
	if err := p.Submit(normalJob); err != nil {
		t.Fatalf("failed to submit normal job: %v", err)
	}

	_ = p.Shutdown(5 * time.Second)

	if panickedJobID != "panic-1" {
		t.Fatalf("expected panic on job 'panic-1', got: %s", panickedJobID)
	}
	if recoveredPanic == nil {
		t.Fatalf("expected recoveredPanic to be non-nil")
	}
	if panicJob.State() != task.StateDLQ {
		t.Fatalf("panicked job should be in StateDLQ, got %s", panicJob.State())
	}
	if !completedSecondJob.Load() {
		t.Fatalf("worker died after panic! Second job was not executed")
	}
}

func TestPool_ErrorClassificationAndRetry(t *testing.T) {
	cfg := Config{
		WorkersCount:  2,
		QueueCapacity: 10,
	}

	var fatalCompleted atomic.Bool
	var retryCount atomic.Int32

	p := NewPool(cfg, func(job *task.Job, err error) {
		if job.Type == "fatal_task" {
			fatalCompleted.Store(true)
		}
	})

	// Fatal error: should not retry, should go straight to DLQ
	p.RegisterHandler("fatal_task", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		return task.NewFatalError(errors.New("invalid payload structure"))
	}))

	// Temporary error: should retry up to MaxRetries
	p.RegisterHandler("temp_task", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		retryCount.Add(1)
		return task.NewTemporaryError(errors.New("rate limit reached"))
	}))

	p.Start()

	fatalJob := task.NewJob("fatal-1", "fatal_task", nil, 3)
	if err := p.Submit(fatalJob); err != nil {
		t.Fatalf("submit fatal job failed: %v", err)
	}

	tempJob := task.NewJob("temp-1", "temp_task", nil, 2)
	if err := p.Submit(tempJob); err != nil {
		t.Fatalf("submit temp job failed: %v", err)
	}

	// Wait for retries
	time.Sleep(500 * time.Millisecond)

	_ = p.Shutdown(5 * time.Second)

	// Verify Fatal Job
	if fatalJob.RetryCount != 0 {
		t.Fatalf("fatal job should not be retried, retry count: %d", fatalJob.RetryCount)
	}
	if fatalJob.State() != task.StateDLQ {
		t.Fatalf("fatal job state should be DLQ, got %s", fatalJob.State())
	}

	// Verify Temporary Job
	// Initial attempt + 2 retries = 3 total runs
	if retryCount.Load() != 3 {
		t.Fatalf("expected 3 total executions (1 original + 2 retries), got %d", retryCount.Load())
	}
	if tempJob.State() != task.StateDLQ {
		t.Fatalf("temp job state after exhausting retries should be DLQ, got %s", tempJob.State())
	}
}

func TestPool_GracefulShutdown(t *testing.T) {
	cfg := Config{
		WorkersCount:    2,
		QueueCapacity:   10,
		ShutdownTimeout: 2 * time.Second,
	}

	var jobsDone atomic.Int32

	p := NewPool(cfg, func(job *task.Job, err error) {
		if err == nil {
			jobsDone.Add(1)
		}
	})

	p.RegisterHandler("sleep_task", task.HandlerFunc(func(ctx context.Context, job task.Job) error {
		select {
		case <-time.After(100 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	p.Start()

	// Submit 5 jobs
	for i := 0; i < 5; i++ {
		_ = p.Submit(task.NewJob(fmt.Sprintf("sleep-%d", i), "sleep_task", nil, 0))
	}

	// Graceful shutdown should drain all 5 jobs cleanly
	err := p.Shutdown(3 * time.Second)
	if err != nil {
		t.Fatalf("expected graceful shutdown without error, got: %v", err)
	}

	if jobsDone.Load() != 5 {
		t.Fatalf("expected all 5 queued jobs to finish, got %d", jobsDone.Load())
	}

	// Submitting after shutdown must be rejected
	err = p.Submit(task.NewJob("post-shutdown", "sleep_task", nil, 0))
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed on post-shutdown submit, got: %v", err)
	}
}
