package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

var (
	ErrPoolClosed   = errors.New("worker pool is closed")
	ErrQueueFull    = errors.New("job queue is full")
	ErrNoHandler    = errors.New("no handler registered for job type")
	ErrJobTimeout   = errors.New("job execution timed out")
	ErrJobPanic     = errors.New("job execution panicked")
)

// Config defines the runtime configuration for the WorkerPool.
type Config struct {
	WorkersCount    int           // Number of concurrent workers (default: runtime.NumCPU())
	QueueCapacity   int           // Capacity of the job channel buffer (default: 100)
	DefaultTimeout  time.Duration // Timeout for each individual job execution (0 = no timeout)
	ShutdownTimeout time.Duration // Grace period for workers to finish during shutdown (default: 15s)
	Logger          *slog.Logger  // Optional logger (default: slog.Default())
}

// ResultCallback is an optional hook invoked when a job finishes.
type ResultCallback func(job *task.Job, err error)

// Pool coordinates worker goroutines, the job queue, and graceful shutdown.
type Pool struct {
	config Config

	jobs       chan *task.Job
	handlers   sync.Map // map[string]task.Handler
	wg         sync.WaitGroup
	closed     atomic.Bool
	stopOnce   sync.Once
	ctx        context.Context
	cancel     context.CancelFunc
	onComplete ResultCallback
	logger     *slog.Logger
}

// NewPool initializes a new worker pool with given configuration.
func NewPool(cfg Config, onComplete ResultCallback) *Pool {
	if cfg.WorkersCount <= 0 {
		cfg.WorkersCount = runtime.NumCPU()
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 100
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Pool{
		config:     cfg,
		jobs:       make(chan *task.Job, cfg.QueueCapacity),
		ctx:        ctx,
		cancel:     cancel,
		onComplete: onComplete,
		logger:     logger,
	}

	return p
}

// RegisterHandler binds a job type string to its corresponding Handler implementation.
func (p *Pool) RegisterHandler(jobType string, handler task.Handler) {
	p.handlers.Store(jobType, handler)
}

// Start spawns the configured number of worker goroutines.
func (p *Pool) Start() {
	for i := 1; i <= p.config.WorkersCount; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
	p.logger.Info("Worker pool started",
		"workers", p.config.WorkersCount,
		"queue_capacity", p.config.QueueCapacity,
	)
}

// Submit enqueues a job without blocking. If the queue is full, it immediately returns ErrQueueFull.
// If the pool is closing or closed, ErrPoolClosed is returned.
func (p *Pool) Submit(job *task.Job) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	select {
	case p.jobs <- job:
		return nil
	default:
		return ErrQueueFull
	}
}

// SubmitWithTimeout attempts to enqueue a job, blocking until space is available or ctx expires.
// Provides backpressure control to callers.
func (p *Pool) SubmitWithTimeout(ctx context.Context, job *task.Job) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return ErrPoolClosed
	case p.jobs <- job:
		return nil
	}
}

// workerLoop is the persistent goroutine that receives jobs from the queue.
func (p *Pool) workerLoop(workerID int) {
	defer p.wg.Done()

	for job := range p.jobs {
		p.safeProcess(workerID, job)
	}
}

// safeProcess wraps job execution in panic recovery and error categorization.
// Crucially, a panic here only affects this single job; the worker goroutine does not die.
func (p *Pool) safeProcess(workerID int, job *task.Job) {
	var jobErr error

	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("Worker recovered from panic during job execution",
				"worker_id", workerID,
				"job_id", job.ID,
				"job_type", job.Type,
				"panic", fmt.Sprintf("%v", r),
			)
			jobErr = fmt.Errorf("%w: %v", ErrJobPanic, r)
			p.handleJobFailure(job, jobErr, true)
		}

		if p.onComplete != nil {
			p.onComplete(job, jobErr)
		}
	}()

	// 1. Validate and mark job as RUNNING
	if err := job.TransitionTo(task.StateRunning); err != nil {
		p.logger.Error("Failed to transition job to RUNNING", "job_id", job.ID, "err", err)
		jobErr = err
		p.handleJobFailure(job, err, true)
		return
	}

	// 2. Lookup Handler for Job.Type
	handlerVal, ok := p.handlers.Load(job.Type)
	if !ok {
		jobErr = fmt.Errorf("%w: %s", ErrNoHandler, job.Type)
		p.logger.Error("No handler found for job", "job_id", job.ID, "type", job.Type)
		p.handleJobFailure(job, jobErr, true)
		return
	}
	handler := handlerVal.(task.Handler)

	// 3. Prepare Context with timeout if configured
	execCtx := p.ctx
	var cancel context.CancelFunc
	if p.config.DefaultTimeout > 0 {
		execCtx, cancel = context.WithTimeout(p.ctx, p.config.DefaultTimeout)
		defer cancel()
	}

	// 4. Execute the handler
	err := handler.Process(execCtx, *job)
	if err != nil {
		jobErr = err
		// Check whether the error was due to context deadline
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			jobErr = fmt.Errorf("%w: %v", ErrJobTimeout, err)
		}
		p.handleJobError(job, jobErr)
		return
	}

	// 5. Job Succeeded
	if err := job.TransitionTo(task.StateCompleted); err != nil {
		p.logger.Error("Failed to mark job as COMPLETED", "job_id", job.ID, "err", err)
		jobErr = err
		return
	}

	p.logger.Debug("Job completed successfully", "worker_id", workerID, "job_id", job.ID)
}

// handleJobError determines whether to retry or route to DLQ based on error classification.
func (p *Pool) handleJobError(job *task.Job, err error) {
	isFatal := task.IsFatal(err)

	if isFatal {
		p.logger.Warn("Fatal error encountered; routing job directly to DLQ",
			"job_id", job.ID,
			"err", err,
		)
		p.handleJobFailure(job, err, true)
		return
	}

	// For temporary or unspecified errors, attempt retry if attempts remain
	if job.CanRetry() {
		job.IncrementRetry()
		_ = job.TransitionTo(task.StateFailed)
		_ = job.TransitionTo(task.StatePending)

		p.logger.Info("Job failed with retryable error, re-submitting to queue",
			"job_id", job.ID,
			"retry_count", job.RetryCount,
			"max_retries", job.MaxRetries,
			"err", err,
		)

		// Re-enqueue the job
		go func(j *task.Job) {
			// Using SubmitWithTimeout with pool context to avoid leaking goroutines on shutdown
			if submitErr := p.SubmitWithTimeout(p.ctx, j); submitErr != nil {
				p.logger.Error("Failed to re-enqueue retried job; sending to DLQ",
					"job_id", j.ID,
					"err", submitErr,
				)
				p.handleJobFailure(j, submitErr, true)
			}
		}(job)
		return
	}

	// Max retries exhausted
	p.logger.Warn("Job retries exhausted; moving to DLQ",
		"job_id", job.ID,
		"retry_count", job.RetryCount,
		"err", err,
	)
	p.handleJobFailure(job, err, true)
}

// handleJobFailure transitions job into FAILED and eventually DLQ.
func (p *Pool) handleJobFailure(job *task.Job, err error, toDLQ bool) {
	if job.State() == task.StateRunning {
		_ = job.TransitionTo(task.StateFailed)
	}

	if toDLQ && job.State() == task.StateFailed {
		_ = job.TransitionTo(task.StateDLQ)
	}
}

// Shutdown initiates a graceful drain of the worker pool.
// 1. Stops accepting new submissions by marking pool closed and closing jobs channel.
// 2. Waits for active workers to process queued items or until timeout expires.
// 3. If timeout expires, cancels the context to abort in-flight workers.
func (p *Pool) Shutdown(timeout time.Duration) error {
	var shutdownErr error

	p.stopOnce.Do(func() {
		p.logger.Info("Initiating graceful shutdown of worker pool...")

		// Step 1: Reject new incoming jobs
		p.closed.Store(true)

		// Step 2: Close channel so workers drain remaining jobs
		close(p.jobs)

		// Step 3: Wait for workers to finish with a deadline
		if timeout <= 0 {
			timeout = p.config.ShutdownTimeout
		}

		doneChan := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(doneChan)
		}()

		select {
		case <-doneChan:
			p.logger.Info("All workers exited cleanly")
		case <-time.After(timeout):
			shutdownErr = fmt.Errorf("shutdown timeout (%v) reached: %w", timeout, context.DeadlineExceeded)
			p.logger.Warn("Shutdown timed out, cancelling in-flight job contexts", "timeout", timeout)
			p.cancel() // cancel in-flight jobs
			<-doneChan // wait for workers to recognize cancellation
		}
	})

	return shutdownErr
}
