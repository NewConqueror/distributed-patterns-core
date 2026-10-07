package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// State represents the lifecycle states of a task/job.
type State string

const (
	StatePending   State = "PENDING"
	StateRunning   State = "RUNNING"
	StateCompleted State = "COMPLETED"
	StateFailed    State = "FAILED"
	StateDLQ       State = "DLQ"
)

var (
	ErrInvalidStateTransition = errors.New("invalid state transition")
)

// validTransitions defines strict transitions between job states.
// PENDING -> RUNNING
// RUNNING -> COMPLETED, FAILED, DLQ
// FAILED  -> PENDING (retry), DLQ (exhausted/fatal)
var validTransitions = map[State]map[State]bool{
	StatePending: {
		StateRunning: true,
	},
	StateRunning: {
		StateCompleted: true,
		StateFailed:    true,
		StateDLQ:       true,
	},
	StateFailed: {
		StatePending: true, // For re-queueing on retry
		StateDLQ:     true, // Exceeded max retries or fatal error
	},
	StateCompleted: {}, // Terminal state: no further transitions allowed
	StateDLQ:       {}, // Terminal state: dead letter queue
}

// ValidateTransition verifies if transitioning from one state to another is permitted.
func ValidateTransition(from, to State) error {
	allowedNextStates, exists := validTransitions[from]
	if !exists || !allowedNextStates[to] {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, from, to)
	}
	return nil
}

// Job represents the atomic unit of work in the orchestrator.
type Job struct {
	mu sync.RWMutex

	ID           string    `json:"id"`
	Type         string    `json:"type"`
	Payload      []byte    `json:"payload"`
	CurrentState State             `json:"current_state"`
	MaxRetries   int               `json:"max_retries"`
	RetryCount   int               `json:"retry_count"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// NewJob creates a new Job initialized in the PENDING state.
func NewJob(id string, jobType string, payload []byte, maxRetries int) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:           id,
		Type:         jobType,
		Payload:      payload,
		CurrentState: StatePending,
		MaxRetries:   maxRetries,
		RetryCount:   0,
		Metadata:     make(map[string]string),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

// SetMetadata sets a metadata key-value in a thread-safe manner.
func (j *Job) SetMetadata(key, value string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Metadata == nil {
		j.Metadata = make(map[string]string)
	}
	j.Metadata[key] = value
}

// GetMetadata gets a metadata value in a thread-safe manner.
func (j *Job) GetMetadata(key string) string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.Metadata == nil {
		return ""
	}
	return j.Metadata[key]
}

// Carrier returns a MapCarrier for OpenTelemetry propagation.
func (j *Job) Carrier() MapCarrier {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Metadata == nil {
		j.Metadata = make(map[string]string)
	}
	return MapCarrier(j.Metadata)
}

// MapCarrier adapts map[string]string to OpenTelemetry propagation.TextMapCarrier.
type MapCarrier map[string]string

func (c MapCarrier) Get(key string) string {
	return c[key]
}

func (c MapCarrier) Set(key, val string) {
	c[key] = val
}

func (c MapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// State returns the current state in a thread-safe manner.
func (j *Job) State() State {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.CurrentState
}

// TransitionTo updates the job's state after validating the transition rule.
func (j *Job) TransitionTo(newState State) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if err := ValidateTransition(j.CurrentState, newState); err != nil {
		return err
	}

	j.CurrentState = newState
	j.UpdatedAt = time.Now().UTC()
	return nil
}

// CanRetry checks if the job is eligible for another retry attempt.
func (j *Job) CanRetry() bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.RetryCount < j.MaxRetries
}

// IncrementRetry increments the retry counter in a thread-safe manner.
func (j *Job) IncrementRetry() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.RetryCount++
	j.UpdatedAt = time.Now().UTC()
}

// Handler defines the contract for executing different types of jobs.
type Handler interface {
	Process(ctx context.Context, job Job) error
}

// HandlerFunc is an adapter to allow the use of ordinary functions as Handlers.
type HandlerFunc func(ctx context.Context, job Job) error

// Process calls f(ctx, job).
func (f HandlerFunc) Process(ctx context.Context, job Job) error {
	return f(ctx, job)
}
