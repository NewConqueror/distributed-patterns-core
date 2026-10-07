package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

// State represents the circuit breaker state.
type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateHalfOpen:
		return "HALF-OPEN"
	case StateOpen:
		return "OPEN"
	default:
		return "UNKNOWN"
	}
}

var (
	// ErrCircuitOpen is returned when the circuit breaker is OPEN and rejects calls.
	ErrCircuitOpen = errors.New("circuit breaker is open: provider unavailable")
)

// CircuitBreakerConfig holds settings for CircuitBreaker.
type CircuitBreakerConfig struct {
	// WindowSize is the number of recent requests tracked (e.g., 10).
	WindowSize int
	// FailureThreshold is how many failures within WindowSize will open the circuit (e.g., 3).
	FailureThreshold int
	// SuccessThreshold is how many consecutive successful probes needed in HalfOpen to close (e.g., 2).
	SuccessThreshold int
	// OpenCooldown is how long the circuit stays OPEN before transitioning to HALF-OPEN.
	OpenCooldown time.Duration
}

// DefaultCircuitBreakerConfig provides sensible production defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		WindowSize:       10,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenCooldown:     5 * time.Second,
	}
}

// CircuitBreaker monitors provider reliability and trips when errors exceed threshold.
type CircuitBreaker struct {
	mu sync.RWMutex

	providerID string
	config     CircuitBreakerConfig

	state        State
	lastStateChange time.Time

	// Sliding window for recent requests (true = success, false = failure)
	history       []bool
	halfOpenSuccessCount int
}

// NewCircuitBreaker creates a new CircuitBreaker for a provider.
func NewCircuitBreaker(providerID string, cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 10
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}
	if cfg.OpenCooldown <= 0 {
		cfg.OpenCooldown = 5 * time.Second
	}

	return &CircuitBreaker{
		providerID:      providerID,
		config:          cfg,
		state:           StateClosed,
		lastStateChange: time.Now(),
		history:         make([]bool, 0, cfg.WindowSize),
	}
}

// ProviderID returns the identifier of the provider this circuit breaker monitors.
func (cb *CircuitBreaker) ProviderID() string {
	return cb.providerID
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// If in OPEN state and cooldown elapsed, transition to HALF-OPEN
	if cb.state == StateOpen && time.Since(cb.lastStateChange) >= cb.config.OpenCooldown {
		cb.state = StateHalfOpen
		cb.lastStateChange = time.Now()
		cb.halfOpenSuccessCount = 0
	}

	return cb.state
}

// Allow checks if a request is permitted to proceed.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateOpen {
		if time.Since(cb.lastStateChange) >= cb.config.OpenCooldown {
			// Probe trial
			cb.state = StateHalfOpen
			cb.lastStateChange = time.Now()
			cb.halfOpenSuccessCount = 0
			return true
		}
		return false
	}

	return true
}

// RecordSuccess records a successful call.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateHalfOpen:
		cb.halfOpenSuccessCount++
		if cb.halfOpenSuccessCount >= cb.config.SuccessThreshold {
			cb.state = StateClosed
			cb.lastStateChange = time.Now()
			cb.history = cb.history[:0]
			cb.halfOpenSuccessCount = 0
		}
	case StateClosed:
		cb.appendHistory(true)
	}
}

// RecordFailure records a failed call and trips the circuit if threshold is reached.
func (cb *CircuitBreaker) RecordFailure(err error) {
	if !cb.isFailure(err) {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateHalfOpen:
		// Any failure in Half-Open immediately trips back to OPEN
		cb.state = StateOpen
		cb.lastStateChange = time.Now()
		cb.halfOpenSuccessCount = 0
	case StateClosed:
		cb.appendHistory(false)
		if cb.countFailures() >= cb.config.FailureThreshold {
			cb.state = StateOpen
			cb.lastStateChange = time.Now()
		}
	}
}

func (cb *CircuitBreaker) appendHistory(success bool) {
	if len(cb.history) >= cb.config.WindowSize {
		// Shift left
		copy(cb.history, cb.history[1:])
		cb.history[len(cb.history)-1] = success
	} else {
		cb.history = append(cb.history, success)
	}
}

func (cb *CircuitBreaker) countFailures() int {
	failures := 0
	for _, res := range cb.history {
		if !res {
			failures++
		}
	}
	return failures
}

// isFailure inspects whether the error warrants tripping the circuit breaker.
// Triggers on: HTTP 429 (Rate Limit), 5xx (Server Error), or Context Timeout / Deadline Exceeded.
func (cb *CircuitBreaker) isFailure(err error) bool {
	if err == nil {
		return false
	}

	// Context deadline exceeded / timeout
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var upstreamErr *models.UpstreamError
	if errors.As(err, &upstreamErr) {
		if upstreamErr.StatusCode == http.StatusTooManyRequests { // 429
			return true
		}
		if upstreamErr.StatusCode >= 500 { // 5xx
			return true
		}
		return upstreamErr.Retryable
	}

	return false
}
