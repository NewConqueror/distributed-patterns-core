package circuitbreaker

import (
	"context"
	"log"
	"sync"
	"time"
)

type State string

const (
	StateClosed State = "CLOSED" // Normal operation
	StateOpen   State = "OPEN"   // Elasticsearch down; consumption paused
)

type HealthChecker interface {
	CheckHealth(ctx context.Context) (status string, isHealthy bool, err error)
}

type CircuitBreaker struct {
	mu             sync.RWMutex
	state          State
	initialBackoff time.Duration
	maxBackoff     time.Duration
	healthChecker  HealthChecker

	resumeChan chan struct{}
}

func NewCircuitBreaker(healthChecker HealthChecker, initialBackoff, maxBackoff time.Duration) *CircuitBreaker {
	if initialBackoff <= 0 {
		initialBackoff = 1 * time.Second
	}
	if maxBackoff <= 0 {
		maxBackoff = 16 * time.Second
	}

	return &CircuitBreaker{
		state:          StateClosed,
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
		healthChecker:  healthChecker,
		resumeChan:     make(chan struct{}),
	}
}

// GetState returns the current circuit breaker state
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// IsOpen returns true if the circuit breaker is currently open (consumption must be paused)
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state == StateOpen
}

// Trip opens the circuit breaker upon detecting a transient error (e.g., ES 5xx or connection refused)
func (cb *CircuitBreaker) Trip(cause error) {
	cb.mu.Lock()
	if cb.state == StateOpen {
		cb.mu.Unlock()
		return // Already open
	}

	cb.state = StateOpen
	cb.resumeChan = make(chan struct{})
	cb.mu.Unlock()

	log.Printf("[Circuit Breaker OPEN] Tripped due to transient error: %v. Pausing Kafka consumption...", cause)

	// Launch background recovery probe with exponential backoff
	go cb.probeHealthUntilRecovered()
}

// WaitIfOpen blocks if the circuit breaker is open until it is closed or context is cancelled
func (cb *CircuitBreaker) WaitIfOpen(ctx context.Context) error {
	cb.mu.RLock()
	if cb.state == StateClosed {
		cb.mu.RUnlock()
		return nil
	}
	waitChan := cb.resumeChan
	cb.mu.RUnlock()

	log.Println("[Circuit Breaker] Consumer paused, waiting for Elasticsearch cluster recovery...")

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitChan:
		log.Println("[Circuit Breaker] Received resume signal. Continuing pipeline...")
		return nil
	}
}

// probeHealthUntilRecovered polls Elasticsearch /_cluster/health using exponential backoff (1s, 2s, 4s, 8s, 16s...)
func (cb *CircuitBreaker) probeHealthUntilRecovered() {
	backoff := cb.initialBackoff
	attempt := 1

	for {
		log.Printf("[Circuit Breaker Probe] Attempt %d: Waiting %v before probing Elasticsearch...", attempt, backoff)
		time.Sleep(backoff)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		status, isHealthy, err := cb.healthChecker.CheckHealth(ctx)
		cancel()

		if err == nil && isHealthy {
			log.Printf("[Circuit Breaker CLOSED] Elasticsearch recovered (Cluster Status: '%s'). Resuming Kafka consumer!", status)
			cb.mu.Lock()
			cb.state = StateClosed
			close(cb.resumeChan)
			cb.mu.Unlock()
			return
		}

		log.Printf("[Circuit Breaker Probe Failed] Attempt %d (Status: '%s', Err: %v). Exponentially increasing backoff...",
			attempt, status, err)

		// Exponential backoff: 1s -> 2s -> 4s -> 8s -> 16s (capped at maxBackoff)
		backoff *= 2
		if backoff > cb.maxBackoff {
			backoff = cb.maxBackoff
		}
		attempt++
	}
}
