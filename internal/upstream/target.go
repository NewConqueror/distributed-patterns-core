package upstream

import (
	"net/url"
	"sync/atomic"
	"time"
)

// Target represents a single backend server in the upstream pool.
type Target struct {
	URL                 *url.URL
	alive               atomic.Bool
	activeConns         atomic.Int64
	consecutiveFailures atomic.Int64
	lastChecked         atomic.Int64 // Unix nano
}

// NewTarget creates an active backend target.
func NewTarget(rawURL string) (*Target, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	t := &Target{
		URL: u,
	}
	t.alive.Store(true)
	return t, nil
}

// IsAlive returns whether the target is currently healthy and eligible for routing.
func (t *Target) IsAlive() bool {
	return t.alive.Load()
}

// SetAlive updates the target's health status.
func (t *Target) SetAlive(alive bool) {
	t.alive.Store(alive)
}

// ActiveConns returns the number of active in-flight requests.
func (t *Target) ActiveConns() int64 {
	return t.activeConns.Load()
}

// IncConn increments the active connection count.
func (t *Target) IncConn() {
	t.activeConns.Add(1)
}

// DecConn decrements the active connection count.
func (t *Target) DecConn() {
	t.activeConns.Add(-1)
}

// ConsecutiveFailures returns consecutive 5xx or network errors.
func (t *Target) ConsecutiveFailures() int64 {
	return t.consecutiveFailures.Load()
}

// RecordSuccess resets consecutive failures and restores health.
func (t *Target) RecordSuccess() {
	t.consecutiveFailures.Store(0)
	t.alive.Store(true)
}

// RecordFailure increments consecutive failures.
// Returns the new failure count.
func (t *Target) RecordFailure() int64 {
	return t.consecutiveFailures.Add(1)
}

// MarkChecked records the timestamp of the last health check.
func (t *Target) MarkChecked() {
	t.lastChecked.Store(time.Now().UnixNano())
}
