package upstream

import (
	"errors"
	"sync/atomic"
)

var (
	ErrNoHealthyTargets = errors.New("no healthy upstream targets available")
)

// Algorithm defines the load balancing strategy.
type Algorithm string

const (
	AlgorithmRoundRobin       Algorithm = "round_robin"
	AlgorithmLeastConnections Algorithm = "least_connections"
)

// Balancer defines the contract for selecting an upstream target from an available pool.
type Balancer interface {
	Select(targets []*Target) (*Target, error)
}

// RoundRobinBalancer selects targets in sequential rotation using an atomic counter.
type RoundRobinBalancer struct {
	counter atomic.Uint64
}

// NewRoundRobinBalancer creates a round robin load balancer.
func NewRoundRobinBalancer() *RoundRobinBalancer {
	return &RoundRobinBalancer{}
}

// Select picks the next healthy target sequentially.
func (rr *RoundRobinBalancer) Select(targets []*Target) (*Target, error) {
	healthy := getHealthyTargets(targets)
	if len(healthy) == 0 {
		return nil, ErrNoHealthyTargets
	}

	idx := rr.counter.Add(1) - 1
	return healthy[idx%uint64(len(healthy))], nil
}

// LeastConnectionsBalancer routes requests to the target with the fewest in-flight connections.
type LeastConnectionsBalancer struct {
	rrFallback atomic.Uint64
}

// NewLeastConnectionsBalancer creates a least connections load balancer.
func NewLeastConnectionsBalancer() *LeastConnectionsBalancer {
	return &LeastConnectionsBalancer{}
}

// Select picks the healthy target with the lowest active connections count.
func (lc *LeastConnectionsBalancer) Select(targets []*Target) (*Target, error) {
	healthy := getHealthyTargets(targets)
	if len(healthy) == 0 {
		return nil, ErrNoHealthyTargets
	}

	bestTarget := healthy[0]
	minConns := bestTarget.ActiveConns()

	for i := 1; i < len(healthy); i++ {
		conns := healthy[i].ActiveConns()
		if conns < minConns {
			minConns = conns
			bestTarget = healthy[i]
		}
	}

	return bestTarget, nil
}

func getHealthyTargets(targets []*Target) []*Target {
	healthy := make([]*Target, 0, len(targets))
	for _, t := range targets {
		if t.IsAlive() {
			healthy = append(healthy, t)
		}
	}
	return healthy
}
