package upstream

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// PoolConfig specifies parameters for upstream target management and health checking.
type PoolConfig struct {
	Targets                 []string
	Algorithm               Algorithm
	HealthPath              string
	HealthInterval          time.Duration
	HealthTimeout           time.Duration
	PassiveFailureThreshold int64 // Number of consecutive failures before marking target down
}

// DefaultPoolConfig provides production defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		Algorithm:               AlgorithmRoundRobin,
		HealthPath:              "/health",
		HealthInterval:          5 * time.Second,
		HealthTimeout:           2 * time.Second,
		PassiveFailureThreshold: 3,
	}
}

// Pool manages a collection of backend servers, routing decisions, and health states.
type Pool struct {
	cfg          PoolConfig
	targets      []*Target
	balancer     Balancer
	healthClient *http.Client

	stopCh  chan struct{}
	closeWg sync.WaitGroup
}

// NewPool creates an upstream pool and initializes the selected load balancer.
func NewPool(cfg PoolConfig) (*Pool, error) {
	if cfg.HealthInterval <= 0 {
		cfg.HealthInterval = 5 * time.Second
	}
	if cfg.HealthTimeout <= 0 {
		cfg.HealthTimeout = 2 * time.Second
	}
	if cfg.PassiveFailureThreshold <= 0 {
		cfg.PassiveFailureThreshold = 3
	}
	if cfg.HealthPath == "" {
		cfg.HealthPath = "/health"
	}

	targets := make([]*Target, 0, len(cfg.Targets))
	for _, raw := range cfg.Targets {
		t, err := NewTarget(raw)
		if err != nil {
			return nil, err
		}
		targets = append(targets, t)
	}

	var balancer Balancer
	if cfg.Algorithm == AlgorithmLeastConnections {
		balancer = NewLeastConnectionsBalancer()
	} else {
		balancer = NewRoundRobinBalancer()
	}

	p := &Pool{
		cfg:      cfg,
		targets:  targets,
		balancer: balancer,
		healthClient: &http.Client{
			Timeout: cfg.HealthTimeout,
		},
		stopCh: make(chan struct{}),
	}

	if cfg.HealthInterval > 0 && len(targets) > 0 {
		p.closeWg.Add(1)
		go p.startActiveHealthChecker()
	}

	return p, nil
}

// Targets returns a snapshot of all targets in the pool.
func (p *Pool) Targets() []*Target {
	return p.targets
}

// Select picks the next target based on the active load balancing algorithm.
func (p *Pool) Select() (*Target, error) {
	return p.balancer.Select(p.targets)
}

// RecordResult observes upstream response status and executes passive circuit breaking.
// If an upstream yields consecutive 5xx or network errors exceeding threshold, it is isolated.
func (p *Pool) RecordResult(target *Target, statusCode int, err error) {
	if target == nil {
		return
	}

	if err != nil || statusCode >= http.StatusInternalServerError {
		failures := target.RecordFailure()
		if failures >= p.cfg.PassiveFailureThreshold {
			target.SetAlive(false) // Passive circuit trip
		}
	} else {
		target.RecordSuccess()
	}
}

// startActiveHealthChecker periodically probes /health on all targets.
func (p *Pool) startActiveHealthChecker() {
	defer p.closeWg.Done()

	ticker := time.NewTicker(p.cfg.HealthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.checkAllTargets()
		}
	}
}

// CheckAllTargets performs an immediate active health probe across all targets.
func (p *Pool) CheckAllTargets() {
	p.checkAllTargets()
}

func (p *Pool) checkAllTargets() {
	var wg sync.WaitGroup
	for _, target := range p.targets {
		wg.Add(1)
		go func(t *Target) {
			defer wg.Done()
			p.probeTarget(t)
		}(target)
	}
	wg.Wait()
}

func (p *Pool) probeTarget(t *Target) {
	t.MarkChecked()

	probeURL, err := url.Parse(t.URL.String())
	if err != nil {
		t.SetAlive(false)
		return
	}
	probeURL.Path = p.cfg.HealthPath

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, probeURL.String(), nil)
	if err != nil {
		t.SetAlive(false)
		return
	}

	resp, err := p.healthClient.Do(req)
	if err != nil {
		t.SetAlive(false)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.RecordSuccess()
	} else {
		t.SetAlive(false)
	}
}

// Close terminates the active health check background worker.
func (p *Pool) Close() error {
	select {
	case <-p.stopCh:
		return nil
	default:
		close(p.stopCh)
	}
	p.closeWg.Wait()
	return nil
}
