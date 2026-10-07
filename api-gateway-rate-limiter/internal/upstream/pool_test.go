package upstream

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_RoundRobin(t *testing.T) {
	urls := []string{"http://10.0.0.1:8080", "http://10.0.0.2:8080", "http://10.0.0.3:8080"}
	pool, err := NewPool(PoolConfig{
		Targets:        urls,
		Algorithm:      AlgorithmRoundRobin,
		HealthInterval: 0, // Disable background check for unit test
	})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	// Should rotate 0, 1, 2, 0, 1, 2
	expectedHosts := []string{"10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080", "10.0.0.1:8080"}
	for i, expected := range expectedHosts {
		target, err := pool.Select()
		if err != nil {
			t.Fatalf("step %d: unexpected error: %v", i, err)
		}
		if target.URL.Host != expected {
			t.Errorf("step %d: expected host %s, got %s", i, expected, target.URL.Host)
		}
	}
}

func TestPool_LeastConnections(t *testing.T) {
	urls := []string{"http://10.0.0.1:8080", "http://10.0.0.2:8080", "http://10.0.0.3:8080"}
	pool, err := NewPool(PoolConfig{
		Targets:        urls,
		Algorithm:      AlgorithmLeastConnections,
		HealthInterval: 0,
	})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	targets := pool.Targets()
	targets[0].activeConns.Store(10)
	targets[1].activeConns.Store(2) // Minimum
	targets[2].activeConns.Store(5)

	selected, err := pool.Select()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if selected.URL.Host != "10.0.0.2:8080" {
		t.Errorf("expected least connected target 10.0.0.2:8080, got %s", selected.URL.Host)
	}
}

func TestPool_PassiveCircuitBreaker(t *testing.T) {
	urls := []string{"http://10.0.0.1:8080", "http://10.0.0.2:8080"}
	pool, err := NewPool(PoolConfig{
		Targets:                 urls,
		Algorithm:               AlgorithmRoundRobin,
		HealthInterval:          0,
		PassiveFailureThreshold: 3,
	})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	t1 := pool.Targets()[0]

	// Simulate 2 failures: target should still be alive
	pool.RecordResult(t1, http.StatusInternalServerError, nil)
	pool.RecordResult(t1, http.StatusBadGateway, nil)
	if !t1.IsAlive() {
		t.Fatal("target should still be alive after 2 failures")
	}

	// 3rd failure: trips passive circuit breaker
	pool.RecordResult(t1, http.StatusInternalServerError, nil)
	if t1.IsAlive() {
		t.Fatal("target must be marked dead after 3 consecutive 5xx errors")
	}

	// Now selection should only return the remaining healthy target (10.0.0.2)
	for i := 0; i < 5; i++ {
		selected, err := pool.Select()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if selected.URL.Host != "10.0.0.2:8080" {
			t.Errorf("expected only alive target 10.0.0.2:8080, got %s", selected.URL.Host)
		}
	}
}

func TestPool_ActiveHealthCheck(t *testing.T) {
	var isHealthy atomic.Bool
	isHealthy.Store(false)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if isHealthy.Load() {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pool, err := NewPool(PoolConfig{
		Targets:                 []string{server.URL},
		HealthPath:              "/health",
		HealthInterval:          50 * time.Millisecond,
		HealthTimeout:           100 * time.Millisecond,
		PassiveFailureThreshold: 2,
	})
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	// Initial probe: server is not healthy -> marked dead
	pool.CheckAllTargets()
	target := pool.Targets()[0]
	if target.IsAlive() {
		t.Fatal("expected target to be marked dead when /health returns 503")
	}

	// Server recovers: returns 200
	isHealthy.Store(true)

	// Wait for active background health check loop to recover target
	deadline := time.Now().Add(1 * time.Second)
	recovered := false
	for time.Now().Before(deadline) {
		if target.IsAlive() {
			recovered = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !recovered {
		t.Fatal("active health checker failed to recover healthy server")
	}
}
