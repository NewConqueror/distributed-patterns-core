package test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"distributed-api-gateway/internal/limiter"
	"distributed-api-gateway/internal/proxy"
	"distributed-api-gateway/internal/rule"
	"distributed-api-gateway/internal/upstream"

	"github.com/redis/go-redis/v9"
)

// setupMockBackend starts an in-memory HTTP upstream backend server.
func setupMockBackend() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Backend-Server", "mock-upstream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
}

// setupGateway creates an in-process Gateway server for benchmarking.
func setupGateway(upstreamURL string, limiterMode string, rdb redis.UniversalClient) (*httptest.Server, func()) {
	pool, _ := upstream.NewPool(upstream.PoolConfig{
		Targets:        []string{upstreamURL},
		Algorithm:      upstream.AlgorithmRoundRobin,
		HealthInterval: 0,
	})

	forwarder := proxy.NewForwarder(nil)

	rulesYAML := `
global:
  capacity: 1000000
  rate: 1000000.0
  window: 60s
`
	reloader, _ := rule.NewReloader([]byte(rulesYAML), rule.ReloaderConfig{})

	var dynLimiter limiter.DynamicLimiter
	if limiterMode == "memory" {
		dynLimiter = limiter.NewMemoryLimiter(limiter.MemoryConfig{
			Rate:            1000000.0,
			Capacity:        1000000,
			TTL:             time.Hour,
			CleanupInterval: time.Hour,
			NumShards:       64,
		})
	} else if limiterMode == "redis" && rdb != nil {
		dynLimiter = limiter.NewRedisSlidingWindowLimiter(limiter.RedisConfig{
			Client:    rdb,
			Limit:     1000000,
			Window:    time.Minute,
			KeyPrefix: "bench_rl",
		})
	}

	proxyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target, _ := pool.Select()
		_, _ = forwarder.Forward(w, r, target.URL)
	})

	var rootHandler http.Handler = proxyHandler
	if dynLimiter != nil {
		dm := rule.NewDynamicMiddleware(reloader, dynLimiter)
		rootHandler = dm.Handler(proxyHandler)
	}

	server := httptest.NewServer(rootHandler)

	cleanup := func() {
		server.Close()
		pool.Close()
		reloader.Close()
		if dynLimiter != nil {
			dynLimiter.Close()
		}
	}

	return server, cleanup
}

func startBenchRedis(b *testing.B) (string, func()) {
	b.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to get free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	cmd := exec.Command("redis-server", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		b.Skipf("redis-server binary not found: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for i := 0; i < 30; i++ {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return addr, func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
}

// 1. Direct Upstream (No Gateway)
func Benchmark_1_DirectUpstream(b *testing.B) {
	backend := setupMockBackend()
	defer backend.Close()

	client := backend.Client()
	url := backend.URL

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(url)
			if err != nil {
				b.Fatalf("get failed: %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
}

// 2. Gateway (No Limiter - Pure Proxy Overhead)
func Benchmark_2_Gateway_NoLimiter(b *testing.B) {
	backend := setupMockBackend()
	defer backend.Close()

	gw, cleanup := setupGateway(backend.URL, "none", nil)
	defer cleanup()

	client := gw.Client()
	url := gw.URL + "/api/v1/items"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(url)
			if err != nil {
				b.Fatalf("get failed: %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
}

// 3. Gateway + In-Memory Token Bucket Limiter
func Benchmark_3_Gateway_InMemoryLimiter(b *testing.B) {
	backend := setupMockBackend()
	defer backend.Close()

	gw, cleanup := setupGateway(backend.URL, "memory", nil)
	defer cleanup()

	client := gw.Client()
	url := gw.URL + "/api/v1/items"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(url)
			if err != nil {
				b.Fatalf("get failed: %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
}

// 4. Gateway + Distributed Redis Sliding Window Limiter
func Benchmark_4_Gateway_RedisLimiter(b *testing.B) {
	addr, redisCleanup := startBenchRedis(b)
	defer redisCleanup()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	backend := setupMockBackend()
	defer backend.Close()

	gw, cleanup := setupGateway(backend.URL, "redis", rdb)
	defer cleanup()

	client := gw.Client()
	url := gw.URL + "/api/v1/items"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(url)
			if err != nil {
				b.Fatalf("get failed: %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
}
