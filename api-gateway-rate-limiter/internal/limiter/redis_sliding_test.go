package limiter

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// startLocalRedisServer starts an ephemeral redis-server on a free port for integration testing.
func startLocalRedisServer(t *testing.T) (string, func()) {
	t.Helper()

	// Find an open port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	cmd := exec.Command("redis-server", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Skipf("redis-server binary not found or failed to start: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// Wait until Redis is ready to accept connections
	ready := false
	for i := 0; i < 30; i++ {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !ready {
		_ = cmd.Process.Kill()
		t.Fatalf("redis server did not become ready in time on %s", addr)
	}

	cleanup := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}

	return addr, cleanup
}

func TestRedisSlidingWindowLimiter_Basic(t *testing.T) {
	addr, cleanup := startLocalRedisServer(t)
	defer cleanup()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	lim := NewRedisSlidingWindowLimiter(RedisConfig{
		Client:    rdb,
		Limit:     3,
		Window:    time.Minute,
		KeyPrefix: "test_basic",
	})
	defer lim.Close()

	ctx := context.Background()
	key := "user-100"

	// 1st, 2nd, 3rd requests must be allowed
	for i := 0; i < 3; i++ {
		res, err := lim.Allow(ctx, key)
		if err != nil {
			t.Fatalf("unexpected error on req %d: %v", i+1, err)
		}
		if !res.Allowed {
			t.Fatalf("expected request %d to be allowed", i+1)
		}
		expectedRemaining := int64(2 - i)
		if res.Remaining != expectedRemaining {
			t.Errorf("req %d: expected remaining %d, got %d", i+1, expectedRemaining, res.Remaining)
		}
	}

	// 4th request must be denied
	res, err := lim.Allow(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error on 4th req: %v", err)
	}
	if res.Allowed {
		t.Fatal("expected 4th request to be denied")
	}
	if res.Remaining != 0 {
		t.Errorf("expected 0 remaining, got %d", res.Remaining)
	}
	if res.RetryAfter <= 0 {
		t.Errorf("expected positive retry-after, got %v", res.RetryAfter)
	}
}

func TestRedisSlidingWindowLimiter_FormulaWeighting(t *testing.T) {
	addr, cleanup := startLocalRedisServer(t)
	defer cleanup()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	window := 10 * time.Second
	limit := int64(10)

	lim := NewRedisSlidingWindowLimiter(RedisConfig{
		Client:    rdb,
		Limit:     limit,
		Window:    window,
		KeyPrefix: "test_formula",
	})
	defer lim.Close()

	ctx := context.Background()
	key := "formula-client"

	nowMs := time.Now().UnixMilli()
	windowMs := window.Milliseconds()
	currWindowID := nowMs / windowMs
	prevWindowID := currWindowID - 1

	// Seed previous window with 8 requests in Redis
	prevKey := fmt.Sprintf("test_formula:{%s}:%d", key, prevWindowID)
	err := rdb.Set(ctx, prevKey, 8, time.Minute).Err()
	if err != nil {
		t.Fatalf("failed to seed previous window: %v", err)
	}

	// Measure the exact time of the call
	callTimeMs := time.Now().UnixMilli()
	res, err := lim.Allow(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Calculate expected values using the exact formula:
	// Estimated = Current(0) + Previous(8) * (1 - ElapsedRatio)
	timeInCurr := float64(callTimeMs % windowMs)
	elapsedRatio := timeInCurr / float64(windowMs)
	weightPrev := 1.0 - elapsedRatio
	expectedEstimated := 8.0 * weightPrev
	expectedRemaining := int64(10.0 - expectedEstimated - 1.0)
	if expectedRemaining < 0 {
		expectedRemaining = 0
	}

	// Allow ±1 difference due to sub-millisecond execution time delta
	diff := res.Remaining - expectedRemaining
	if diff < -1 || diff > 1 {
		t.Errorf("expected remaining around %d (elapsedRatio: %.3f, weight: %.3f), got %d",
			expectedRemaining, elapsedRatio, weightPrev, res.Remaining)
	}
}

func TestRedisSlidingWindowLimiter_Concurrency(t *testing.T) {
	addr, cleanup := startLocalRedisServer(t)
	defer cleanup()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	limit := int64(50)
	lim := NewRedisSlidingWindowLimiter(RedisConfig{
		Client:    rdb,
		Limit:     limit,
		Window:    time.Minute,
		KeyPrefix: "test_concurrency",
	})
	defer lim.Close()

	ctx := context.Background()
	key := "concurrent-user"

	var allowedCount int64
	var deniedCount int64

	workers := 25
	requestsPerWorker := 4 // Total 100 requests for limit 50
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerWorker; j++ {
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
				res, err := lim.Allow(ctx, key)
				if err != nil {
					t.Errorf("redis allow error: %v", err)
					return
				}
				if res.Allowed {
					atomic.AddInt64(&allowedCount, 1)
				} else {
					atomic.AddInt64(&deniedCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	if allowedCount != limit {
		t.Errorf("expected exactly %d allowed requests, got %d", limit, allowedCount)
	}
	if deniedCount != int64(workers*requestsPerWorker)-limit {
		t.Errorf("expected %d denied requests, got %d", int64(workers*requestsPerWorker)-limit, deniedCount)
	}
}
