package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryLimiter_Basic(t *testing.T) {
	cfg := MemoryConfig{
		Rate:            10.0,
		Capacity:        5,
		TTL:             time.Minute,
		CleanupInterval: time.Minute,
		NumShards:       8,
	}
	lim := NewMemoryLimiter(cfg)
	defer lim.Close()

	ctx := context.Background()
	key := "client-1"

	// Should allow 5 burst requests
	for i := 0; i < 5; i++ {
		res, err := lim.Allow(ctx, key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Allowed {
			t.Fatalf("request %d expected to be allowed", i+1)
		}
		expectedRemaining := int64(4 - i)
		if res.Remaining != expectedRemaining {
			t.Errorf("request %d: expected remaining %d, got %d", i+1, expectedRemaining, res.Remaining)
		}
	}

	// 6th request must be rejected
	res, err := lim.Allow(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Allowed {
		t.Fatalf("expected 6th request to be blocked")
	}
	if res.RetryAfter <= 0 {
		t.Errorf("expected positive RetryAfter, got %v", res.RetryAfter)
	}
}

func TestMemoryLimiter_TokenReplenishment(t *testing.T) {
	cfg := MemoryConfig{
		Rate:            20.0, // 20 tokens/sec => 1 token every 50ms
		Capacity:        2,
		TTL:             time.Minute,
		CleanupInterval: time.Minute,
		NumShards:       4,
	}
	lim := NewMemoryLimiter(cfg)
	defer lim.Close()

	ctx := context.Background()
	key := "replenish-client"

	// Exhaust 2 tokens
	lim.Allow(ctx, key)
	lim.Allow(ctx, key)

	res, _ := lim.Allow(ctx, key)
	if res.Allowed {
		t.Fatal("expected request to be denied after exhausting capacity")
	}

	// Wait 120ms (enough to replenish at least 2 tokens)
	time.Sleep(120 * time.Millisecond)

	res, err := lim.Allow(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("expected request to be allowed after token replenishment")
	}
}

func TestMemoryLimiter_Concurrency(t *testing.T) {
	cfg := MemoryConfig{
		Rate:            100.0,
		Capacity:        100,
		TTL:             time.Minute,
		CleanupInterval: time.Minute,
		NumShards:       16,
	}
	lim := NewMemoryLimiter(cfg)
	defer lim.Close()

	ctx := context.Background()
	var allowedCount int64
	var deniedCount int64

	var wg sync.WaitGroup
	workers := 50
	requestsPerWorker := 4 // Total 200 requests for capacity 100

	key := "shared-key"

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerWorker; j++ {
				res, err := lim.Allow(ctx, key)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
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

	if allowedCount != 100 {
		t.Errorf("expected exactly 100 allowed requests, got %d", allowedCount)
	}
	if deniedCount != 100 {
		t.Errorf("expected exactly 100 denied requests, got %d", deniedCount)
	}
}

func TestMemoryLimiter_TTLCleanup(t *testing.T) {
	cfg := MemoryConfig{
		Rate:            10.0,
		Capacity:        10,
		TTL:             50 * time.Millisecond,
		CleanupInterval: 20 * time.Millisecond,
		NumShards:       4,
	}
	lim := NewMemoryLimiter(cfg)
	defer lim.Close()

	ctx := context.Background()

	// Insert 10 distinct client keys
	for i := 0; i < 10; i++ {
		_, err := lim.Allow(ctx, fmt.Sprintf("temp-client-%d", i))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if lim.KeyCount() != 10 {
		t.Fatalf("expected 10 keys before cleanup, got %d", lim.KeyCount())
	}

	// Sleep past TTL and cleanup interval
	time.Sleep(120 * time.Millisecond)

	if remaining := lim.KeyCount(); remaining != 0 {
		t.Fatalf("expected 0 keys after TTL cleanup, got %d", remaining)
	}
}

func BenchmarkMemoryLimiter_Allow(b *testing.B) {
	cfg := MemoryConfig{
		Rate:            1000000.0,
		Capacity:        1000000,
		TTL:             time.Hour,
		CleanupInterval: time.Hour,
		NumShards:       64,
	}
	lim := NewMemoryLimiter(cfg)
	defer lim.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerID := time.Now().UnixNano()
		key := fmt.Sprintf("bench-client-%d", workerID)
		for pb.Next() {
			_, _ = lim.Allow(ctx, key)
		}
	})
}
