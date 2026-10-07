package buffer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
)

func TestRingBuffer_FIFO(t *testing.T) {
	rb := NewRingBuffer(5, DropOldest)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		rb.Push(&telemetryv1.LocationPing{DriverId: fmt.Sprintf("driver-%d", i)})
	}

	for i := 1; i <= 3; i++ {
		item, err := rb.Pop(ctx)
		if err != nil {
			t.Fatalf("Unexpected error on Pop: %v", err)
		}
		expectedID := fmt.Sprintf("driver-%d", i)
		if item.DriverId != expectedID {
			t.Fatalf("Expected %s, got %s", expectedID, item.DriverId)
		}
	}
}

func TestRingBuffer_DropOldest(t *testing.T) {
	capacity := 3
	rb := NewRingBuffer(capacity, DropOldest)
	ctx := context.Background()

	// Push 5 items into capacity-3 buffer
	for i := 1; i <= 5; i++ {
		dropped, wasOldest := rb.Push(&telemetryv1.LocationPing{DriverId: fmt.Sprintf("driver-%d", i)})
		if i <= 3 && dropped {
			t.Fatalf("Item %d shouldn't have been dropped", i)
		}
		if i > 3 && (!dropped || !wasOldest) {
			t.Fatalf("Item %d should have triggered DropOldest", i)
		}
	}

	stats := rb.Stats()
	if stats.CurrentDepth != 3 {
		t.Fatalf("Expected depth 3, got %d", stats.CurrentDepth)
	}
	if stats.TotalDropped != 2 {
		t.Fatalf("Expected 2 dropped, got %d", stats.TotalDropped)
	}

	// The remaining items should be 3, 4, 5 (1 and 2 dropped)
	expectedIDs := []string{"driver-3", "driver-4", "driver-5"}
	for _, exp := range expectedIDs {
		item, err := rb.Pop(ctx)
		if err != nil {
			t.Fatalf("Pop error: %v", err)
		}
		if item.DriverId != exp {
			t.Fatalf("Expected %s, got %s", exp, item.DriverId)
		}
	}
}

func TestRingBuffer_DropNewest(t *testing.T) {
	capacity := 2
	rb := NewRingBuffer(capacity, DropNewest)
	ctx := context.Background()

	// Push 3 items
	d1, _ := rb.Push(&telemetryv1.LocationPing{DriverId: "driver-1"})
	d2, _ := rb.Push(&telemetryv1.LocationPing{DriverId: "driver-2"})
	d3, wasOldest := rb.Push(&telemetryv1.LocationPing{DriverId: "driver-3"})

	if d1 || d2 {
		t.Fatal("driver-1 or driver-2 shouldn't be dropped")
	}
	if !d3 || wasOldest {
		t.Fatal("driver-3 should be dropped as newest")
	}

	item1, _ := rb.Pop(ctx)
	item2, _ := rb.Pop(ctx)

	if item1.DriverId != "driver-1" || item2.DriverId != "driver-2" {
		t.Fatalf("Unexpected items in buffer: %s, %s", item1.DriverId, item2.DriverId)
	}
}

func TestRingBuffer_ContextCancellation(t *testing.T) {
	rb := NewRingBuffer(5, DropOldest)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := rb.Pop(ctx)
	if err == nil {
		t.Fatal("Expected error on Pop with canceled context, got nil")
	}
}

func TestRingBuffer_ConcurrentProducersAndConsumers(t *testing.T) {
	capacity := 100
	rb := NewRingBuffer(capacity, DropOldest)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	numProducers := 10
	itemsPerProducer := 500
	var wg sync.WaitGroup

	// Producers
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				rb.Push(&telemetryv1.LocationPing{
					DriverId: fmt.Sprintf("p%d-item%d", pID, i),
				})
			}
		}(p)
	}

	// Consumer
	receivedCount := 0
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			_, err := rb.Pop(ctx)
			if err != nil {
				return
			}
			receivedCount++
		}
	}()

	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-consumerDone

	stats := rb.Stats()
	totalAccounted := stats.TotalDequeued + uint64(stats.CurrentDepth) + stats.TotalDropped
	if totalAccounted != uint64(numProducers*itemsPerProducer) {
		t.Fatalf("Data mismatch: accounted %d != produced %d", totalAccounted, numProducers*itemsPerProducer)
	}
}
