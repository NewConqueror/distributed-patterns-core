package buffer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
)

var (
	// ErrBufferClosed is returned when an operation is attempted on a closed buffer.
	ErrBufferClosed = errors.New("buffer is closed")
)

// DropStrategy defines how the buffer handles incoming items when at maximum capacity.
type DropStrategy int

const (
	// DropOldest evicts the oldest item in the buffer to accommodate the new item.
	// This is the preferred strategy for real-time tracking (fresh telemetry > stale telemetry).
	DropOldest DropStrategy = iota

	// DropNewest discards the incoming item, preserving previously queued items.
	DropNewest
)

// Stats holds snapshot metrics for the ring buffer.
type Stats struct {
	Capacity      int
	CurrentDepth  int
	TotalEnqueued uint64
	TotalDequeued uint64
	TotalDropped  uint64
}

// RingBuffer is a thread-safe, bounded ring buffer designed for high-throughput
// telemetry ingestion with explicit backpressure and drop policies.
type RingBuffer struct {
	capacity int
	strategy DropStrategy

	mu        sync.Mutex
	cond      *sync.Cond
	items     []*telemetryv1.LocationPing
	head      int // index of the oldest item
	tail      int // index where next item will be inserted
	count     int
	isClosed  bool

	// Metrics counters
	totalEnqueued atomic.Uint64
	totalDequeued atomic.Uint64
	totalDropped  atomic.Uint64
}

// NewRingBuffer creates a new RingBuffer with a fixed capacity and drop strategy.
func NewRingBuffer(capacity int, strategy DropStrategy) *RingBuffer {
	if capacity <= 0 {
		capacity = 1024
	}
	rb := &RingBuffer{
		capacity: capacity,
		strategy: strategy,
		items:    make([]*telemetryv1.LocationPing, capacity),
	}
	rb.cond = sync.NewCond(&rb.mu)
	return rb
}

// Push inserts an item into the buffer.
// If the buffer is full, it executes the configured DropStrategy.
// Returns (dropped bool, droppedOldest bool).
func (rb *RingBuffer) Push(item *telemetryv1.LocationPing) (bool, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.isClosed {
		return true, false
	}

	dropped := false
	droppedOldest := false

	if rb.count == rb.capacity {
		dropped = true
		rb.totalDropped.Add(1)

		if rb.strategy == DropNewest {
			// Drop incoming item immediately
			return true, false
		}

		// Drop oldest item: advance head to discard the oldest item
		rb.items[rb.head] = nil // Avoid memory leak / GC retention
		rb.head = (rb.head + 1) % rb.capacity
		rb.count--
		droppedOldest = true
	}

	// Insert item at tail
	rb.items[rb.tail] = item
	rb.tail = (rb.tail + 1) % rb.capacity
	rb.count++
	rb.totalEnqueued.Add(1)

	// Wake up waiting consumers
	rb.cond.Signal()

	return dropped, droppedOldest
}

// Pop retrieves the next item from the buffer, blocking until an item is available
// or the context is cancelled / buffer is closed.
func (rb *RingBuffer) Pop(ctx context.Context) (*telemetryv1.LocationPing, error) {
	// Goroutine to broadcast cond when context is cancelled
	doneCh := make(chan struct{})
	defer close(doneCh)

	go func() {
		select {
		case <-ctx.Done():
			rb.mu.Lock()
			rb.cond.Broadcast()
			rb.mu.Unlock()
		case <-doneCh:
		}
	}()

	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.count == 0 {
		if rb.isClosed {
			return nil, ErrBufferClosed
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		rb.cond.Wait()
	}

	item := rb.items[rb.head]
	rb.items[rb.head] = nil // Clear pointer for GC
	rb.head = (rb.head + 1) % rb.capacity
	rb.count--
	rb.totalDequeued.Add(1)

	return item, nil
}

// PopBatch drains up to maxBatch items or returns whatever is available within the timeout.
// This enables efficient bulk ingestion into hot/cold storage pipelines.
func (rb *RingBuffer) PopBatch(ctx context.Context, maxBatch int, timeout time.Duration) ([]*telemetryv1.LocationPing, error) {
	if maxBatch <= 0 {
		maxBatch = 100
	}

	deadline := time.Now().Add(timeout)
	batch := make([]*telemetryv1.LocationPing, 0, maxBatch)

	for len(batch) < maxBatch {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		subCtx, cancel := context.WithTimeout(ctx, remaining)
		item, err := rb.Pop(subCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			if errors.Is(err, context.Canceled) {
				if len(batch) > 0 {
					return batch, nil
				}
				return nil, err
			}
			if errors.Is(err, ErrBufferClosed) {
				if len(batch) > 0 {
					return batch, nil
				}
				return nil, err
			}
			return nil, err
		}

		batch = append(batch, item)
	}

	return batch, nil
}

// Close marks the buffer as closed and signals all waiting consumers.
func (rb *RingBuffer) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if !rb.isClosed {
		rb.isClosed = true
		rb.cond.Broadcast()
	}
}

// Stats returns a snapshot of buffer activity metrics.
func (rb *RingBuffer) Stats() Stats {
	rb.mu.Lock()
	depth := rb.count
	rb.mu.Unlock()

	return Stats{
		Capacity:      rb.capacity,
		CurrentDepth:  depth,
		TotalEnqueued: rb.totalEnqueued.Load(),
		TotalDequeued: rb.totalDequeued.Load(),
		TotalDropped:  rb.totalDropped.Load(),
	}
}
