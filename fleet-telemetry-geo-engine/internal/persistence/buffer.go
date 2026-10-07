package persistence

import (
	"sync"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
)

// TelemetryRecord is the in-memory representation of a driver telemetry ping
// staged for bulk insertion into PostgreSQL.
type TelemetryRecord struct {
	DriverID   string
	Latitude   float64
	Longitude  float64
	Bearing    float32
	Speed      float32
	RecordedAt time.Time
}

// BatchBuffer provides a thread-safe in-memory accumulation buffer for incoming
// GPS telemetry pings before they are flushed to durable cold storage in bulk.
type BatchBuffer struct {
	mu            sync.Mutex
	records       []*TelemetryRecord
	batchCapacity int
}

// NewBatchBuffer creates a new buffer with initial slice pre-allocation to prevent reallocations.
func NewBatchBuffer(batchCapacity int) *BatchBuffer {
	if batchCapacity <= 0 {
		batchCapacity = 250
	}

	return &BatchBuffer{
		records:       make([]*TelemetryRecord, 0, batchCapacity),
		batchCapacity: batchCapacity,
	}
}

// Push appends a ping to the in-memory accumulation pool in O(1) time.
// It returns the current count and a boolean indicating whether the batch threshold was reached.
func (b *BatchBuffer) Push(ping *telemetryv1.LocationPing) (int, bool) {
	rec := &TelemetryRecord{
		DriverID:   ping.DriverId,
		Latitude:   ping.Latitude,
		Longitude:  ping.Longitude,
		Bearing:    ping.Bearing,
		Speed:      ping.Speed,
		RecordedAt: time.UnixMilli(ping.Timestamp),
	}

	b.mu.Lock()
	b.records = append(b.records, rec)
	count := len(b.records)
	thresholdMet := count >= b.batchCapacity
	b.mu.Unlock()

	return count, thresholdMet
}

// Drain atomically detaches and returns the accumulated records for flushing,
// immediately resetting the internal slice with a fresh pre-allocated buffer.
// This ensures that the buffer lock is held for only a few nanoseconds.
func (b *BatchBuffer) Drain() []*TelemetryRecord {
	b.mu.Lock()
	if len(b.records) == 0 {
		b.mu.Unlock()
		return nil
	}

	detached := b.records
	b.records = make([]*TelemetryRecord, 0, b.batchCapacity)
	b.mu.Unlock()

	return detached
}

// Size returns the current number of queued records.
func (b *BatchBuffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}
