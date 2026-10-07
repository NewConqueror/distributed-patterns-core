package persistence

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
)

// mockRepository tracks flushed batches in memory for testing BatchEngine trigger policies.
type mockRepository struct {
	mu           sync.Mutex
	batches      [][]*TelemetryRecord
	totalRecords atomic.Int64
	flushDelay   time.Duration
}

func newMockRepository() *mockRepository {
	return &mockRepository{
		batches: make([][]*TelemetryRecord, 0),
	}
}

func (m *mockRepository) InitSchema(ctx context.Context) error {
	return nil
}

func (m *mockRepository) BulkInsert(ctx context.Context, records []*TelemetryRecord) error {
	if m.flushDelay > 0 {
		time.Sleep(m.flushDelay)
	}

	m.mu.Lock()
	m.batches = append(m.batches, records)
	m.mu.Unlock()

	m.totalRecords.Add(int64(len(records)))
	return nil
}

func (m *mockRepository) Close() {}

func (m *mockRepository) Batches() [][]*TelemetryRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([][]*TelemetryRecord, len(m.batches))
	copy(res, m.batches)
	return res
}

func TestBatchBuffer_PushAndDrain(t *testing.T) {
	buf := NewBatchBuffer(5)

	if buf.Size() != 0 {
		t.Fatalf("Expected empty buffer, got size %d", buf.Size())
	}

	// Push 4 records (below threshold)
	for i := 0; i < 4; i++ {
		ping := &telemetryv1.LocationPing{
			DriverId:  fmt.Sprintf("driver-%d", i),
			Latitude:  41.0,
			Longitude: 29.0,
			Timestamp: time.Now().UnixMilli(),
		}
		count, met := buf.Push(ping)
		if count != i+1 {
			t.Errorf("Expected count %d, got %d", i+1, count)
		}
		if met {
			t.Errorf("Threshold should not be met at count %d (cap: 5)", count)
		}
	}

	// Push 5th record (meets threshold)
	ping := &telemetryv1.LocationPing{
		DriverId:  "driver-5",
		Latitude:  41.0,
		Longitude: 29.0,
		Timestamp: time.Now().UnixMilli(),
	}
	count, met := buf.Push(ping)
	if count != 5 || !met {
		t.Fatalf("Expected count 5 and thresholdMet=true, got count=%d, met=%v", count, met)
	}

	// Drain
	detached := buf.Drain()
	if len(detached) != 5 {
		t.Fatalf("Expected 5 detached records, got %d", len(detached))
	}
	if buf.Size() != 0 {
		t.Fatalf("Buffer should be empty after drain, got %d", buf.Size())
	}

	// Drain empty buffer
	empty := buf.Drain()
	if empty != nil {
		t.Fatalf("Expected nil on empty drain, got slice with len %d", len(empty))
	}
}

func TestBatchEngine_SizeTrigger(t *testing.T) {
	mockRepo := newMockRepository()
	cfg := BatchConfig{
		BatchSize:     10,              // Trigger at 10 items
		FlushInterval: 5 * time.Second, // Long timer so only size triggers
		QueueCapacity: 10,
		BulkMode:      BulkModeCopy,
	}

	engine := NewBatchEngine(mockRepo, cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	engine.Start(ctx)
	defer engine.Stop()

	// Push 10 pings
	now := time.Now().UnixMilli()
	for i := 0; i < 10; i++ {
		ping := &telemetryv1.LocationPing{
			DriverId:  fmt.Sprintf("driver-%d", i),
			Latitude:  41.0 + float64(i)*0.01,
			Longitude: 29.0,
			Timestamp: now,
		}
		if err := engine.ProcessPing(ctx, ping); err != nil {
			t.Fatalf("ProcessPing failed: %v", err)
		}
	}

	// Wait for worker to flush size-triggered batch
	deadline := time.Now().Add(1 * time.Second)
	for mockRepo.totalRecords.Load() < 10 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if mockRepo.totalRecords.Load() != 10 {
		t.Fatalf("Expected 10 flushed records via size trigger, got %d", mockRepo.totalRecords.Load())
	}

	batches := mockRepo.Batches()
	if len(batches) != 1 {
		t.Fatalf("Expected 1 batch, got %d", len(batches))
	}
	if len(batches[0]) != 10 {
		t.Fatalf("Expected batch of size 10, got %d", len(batches[0]))
	}
}

func TestBatchEngine_TimeTrigger(t *testing.T) {
	mockRepo := newMockRepository()
	cfg := BatchConfig{
		BatchSize:     100,                     // Very high so size won't trigger
		FlushInterval: 150 * time.Millisecond, // Fast ticker
		QueueCapacity: 10,
		BulkMode:      BulkModeCopy,
	}

	engine := NewBatchEngine(mockRepo, cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	engine.Start(ctx)
	defer engine.Stop()

	// Push only 4 pings (well below BatchSize of 100)
	now := time.Now().UnixMilli()
	for i := 0; i < 4; i++ {
		ping := &telemetryv1.LocationPing{
			DriverId:  fmt.Sprintf("driver-%d", i),
			Latitude:  41.0,
			Longitude: 29.0,
			Timestamp: now,
		}
		if err := engine.ProcessPing(ctx, ping); err != nil {
			t.Fatalf("ProcessPing failed: %v", err)
		}
	}

	// Wait for ticker (150ms) to fire and flush
	deadline := time.Now().Add(1 * time.Second)
	for mockRepo.totalRecords.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(30 * time.Millisecond)
	}

	if mockRepo.totalRecords.Load() != 4 {
		t.Fatalf("Expected 4 records flushed via time trigger, got %d", mockRepo.totalRecords.Load())
	}

	batches := mockRepo.Batches()
	if len(batches) != 1 {
		t.Fatalf("Expected 1 batch, got %d", len(batches))
	}
}

func TestBatchEngine_GracefulShutdownFlush(t *testing.T) {
	mockRepo := newMockRepository()
	cfg := BatchConfig{
		BatchSize:     500,             // Size won't trigger
		FlushInterval: 1 * time.Minute, // Timer won't trigger
		QueueCapacity: 10,
		BulkMode:      BulkModeCopy,
	}

	engine := NewBatchEngine(mockRepo, cfg, nil)
	ctx := context.Background()

	engine.Start(ctx)

	// Push 7 pings
	now := time.Now().UnixMilli()
	for i := 0; i < 7; i++ {
		ping := &telemetryv1.LocationPing{
			DriverId:  fmt.Sprintf("driver-%d", i),
			Latitude:  41.0,
			Longitude: 29.0,
			Timestamp: now,
		}
		_ = engine.ProcessPing(ctx, ping)
	}

	// Immediately call Stop()
	engine.Stop()

	// All 7 records must be flushed cleanly without data loss
	if mockRepo.totalRecords.Load() != 7 {
		t.Fatalf("Expected 7 records flushed on graceful shutdown, got %d", mockRepo.totalRecords.Load())
	}
}

func TestPostgresRepository_LiveIntegration(t *testing.T) {
	pgCfg := DefaultPostgresConfig()
	batchCfg := DefaultBatchConfig()

	repo, err := NewPostgresRepository(pgCfg, batchCfg, nil)
	if err != nil {
		t.Skipf("PostgreSQL not accessible at %s:%d, skipping live test: %v", pgCfg.Host, pgCfg.Port, err)
		return
	}
	defer repo.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize schema
	if err := repo.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema failed: %v", err)
	}

	// Test 1: Bulk Insert using COPY
	now := time.Now()
	testBatchCopy := []*TelemetryRecord{
		{
			DriverID:   "test-copy-driver-1",
			Latitude:   41.0082,
			Longitude:  28.9784,
			Bearing:    45.0,
			Speed:      50.0,
			RecordedAt: now,
		},
		{
			DriverID:   "test-copy-driver-2",
			Latitude:   41.0370,
			Longitude:  28.9850,
			Bearing:    90.0,
			Speed:      60.0,
			RecordedAt: now,
		},
	}

	repo.batchCfg.BulkMode = BulkModeCopy
	if err := repo.BulkInsert(ctx, testBatchCopy); err != nil {
		t.Fatalf("BulkInsert COPY failed: %v", err)
	}

	// Test 2: Bulk Insert using Multi-row INSERT
	testBatchMulti := []*TelemetryRecord{
		{
			DriverID:   "test-multi-driver-1",
			Latitude:   40.9910,
			Longitude:  29.0250,
			Bearing:    180.0,
			Speed:      40.0,
			RecordedAt: now,
		},
	}

	repo.batchCfg.BulkMode = BulkModeMultiInsert
	if err := repo.BulkInsert(ctx, testBatchMulti); err != nil {
		t.Fatalf("BulkInsert Multi-row failed: %v", err)
	}

	// Verify insertion in database
	var count int
	err = repo.Pool().QueryRow(ctx,
		"SELECT count(*) FROM driver_locations WHERE driver_id IN ('test-copy-driver-1', 'test-copy-driver-2', 'test-multi-driver-1')",
	).Scan(&count)
	if err != nil {
		t.Fatalf("Failed querying inserted test records: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 records inserted in database, found %d", count)
	}

	// Cleanup test records
	_, _ = repo.Pool().Exec(ctx,
		"DELETE FROM driver_locations WHERE driver_id IN ('test-copy-driver-1', 'test-copy-driver-2', 'test-multi-driver-1')",
	)
}
