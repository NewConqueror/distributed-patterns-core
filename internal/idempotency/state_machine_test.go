package idempotency_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/idempotency"
)

// memoryStorage, testler için in-memory thread-safe Storage implementasyonu
type memoryStorage struct {
	mu      sync.RWMutex
	records map[string]*idempotency.Record
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{
		records: make(map[string]*idempotency.Record),
	}
}

func (m *memoryStorage) CreateInFlight(ctx context.Context, key, requestHash string, lockedAt time.Time) (*idempotency.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if rec, exists := m.records[key]; exists {
		return rec, idempotency.ErrKeyAlreadyExists
	}

	rec := &idempotency.Record{
		Key:         key,
		RequestHash: requestHash,
		Status:      idempotency.StatusInFlight,
		LockedAt:    lockedAt,
		CreatedAt:   lockedAt,
		UpdatedAt:   lockedAt,
	}
	m.records[key] = rec
	return nil, nil
}

func (m *memoryStorage) Resolve(ctx context.Context, key string, responseCode int, responseBody []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, exists := m.records[key]
	if !exists {
		return idempotency.ErrKeyNotFound
	}
	rec.Status = idempotency.StatusResolved
	rec.ResponseCode = responseCode
	rec.ResponseBody = responseBody
	rec.UpdatedAt = time.Now().UTC()
	return nil
}

func (m *memoryStorage) Fail(ctx context.Context, key string, responseCode int, responseBody []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, exists := m.records[key]
	if !exists {
		return idempotency.ErrKeyNotFound
	}
	rec.Status = idempotency.StatusFailed
	rec.ResponseCode = responseCode
	rec.ResponseBody = responseBody
	rec.UpdatedAt = time.Now().UTC()
	return nil
}

func (m *memoryStorage) Get(ctx context.Context, key string) (*idempotency.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, exists := m.records[key]
	if !exists {
		return nil, idempotency.ErrKeyNotFound
	}
	return rec, nil
}

func TestStateMachine_Scenarios(t *testing.T) {
	ctx := context.Background()
	storage := newMemoryStorage()
	sm := idempotency.NewStateMachine(storage)

	key1 := "test-uuid-1"
	payload1 := []byte(`{"amount": 1000, "currency": "USD"}`)

	executedCount := 0
	sampleExecutor := func(ctx context.Context) (int, []byte, error) {
		executedCount++
		return 201, []byte(`{"status": "success", "tx_id": "tx-123"}`), nil
	}

	// -------------------------------------------------------------
	// Senaryo 1: Key ilk defa geliyorsa (IN_FLIGHT -> RESOLVED)
	// -------------------------------------------------------------
	res1, err := sm.Process(ctx, key1, payload1, sampleExecutor)
	if err != nil {
		t.Fatalf("Senaryo 1 başarısız: %v", err)
	}
	if res1.IsCached {
		t.Errorf("ilk istek önbellekten dönmemeli")
	}
	if res1.ResponseCode != 201 {
		t.Errorf("beklenen 201, alınan: %d", res1.ResponseCode)
	}
	if executedCount != 1 {
		t.Errorf("işlem tam 1 kez çalıştırılmalıydı, çalışan: %d", executedCount)
	}

	// -------------------------------------------------------------
	// Senaryo 2: Aynı key, aynı hash ile ve RESOLVED ise
	// DB işlemine girmeden doğrudan önbellekten dönmeli
	// -------------------------------------------------------------
	res2, err := sm.Process(ctx, key1, payload1, sampleExecutor)
	if err != nil {
		t.Fatalf("Senaryo 2 başarısız: %v", err)
	}
	if !res2.IsCached {
		t.Errorf("ikinci istek önbellekten (IsCached=true) dönmeliydi")
	}
	if res2.ResponseCode != 201 {
		t.Errorf("önbellek response_code 201 olmalı, alınan: %d", res2.ResponseCode)
	}
	if executedCount != 1 {
		t.Errorf("executor tekrar çağrılmamalıydı, çağrı sayısı: %d", executedCount)
	}

	// -------------------------------------------------------------
	// Senaryo 3: Aynı key, farklı hash ile geliyorsa
	// 422 Unprocessable Entity (Key Reuse Hatası)
	// -------------------------------------------------------------
	differentPayload := []byte(`{"amount": 9999, "currency": "USD"}`)
	_, err = sm.Process(ctx, key1, differentPayload, sampleExecutor)
	if err == nil || !errors.Is(err, idempotency.ErrIdempotencyKeyReuseWithDifferentPayload) {
		t.Fatalf("Senaryo 3 için ErrIdempotencyKeyReuseWithDifferentPayload bekleniyordu, alınan: %v", err)
	}

	// -------------------------------------------------------------
	// Senaryo 4: Aynı key, IN_FLIGHT durumundayken tekrar geliyorsa
	// 409 Conflict verilmeli
	// -------------------------------------------------------------
	keyInFlight := "test-uuid-inflight"
	payloadInFlight := []byte(`{"amount": 500}`)

	// Manuel olarak IN_FLIGHT durumuna sokuyoruz
	_, _ = storage.CreateInFlight(ctx, keyInFlight, idempotency.ComputeRequestHash(payloadInFlight), time.Now().UTC())

	_, err = sm.Process(ctx, keyInFlight, payloadInFlight, sampleExecutor)
	if err == nil || !errors.Is(err, idempotency.ErrIdempotencyKeyInFlight) {
		t.Fatalf("Senaryo 4 için ErrIdempotencyKeyInFlight bekleniyordu, alınan: %v", err)
	}
}
