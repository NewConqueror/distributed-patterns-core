package snapshot_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/snapshot"
)

// memorySnapshotStorage, in-memory test deposu
type memorySnapshotStorage struct {
	mu        sync.RWMutex
	snapshots map[string][]*snapshot.AccountSnapshot // accountID -> snapshots
	entries   map[string][]domain.Entry              // accountID -> entries
}

func newMemorySnapshotStorage() *memorySnapshotStorage {
	return &memorySnapshotStorage{
		snapshots: make(map[string][]*snapshot.AccountSnapshot),
		entries:   make(map[string][]domain.Entry),
	}
}

func (m *memorySnapshotStorage) SaveSnapshot(ctx context.Context, s snapshot.AccountSnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots[s.AccountID] = append(m.snapshots[s.AccountID], &s)
	return nil
}

func (m *memorySnapshotStorage) GetLatestSnapshot(ctx context.Context, accountID string) (*snapshot.AccountSnapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.snapshots[accountID]
	if len(list) == 0 {
		return nil, nil
	}
	return list[len(list)-1], nil
}

func (m *memorySnapshotStorage) GetEntriesAfter(ctx context.Context, accountID string, lastEntryID *string) ([]domain.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	all := m.entries[accountID]
	if lastEntryID == nil {
		result := make([]domain.Entry, len(all))
		copy(result, all)
		return result, nil
	}

	foundIdx := -1
	for i, e := range all {
		if e.ID() == *lastEntryID {
			foundIdx = i
			break
		}
	}

	if foundIdx == -1 {
		// Bulunamadıysa hepsi
		result := make([]domain.Entry, len(all))
		copy(result, all)
		return result, nil
	}

	delta := all[foundIdx+1:]
	result := make([]domain.Entry, len(delta))
	copy(result, delta)
	return result, nil
}

func (m *memorySnapshotStorage) CountEntriesAfter(ctx context.Context, accountID string, lastEntryID *string) (int64, error) {
	entries, err := m.GetEntriesAfter(ctx, accountID, lastEntryID)
	if err != nil {
		return 0, err
	}
	return int64(len(entries)), nil
}

func (m *memorySnapshotStorage) AddEntry(accountID string, entry domain.Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[accountID] = append(m.entries[accountID], entry)
}

// memoryAccountProvider
type memoryAccountProvider struct {
	accounts map[string]domain.Account
}

func (p *memoryAccountProvider) GetAccountByID(ctx context.Context, id string) (domain.Account, error) {
	acc, ok := p.accounts[id]
	if !ok {
		return domain.Account{}, fmt.Errorf("hesap bulunamadı: %s", id)
	}
	return acc, nil
}

func (p *memoryAccountProvider) ListAccounts(ctx context.Context) ([]domain.Account, error) {
	var list []domain.Account
	for _, a := range p.accounts {
		list = append(list, a)
	}
	return list, nil
}

func TestSnapshotEngine_FormulaCalculation(t *testing.T) {
	ctx := context.Background()
	storage := newMemorySnapshotStorage()
	engine := snapshot.NewEngine(storage)

	acc, err := domain.NewAccount("acc-cash", "Kasa Hesabı", domain.AccountTypeAsset, domain.USD)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Snapshot olmadan: 3 adet girdi ekle (+100, +50, -30 = 120 USD)
	e1, _ := domain.NewEntry("e1", "acc-cash", domain.DirectionDebit, domain.MustNewMoney(10000, domain.USD))
	e2, _ := domain.NewEntry("e2", "acc-cash", domain.DirectionDebit, domain.MustNewMoney(5000, domain.USD))
	e3, _ := domain.NewEntry("e3", "acc-cash", domain.DirectionCredit, domain.MustNewMoney(3000, domain.USD))

	storage.AddEntry("acc-cash", e1)
	storage.AddEntry("acc-cash", e2)
	storage.AddEntry("acc-cash", e3)

	bal1, lastID1, count1, err := engine.CalculateCurrentBalance(ctx, acc)
	if err != nil {
		t.Fatalf("hesaplama hatası: %v", err)
	}
	if bal1.Amount() != 12000 {
		t.Errorf("beklenen bakiye 12000, alınan: %d", bal1.Amount())
	}
	if *lastID1 != "e3" {
		t.Errorf("beklenen lastEntryID e3, alınan: %s", *lastID1)
	}
	if count1 != 3 {
		t.Errorf("beklenen count 3, alınan: %d", count1)
	}

	// 2. e3 noktasında bir Snapshot oluştur (Bakiye: 12000, LastEntry: e3, Count: 3)
	snap1 := snapshot.NewAccountSnapshot("snap-1", "acc-cash", bal1, lastID1, count1, time.Now().UTC())
	if err := storage.SaveSnapshot(ctx, snap1); err != nil {
		t.Fatal(err)
	}

	// 3. Snapshot sonrasında yeni delta girdiler ekle: (+80, -20 = net +60 USD)
	e4, _ := domain.NewEntry("e4", "acc-cash", domain.DirectionDebit, domain.MustNewMoney(8000, domain.USD))
	e5, _ := domain.NewEntry("e5", "acc-cash", domain.DirectionCredit, domain.MustNewMoney(2000, domain.USD))
	storage.AddEntry("acc-cash", e4)
	storage.AddEntry("acc-cash", e5)

	// 4. Anlık Bakiye = Snapshot Tutarı (12000) + Delta (8000 - 2000 = 6000) = 18000 USD
	bal2, lastID2, count2, err := engine.CalculateCurrentBalance(ctx, acc)
	if err != nil {
		t.Fatalf("hesaplama hatası: %v", err)
	}
	if bal2.Amount() != 18000 {
		t.Errorf("beklenen anlık bakiye 18000 (12000 snapshot + 6000 delta), alınan: %d", bal2.Amount())
	}
	if *lastID2 != "e5" {
		t.Errorf("beklenen lastEntryID e5, alınan: %s", *lastID2)
	}
	if count2 != 5 {
		t.Errorf("beklenen toplam girdi 5, alınan: %d", count2)
	}
}

func TestSnapshotWorker_AutomaticPipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage := newMemorySnapshotStorage()
	acc, _ := domain.NewAccount("acc-test", "Test", domain.AccountTypeAsset, domain.USD)
	provider := &memoryAccountProvider{
		accounts: map[string]domain.Account{
			"acc-test": acc,
		},
	}

	// Her 3 işlemde bir snapshot alsın (ThresholdN = 3)
	cfg := snapshot.WorkerConfig{
		ThresholdN:   3,
		CronInterval: 50 * time.Millisecond,
		QueueSize:    50,
	}

	worker := snapshot.NewSnapshotWorker(cfg, storage, provider)
	worker.Start(ctx)
	defer worker.Stop()

	// 1. İki işlem ekle (eşik 3, henüz snapshot alınmamalı)
	e1, _ := domain.NewEntry("e1", "acc-test", domain.DirectionDebit, domain.MustNewMoney(1000, domain.USD))
	e2, _ := domain.NewEntry("e2", "acc-test", domain.DirectionDebit, domain.MustNewMoney(2000, domain.USD))
	storage.AddEntry("acc-test", e1)
	storage.AddEntry("acc-test", e2)

	worker.NotifyAccountActivity("acc-test")
	time.Sleep(20 * time.Millisecond)

	snap, _ := storage.GetLatestSnapshot(ctx, "acc-test")
	if snap != nil {
		t.Fatalf("eşik (3) aşılmadan snapshot alınmamalıydı")
	}

	// 2. 3. işlemi ekle -> Eşik sağlandı -> Snapshot alınmalı!
	e3, _ := domain.NewEntry("e3", "acc-test", domain.DirectionDebit, domain.MustNewMoney(3000, domain.USD))
	storage.AddEntry("acc-test", e3)

	worker.NotifyAccountActivity("acc-test")

	// Worker'ın asenkron işlemesi için bekle
	var foundSnap *snapshot.AccountSnapshot
	for i := 0; i < 20; i++ {
		time.Sleep(15 * time.Millisecond)
		foundSnap, _ = storage.GetLatestSnapshot(ctx, "acc-test")
		if foundSnap != nil {
			break
		}
	}

	if foundSnap == nil {
		t.Fatalf("3 işlem sonrasında asenkron snapshot oluşturulmadı!")
	}
	if foundSnap.Balance.Amount() != 6000 {
		t.Errorf("snapshot bakiyesi 6000 olmalıydı, alınan: %d", foundSnap.Balance.Amount())
	}
	if foundSnap.EntryCount != 3 {
		t.Errorf("snapshot entry_count 3 olmalıydı, alınan: %d", foundSnap.EntryCount)
	}
}
