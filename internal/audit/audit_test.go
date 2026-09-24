package audit_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/audit"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

type memoryAuditStorage struct {
	mu      sync.RWMutex
	records map[string][]audit.ChainedRecord
}

func newMemoryAuditStorage() *memoryAuditStorage {
	return &memoryAuditStorage{
		records: make(map[string][]audit.ChainedRecord),
	}
}

func (m *memoryAuditStorage) GetChainedEntriesForAccount(ctx context.Context, accountID string) ([]audit.ChainedRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.records[accountID]
	res := make([]audit.ChainedRecord, len(list))
	copy(res, list)
	return res, nil
}

func (m *memoryAuditStorage) ListAccountIDs(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ids []string
	for id := range m.records {
		ids = append(ids, id)
	}
	return ids, nil
}

func (m *memoryAuditStorage) AddRecord(accountID string, rec audit.ChainedRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[accountID] = append(m.records[accountID], rec)
}

func buildValidChain(accountID string, count int) []audit.ChainedRecord {
	records := make([]audit.ChainedRecord, count)
	prevHash := audit.GenesisHash(accountID)
	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	for i := 0; i < count; i++ {
		seq := int64(i + 1)
		txID := "tx-" + string(rune('A'+i))
		amount := int64((i + 1) * 1000)
		t := baseTime.Add(time.Duration(i) * time.Minute)

		currHash := audit.ComputeHash(prevHash, txID, amount, t)
		rec := audit.ChainedRecord{
			ID:            "e-" + string(rune('A'+i)),
			TransactionID: txID,
			AccountID:     accountID,
			Direction:     "DEBIT",
			Amount:        amount,
			PrevHash:      prevHash,
			CurrentHash:   currHash,
			SequenceNum:   seq,
			CreatedAt:     t,
		}
		records[i] = rec
		prevHash = currHash
	}
	return records
}

func TestAuditor_ValidChain_Passes(t *testing.T) {
	ctx := context.Background()
	storage := newMemoryAuditStorage()
	auditor := audit.NewAuditor(storage)

	accID := "acc-clean"
	chain := buildValidChain(accID, 5)
	for _, r := range chain {
		storage.AddRecord(accID, r)
	}

	result, err := auditor.AuditAccount(ctx, accID)
	if err != nil {
		t.Fatalf("temiz zincir denetimi başarısız olmamalıydı: %v", err)
	}
	if result.IsTampered {
		t.Fatalf("temiz zincir bozulmuş olarak işaretlenmemeli")
	}
	if result.TotalEntries != 5 {
		t.Errorf("5 girdi denetlenmeliydi, denetlenen: %d", result.TotalEntries)
	}
}

func TestAuditor_TamperedAmount_Detected(t *testing.T) {
	ctx := context.Background()
	storage := newMemoryAuditStorage()
	auditor := audit.NewAuditor(storage)

	accID := "acc-tampered-amt"
	chain := buildValidChain(accID, 4)

	// Simülasyon: Doğrudan SQL ile 3. girdinin tutarı 3000'den 99999'a çekilmiş!
	chain[2].Amount = 99999

	for _, r := range chain {
		storage.AddRecord(accID, r)
	}

	result, err := auditor.AuditAccount(ctx, accID)
	if err == nil {
		t.Fatal("tutar manipülasyonu yakalanamadı!")
	}
	if !result.IsTampered {
		t.Fatal("result.IsTampered true olmalıydı")
	}

	var tamperedErr *audit.DataTamperedError
	if !errors.As(err, &tamperedErr) {
		t.Fatalf("DataTamperedError bekleniyordu, alınan: %T (%v)", err, err)
	}

	if tamperedErr.RecordID != chain[2].ID {
		t.Errorf("bozuk kayıt ID e-C olmalıydı, bulunan: %s", tamperedErr.RecordID)
	}
	if tamperedErr.SequenceNum != 3 {
		t.Errorf("bozuk sıra no 3 olmalıydı, bulunan: %d", tamperedErr.SequenceNum)
	}
	if !errors.Is(err, domain.ErrDataTampered) {
		t.Errorf("domain.ErrDataTampered ile eşleşmeliydi")
	}
}

func TestAuditor_TamperedTimestamp_Detected(t *testing.T) {
	ctx := context.Background()
	storage := newMemoryAuditStorage()
	auditor := audit.NewAuditor(storage)

	accID := "acc-tampered-time"
	chain := buildValidChain(accID, 3)

	// Simülasyon: Doğrudan SQL ile 2. girdinin oluşturulma zamanı değiştirilmiş!
	chain[1].CreatedAt = chain[1].CreatedAt.Add(5 * time.Hour)

	for _, r := range chain {
		storage.AddRecord(accID, r)
	}

	_, err := auditor.AuditAccount(ctx, accID)
	if err == nil {
		t.Fatal("zaman damgası manipülasyonu yakalanamadı!")
	}
	var tamperedErr *audit.DataTamperedError
	if !errors.As(err, &tamperedErr) {
		t.Fatalf("DataTamperedError bekleniyordu, alınan: %v", err)
	}
	if tamperedErr.RecordID != chain[1].ID {
		t.Errorf("beklenen manipüle kayıt %s, bulunan: %s", chain[1].ID, tamperedErr.RecordID)
	}
}

func TestAuditor_InjectedOrDeletedRow_Detected(t *testing.T) {
	ctx := context.Background()
	storage := newMemoryAuditStorage()
	auditor := audit.NewAuditor(storage)

	accID := "acc-deleted-row"
	chain := buildValidChain(accID, 4)

	// Simülasyon: 2. kayıt veritabanından silinmiş (Sequence 1'den 3'e atlıyor)
	storage.AddRecord(accID, chain[0])
	// chain[1] atlandı
	storage.AddRecord(accID, chain[2])
	storage.AddRecord(accID, chain[3])

	_, err := auditor.AuditAccount(ctx, accID)
	if err == nil {
		t.Fatal("satır silinmesi / kopukluk yakalanamadı!")
	}
	var tamperedErr *audit.DataTamperedError
	if !errors.As(err, &tamperedErr) {
		t.Fatalf("DataTamperedError bekleniyordu, alınan: %v", err)
	}
}

func TestAuditWorker_PeriodicExecutionAndAlert(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage := newMemoryAuditStorage()
	accClean := "acc-clean"
	accBad := "acc-bad"

	for _, r := range buildValidChain(accClean, 2) {
		storage.AddRecord(accClean, r)
	}

	badChain := buildValidChain(accBad, 2)
	badChain[1].Amount = 8888 // Bozuk
	for _, r := range badChain {
		storage.AddRecord(accBad, r)
	}

	alertTriggered := false
	var alertMu sync.Mutex

	cfg := audit.WorkerConfig{
		Interval: 20 * time.Millisecond,
	}

	worker := audit.NewAuditWorker(cfg, audit.NewAuditor(storage), func(report *audit.LedgerAuditReport) {
		alertMu.Lock()
		defer alertMu.Unlock()
		alertTriggered = true
	})

	worker.Start(ctx)
	defer worker.Stop()

	// Worker'ın denetimi çalıştırması için bekle
	time.Sleep(60 * time.Millisecond)

	alertMu.Lock()
	triggered := alertTriggered
	alertMu.Unlock()

	if !triggered {
		t.Fatal("AuditWorker bozuk defter için alarm tetiklemedi!")
	}

	rep := worker.GetLastReport()
	if rep == nil || rep.IsClean {
		t.Fatal("rapor bozuk (IsClean=false) olmalıydı")
	}
	if len(rep.TamperedAccounts) != 1 || rep.TamperedAccounts[0] != accBad {
		t.Errorf("bozuk hesap acc-bad olarak listelenmeliydi: %v", rep.TamperedAccounts)
	}
}
