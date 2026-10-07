package snapshot

import (
	"context"
	"fmt"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

// Storage, Snapshotting Engine için gerekli veri erişim sözleşmesidir.
type Storage interface {
	// SaveSnapshot, yeni bir snapshot kaydeder.
	SaveSnapshot(ctx context.Context, snapshot AccountSnapshot) error

	// GetLatestSnapshot, bir hesaba ait en son oluşturulan snapshot'ı getirir (yoksa nil döner).
	GetLatestSnapshot(ctx context.Context, accountID string) (*AccountSnapshot, error)

	// GetEntriesAfter, belirtilen entry ID'sinden sonra eklenen hesap girdilerini sıralı getirir.
	// lastEntryID nil ise hesabın başlangıçtan itibaren tüm girdilerini getirir.
	GetEntriesAfter(ctx context.Context, accountID string, lastEntryID *string) ([]domain.Entry, error)

	// CountEntriesAfter, son snapshot'tan sonra eklenen girdi adedini döner.
	CountEntriesAfter(ctx context.Context, accountID string, lastEntryID *string) (int64, error)
}

// Engine, snapshot tabanlı hızlı anlık bakiye hesaplama motorudur.
type Engine struct {
	storage Storage
}

// NewEngine, yeni bir Snapshot Engine oluşturur.
func NewEngine(storage Storage) *Engine {
	return &Engine{
		storage: storage,
	}
}

// CalculateCurrentBalance, anlık bakiye formülünü uygulayarak hesabın bakiyesini hesaplar:
// Anlık Bakiye = Snapshot Tutarı + sum(Snapshot Sonrası Entry'ler)
//
// Milyonlarca kaydın her defasında baştan taranmasını önler; O(1) snapshot + O(delta) entry taramasıyla çalışır.
func (e *Engine) CalculateCurrentBalance(ctx context.Context, account domain.Account) (domain.Money, *string, int64, error) {
	// 1. En son snapshot'ı getir
	latestSnap, err := e.storage.GetLatestSnapshot(ctx, account.ID())
	if err != nil {
		return domain.Money{}, nil, 0, fmt.Errorf("en son snapshot getirilemedi: %w", err)
	}

	var currentBalance domain.Money
	var lastEntryID *string
	var totalProcessedCount int64

	if latestSnap != nil {
		// Snapshot mevcut: Başlangıç bakiyesi = Snapshot Tutarı
		currentBalance = latestSnap.Balance
		lastEntryID = latestSnap.LastEntryID
		totalProcessedCount = latestSnap.EntryCount
	} else {
		// Snapshot henüz yok: Başlangıç bakiyesi = 0
		zeroMoney, err := domain.NewZeroMoney(account.Currency())
		if err != nil {
			return domain.Money{}, nil, 0, err
		}
		currentBalance = zeroMoney
		lastEntryID = nil
		totalProcessedCount = 0
	}

	// 2. Snapshot sonrası eklenen (delta) girdileri getir
	deltaEntries, err := e.storage.GetEntriesAfter(ctx, account.ID(), lastEntryID)
	if err != nil {
		return domain.Money{}, nil, 0, fmt.Errorf("delta girdiler sorgulanamadı: %w", err)
	}

	// 3. Delta girdileri normal bakiye yönüne göre ekle/çıkar:
	// Normal yönle aynıysa: +tutar
	// Normal yöne zıtsa: -tutar
	normalDir := account.AccountType().NormalBalance()
	for _, entry := range deltaEntries {
		if entry.Direction() == normalDir {
			currentBalance, err = currentBalance.Add(entry.Money())
		} else {
			currentBalance, err = currentBalance.Sub(entry.Money())
		}
		if err != nil {
			return domain.Money{}, nil, 0, fmt.Errorf("delta girdi bakiye eklenirken taşma/hata: %w", err)
		}

		entryID := entry.ID()
		lastEntryID = &entryID
		totalProcessedCount++
	}

	return currentBalance, lastEntryID, totalProcessedCount, nil
}
