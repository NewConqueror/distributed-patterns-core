package snapshot

import (
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

// AccountSnapshot, bir hesabın belirli bir andaki bakiye ve hareket kontrol noktasını (checkpoint) temsil eder.
type AccountSnapshot struct {
	ID          string       // Snapshot benzersiz kimliği
	AccountID   string       // İlgili hesap ID'si
	Balance     domain.Money // Snapshot anındaki bakiye
	LastEntryID *string      // Bu snapshot'a dahil edilen en son girdinin (entry) ID'si (nil olabilir)
	EntryCount  int64        // Snapshot anına kadar olan toplam girdi adedi
	SnapshotAt  time.Time    // Snapshot alınma zamanı
}

// NewAccountSnapshot, yeni bir AccountSnapshot nesnesi oluşturur.
func NewAccountSnapshot(id, accountID string, balance domain.Money, lastEntryID *string, entryCount int64, snapshotAt time.Time) AccountSnapshot {
	if snapshotAt.IsZero() {
		snapshotAt = time.Now().UTC()
	}
	return AccountSnapshot{
		ID:          id,
		AccountID:   accountID,
		Balance:     balance,
		LastEntryID: lastEntryID,
		EntryCount:  entryCount,
		SnapshotAt:  snapshotAt,
	}
}
