package snapshot

import (
	"context"
	"fmt"
	"sync"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

// AccountProvider, worker'ın hesap bilgilerini alabilmesi için arayüz.
type AccountProvider interface {
	GetAccountByID(ctx context.Context, id string) (domain.Account, error)
	ListAccounts(ctx context.Context) ([]domain.Account, error)
}

// WorkerConfig, SnapshotWorker çalışma parametreleri
type WorkerConfig struct {
	ThresholdN   int64         // Her N işlemde bir snapshot alınır (örn. 10 veya 100)
	CronInterval time.Duration // Periyodik kontrol aralığı (örn. 10 saniye)
	QueueSize    int           // Asenkron bildirim kanalı tampon boyutu
}

// DefaultWorkerConfig, varsayılan yapılandırma
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		ThresholdN:   10,
		CronInterval: 5 * time.Second,
		QueueSize:    500,
	}
}

// SnapshotWorker, arka planda çalışan ve her N işlemde bir asenkron hesap snapshot'ı alan pipeline'dır.
type SnapshotWorker struct {
	cfg             WorkerConfig
	storage         Storage
	accountProvider AccountProvider
	engine          *Engine
	notifyChan      chan string
	stopChan        chan struct{}
	wg              sync.WaitGroup
	running         bool
	mu              sync.Mutex
}

// NewSnapshotWorker, yeni bir SnapshotWorker başlatır.
func NewSnapshotWorker(cfg WorkerConfig, storage Storage, accountProvider AccountProvider) *SnapshotWorker {
	if cfg.ThresholdN <= 0 {
		cfg.ThresholdN = 10
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 500
	}

	return &SnapshotWorker{
		cfg:             cfg,
		storage:         storage,
		accountProvider: accountProvider,
		engine:          NewEngine(storage),
		notifyChan:      make(chan string, cfg.QueueSize),
		stopChan:        make(chan struct{}),
	}
}

// Start, arka plan worker ve periyodik cron döngüsünü başlatır.
func (w *SnapshotWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()

	w.wg.Add(1)
	go w.runLoop(ctx)
}

// NotifyAccountActivity, bir hesaba yeni hareketler eklendiğinde worker'a asenkron bildirim gönderir.
func (w *SnapshotWorker) NotifyAccountActivity(accountID string) {
	select {
	case w.notifyChan <- accountID:
	default:
		// Tampon doluysa drop et; periyodik cron yakalayacaktır (non-blocking)
	}
}

// Stop, worker'ı zarifçe (graceful) durdurur.
func (w *SnapshotWorker) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	close(w.stopChan)
	w.mu.Unlock()

	w.wg.Wait()
}

// runLoop, kanal bildirimlerini ve periyodik cron tiklerini dinler.
func (w *SnapshotWorker) runLoop(ctx context.Context) {
	defer w.wg.Done()

	var ticker *time.Ticker
	if w.cfg.CronInterval > 0 {
		ticker = time.NewTicker(w.cfg.CronInterval)
		defer ticker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopChan:
			return

		case accountID := <-w.notifyChan:
			_ = w.evaluateAndSnapshotAccount(ctx, accountID)

		case <-ticker.C:
			// Periyodik kontrol: tüm hesapları tara ve eşiği aşanlar için snapshot al
			if w.accountProvider != nil {
				accounts, err := w.accountProvider.ListAccounts(ctx)
				if err == nil {
					for _, acc := range accounts {
						_ = w.evaluateAndSnapshotAccount(ctx, acc.ID())
					}
				}
			}
		}
	}
}

// evaluateAndSnapshotAccount, hesabın son snapshot'tan sonra N veya daha fazla girdi alıp almadığını kontrol eder.
// Eğer ThresholdN aşılmışsa yeni bir snapshot kaydeder.
func (w *SnapshotWorker) evaluateAndSnapshotAccount(ctx context.Context, accountID string) error {
	latestSnap, err := w.storage.GetLatestSnapshot(ctx, accountID)
	if err != nil {
		return err
	}

	var lastEntryID *string
	if latestSnap != nil {
		lastEntryID = latestSnap.LastEntryID
	}

	deltaCount, err := w.storage.CountEntriesAfter(ctx, accountID, lastEntryID)
	if err != nil {
		return err
	}

	// Eğer delta girdi adedi N eşiğinden küçükse snapshot almaya gerek yok
	if deltaCount < w.cfg.ThresholdN {
		return nil
	}

	// Eşik aşıldı -> Yeni bir snapshot oluştur
	acc, err := w.accountProvider.GetAccountByID(ctx, accountID)
	if err != nil {
		return fmt.Errorf("hesap bulunamadı: %w", err)
	}

	balance, newLastEntryID, totalCount, err := w.engine.CalculateCurrentBalance(ctx, acc)
	if err != nil {
		return fmt.Errorf("snapshot bakiyesi hesaplanamadı: %w", err)
	}

	snapID := fmt.Sprintf("snap-%s-%d", accountID, time.Now().UnixNano())
	newSnap := NewAccountSnapshot(snapID, accountID, balance, newLastEntryID, totalCount, time.Now().UTC())

	if err := w.storage.SaveSnapshot(ctx, newSnap); err != nil {
		return fmt.Errorf("yeni snapshot kaydedilemedi: %w", err)
	}

	return nil
}

// ForceSnapshotNow, testler veya acil durumlar için hemen snapshot almayı sağlar.
func (w *SnapshotWorker) ForceSnapshotNow(ctx context.Context, accountID string) error {
	acc, err := w.accountProvider.GetAccountByID(ctx, accountID)
	if err != nil {
		return err
	}

	balance, lastEntryID, totalCount, err := w.engine.CalculateCurrentBalance(ctx, acc)
	if err != nil {
		return err
	}

	snapID := fmt.Sprintf("snap-%s-%d", accountID, time.Now().UnixNano())
	newSnap := NewAccountSnapshot(snapID, accountID, balance, lastEntryID, totalCount, time.Now().UTC())

	return w.storage.SaveSnapshot(ctx, newSnap)
}
