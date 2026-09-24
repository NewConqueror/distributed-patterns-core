package audit

import (
	"context"
	"log"
	"sync"
	"time"
)

// WorkerConfig, periyodik denetim worker'ının yapılandırması
type WorkerConfig struct {
	Interval time.Duration // Denetim periyodu (örn. 30 saniye veya 5 dakika)
}

// DefaultWorkerConfig, varsayılan denetim yapılandırması
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		Interval: 30 * time.Second,
	}
}

// AuditWorker, arka planda bağımsız çalışan ve defteri periyodik olarak tarayan denetim iş parçacığıdır.
type AuditWorker struct {
	cfg        WorkerConfig
	auditor    *Auditor
	stopChan   chan struct{}
	lastReport *LedgerAuditReport
	mu         sync.RWMutex
	wg         sync.WaitGroup
	running    bool
	onTampered func(report *LedgerAuditReport) // İhlal anında tetiklenen callback
}

// NewAuditWorker, yeni bir AuditWorker örneği başlatır.
func NewAuditWorker(cfg WorkerConfig, auditor *Auditor, onTampered func(report *LedgerAuditReport)) *AuditWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	return &AuditWorker{
		cfg:        cfg,
		auditor:    auditor,
		stopChan:   make(chan struct{}),
		onTampered: onTampered,
	}
}

// Start, arka planda periyodik denetim döngüsünü başlatır.
func (w *AuditWorker) Start(ctx context.Context) {
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

// Stop, denetim iş parçacığını durdurur.
func (w *AuditWorker) Stop() {
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

// GetLastReport, en son tamamlanan denetim raporunu döner.
func (w *AuditWorker) GetLastReport() *LedgerAuditReport {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastReport
}

// RunAuditNow, hemen bir defter denetimi çalıştırır ve raporu döner.
func (w *AuditWorker) RunAuditNow(ctx context.Context) (*LedgerAuditReport, error) {
	report, err := w.auditor.AuditEntireLedger(ctx)
	w.mu.Lock()
	w.lastReport = report
	w.mu.Unlock()

	if report != nil && !report.IsClean && w.onTampered != nil {
		w.onTampered(report)
	}

	return report, err
}

func (w *AuditWorker) runLoop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopChan:
			return
		case <-ticker.C:
			report, err := w.auditor.AuditEntireLedger(ctx)
			if err != nil {
				log.Printf("[AUDITOR ERROR] Defter denetimi başarısız: %v\n", err)
			}

			w.mu.Lock()
			w.lastReport = report
			w.mu.Unlock()

			if report != nil && !report.IsClean {
				log.Printf("[CRITICAL ALARM] Defterde kriptografik manipülasyon tespit edildi! Bozuk hesaplar: %v\n", report.TamperedAccounts)
				if w.onTampered != nil {
					w.onTampered(report)
				}
			}
		}
	}
}
