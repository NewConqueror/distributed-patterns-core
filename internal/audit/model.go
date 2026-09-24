package audit

import (
	"errors"
	"fmt"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
)

// ChainedRecord, veritabanındaki hash zinciri sütunlarını taşıyan denetim kaydıdır.
type ChainedRecord struct {
	ID            string
	TransactionID string
	AccountID     string
	Direction     string
	Amount        int64
	PrevHash      string
	CurrentHash   string
	SequenceNum   int64
	CreatedAt     time.Time
}

// DataTamperedError (DataTamperedException), defterde kriptografik zincirin koptuğunu
// veya verinin doğrudan SQL ile değiştirildiğini tam lokasyonla bildiren hata türüdür.
type DataTamperedError struct {
	AccountID    string
	RecordID     string
	SequenceNum  int64
	ExpectedHash string
	ActualHash   string
	Reason       string
}

func (e *DataTamperedError) Error() string {
	return fmt.Sprintf("DataTamperedException: Kriptografik denetim zinciri bozuldu! "+
		"Hesap: %s, Kayıt: %s, Sıra: %d, Beklenen Hash: %s, Kayıtlı Hash: %s, Neden: %s",
		e.AccountID, e.RecordID, e.SequenceNum, e.ExpectedHash, e.ActualHash, e.Reason)
}

func (e *DataTamperedError) Is(target error) bool {
	return target == domain.ErrDataTampered || errors.Is(target, domain.ErrDataTampered)
}

// AuditResult, tek bir hesap için denetim sonucu
type AuditResult struct {
	AccountID     string
	TotalEntries  int64
	LastHash      string
	IsTampered    bool
	TamperedError *DataTamperedError
	AuditedAt     time.Time
}

// LedgerAuditReport, tüm defterin denetim raporu
type LedgerAuditReport struct {
	TotalAccountsAudited int
	TotalEntriesAudited  int64
	TamperedAccounts     []string
	Errors               []*DataTamperedError
	IsClean              bool
	StartedAt            time.Time
	FinishedAt           time.Time
	Duration             time.Duration
}
