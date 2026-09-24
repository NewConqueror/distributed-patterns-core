package audit

import (
	"context"
	"fmt"
	"time"
)

// Storage, denetleyicinin (Auditor) defteri tarayabilmesi için arayüz.
type Storage interface {
	// GetChainedEntriesForAccount, bir hesaba ait tüm girdileri sequence_num sırasıyla döner.
	GetChainedEntriesForAccount(ctx context.Context, accountID string) ([]ChainedRecord, error)

	// ListAccountIDs, sistemdeki tüm hesap kimliklerini döner.
	ListAccountIDs(ctx context.Context) ([]string, error)
}

// Auditor, defterdeki kriptografik hash zincirlerini doğrulayan bağımsız denetim motorudur.
type Auditor struct {
	storage Storage
}

// NewAuditor, yeni bir denetleyici başlatır.
func NewAuditor(storage Storage) *Auditor {
	return &Auditor{
		storage: storage,
	}
}

// AuditAccount, belirli bir hesaba ait hash zincirini baştan sona (Genesis'ten günümüze) doğrular.
// Herhangi bir manipülasyon tespit edilirse zincirin koptuğu satırı tam olarak raporlar.
func (a *Auditor) AuditAccount(ctx context.Context, accountID string) (*AuditResult, error) {
	records, err := a.storage.GetChainedEntriesForAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("hesap kayıtları getirilemedi: %w", err)
	}

	result := &AuditResult{
		AccountID:    accountID,
		TotalEntries: int64(len(records)),
		IsTampered:   false,
		AuditedAt:    time.Now().UTC(),
	}

	if len(records) == 0 {
		return result, nil
	}

	// Genesis hash: Hesap için tanımlı sabit seed hash
	expectedPrevHash := GenesisHash(accountID)

	for i, record := range records {
		expectedSeq := int64(i + 1)

		// 1. Sıra Numarası Kontrolü (Araya satır enjekte etme veya satır silme tespiti)
		if record.SequenceNum != expectedSeq {
			tamperedErr := &DataTamperedError{
				AccountID:    accountID,
				RecordID:     record.ID,
				SequenceNum:  record.SequenceNum,
				ExpectedHash: fmt.Sprintf("Sıra No: %d", expectedSeq),
				ActualHash:   fmt.Sprintf("Sıra No: %d", record.SequenceNum),
				Reason:       "Sıra numarası zinciri koptu! Araya satır enjekte edilmiş veya bir satır silinmiş.",
			}
			result.IsTampered = true
			result.TamperedError = tamperedErr
			return result, tamperedErr
		}

		// 2. Önceki Hash Bağlantı Kontrolü (Hash Chain Linkage)
		if record.PrevHash != expectedPrevHash {
			tamperedErr := &DataTamperedError{
				AccountID:    accountID,
				RecordID:     record.ID,
				SequenceNum:  record.SequenceNum,
				ExpectedHash: expectedPrevHash,
				ActualHash:   record.PrevHash,
				Reason:       "Önceki hash (prev_hash) zinciri uyuşmuyor! Zincir kırıldı.",
			}
			result.IsTampered = true
			result.TamperedError = tamperedErr
			return result, tamperedErr
		}

		// 3. Mevcut Hash Bütünlük Kontrolü (İçerik/Tutar Manipülasyon Tespiti)
		// Formül: Hash_n = SHA256(Hash_{n-1} + TransactionID + Amount + Timestamp)
		recalculatedHash := ComputeHash(record.PrevHash, record.TransactionID, record.Amount, record.CreatedAt)
		if record.CurrentHash != recalculatedHash {
			tamperedErr := &DataTamperedError{
				AccountID:    accountID,
				RecordID:     record.ID,
				SequenceNum:  record.SequenceNum,
				ExpectedHash: recalculatedHash,
				ActualHash:   record.CurrentHash,
				Reason:       "Kayıt verisi (tutar, zaman veya işlem ID) doğrudan SQL ile değiştirilmiş!",
			}
			result.IsTampered = true
			result.TamperedError = tamperedErr
			return result, tamperedErr
		}

		// Sonraki adım için beklenti güncelle
		expectedPrevHash = record.CurrentHash
		result.LastHash = record.CurrentHash
	}

	return result, nil
}

// AuditEntireLedger, tüm defteri baştan sona tarayarak tüm hesapların hash zincirlerini denetler.
func (a *Auditor) AuditEntireLedger(ctx context.Context) (*LedgerAuditReport, error) {
	started := time.Now().UTC()
	accountIDs, err := a.storage.ListAccountIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("hesap listesi alınamadı: %w", err)
	}

	report := &LedgerAuditReport{
		TotalAccountsAudited: len(accountIDs),
		TotalEntriesAudited:  0,
		TamperedAccounts:     nil,
		Errors:               nil,
		IsClean:              true,
		StartedAt:            started,
	}

	for _, accID := range accountIDs {
		res, err := a.AuditAccount(ctx, accID)
		if err != nil {
			report.IsClean = false
			report.TamperedAccounts = append(report.TamperedAccounts, accID)
			if res != nil && res.TamperedError != nil {
				report.Errors = append(report.Errors, res.TamperedError)
			}
		}
		if res != nil {
			report.TotalEntriesAudited += res.TotalEntries
		}
	}

	finished := time.Now().UTC()
	report.FinishedAt = finished
	report.Duration = finished.Sub(started)

	return report, nil
}
