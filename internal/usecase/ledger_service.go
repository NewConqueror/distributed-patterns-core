package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/concurrency"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/idempotency"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/repository"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/snapshot"
)

// EntryDTO, dış dünyadan gelen işlem girdisi formatıdır.
type EntryDTO struct {
	AccountID string `json:"account_id"`
	Direction string `json:"direction"` // DEBIT veya CREDIT
	Amount    int64  `json:"amount"`    // En küçük para biriminde (cent)
}

// CreateTransactionRequest, HTTP istek gövdesi (payload)
type CreateTransactionRequest struct {
	TransactionID string     `json:"transaction_id"`
	Description   string     `json:"description"`
	Currency      string     `json:"currency"`
	Entries       []EntryDTO `json:"entries"`
}

// TransactionResponse, başarılı işlem yanıtı
type TransactionResponse struct {
	TransactionID string    `json:"transaction_id"`
	Status        string    `json:"status"`
	Currency      string    `json:"currency"`
	TotalAmount   int64     `json:"total_amount"`
	CreatedAt     time.Time `json:"created_at"`
	IsCached      bool      `json:"is_cached"`
}

// LedgerService, idempotent işlem yönetimini, kilitleme stratejilerini ve snapshot motorunu koordine eden servis katmanıdır.
type LedgerService struct {
	repo           *repository.PostgresLedgerRepository
	stateMachine   *idempotency.StateMachine
	snapshotWorker *snapshot.SnapshotWorker
}

// NewLedgerService, yeni bir LedgerService örneği oluşturur.
func NewLedgerService(repo *repository.PostgresLedgerRepository) *LedgerService {
	sm := idempotency.NewStateMachine(repo)
	return &LedgerService{
		repo:         repo,
		stateMachine: sm,
	}
}

// SetSnapshotWorker, asenkron snapshot pipeline worker'ını servise bağlar.
func (s *LedgerService) SetSnapshotWorker(w *snapshot.SnapshotWorker) {
	s.snapshotWorker = w
}

// parseAndValidateRequest, istek gövdesini doğrular ve domain Transaction nesnesini üretir.
func (s *LedgerService) parseAndValidateRequest(rawPayload []byte) (domain.Transaction, error) {
	var req CreateTransactionRequest
	if err := json.Unmarshal(rawPayload, &req); err != nil {
		return domain.Transaction{}, fmt.Errorf("geçersiz istek gövdesi (JSON hatası): %w", err)
	}

	curr, err := domain.NewCurrency(req.Currency, 2)
	if err != nil {
		return domain.Transaction{}, err
	}

	var entries []domain.Entry
	for i, e := range req.Entries {
		money, err := domain.NewPositiveMoney(e.Amount, curr)
		if err != nil {
			return domain.Transaction{}, fmt.Errorf("girdi #%d geçersiz tutar: %w", i+1, err)
		}
		entryID := fmt.Sprintf("%s-e%d", req.TransactionID, i+1)
		entry, err := domain.NewEntry(entryID, e.AccountID, domain.Direction(e.Direction), money)
		if err != nil {
			return domain.Transaction{}, fmt.Errorf("girdi #%d doğrulanamadı: %w", i+1, err)
		}
		entries = append(entries, entry)
	}

	return domain.NewTransaction(req.TransactionID, time.Now().UTC(), req.Description, entries)
}

// CreateTransaction, varsayılan olarak atomik veritabanı kaydıyla işlemi yürütür.
func (s *LedgerService) CreateTransaction(ctx context.Context, idempotencyKey string, rawPayload []byte) (*TransactionResponse, int, error) {
	return s.CreateTransactionWithPessimisticLock(ctx, idempotencyKey, rawPayload)
}

// CreateTransactionWithPessimisticLock, Pessimistic Locking (SELECT ... FOR UPDATE) ile
// hesapları deterministik sırada kilitleyerek deadlock riski olmadan işlemi yürütür.
func (s *LedgerService) CreateTransactionWithPessimisticLock(ctx context.Context, idempotencyKey string, rawPayload []byte) (*TransactionResponse, int, error) {
	tx, err := s.parseAndValidateRequest(rawPayload)
	if err != nil {
		return nil, 422, err
	}

	reqHash := idempotency.ComputeRequestHash(rawPayload)

	result, err := s.stateMachine.Process(ctx, idempotencyKey, rawPayload, func(execCtx context.Context) (int, []byte, error) {
		resp := TransactionResponse{
			TransactionID: tx.ID(),
			Status:        "RESOLVED",
			Currency:      tx.Currency().Code(),
			TotalAmount:   tx.TotalAmount().Amount(),
			CreatedAt:     tx.Timestamp(),
			IsCached:      false,
		}
		respBytes, err := json.Marshal(resp)
		if err != nil {
			return 500, nil, err
		}

		// Pessimistic kilitleme ve atomik kayıt
		err = s.repo.ExecuteAtomicLedgerTransactionWithPessimisticLock(execCtx, idempotencyKey, reqHash, tx, 201, respBytes)
		if err != nil {
			return 500, nil, fmt.Errorf("pessimistic kayıt hatası: %w", err)
		}

		// İşlem başarılı -> Etkilenen hesapları snapshot worker'a bildir
		if s.snapshotWorker != nil {
			for _, e := range tx.Entries() {
				s.snapshotWorker.NotifyAccountActivity(e.AccountID())
			}
		}

		return 201, respBytes, nil
	})

	return s.handleResult(result, err)
}

// CreateTransactionWithOCC, Optimistic Concurrency Control (OCC) kullanarak işlemi yürütür.
// Versiyon çakışması (ErrOptimisticLockConflict) olursa üst katmanda Exponential Backoff ile yeniden dener.
func (s *LedgerService) CreateTransactionWithOCC(ctx context.Context, idempotencyKey string, rawPayload []byte, retryCfg concurrency.RetryConfig) (*TransactionResponse, int, error) {
	tx, err := s.parseAndValidateRequest(rawPayload)
	if err != nil {
		return nil, 422, err
	}

	reqHash := idempotency.ComputeRequestHash(rawPayload)

	result, err := s.stateMachine.Process(ctx, idempotencyKey, rawPayload, func(execCtx context.Context) (int, []byte, error) {
		resp := TransactionResponse{
			TransactionID: tx.ID(),
			Status:        "RESOLVED",
			Currency:      tx.Currency().Code(),
			TotalAmount:   tx.TotalAmount().Amount(),
			CreatedAt:     tx.Timestamp(),
			IsCached:      false,
		}
		respBytes, err := json.Marshal(resp)
		if err != nil {
			return 500, nil, err
		}

		// OCC yeniden deneme (retry) döngüsü
		retryErr := concurrency.ExecuteWithRetry(execCtx, retryCfg, func(attempt int) error {
			// İlgili hesapların güncel versiyonlarını oku
			expectedVersions := make(map[string]int64)
			for _, e := range tx.Entries() {
				acc, err := s.repo.GetAccountByID(execCtx, e.AccountID())
				if err != nil {
					return err
				}
				expectedVersions[e.AccountID()] = acc.Version()
			}

			// OCC ile atomik olarak versiyonu kontrol et ve kaydet
			return s.repo.ExecuteAtomicLedgerTransactionWithOCC(execCtx, idempotencyKey, reqHash, tx, expectedVersions, 201, respBytes)
		})

		if retryErr != nil {
			return 500, nil, retryErr
		}

		// Başarılı işlem sonrası snapshot worker'a bildirim
		if s.snapshotWorker != nil {
			for _, e := range tx.Entries() {
				s.snapshotWorker.NotifyAccountActivity(e.AccountID())
			}
		}

		return 201, respBytes, nil
	})

	return s.handleResult(result, err)
}

// GetAccountBalance, Snapshotting Engine kullanarak hesabın anlık bakiyesini hızlıca hesaplar.
func (s *LedgerService) GetAccountBalance(ctx context.Context, accountID string) (domain.Money, error) {
	return s.repo.GetAccountBalanceFast(ctx, accountID)
}

func (s *LedgerService) handleResult(result *idempotency.Result, err error) (*TransactionResponse, int, error) {
	if err != nil {
		if err == idempotency.ErrIdempotencyKeyReuseWithDifferentPayload {
			return nil, 422, err
		}
		if err == idempotency.ErrIdempotencyKeyInFlight {
			return nil, 409, err
		}
		return nil, 500, err
	}

	var finalResp TransactionResponse
	if err := json.Unmarshal(result.ResponseBody, &finalResp); err != nil {
		return nil, 500, fmt.Errorf("yanıt çözümlenemedi: %w", err)
	}
	finalResp.IsCached = result.IsCached

	return &finalResp, result.ResponseCode, nil
}
