package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/idempotency"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/repository"
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

// LedgerService, idempotent işlem yönetimini koordine eden servis katmanıdır.
type LedgerService struct {
	repo         *repository.PostgresLedgerRepository
	stateMachine *idempotency.StateMachine
}

// NewLedgerService, yeni bir LedgerService örneği oluşturur.
func NewLedgerService(repo *repository.PostgresLedgerRepository) *LedgerService {
	sm := idempotency.NewStateMachine(repo)
	return &LedgerService{
		repo:         repo,
		stateMachine: sm,
	}
}

// CreateTransaction, Idempotency-Key doğrulaması ve atomik veritabanı kaydıyla işlemi yürütür.
func (s *LedgerService) CreateTransaction(ctx context.Context, idempotencyKey string, rawPayload []byte) (*TransactionResponse, int, error) {
	var req CreateTransactionRequest
	if err := json.Unmarshal(rawPayload, &req); err != nil {
		return nil, 400, fmt.Errorf("geçersiz istek gövdesi (JSON hatası): %w", err)
	}

	curr, err := domain.NewCurrency(req.Currency, 2)
	if err != nil {
		return nil, 400, err
	}

	// Domain Entries oluştur
	var entries []domain.Entry
	for i, e := range req.Entries {
		money, err := domain.NewPositiveMoney(e.Amount, curr)
		if err != nil {
			return nil, 400, fmt.Errorf("girdi #%d geçersiz tutar: %w", i+1, err)
		}
		entryID := fmt.Sprintf("%s-e%d", req.TransactionID, i+1)
		entry, err := domain.NewEntry(entryID, e.AccountID, domain.Direction(e.Direction), money)
		if err != nil {
			return nil, 400, fmt.Errorf("girdi #%d doğrulanamadı: %w", i+1, err)
		}
		entries = append(entries, entry)
	}

	// Domain Transaction oluştur ve çift taraflı denkliği doğrula
	tx, err := domain.NewTransaction(req.TransactionID, time.Now().UTC(), req.Description, entries)
	if err != nil {
		return nil, 422, fmt.Errorf("işlem çift taraflı muhasebe kuralına uymuyor: %w", err)
	}

	// Idempotency State Machine üzerinden yürüt
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

		// TEK BIR BEGIN ... COMMIT BLOĞUNDA:
		// 1. Idempotency IN_FLIGHT
		// 2. Transaction & Entries INSERT
		// 3. Idempotency RESOLVED
		// 4. COMMIT (PostgreSQL DEFERRABLE TRIGGER tetiklenir)
		err = s.repo.ExecuteAtomicLedgerTransaction(execCtx, idempotencyKey, reqHash, tx, 201, respBytes)
		if err != nil {
			return 500, nil, fmt.Errorf("veritabanı atomik işlemi başarısız: %w", err)
		}

		return 201, respBytes, nil
	})

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
