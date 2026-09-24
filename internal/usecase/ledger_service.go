package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/audit"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/concurrency"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/idempotency"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/repository"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/observability"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/snapshot"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	auditor        *audit.Auditor
	auditWorker    *audit.AuditWorker
}

// NewLedgerService, yeni bir LedgerService örneği oluşturur.
func NewLedgerService(repo *repository.PostgresLedgerRepository) *LedgerService {
	sm := idempotency.NewStateMachine(repo)
	aud := audit.NewAuditor(repo)
	return &LedgerService{
		repo:         repo,
		stateMachine: sm,
		auditor:      aud,
	}
}

// SetSnapshotWorker, asenkron snapshot pipeline worker'ını servise bağlar.
func (s *LedgerService) SetSnapshotWorker(w *snapshot.SnapshotWorker) {
	s.snapshotWorker = w
}

// SetAuditWorker, periyodik audit worker'ını servise bağlar.
func (s *LedgerService) SetAuditWorker(w *audit.AuditWorker) {
	s.auditWorker = w
}

// AuditAccount, belirli bir hesabın kriptografik hash zincirini denetler.
func (s *LedgerService) AuditAccount(ctx context.Context, accountID string) (*audit.AuditResult, error) {
	ctx, span := observability.Tracer().Start(ctx, "LedgerService.AuditAccount")
	defer span.End()
	span.SetAttributes(attribute.String("account.id", accountID))

	res, err := s.auditor.AuditAccount(ctx, accountID)
	if err != nil {
		span.RecordError(err)
		observability.GetMetrics().ReconciliationErrors.WithLabelValues("audit_error").Inc()
		return nil, err
	}
	if res != nil && res.IsTampered {
		observability.GetMetrics().ReconciliationErrors.WithLabelValues("tampered_hash").Inc()
	}
	return res, nil
}

// AuditEntireLedger, tüm defteri baştan sona tarayarak hash zincirlerini denetler.
func (s *LedgerService) AuditEntireLedger(ctx context.Context) (*audit.LedgerAuditReport, error) {
	ctx, span := observability.Tracer().Start(ctx, "LedgerService.AuditEntireLedger")
	defer span.End()

	report, err := s.auditor.AuditEntireLedger(ctx)
	if err != nil {
		span.RecordError(err)
		observability.GetMetrics().ReconciliationErrors.WithLabelValues("audit_error").Inc()
		return nil, err
	}

	if report != nil && !report.IsClean {
		tamperedCount := len(report.TamperedAccounts)
		observability.GetMetrics().ReconciliationErrors.WithLabelValues("tampered_hash").Add(float64(tamperedCount))
		span.SetAttributes(attribute.Int("tampered.count", tamperedCount))
	}
	return report, nil
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
	ctx, span := observability.Tracer().Start(ctx, "LedgerService.CreateTransactionWithPessimisticLock")
	defer span.End()

	startTime := time.Now()
	metrics := observability.GetMetrics()
	metrics.ActiveTransactions.Inc()
	defer metrics.ActiveTransactions.Dec()

	span.SetAttributes(
		attribute.String("idempotency.key", idempotencyKey),
		attribute.String("concurrency.strategy", "pessimistic"),
	)

	tx, err := s.parseAndValidateRequest(rawPayload)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		metrics.TransactionDuration.WithLabelValues("422", "pessimistic").Observe(time.Since(startTime).Seconds())
		metrics.IdempotencyRequests.WithLabelValues("invalid_request").Inc()
		return nil, 422, err
	}

	span.SetAttributes(
		attribute.String("transaction.id", tx.ID()),
		attribute.String("currency", tx.Currency().Code()),
		attribute.Int64("amount", tx.TotalAmount().Amount()),
		attribute.Int("entries.count", len(tx.Entries())),
	)

	reqHash := idempotency.ComputeRequestHash(rawPayload)

	result, err := s.stateMachine.Process(ctx, idempotencyKey, rawPayload, func(execCtx context.Context) (int, []byte, error) {
		execCtx, execSpan := observability.Tracer().Start(execCtx, "LedgerService.ExecuteAtomicPessimisticLock")
		defer execSpan.End()

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
			execSpan.RecordError(err)
			return 500, nil, err
		}

		// Pessimistic kilitleme ve atomik kayıt
		err = s.repo.ExecuteAtomicLedgerTransactionWithPessimisticLock(execCtx, idempotencyKey, reqHash, tx, 201, respBytes)
		if err != nil {
			execSpan.RecordError(err)
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

	resp, code, finalErr := s.handleResult(result, err)
	s.recordObservability(span, metrics, startTime, "pessimistic", resp, code, finalErr)
	return resp, code, finalErr
}

// CreateTransactionWithOCC, Optimistic Concurrency Control (OCC) kullanarak işlemi yürütür.
// Versiyon çakışması (ErrOptimisticLockConflict) olursa üst katmanda Exponential Backoff ile yeniden dener.
func (s *LedgerService) CreateTransactionWithOCC(ctx context.Context, idempotencyKey string, rawPayload []byte, retryCfg concurrency.RetryConfig) (*TransactionResponse, int, error) {
	ctx, span := observability.Tracer().Start(ctx, "LedgerService.CreateTransactionWithOCC")
	defer span.End()

	startTime := time.Now()
	metrics := observability.GetMetrics()
	metrics.ActiveTransactions.Inc()
	defer metrics.ActiveTransactions.Dec()

	span.SetAttributes(
		attribute.String("idempotency.key", idempotencyKey),
		attribute.String("concurrency.strategy", "occ"),
	)

	tx, err := s.parseAndValidateRequest(rawPayload)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		metrics.TransactionDuration.WithLabelValues("422", "occ").Observe(time.Since(startTime).Seconds())
		metrics.IdempotencyRequests.WithLabelValues("invalid_request").Inc()
		return nil, 422, err
	}

	span.SetAttributes(
		attribute.String("transaction.id", tx.ID()),
		attribute.String("currency", tx.Currency().Code()),
		attribute.Int64("amount", tx.TotalAmount().Amount()),
		attribute.Int("entries.count", len(tx.Entries())),
	)

	reqHash := idempotency.ComputeRequestHash(rawPayload)

	result, err := s.stateMachine.Process(ctx, idempotencyKey, rawPayload, func(execCtx context.Context) (int, []byte, error) {
		execCtx, execSpan := observability.Tracer().Start(execCtx, "LedgerService.ExecuteAtomicOCC")
		defer execSpan.End()

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
			execSpan.RecordError(err)
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
			execSpan.RecordError(retryErr)
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

	resp, code, finalErr := s.handleResult(result, err)
	s.recordObservability(span, metrics, startTime, "occ", resp, code, finalErr)
	return resp, code, finalErr
}

func (s *LedgerService) recordObservability(
	span trace.Span,
	metrics *observability.Metrics,
	startTime time.Time,
	strategy string,
	resp *TransactionResponse,
	code int,
	finalErr error,
) {
	if finalErr != nil {
		span.RecordError(finalErr)
		span.SetStatus(codes.Error, finalErr.Error())
		if errors.Is(finalErr, idempotency.ErrIdempotencyKeyReuseWithDifferentPayload) {
			metrics.IdempotencyRequests.WithLabelValues("key_reuse_error").Inc()
		} else if errors.Is(finalErr, idempotency.ErrIdempotencyKeyInFlight) {
			metrics.IdempotencyRequests.WithLabelValues("in_flight").Inc()
		} else {
			metrics.IdempotencyRequests.WithLabelValues("error").Inc()
		}
	} else {
		span.SetStatus(codes.Ok, "success")
		if resp != nil && resp.IsCached {
			span.SetAttributes(attribute.Bool("idempotency.cache_hit", true))
			metrics.IdempotencyRequests.WithLabelValues("hit").Inc()
		} else {
			span.SetAttributes(attribute.Bool("idempotency.cache_hit", false))
			metrics.IdempotencyRequests.WithLabelValues("miss").Inc()
		}
	}

	metrics.TransactionDuration.WithLabelValues(fmt.Sprintf("%d", code), strategy).Observe(time.Since(startTime).Seconds())
}

// GetAccountBalance, Snapshotting Engine kullanarak hesabın anlık bakiyesini hızlıca hesaplar.
func (s *LedgerService) GetAccountBalance(ctx context.Context, accountID string) (domain.Money, error) {
	return s.repo.GetAccountBalanceFast(ctx, accountID)
}

func (s *LedgerService) handleResult(result *idempotency.Result, err error) (*TransactionResponse, int, error) {
	if err != nil {
		if errors.Is(err, idempotency.ErrIdempotencyKeyReuseWithDifferentPayload) {
			return nil, 422, err
		}
		if errors.Is(err, idempotency.ErrIdempotencyKeyInFlight) {
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
