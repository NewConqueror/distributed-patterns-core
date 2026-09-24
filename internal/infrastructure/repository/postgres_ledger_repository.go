package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/idempotency"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresLedgerRepository, PostgreSQL destekli salt-eklemeli (Append-Only) defteri kebir ve idempotency deposudur.
type PostgresLedgerRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresLedgerRepository, yeni bir PostgreSQL repository nesnesi oluşturur.
func NewPostgresLedgerRepository(pool *pgxpool.Pool) *PostgresLedgerRepository {
	return &PostgresLedgerRepository{
		pool: pool,
	}
}

// -----------------------------------------------------------------------------
// domain.LedgerRepository Metotları (Asla Update veya Delete İÇERMEZ)
// -----------------------------------------------------------------------------

// SaveAccount, sisteme yeni bir hesap ekler.
func (r *PostgresLedgerRepository) SaveAccount(ctx context.Context, account domain.Account) error {
	query := `
		INSERT INTO accounts (id, name, account_type, currency, created_at)
		VALUES ($1, $2, $3, $4, NOW())
	`
	_, err := r.pool.Exec(ctx, query,
		account.ID(),
		account.Name(),
		string(account.AccountType()),
		account.Currency().Code(),
	)
	if err != nil {
		return fmt.Errorf("hesap kaydedilemedi: %w", err)
	}
	return nil
}

// GetAccountByID, ID'ye göre hesabı ve ilişkili hareketlerden hesaplanan güncel bakiyesini getirir.
func (r *PostgresLedgerRepository) GetAccountByID(ctx context.Context, id string) (domain.Account, error) {
	query := `
		SELECT id, name, account_type, currency
		FROM accounts
		WHERE id = $1
	`
	var accID, name, accTypeStr, currCode string
	err := r.pool.QueryRow(ctx, query, id).Scan(&accID, &name, &accTypeStr, &currCode)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Account{}, fmt.Errorf("hesap bulunamadı (ID: %s)", id)
		}
		return domain.Account{}, fmt.Errorf("hesap sorgulanırken hata: %w", err)
	}

	curr, err := domain.NewCurrency(currCode, 2)
	if err != nil {
		return domain.Account{}, err
	}

	acc, err := domain.NewAccount(accID, name, domain.AccountType(accTypeStr), curr)
	if err != nil {
		return domain.Account{}, err
	}

	// Hesaba ait tüm girdileri çekerek bakiyeyi dinamik/değişmez hesapla
	entriesQuery := `
		SELECT id, account_id, direction, amount
		FROM entries
		WHERE account_id = $1
		ORDER BY created_at ASC
	`
	rows, err := r.pool.Query(ctx, entriesQuery, id)
	if err != nil {
		return domain.Account{}, fmt.Errorf("hesap hareketleri sorgulanamadı: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entryID, eAccID, dirStr string
		var amount int64
		if err := rows.Scan(&entryID, &eAccID, &dirStr, &amount); err != nil {
			return domain.Account{}, err
		}

		money, err := domain.NewMoney(amount, curr)
		if err != nil {
			return domain.Account{}, err
		}

		entry, err := domain.NewEntry(entryID, eAccID, domain.Direction(dirStr), money)
		if err != nil {
			return domain.Account{}, err
		}

		acc, err = acc.ApplyEntry(entry)
		if err != nil {
			return domain.Account{}, fmt.Errorf("hareket uygulanırken hata: %w", err)
		}
	}

	return acc, nil
}

// ListAccounts, tüm hesapları getirir.
func (r *PostgresLedgerRepository) ListAccounts(ctx context.Context) ([]domain.Account, error) {
	rows, err := r.pool.Query(ctx, "SELECT id FROM accounts ORDER BY created_at ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accountIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		accountIDs = append(accountIDs, id)
	}

	var accounts []domain.Account
	for _, id := range accountIDs {
		acc, err := r.GetAccountByID(ctx, id)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, acc)
	}

	return accounts, nil
}

// SaveTransaction, bir işlemi ve altındaki tüm girdileri veritabanına tek bir atomik transaction içinde kaydeder.
func (r *PostgresLedgerRepository) SaveTransaction(ctx context.Context, tx domain.Transaction) error {
	pgTx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("veritabanı transaction başlatılamadı: %w", err)
	}
	defer func() { _ = pgTx.Rollback(ctx) }()

	if err := r.insertTransactionInternal(ctx, pgTx, tx); err != nil {
		return err
	}

	// COMMIT aşamasında PostgreSQL DEFERRABLE INITIALLY DEFERRED bakiye kontrolü tetiklenir!
	if err := pgTx.Commit(ctx); err != nil {
		return fmt.Errorf("transaction commit edilemedi (muhasebe denkliği veya veritabanı hatası): %w", err)
	}

	return nil
}

// insertTransactionInternal, mevcut bir pgx.Tx içinde işlem ve girdileri ekler.
func (r *PostgresLedgerRepository) insertTransactionInternal(ctx context.Context, pgTx pgx.Tx, tx domain.Transaction) error {
	txQuery := `
		INSERT INTO transactions (id, description, currency, reversal_of_id, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := pgTx.Exec(ctx, txQuery,
		tx.ID(),
		tx.Description(),
		tx.Currency().Code(),
		tx.ReversalOfID(),
		tx.Timestamp(),
	)
	if err != nil {
		return fmt.Errorf("işlem kaydedilemedi: %w", err)
	}

	entryQuery := `
		INSERT INTO entries (id, transaction_id, account_id, direction, amount, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	for _, entry := range tx.Entries() {
		_, err := pgTx.Exec(ctx, entryQuery,
			entry.ID(),
			tx.ID(),
			entry.AccountID(),
			string(entry.Direction()),
			entry.Money().Amount(),
			tx.Timestamp(),
		)
		if err != nil {
			return fmt.Errorf("girdi kaydedilemedi (%s): %w", entry.ID(), err)
		}
	}

	return nil
}

// GetTransactionByID, ID'ye göre işlemi ve tüm girdilerini getirir.
func (r *PostgresLedgerRepository) GetTransactionByID(ctx context.Context, id string) (domain.Transaction, error) {
	txQuery := `
		SELECT id, description, currency, reversal_of_id, created_at
		FROM transactions
		WHERE id = $1
	`
	var txID, desc, currCode string
	var revOfID *string
	var createdAt time.Time

	err := r.pool.QueryRow(ctx, txQuery, id).Scan(&txID, &desc, &currCode, &revOfID, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Transaction{}, fmt.Errorf("işlem bulunamadı (ID: %s)", id)
		}
		return domain.Transaction{}, fmt.Errorf("işlem sorgulanamadı: %w", err)
	}

	curr, err := domain.NewCurrency(currCode, 2)
	if err != nil {
		return domain.Transaction{}, err
	}

	entriesQuery := `
		SELECT id, account_id, direction, amount
		FROM entries
		WHERE transaction_id = $1
		ORDER BY created_at ASC
	`
	rows, err := r.pool.Query(ctx, entriesQuery, id)
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("işlem girdileri sorgulanamadı: %w", err)
	}
	defer rows.Close()

	var entries []domain.Entry
	for rows.Next() {
		var eID, aID, dirStr string
		var amount int64
		if err := rows.Scan(&eID, &aID, &dirStr, &amount); err != nil {
			return domain.Transaction{}, err
		}
		money, err := domain.NewMoney(amount, curr)
		if err != nil {
			return domain.Transaction{}, err
		}
		entry, err := domain.NewEntry(eID, aID, domain.Direction(dirStr), money)
		if err != nil {
			return domain.Transaction{}, err
		}
		entries = append(entries, entry)
	}

	return domain.NewTransaction(txID, createdAt, desc, entries)
}

// GetTransactionsByAccountID, belirli bir hesaba ait tüm işlemleri döner.
func (r *PostgresLedgerRepository) GetTransactionsByAccountID(ctx context.Context, accountID string) ([]domain.Transaction, error) {
	query := `
		SELECT DISTINCT t.id
		FROM transactions t
		JOIN entries e ON t.id = e.transaction_id
		WHERE e.account_id = $1
		ORDER BY t.id ASC
	`
	rows, err := r.pool.Query(ctx, query, accountID)
	if err != nil {
		return nil, fmt.Errorf("hesap işlemleri sorgulanamadı: %w", err)
	}
	defer rows.Close()

	var txIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		txIDs = append(txIDs, id)
	}

	var results []domain.Transaction
	for _, id := range txIDs {
		tx, err := r.GetTransactionByID(ctx, id)
		if err != nil {
			return nil, err
		}
		results = append(results, tx)
	}

	return results, nil
}

// -----------------------------------------------------------------------------
// idempotency.Storage Metotları
// -----------------------------------------------------------------------------

// CreateInFlight, anahtarı IN_FLIGHT durumuyla atomik olarak kaydeder.
func (r *PostgresLedgerRepository) CreateInFlight(ctx context.Context, key, requestHash string, lockedAt time.Time) (*idempotency.Record, error) {
	insertQuery := `
		INSERT INTO idempotency_keys (key, request_hash, status, locked_at, created_at, updated_at)
		VALUES ($1, $2, 'IN_FLIGHT', $3, $3, $3)
		ON CONFLICT (key) DO NOTHING
	`
	cmdTag, err := r.pool.Exec(ctx, insertQuery, key, requestHash, lockedAt)
	if err != nil {
		return nil, fmt.Errorf("idempotency anahtarı eklenemedi: %w", err)
	}

	// Eğer satır eklendiyse (1 satır), ilk defa gelen istektir
	if cmdTag.RowsAffected() == 1 {
		return nil, nil
	}

	// Satır eklenemediyse, anahtar zaten mevcuttur; mevcut kaydı getir
	return r.Get(ctx, key)
}

// Resolve, işlemi tamamlanan kaydın durumunu RESOLVED yapar ve yanıtı kaydeder.
func (r *PostgresLedgerRepository) Resolve(ctx context.Context, key string, responseCode int, responseBody []byte) error {
	query := `
		UPDATE idempotency_keys
		SET status = 'RESOLVED',
		    response_code = $1,
		    response_body = $2::jsonb,
		    updated_at = NOW()
		WHERE key = $3
	`
	_, err := r.pool.Exec(ctx, query, responseCode, responseBody, key)
	if err != nil {
		return fmt.Errorf("idempotency durumu RESOLVED yapılamadı: %w", err)
	}
	return nil
}

// Fail, başarısız olan kaydın durumunu FAILED yapar.
func (r *PostgresLedgerRepository) Fail(ctx context.Context, key string, responseCode int, responseBody []byte) error {
	query := `
		UPDATE idempotency_keys
		SET status = 'FAILED',
		    response_code = $1,
		    response_body = $2::jsonb,
		    updated_at = NOW()
		WHERE key = $3
	`
	_, err := r.pool.Exec(ctx, query, responseCode, responseBody, key)
	if err != nil {
		return fmt.Errorf("idempotency durumu FAILED yapılamadı: %w", err)
	}
	return nil
}

// Get, anahtar kaydını getirir.
func (r *PostgresLedgerRepository) Get(ctx context.Context, key string) (*idempotency.Record, error) {
	query := `
		SELECT key, request_hash, status, response_code, response_body, locked_at, created_at, updated_at
		FROM idempotency_keys
		WHERE key = $1
	`
	var rec idempotency.Record
	var statusStr string
	var respCode *int
	var respBody []byte

	err := r.pool.QueryRow(ctx, query, key).Scan(
		&rec.Key,
		&rec.RequestHash,
		&statusStr,
		&respCode,
		&respBody,
		&rec.LockedAt,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, idempotency.ErrKeyNotFound
		}
		return nil, fmt.Errorf("idempotency kaydı sorgulanamadı: %w", err)
	}

	rec.Status = idempotency.Status(statusStr)
	if respCode != nil {
		rec.ResponseCode = *respCode
	}
	rec.ResponseBody = respBody

	return &rec, nil
}

// -----------------------------------------------------------------------------
// 2.3. Atomik Kayıt & Transaction Script (Single BEGIN ... COMMIT Block)
// -----------------------------------------------------------------------------

// ExecuteAtomicLedgerTransaction, İdempotency kaydı açma, transaction oluşturma,
// entries ekleme ve idempotency durumunu güncelleme adımlarını TEK BİR BEGIN ... COMMIT bloğunda icra eder.
// PostgreSQL DEFERRABLE INITIALLY DEFERRED tetikleyicisi commit aşamasında denkliği garanti eder.
func (r *PostgresLedgerRepository) ExecuteAtomicLedgerTransaction(
	ctx context.Context,
	idempKey string,
	requestHash string,
	tx domain.Transaction,
	responseCode int,
	responseBody []byte,
) error {
	pgTx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("atomik transaction başlatılamadı: %w", err)
	}
	defer func() { _ = pgTx.Rollback(ctx) }()

	// 1. İdempotency kaydı oluştur
	idempInsert := `
		INSERT INTO idempotency_keys (key, request_hash, status, locked_at, created_at, updated_at)
		VALUES ($1, $2, 'IN_FLIGHT', NOW(), NOW(), NOW())
	`
	if _, err := pgTx.Exec(ctx, idempInsert, idempKey, requestHash); err != nil {
		return fmt.Errorf("idempotency anahtarı kilitlenemedi: %w", err)
	}

	// 2. Transaction ve Girdileri Ekle
	if err := r.insertTransactionInternal(ctx, pgTx, tx); err != nil {
		return err
	}

	// 3. İdempotency Durumunu RESOLVED Yap
	resolveQuery := `
		UPDATE idempotency_keys
		SET status = 'RESOLVED',
		    response_code = $1,
		    response_body = $2::jsonb,
		    updated_at = NOW()
		WHERE key = $3
	`
	if _, err := pgTx.Exec(ctx, resolveQuery, responseCode, responseBody, idempKey); err != nil {
		return fmt.Errorf("idempotency tamamlanamadı: %w", err)
	}

	// 4. COMMIT! (PostgreSQL DEFERRABLE trigger burada çalışır ve denkliği onaylar)
	if err := pgTx.Commit(ctx); err != nil {
		return fmt.Errorf("atomik kayıt commit edilemedi: %w", err)
	}

	return nil
}
