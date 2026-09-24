package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/audit"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/concurrency"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/idempotency"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/snapshot"

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
		INSERT INTO accounts (id, name, account_type, currency, version, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`
	_, err := r.pool.Exec(ctx, query,
		account.ID(),
		account.Name(),
		string(account.AccountType()),
		account.Currency().Code(),
		account.Version(),
	)
	if err != nil {
		return fmt.Errorf("hesap kaydedilemedi: %w", err)
	}
	return nil
}

// GetAccountByID, ID'ye göre hesabı ve ilişkili hareketlerden hesaplanan güncel bakiyesini getirir.
func (r *PostgresLedgerRepository) GetAccountByID(ctx context.Context, id string) (domain.Account, error) {
	query := `
		SELECT id, name, account_type, currency, version
		FROM accounts
		WHERE id = $1
	`
	var accID, name, accTypeStr, currCode string
	var version int64
	err := r.pool.QueryRow(ctx, query, id).Scan(&accID, &name, &accTypeStr, &currCode, &version)
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

	acc, err := domain.NewAccountWithVersion(accID, name, domain.AccountType(accTypeStr), curr, version)
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

// insertTransactionInternal, mevcut bir pgx.Tx içinde işlem ve girdileri kriptografik hash zinciriyle ekler.
func (r *PostgresLedgerRepository) insertTransactionInternal(ctx context.Context, pgTx pgx.Tx, tx domain.Transaction) error {
	// 1. Transactions Global Hash Zinciri
	var lastTxHash string
	var lastTxSeq int64
	err := pgTx.QueryRow(ctx, `
		SELECT current_hash, sequence_num 
		FROM transactions 
		ORDER BY sequence_num DESC 
		LIMIT 1
	`).Scan(&lastTxHash, &lastTxSeq)

	var prevTxHash string
	var txSeq int64
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			prevTxHash = audit.ZeroGenesisHash
			txSeq = 1
		} else {
			return fmt.Errorf("önceki işlem hash'i sorgulanamadı: %w", err)
		}
	} else {
		prevTxHash = lastTxHash
		txSeq = lastTxSeq + 1
	}

	currTxHash := audit.ComputeHash(prevTxHash, tx.ID(), tx.TotalAmount().Amount(), tx.Timestamp())

	txQuery := `
		INSERT INTO transactions (id, description, currency, reversal_of_id, prev_hash, current_hash, sequence_num, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err = pgTx.Exec(ctx, txQuery,
		tx.ID(),
		tx.Description(),
		tx.Currency().Code(),
		tx.ReversalOfID(),
		prevTxHash,
		currTxHash,
		txSeq,
		tx.Timestamp(),
	)
	if err != nil {
		return fmt.Errorf("işlem kaydedilemedi: %w", err)
	}

	// 2. Entries Hesap Bazlı Hash Zinciri
	accountLastHash := make(map[string]string)
	accountLastSeq := make(map[string]int64)

	entryQuery := `
		INSERT INTO entries (id, transaction_id, account_id, direction, amount, prev_hash, current_hash, sequence_num, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	for _, entry := range tx.Entries() {
		accID := entry.AccountID()
		prevHash, ok := accountLastHash[accID]
		var seq int64

		if !ok {
			var dbHash string
			var dbSeq int64
			err := pgTx.QueryRow(ctx, `
				SELECT current_hash, sequence_num 
				FROM entries 
				WHERE account_id = $1 
				ORDER BY sequence_num DESC 
				LIMIT 1
			`, accID).Scan(&dbHash, &dbSeq)

			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					// Genesis Blok: İlk işlemde hesap için tanımlı sabit seed hash kullanılır
					prevHash = audit.GenesisHash(accID)
					seq = 1
				} else {
					return fmt.Errorf("hesap önceki girdi hash'i sorgulanamadı: %w", err)
				}
			} else {
				prevHash = dbHash
				seq = dbSeq + 1
			}
		} else {
			seq = accountLastSeq[accID] + 1
		}

		// Hash_n = SHA256(Hash_{n-1} + TransactionID + Amount + Timestamp)
		currHash := audit.ComputeHash(prevHash, tx.ID(), entry.Money().Amount(), tx.Timestamp())
		accountLastHash[accID] = currHash
		accountLastSeq[accID] = seq

		_, err = pgTx.Exec(ctx, entryQuery,
			entry.ID(),
			tx.ID(),
			entry.AccountID(),
			string(entry.Direction()),
			entry.Money().Amount(),
			prevHash,
			currHash,
			seq,
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

// -----------------------------------------------------------------------------
// 3.1. Eşzamanlılık & Kilitleme Stratejileri (Pessimistic vs. OCC)
// -----------------------------------------------------------------------------

// LockAccountsOrdered, verilen hesap ID'lerini deterministik olarak sıralar (OrderAccountIDs)
// ve PostgreSQL üzerinde SELECT ... FOR UPDATE ile kilitler.
// Deadlock Önleme: Tüm işlemler hesapları aynı global sırada kilitlediğinden döngüsel bekleme oluşmaz.
func (r *PostgresLedgerRepository) LockAccountsOrdered(ctx context.Context, pgTx pgx.Tx, accountIDs []string) (map[string]domain.Account, error) {
	orderedIDs := concurrency.OrderAccountIDs(accountIDs)
	lockedAccounts := make(map[string]domain.Account, len(orderedIDs))

	lockQuery := `
		SELECT id, name, account_type, currency, version
		FROM accounts
		WHERE id = $1
		FOR UPDATE
	`

	for _, accID := range orderedIDs {
		var id, name, accTypeStr, currCode string
		var version int64
		err := pgTx.QueryRow(ctx, lockQuery, accID).Scan(&id, &name, &accTypeStr, &currCode, &version)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("kilitlenecek hesap bulunamadı (ID: %s)", accID)
			}
			return nil, fmt.Errorf("hesap kilitlenemedi (ID: %s): %w", accID, err)
		}

		curr, err := domain.NewCurrency(currCode, 2)
		if err != nil {
			return nil, err
		}

		acc, err := domain.NewAccountWithVersion(id, name, domain.AccountType(accTypeStr), curr, version)
		if err != nil {
			return nil, err
		}

		lockedAccounts[accID] = acc
	}

	return lockedAccounts, nil
}

// UpdateAccountVersionOCC, Optimistic Concurrency Control (OCC) gereği hesap versiyonunu 1 arttırır.
// Eğer beklenen versiyon (expectedVersion) veritabanındaki ile uyuşmuyorsa ErrOptimisticLockConflict döner.
func (r *PostgresLedgerRepository) UpdateAccountVersionOCC(ctx context.Context, pgTx pgx.Tx, accountID string, expectedVersion int64) error {
	query := `
		UPDATE accounts
		SET version = version + 1
		WHERE id = $1 AND version = $2
	`
	cmdTag, err := pgTx.Exec(ctx, query, accountID, expectedVersion)
	if err != nil {
		return fmt.Errorf("hesap versiyonu güncellenemedi: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("%w: hesap %s için beklenen versiyon: %d",
			domain.ErrOptimisticLockConflict, accountID, expectedVersion)
	}

	return nil
}

// ExecuteAtomicLedgerTransactionWithPessimisticLock, Pessimistic Locking kullanarak
// hesapları sıralı kilitler ve işlemi atomik olarak yürütür.
func (r *PostgresLedgerRepository) ExecuteAtomicLedgerTransactionWithPessimisticLock(
	ctx context.Context,
	idempKey string,
	requestHash string,
	tx domain.Transaction,
	responseCode int,
	responseBody []byte,
) error {
	pgTx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("transaction başlatılamadı: %w", err)
	}
	defer func() { _ = pgTx.Rollback(ctx) }()

	// 1. İdempotency IN_FLIGHT
	idempInsert := `
		INSERT INTO idempotency_keys (key, request_hash, status, locked_at, created_at, updated_at)
		VALUES ($1, $2, 'IN_FLIGHT', NOW(), NOW(), NOW())
	`
	if _, err := pgTx.Exec(ctx, idempInsert, idempKey, requestHash); err != nil {
		return fmt.Errorf("idempotency anahtarı kilitlenemedi: %w", err)
	}

	// 2. İlgili tüm hesapları deterministik sırayla KİLİTLE (Pessimistic SELECT ... FOR UPDATE)
	var accountIDs []string
	for _, e := range tx.Entries() {
		accountIDs = append(accountIDs, e.AccountID())
	}
	if _, err := r.LockAccountsOrdered(ctx, pgTx, accountIDs); err != nil {
		return err
	}

	// 3. Transaction ve Girdileri Ekle
	if err := r.insertTransactionInternal(ctx, pgTx, tx); err != nil {
		return err
	}

	// 4. İdempotency RESOLVED
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

	// 5. COMMIT (PostgreSQL DEFERRABLE trigger bakiye denkliğini onaylar)
	if err := pgTx.Commit(ctx); err != nil {
		return fmt.Errorf("pessimistic transaction commit edilemedi: %w", err)
	}

	return nil
}

// ExecuteAtomicLedgerTransactionWithOCC, Optimistic Concurrency Control kullanarak
// beklenen versiyonları denetler, çakışma varsa ErrOptimisticLockConflict fırlatır.
func (r *PostgresLedgerRepository) ExecuteAtomicLedgerTransactionWithOCC(
	ctx context.Context,
	idempKey string,
	requestHash string,
	tx domain.Transaction,
	expectedVersions map[string]int64,
	responseCode int,
	responseBody []byte,
) error {
	pgTx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("transaction başlatılamadı: %w", err)
	}
	defer func() { _ = pgTx.Rollback(ctx) }()

	// 1. İdempotency IN_FLIGHT
	idempInsert := `
		INSERT INTO idempotency_keys (key, request_hash, status, locked_at, created_at, updated_at)
		VALUES ($1, $2, 'IN_FLIGHT', NOW(), NOW(), NOW())
	`
	if _, err := pgTx.Exec(ctx, idempInsert, idempKey, requestHash); err != nil {
		return fmt.Errorf("idempotency anahtarı kilitlenemedi: %w", err)
	}

	// 2. OCC Versiyon Kontrolü ve Güncellemesi
	for _, e := range tx.Entries() {
		expVer, exists := expectedVersions[e.AccountID()]
		if !exists {
			return fmt.Errorf("hesap %s için beklenen versiyon sağlanmadı", e.AccountID())
		}
		if err := r.UpdateAccountVersionOCC(ctx, pgTx, e.AccountID(), expVer); err != nil {
			return err
		}
	}

	// 3. Transaction ve Girdileri Ekle
	if err := r.insertTransactionInternal(ctx, pgTx, tx); err != nil {
		return err
	}

	// 4. İdempotency RESOLVED
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

	// 5. COMMIT
	if err := pgTx.Commit(ctx); err != nil {
		return fmt.Errorf("OCC transaction commit edilemedi: %w", err)
	}

	return nil
}

// -----------------------------------------------------------------------------
// 3.2. Snapshotting Engine Metotları (snapshot.Storage Arayüzü)
// -----------------------------------------------------------------------------

// SaveSnapshot, yeni bir hesap checkpoint'i kaydeder.
func (r *PostgresLedgerRepository) SaveSnapshot(ctx context.Context, s snapshot.AccountSnapshot) error {
	query := `
		INSERT INTO account_snapshots (id, account_id, balance, last_entry_id, entry_count, snapshot_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := r.pool.Exec(ctx, query,
		s.ID,
		s.AccountID,
		s.Balance.Amount(),
		s.LastEntryID,
		s.EntryCount,
		s.SnapshotAt,
	)
	if err != nil {
		return fmt.Errorf("snapshot kaydedilemedi: %w", err)
	}
	return nil
}

// GetLatestSnapshot, hesaba ait en güncel snapshot'ı getirir.
func (r *PostgresLedgerRepository) GetLatestSnapshot(ctx context.Context, accountID string) (*snapshot.AccountSnapshot, error) {
	query := `
		SELECT s.id, s.account_id, s.balance, s.last_entry_id, s.entry_count, s.snapshot_at, a.currency
		FROM account_snapshots s
		JOIN accounts a ON s.account_id = a.id
		WHERE s.account_id = $1
		ORDER BY s.snapshot_at DESC
		LIMIT 1
	`
	var snapID, accID, currCode string
	var balanceAmount, entryCount int64
	var lastEntryID *string
	var snapshotAt time.Time

	err := r.pool.QueryRow(ctx, query, accountID).Scan(
		&snapID,
		&accID,
		&balanceAmount,
		&lastEntryID,
		&entryCount,
		&snapshotAt,
		&currCode,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Henüz snapshot yok
		}
		return nil, fmt.Errorf("en son snapshot sorgulanamadı: %w", err)
	}

	curr, err := domain.NewCurrency(currCode, 2)
	if err != nil {
		return nil, err
	}
	money, err := domain.NewMoney(balanceAmount, curr)
	if err != nil {
		return nil, err
	}

	snap := snapshot.NewAccountSnapshot(snapID, accID, money, lastEntryID, entryCount, snapshotAt)
	return &snap, nil
}

// GetEntriesAfter, belirtilen lastEntryID'den sonra eklenen girdileri created_at sırasıyla getirir.
func (r *PostgresLedgerRepository) GetEntriesAfter(ctx context.Context, accountID string, lastEntryID *string) ([]domain.Entry, error) {
	var query string
	var args []any

	if lastEntryID == nil {
		query = `
			SELECT e.id, e.account_id, e.direction, e.amount, a.currency
			FROM entries e
			JOIN accounts a ON e.account_id = a.id
			WHERE e.account_id = $1
			ORDER BY e.created_at ASC
		`
		args = []any{accountID}
	} else {
		query = `
			SELECT e.id, e.account_id, e.direction, e.amount, a.currency
			FROM entries e
			JOIN accounts a ON e.account_id = a.id
			WHERE e.account_id = $1
			  AND e.created_at > (SELECT created_at FROM entries WHERE id = $2)
			ORDER BY e.created_at ASC
		`
		args = []any{accountID, *lastEntryID}
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("delta girdiler sorgulanamadı: %w", err)
	}
	defer rows.Close()

	var entries []domain.Entry
	for rows.Next() {
		var eID, aID, dirStr, currCode string
		var amount int64
		if err := rows.Scan(&eID, &aID, &dirStr, &amount, &currCode); err != nil {
			return nil, err
		}
		curr, err := domain.NewCurrency(currCode, 2)
		if err != nil {
			return nil, err
		}
		m, err := domain.NewMoney(amount, curr)
		if err != nil {
			return nil, err
		}
		e, err := domain.NewEntry(eID, aID, domain.Direction(dirStr), m)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}

	return entries, nil
}

// CountEntriesAfter, son snapshot'tan sonra eklenen girdi adedini sayar.
func (r *PostgresLedgerRepository) CountEntriesAfter(ctx context.Context, accountID string, lastEntryID *string) (int64, error) {
	var query string
	var args []any

	if lastEntryID == nil {
		query = `SELECT COUNT(*) FROM entries WHERE account_id = $1`
		args = []any{accountID}
	} else {
		query = `
			SELECT COUNT(*)
			FROM entries
			WHERE account_id = $1
			  AND created_at > (SELECT created_at FROM entries WHERE id = $2)
		`
		args = []any{accountID, *lastEntryID}
	}

	var count int64
	err := r.pool.QueryRow(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("delta girdiler sayılamadı: %w", err)
	}
	return count, nil
}

// GetAccountBalanceFast, Snapshotting Engine kullanarak hesabın anlık bakiyesini
// O(1) Snapshot + O(delta) formülüyle çok hızlı bir şekilde hesaplar.
func (r *PostgresLedgerRepository) GetAccountBalanceFast(ctx context.Context, accountID string) (domain.Money, error) {
	acc, err := r.GetAccountByID(ctx, accountID)
	if err != nil {
		return domain.Money{}, err
	}

	engine := snapshot.NewEngine(r)
	balance, _, _, err := engine.CalculateCurrentBalance(ctx, acc)
	if err != nil {
		return domain.Money{}, err
	}

	return balance, nil
}

// -----------------------------------------------------------------------------
// 4.1 & 4.2. Kriptografik Denetim İzi (audit.Storage Arayüzü)
// -----------------------------------------------------------------------------

// GetChainedEntriesForAccount, bir hesaba ait tüm girdileri sequence_num artan sırada getirir.
func (r *PostgresLedgerRepository) GetChainedEntriesForAccount(ctx context.Context, accountID string) ([]audit.ChainedRecord, error) {
	query := `
		SELECT id, transaction_id, account_id, direction, amount, prev_hash, current_hash, sequence_num, created_at
		FROM entries
		WHERE account_id = $1
		ORDER BY sequence_num ASC
	`
	rows, err := r.pool.Query(ctx, query, accountID)
	if err != nil {
		return nil, fmt.Errorf("zincir kayıtları sorgulanamadı: %w", err)
	}
	defer rows.Close()

	var records []audit.ChainedRecord
	for rows.Next() {
		var rec audit.ChainedRecord
		if err := rows.Scan(
			&rec.ID,
			&rec.TransactionID,
			&rec.AccountID,
			&rec.Direction,
			&rec.Amount,
			&rec.PrevHash,
			&rec.CurrentHash,
			&rec.SequenceNum,
			&rec.CreatedAt,
		); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, nil
}

// ListAccountIDs, sistemdeki tüm hesap kimliklerini döner.
func (r *PostgresLedgerRepository) ListAccountIDs(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, "SELECT id FROM accounts ORDER BY created_at ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
