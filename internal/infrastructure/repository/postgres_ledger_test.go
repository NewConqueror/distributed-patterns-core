package repository_test

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/audit"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/db"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/repository"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/snapshot"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := db.DefaultDockerConfig()
	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		cfg.Port = 5432
		pool, err = db.NewPool(ctx, cfg)
		if err != nil {
			t.Skipf("PostgreSQL veritabanı aktif değil, entegrasyon testi atlanıyor: %v", err)
			return nil
		}
	}

	// 1, 2 ve 3. Faz migration şemalarını sırayla yükle
	m1, _ := filepath.Abs("../../../migrations/001_init_schema.sql")
	_ = db.RunMigrations(context.Background(), pool, m1)
	m2, _ := filepath.Abs("../../../migrations/002_concurrency_and_snapshots.sql")
	_ = db.RunMigrations(context.Background(), pool, m2)
	m3, _ := filepath.Abs("../../../migrations/003_cryptographic_audit_trail.sql")
	_ = db.RunMigrations(context.Background(), pool, m3)

	return pool
}

func setupTestAccounts(t *testing.T, repo *repository.PostgresLedgerRepository) (string, string) {
	ctx := context.Background()
	acc1ID := "acc-test-cash-" + time.Now().Format("150405.000000")
	acc2ID := "acc-test-equity-" + time.Now().Format("150405.000000")

	cash, err := domain.NewAccount(acc1ID, "Kasa", domain.AccountTypeAsset, domain.USD)
	if err != nil {
		t.Fatal(err)
	}
	equity, err := domain.NewAccount(acc2ID, "Sermaye", domain.AccountTypeEquity, domain.USD)
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.SaveAccount(ctx, cash); err != nil {
		t.Fatalf("kasa hesabı açılamadı: %v", err)
	}
	if err := repo.SaveAccount(ctx, equity); err != nil {
		t.Fatalf("sermaye hesabı açılamadı: %v", err)
	}

	return acc1ID, acc2ID
}

func TestPostgres_DBConstraintsAndTriggers(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresLedgerRepository(pool)
	acc1, acc2 := setupTestAccounts(t, repo)

	// 1. Dengeli bir işlem kaydet
	txID := "tx-test-1-" + time.Now().Format("150405.000000")
	m := domain.MustNewMoney(5000, domain.USD)
	e1, _ := domain.NewEntry("e-1-"+txID, acc1, domain.DirectionDebit, m)
	e2, _ := domain.NewEntry("e-2-"+txID, acc2, domain.DirectionCredit, m)
	tx, err := domain.NewTransaction(txID, time.Now().UTC(), "Sermaye Girişi", []domain.Entry{e1, e2})
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.SaveTransaction(ctx, tx); err != nil {
		t.Fatalf("işlem kaydedilemedi: %v", err)
	}

	// Test 2.1: Immutability Trigger
	t.Run("Immutability_UpdateEntry_RejectedByTrigger", func(t *testing.T) {
		_, err := pool.Exec(ctx, "UPDATE entries SET amount = 9999 WHERE id = $1", e1.ID())
		if err == nil {
			t.Fatal("UPDATE entries başarılı oldu, tetikleyici çalışmadı!")
		}
		if !strings.Contains(err.Error(), "Finansal Değişmezlik İhlali") {
			t.Fatalf("beklenen tetikleyici hatası alınmadı: %v", err)
		}
	})

	t.Run("Immutability_DeleteEntry_RejectedByTrigger", func(t *testing.T) {
		_, err := pool.Exec(ctx, "DELETE FROM entries WHERE id = $1", e1.ID())
		if err == nil {
			t.Fatal("DELETE entries başarılı oldu, tetikleyici çalışmadı!")
		}
		if !strings.Contains(err.Error(), "Finansal Değişmezlik İhlali") {
			t.Fatalf("beklenen tetikleyici hatası alınmadı: %v", err)
		}
	})

	// Test 2.1: Check Constraint
	t.Run("CheckConstraint_NonPositiveAmount_Rejected", func(t *testing.T) {
		badEntryQuery := `
			INSERT INTO entries (id, transaction_id, account_id, direction, amount, created_at)
			VALUES ($1, $2, $3, 'DEBIT', 0, NOW())
		`
		_, err := pool.Exec(ctx, badEntryQuery, "e-bad-0", txID, acc1)
		if err == nil {
			t.Fatal("amount = 0 kaydı kabul edildi, CHECK kısıtı ihlal edildi!")
		}
	})

	// Test 2.3: Deferrable Constraint Trigger
	t.Run("DeferredTrigger_UnbalancedTransaction_RejectedOnCommit", func(t *testing.T) {
		pgTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = pgTx.Rollback(ctx) }()

		unbalancedTxID := "tx-unbalanced-" + time.Now().Format("150405.000000")
		_, err = pgTx.Exec(ctx, "INSERT INTO transactions (id, description, currency) VALUES ($1, 'Dengesiz', 'USD')", unbalancedTxID)
		if err != nil {
			t.Fatal(err)
		}

		_, err = pgTx.Exec(ctx, `
			INSERT INTO entries (id, transaction_id, account_id, direction, amount)
			VALUES ($1, $2, $3, 'DEBIT', 10000)
		`, "e-ub-1", unbalancedTxID, acc1)
		if err != nil {
			t.Fatal(err)
		}

		_, err = pgTx.Exec(ctx, `
			INSERT INTO entries (id, transaction_id, account_id, direction, amount)
			VALUES ($1, $2, $3, 'CREDIT', 7000)
		`, "e-ub-2", unbalancedTxID, acc2)
		if err != nil {
			t.Fatal(err)
		}

		err = pgTx.Commit(ctx)
		if err == nil {
			t.Fatal("dengesiz işlem COMMIT edilebildi! DEFERRABLE TRIGGER çalışmadı!")
		}
	})
}

func TestPostgres_PessimisticLocking_DeadlockPrevention(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresLedgerRepository(pool)
	accA, accB := setupTestAccounts(t, repo)

	// Senaryo: Eşzamanlı iki işlem:
	// İşlem 1: A'dan B'ye transfer
	// İşlem 2: B'den A'ya transfer
	// Her ikisi de aynı anda hesapları kilitler.
	// OrderAccountIDs sayesinde her iki işlem de kilitleri A sonra B sırasında talep eder.
	// Asla deadlock (40P01) oluşmaz!

	var wg sync.WaitGroup
	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		m := domain.MustNewMoney(1000, domain.USD)
		e1, _ := domain.NewEntry("e1-1", accA, domain.DirectionDebit, m)
		e2, _ := domain.NewEntry("e1-2", accB, domain.DirectionCredit, m)
		tx, _ := domain.NewTransaction("tx-conc-1", time.Now().UTC(), "A->B", []domain.Entry{e1, e2})

		errs[0] = repo.ExecuteAtomicLedgerTransactionWithPessimisticLock(
			ctx, "key-conc-1", "hash-1", tx, 201, []byte(`{"ok": true}`),
		)
	}()

	go func() {
		defer wg.Done()
		m := domain.MustNewMoney(500, domain.USD)
		e1, _ := domain.NewEntry("e2-1", accB, domain.DirectionDebit, m)
		e2, _ := domain.NewEntry("e2-2", accA, domain.DirectionCredit, m)
		tx, _ := domain.NewTransaction("tx-conc-2", time.Now().UTC(), "B->A", []domain.Entry{e1, e2})

		errs[1] = repo.ExecuteAtomicLedgerTransactionWithPessimisticLock(
			ctx, "key-conc-2", "hash-2", tx, 201, []byte(`{"ok": true}`),
		)
	}()

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("[%d] işlem başarısız oldu (deadlock riski): %v", i+1, err)
		}
	}
}

func TestPostgres_SnapshottingAndFastBalance(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresLedgerRepository(pool)
	accID, offsetAccID := setupTestAccounts(t, repo)

	// 1. Üç işlem kaydet (+1000, +2000, +3000 = 6000 cents)
	for i := 1; i <= 3; i++ {
		m := domain.MustNewMoney(int64(i*1000), domain.USD)
		e1, _ := domain.NewEntry(time.Now().Format("150405.000000")+"-e1", accID, domain.DirectionDebit, m)
		e2, _ := domain.NewEntry(time.Now().Format("150405.000000")+"-e2", offsetAccID, domain.DirectionCredit, m)
		tx, _ := domain.NewTransaction(time.Now().Format("150405.000000")+"-tx", time.Now().UTC(), "İşlem", []domain.Entry{e1, e2})
		_ = repo.SaveTransaction(ctx, tx)
		time.Sleep(5 * time.Millisecond)
	}

	// 2. Bir snapshot kaydet (6000 USD)
	snapBal := domain.MustNewMoney(6000, domain.USD)
	lastEntry := "snap-entry-mark"
	snap := snapshot.NewAccountSnapshot("snap-test-1", accID, snapBal, &lastEntry, 3, time.Now().UTC())
	if err := repo.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("snapshot kaydedilemedi: %v", err)
	}

	// 3. Snapshot sonrasında 1 işlem daha kaydet (+4000 cents -> Toplam: 10000)
	mExtra := domain.MustNewMoney(4000, domain.USD)
	e3, _ := domain.NewEntry(time.Now().Format("150405.000000")+"-e3", accID, domain.DirectionDebit, mExtra)
	e4, _ := domain.NewEntry(time.Now().Format("150405.000000")+"-e4", offsetAccID, domain.DirectionCredit, mExtra)
	txExtra, _ := domain.NewTransaction(time.Now().Format("150405.000000")+"-tx2", time.Now().UTC(), "Sonraki İşlem", []domain.Entry{e3, e4})
	_ = repo.SaveTransaction(ctx, txExtra)

	// 4. Anlık bakiye motorunu çalıştır
	fastBalance, err := repo.GetAccountBalanceFast(ctx, accID)
	if err != nil {
		t.Fatalf("hızlı bakiye hesaplanamadı: %v", err)
	}

	// 5. Doğrulama: Anlık bakiye 10000 olmalı
	if fastBalance.Amount() != 10000 {
		t.Errorf("beklenen bakiye 10000 (6000 snapshot + 4000 delta), alınan: %d", fastBalance.Amount())
	}
}

func TestPostgres_CryptographicHashChainAndAudit(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresLedgerRepository(pool)
	accID, offsetAccID := setupTestAccounts(t, repo)

	// 1. İki işlem kaydet
	m1 := domain.MustNewMoney(5000, domain.USD)
	e1, _ := domain.NewEntry("e-audit-1", accID, domain.DirectionDebit, m1)
	e2, _ := domain.NewEntry("e-audit-2", offsetAccID, domain.DirectionCredit, m1)
	tx1, _ := domain.NewTransaction("tx-audit-1", time.Now().UTC(), "İşlem 1", []domain.Entry{e1, e2})
	if err := repo.SaveTransaction(ctx, tx1); err != nil {
		t.Fatal(err)
	}

	m2 := domain.MustNewMoney(3000, domain.USD)
	e3, _ := domain.NewEntry("e-audit-3", accID, domain.DirectionDebit, m2)
	e4, _ := domain.NewEntry("e-audit-4", offsetAccID, domain.DirectionCredit, m2)
	tx2, _ := domain.NewTransaction("tx-audit-2", time.Now().UTC(), "İşlem 2", []domain.Entry{e3, e4})
	if err := repo.SaveTransaction(ctx, tx2); err != nil {
		t.Fatal(err)
	}

	// 2. Hash zincirini doğrula
	auditor := audit.NewAuditor(repo)
	res, err := auditor.AuditAccount(ctx, accID)
	if err != nil {
		t.Fatalf("denetim başarısız olmamalıydı: %v", err)
	}
	if res.IsTampered {
		t.Fatal("zincir manipüle edilmiş olarak işaretlenmemeli")
	}
	if res.TotalEntries != 2 {
		t.Errorf("2 kayıt denetlenmeliydi, denetlenen: %d", res.TotalEntries)
	}
}
