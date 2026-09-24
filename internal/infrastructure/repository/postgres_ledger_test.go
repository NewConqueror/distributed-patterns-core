package repository_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/db"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/repository"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Önce 5433 (docker), sonra 5432 dener
	cfg := db.DefaultDockerConfig()
	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		// Fallback local port 5432
		cfg.Port = 5432
		pool, err = db.NewPool(ctx, cfg)
		if err != nil {
			t.Skipf("PostgreSQL veritabanı aktif değil, entegrasyon testi atlanıyor: %v", err)
			return nil
		}
	}

	// Şemayı yükle
	migrationPath, _ := filepath.Abs("../../../migrations/001_init_schema.sql")
	if err := db.RunMigrations(context.Background(), pool, migrationPath); err != nil {
		t.Fatalf("migration çalıştırılamadı: %v", err)
	}

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

	// -------------------------------------------------------------
	// Test 2.1: Immutability Trigger (UPDATE / DELETE Yasak)
	// -------------------------------------------------------------
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

	t.Run("Immutability_UpdateTransaction_RejectedByTrigger", func(t *testing.T) {
		_, err := pool.Exec(ctx, "UPDATE transactions SET description = 'Hileli' WHERE id = $1", txID)
		if err == nil {
			t.Fatal("UPDATE transactions başarılı oldu, tetikleyici çalışmadı!")
		}
		if !strings.Contains(err.Error(), "Finansal Değişmezlik İhlali") {
			t.Fatalf("beklenen tetikleyici hatası alınmadı: %v", err)
		}
	})

	// -------------------------------------------------------------
	// Test 2.1: entries.amount > 0 CHECK Constraint
	// -------------------------------------------------------------
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

	// -------------------------------------------------------------
	// Test 2.3: DEFERRABLE INITIALLY DEFERRED Constraint Trigger
	// Doğrudan SQL ile dengesiz işlem yazılmaya çalışıldığında COMMIT anında patlamalı!
	// -------------------------------------------------------------
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

		// 100 USD Borç
		_, err = pgTx.Exec(ctx, `
			INSERT INTO entries (id, transaction_id, account_id, direction, amount)
			VALUES ($1, $2, $3, 'DEBIT', 10000)
		`, "e-ub-1", unbalancedTxID, acc1)
		if err != nil {
			t.Fatal(err)
		}

		// 70 USD Alacak (30 USD eksik -> Dengesiz!)
		_, err = pgTx.Exec(ctx, `
			INSERT INTO entries (id, transaction_id, account_id, direction, amount)
			VALUES ($1, $2, $3, 'CREDIT', 7000)
		`, "e-ub-2", unbalancedTxID, acc2)
		if err != nil {
			t.Fatal(err)
		}

		// COMMIT aşamasında tetikleyici fırlamalı!
		err = pgTx.Commit(ctx)
		if err == nil {
			t.Fatal("dengesiz işlem COMMIT edilebildi! DEFERRABLE TRIGGER çalışmadı!")
		}
		if !strings.Contains(err.Error(), "Çift Taraflı Muhasebe Denkliği İhlali") {
			t.Fatalf("beklenen bakiye denkliği hatası alınmadı: %v", err)
		}
	})
}
