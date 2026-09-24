package usecase_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/db"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/repository"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/usecase"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := db.DefaultDockerConfig()
	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		localCfg := db.DefaultLocalConfig()
		pool, err = db.NewPool(ctx, localCfg)
		if err != nil {
			t.Skipf("PostgreSQL veritabanı aktif değil, entegrasyon testi atlanıyor: %v", err)
			return nil
		}
	}

	m1, _ := filepath.Abs("../../migrations/001_init_schema.sql")
	_ = db.RunMigrations(context.Background(), pool, m1)
	m2, _ := filepath.Abs("../../migrations/002_concurrency_and_snapshots.sql")
	_ = db.RunMigrations(context.Background(), pool, m2)
	m3, _ := filepath.Abs("../../migrations/003_cryptographic_audit_trail.sql")
	_ = db.RunMigrations(context.Background(), pool, m3)

	return pool
}

func TestLedgerService_EndToEnd_IdempotencyAndDoubleEntry(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresLedgerRepository(pool)
	service := usecase.NewLedgerService(repo)

	// Hesapları aç
	accCashID := "acc-cash-" + time.Now().Format("150405.000000")
	accRevID := "acc-rev-" + time.Now().Format("150405.000000")

	cashAcc, _ := domain.NewAccount(accCashID, "Kasa", domain.AccountTypeAsset, domain.USD)
	revAcc, _ := domain.NewAccount(accRevID, "Gelir", domain.AccountTypeRevenue, domain.USD)

	_ = repo.SaveAccount(ctx, cashAcc)
	_ = repo.SaveAccount(ctx, revAcc)

	idempKey := "uuid-" + time.Now().Format("150405.000000")
	txID := "tx-" + time.Now().Format("150405.000000")

	payload1 := []byte(`{
		"transaction_id": "` + txID + `",
		"description": "Müşteri Tahsilatı",
		"currency": "USD",
		"entries": [
			{"account_id": "` + accCashID + `", "direction": "DEBIT", "amount": 25000},
			{"account_id": "` + accRevID + `", "direction": "CREDIT", "amount": 25000}
		]
	}`)

	// -------------------------------------------------------------
	// Senaryo 1: İlk İstek (IN_FLIGHT -> RESOLVED)
	// -------------------------------------------------------------
	resp1, code1, err := service.CreateTransaction(ctx, idempKey, payload1)
	if err != nil {
		t.Fatalf("Senaryo 1 başarısız: %v", err)
	}
	if code1 != 201 {
		t.Errorf("beklenen 201, alınan: %d", code1)
	}
	if resp1.IsCached {
		t.Errorf("ilk istek önbellek olmamalı")
	}

	// -------------------------------------------------------------
	// Senaryo 2: Aynı Key + Aynı Body -> Doğrudan Kaydedilmiş Yanıt (IsCached=true)
	// -------------------------------------------------------------
	resp2, code2, err := service.CreateTransaction(ctx, idempKey, payload1)
	if err != nil {
		t.Fatalf("Senaryo 2 başarısız: %v", err)
	}
	if code2 != 201 {
		t.Errorf("beklenen 201, alınan: %d", code2)
	}
	if !resp2.IsCached {
		t.Errorf("ikinci istek önbellekten dönmeliydi (IsCached=true)")
	}

	// -------------------------------------------------------------
	// Senaryo 3: Aynı Key + Farklı Body -> 422 Unprocessable Entity
	// -------------------------------------------------------------
	payloadDifferent := []byte(`{
		"transaction_id": "` + txID + `",
		"description": "Farklı Tutar",
		"currency": "USD",
		"entries": [
			{"account_id": "` + accCashID + `", "direction": "DEBIT", "amount": 99999},
			{"account_id": "` + accRevID + `", "direction": "CREDIT", "amount": 99999}
		]
	}`)
	_, code3, err := service.CreateTransaction(ctx, idempKey, payloadDifferent)
	if err == nil || code3 != 422 {
		t.Fatalf("farklı payload ile aynı key 422 vermeliydi, alınan kod: %d, err: %v", code3, err)
	}

	// -------------------------------------------------------------
	// Bakiye Doğrulaması: Kasa hesabının bakiyesi 250.00 USD olmalı
	// -------------------------------------------------------------
	updatedCash, err := repo.GetAccountByID(ctx, accCashID)
	if err != nil {
		t.Fatal(err)
	}
	if updatedCash.Balance().Amount() != 25000 {
		t.Errorf("kasa bakiyesi 25000 olmalıydı, alınan: %d", updatedCash.Balance().Amount())
	}
}
