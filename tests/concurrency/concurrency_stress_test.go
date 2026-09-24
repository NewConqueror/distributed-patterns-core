package concurrency_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/audit"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/db"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/repository"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/snapshot"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/usecase"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cfg := db.DefaultDockerConfig()
	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		localCfg := db.DefaultLocalConfig()
		pool, err = db.NewPool(ctx, localCfg)
		if err != nil {
			t.Skipf("PostgreSQL veritabanı aktif değil, eşzamanlılık testi atlanıyor: %v", err)
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

func TestConcurrency_50Goroutines_DepositAndWithdraw(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresLedgerRepository(pool)
	service := usecase.NewLedgerService(repo)

	// 1. Hesapları oluştur
	nowID := fmt.Sprintf("%d", time.Now().UnixNano())
	targetAccID := "acc-target-" + nowID // Ana hesap
	offsetAccID := "acc-offset-" + nowID // Karşı hesap (Kasa/Banka)

	targetAcc, err := domain.NewAccount(targetAccID, "Müşteri Varlık Hesabı", domain.AccountTypeAsset, domain.USD)
	if err != nil {
		t.Fatal(err)
	}
	offsetAcc, err := domain.NewAccount(offsetAccID, "Merkez Takas Hesabı", domain.AccountTypeLiability, domain.USD)
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.SaveAccount(ctx, targetAcc); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveAccount(ctx, offsetAcc); err != nil {
		t.Fatal(err)
	}

	// 2. Başlangıç Sermayesi: 500.00 USD (50,000 cents) yatır
	initDeposit := 50000
	initPayload, _ := json.Marshal(usecase.CreateTransactionRequest{
		TransactionID: "tx-init-" + nowID,
		Description:   "Başlangıç Sermayesi",
		Currency:      "USD",
		Entries: []usecase.EntryDTO{
			{AccountID: targetAccID, Direction: "DEBIT", Amount: int64(initDeposit)},
			{AccountID: offsetAccID, Direction: "CREDIT", Amount: int64(initDeposit)},
		},
	})
	_, initCode, err := service.CreateTransactionWithPessimisticLock(ctx, uuid.NewString(), initPayload)
	if err != nil || initCode != 201 {
		t.Fatalf("başlangıç işlemi başarısız: %d, %v", initCode, err)
	}

	// 3. Eşzamanlı 50 Goroutine Hazırla:
	// 25 goroutine: +10.00 USD (1,000 cents) Yatırma (DEBIT)
	// 25 goroutine: -10.00 USD (1,000 cents) Çekme (CREDIT)
	totalWorkers := 50
	depositsCount := 25
	withdrawalsCount := 25
	amountPerOp := int64(1000) // 10 USD

	startGate := make(chan struct{}) // 50 worker'ı tam aynı anda salmak için senkronizasyon kapısı
	var wg sync.WaitGroup
	errChan := make(chan error, totalWorkers)

	for i := 0; i < totalWorkers; i++ {
		wg.Add(1)
		workerIdx := i
		isDeposit := workerIdx < depositsCount

		go func() {
			defer wg.Done()
			<-startGate // Tüm goroutine'ler kapıda bekler, aynı milisaniyede yarışa başlar

			txID := fmt.Sprintf("tx-%s-w%d", nowID, workerIdx)
			idempKey := uuid.NewString()

			var entries []usecase.EntryDTO
			var desc string
			if isDeposit {
				// Yatırma: Target hesaba Borç (Debit), Karşı hesaba Alacak (Credit)
				desc = fmt.Sprintf("Worker %d Para Yatırma", workerIdx)
				entries = []usecase.EntryDTO{
					{AccountID: targetAccID, Direction: "DEBIT", Amount: amountPerOp},
					{AccountID: offsetAccID, Direction: "CREDIT", Amount: amountPerOp},
				}
			} else {
				// Çekme: Target hesaba Alacak (Credit), Karşı hesaba Borç (Debit)
				desc = fmt.Sprintf("Worker %d Para Çekme", workerIdx)
				entries = []usecase.EntryDTO{
					{AccountID: targetAccID, Direction: "CREDIT", Amount: amountPerOp},
					{AccountID: offsetAccID, Direction: "DEBIT", Amount: amountPerOp},
				}
			}

			payload, _ := json.Marshal(usecase.CreateTransactionRequest{
				TransactionID: txID,
				Description:   desc,
				Currency:      "USD",
				Entries:       entries,
			})

			_, code, err := service.CreateTransactionWithPessimisticLock(ctx, idempKey, payload)
			if err != nil || code != 201 {
				errChan <- fmt.Errorf("worker %d başarısız (kod: %d): %w", workerIdx, code, err)
			}
		}()
	}

	// Kapıyı aç, 50 goroutine aynı anda vursun
	close(startGate)
	wg.Wait()
	close(errChan)

	// Hataları kontrol et
	for workerErr := range errChan {
		t.Errorf("eşzamanlılık hatası: %v", workerErr)
	}

	// 4. Doğrulama 1: Bakiye Denkliği
	// Başlangıç: 50,000 cents
	// + 25 * 1,000 = +25,000 cents
	// - 25 * 1,000 = -25,000 cents
	// Nihai Bakiye: 50,000 cents (500.00 USD)
	expectedFinalBalance := int64(initDeposit)

	updatedTarget, err := repo.GetAccountByID(ctx, targetAccID)
	if err != nil {
		t.Fatalf("güncel hesap sorgulanamadı: %v", err)
	}
	if updatedTarget.Balance().Amount() != expectedFinalBalance {
		t.Fatalf("BAKİYE TUTARSIZLIĞI! Beklenen: %d, Alınan: %d",
			expectedFinalBalance, updatedTarget.Balance().Amount())
	}
	if updatedTarget.Balance().IsNegative() {
		t.Fatalf("KRİTİK HATA: Hesap bakiyesi negatif oldu!")
	}

	// 5. Doğrulama 2: Snapshotting Engine Doğrulaması
	// Snapshot Tutarı + Delta Entry toplamının tam uyuştuğunu doğrula
	snapWorker := snapshot.NewSnapshotWorker(snapshot.DefaultWorkerConfig(), repo, repo)
	if err := snapWorker.ForceSnapshotNow(ctx, targetAccID); err != nil {
		t.Fatalf("snapshot oluşturulamadı: %v", err)
	}

	// Snapshot anındaki bakiye sorgusu
	fastBal1, err := repo.GetAccountBalanceFast(ctx, targetAccID)
	if err != nil {
		t.Fatalf("hızlı bakiye hesaplanamadı: %v", err)
	}
	if fastBal1.Amount() != expectedFinalBalance {
		t.Errorf("Snapshot motoru bakiyesi uyuşmuyor: beklenen %d, alınan: %d",
			expectedFinalBalance, fastBal1.Amount())
	}

	// Snapshot sonrasında 1 ek işlem (+500 cents = +5.00 USD) ekle
	deltaID := fmt.Sprintf("%d", time.Now().UnixNano())
	deltaPayload, _ := json.Marshal(usecase.CreateTransactionRequest{
		TransactionID: "tx-delta-" + deltaID,
		Description:   "Snapshot Sonrası Delta İşlem",
		Currency:      "USD",
		Entries: []usecase.EntryDTO{
			{AccountID: targetAccID, Direction: "DEBIT", Amount: 500},
			{AccountID: offsetAccID, Direction: "CREDIT", Amount: 500},
		},
	})
	_, deltaCode, err := service.CreateTransactionWithPessimisticLock(ctx, uuid.NewString(), deltaPayload)
	if err != nil || deltaCode != 201 {
		t.Fatalf("delta işlem eklenemedi: %d, %v", deltaCode, err)
	}

	// Formül: Anlık Bakiye = Snapshot Tutarı (50,000) + Delta (500) = 50,500 cents
	fastBal2, err := repo.GetAccountBalanceFast(ctx, targetAccID)
	if err != nil {
		t.Fatalf("hızlı bakiye hesaplanamadı: %v", err)
	}
	expectedWithDelta := expectedFinalBalance + 500
	if fastBal2.Amount() != expectedWithDelta {
		t.Errorf("Snapshot sonrası formül uyuşmuyor: beklenen %d (50000 snap + 500 delta), alınan: %d",
			expectedWithDelta, fastBal2.Amount())
	}

	// 6. Doğrulama 3: Kriptografik Denetim İzi (Audit Trail)
	// 50 eşzamanlı işlem sonrasında hash zincirinin bozulmadığını matematiksel olarak ispatla
	auditor := audit.NewAuditor(repo)
	auditRes, err := auditor.AuditAccount(ctx, targetAccID)
	if err != nil {
		t.Fatalf("kriptografik denetim başarısız oldu: %v", err)
	}
	if auditRes.IsTampered {
		t.Fatalf("KRİTİK GÜVENLİK AÇIĞI: Eşzamanlı işlemler hash zincirini kırdı! Detay: %v", auditRes.TamperedError)
	}
	expectedTotalEntries := int64(1 + depositsCount + withdrawalsCount + 1) // 1 init + 50 ops + 1 delta = 52
	if auditRes.TotalEntries != expectedTotalEntries {
		t.Errorf("toplam girdi sayısı uyuşmuyor: beklenen %d, denetlenen: %d",
			expectedTotalEntries, auditRes.TotalEntries)
	}

	t.Logf("BAŞARILI: 50 eşzamanlı goroutine ile yarış koşulsuz işlem tamamlandı! Nihai bakiye: %s, Denetlenen girdi: %d",
		updatedTarget.Balance().String(), auditRes.TotalEntries)
}
