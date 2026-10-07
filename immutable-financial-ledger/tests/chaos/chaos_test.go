package chaos_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/audit"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/db"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/repository"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/usecase"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var pool *pgxpool.Pool
	var err error

	if _, statErr := os.Stat("/var/run/postgresql/.s.PGSQL.5432"); statErr == nil {
		localCfg := db.DefaultLocalConfig()
		pool, err = db.NewPool(ctx, localCfg)
	}

	if pool == nil || err != nil {
		cfg := db.DefaultDockerConfig()
		pool, err = db.NewPool(ctx, cfg)
		if err != nil {
			localCfg := db.DefaultLocalConfig()
			pool, err = db.NewPool(ctx, localCfg)
			if err != nil {
				t.Skipf("PostgreSQL veritabanı aktif değil, kaos testi atlanıyor: %v", err)
				return nil
			}
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

// TestHelperChaosProcess, SIGKILL simülasyonu için ayrı bir işletim sistemi prosesi olarak çalıştırılır.
// BE_CHAOS_WORKER=1 ortam değişkeni ile çağrıldığında çalışır; aksi halde normal test koşusunda pas geçilir.
func TestHelperChaosProcess(t *testing.T) {
	if os.Getenv("BE_CHAOS_WORKER") != "1" {
		return
	}

	ctx := context.Background()
	pool := getTestPool(t)
	if pool == nil {
		os.Exit(1)
	}
	defer pool.Close()

	txID := os.Getenv("CHAOS_TX_ID")
	idempKey := os.Getenv("CHAOS_IDEMP_KEY")
	acc1 := os.Getenv("CHAOS_ACC_1")
	acc2 := os.Getenv("CHAOS_ACC_2")

	// 1. PostgreSQL transaction başlat
	pgTx, err := pool.Begin(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgTx begin error: %v\n", err)
		os.Exit(1)
	}

	// 2. Idempotency kaydını IN_FLIGHT olarak ekle
	_, err = pgTx.Exec(ctx, `
		INSERT INTO idempotency_keys (key, request_hash, status, locked_at)
		VALUES ($1, $2, 'IN_FLIGHT', NOW())
		ON CONFLICT (key) DO UPDATE SET status = 'IN_FLIGHT', locked_at = NOW()
	`, idempKey, "hash-"+txID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "idempotency insert error: %v\n", err)
		os.Exit(1)
	}

	// 3. Transactions kaydını ekle
	_, err = pgTx.Exec(ctx, `
		INSERT INTO transactions (id, description, currency, prev_hash, current_hash, sequence_num, created_at)
		VALUES ($1, 'Kaos testi işlemi', 'USD', $2, $3, 999999, NOW())
	`, txID, audit.ZeroGenesisHash, "fake-current-hash")
	if err != nil {
		fmt.Fprintf(os.Stderr, "transaction insert error: %v\n", err)
		os.Exit(1)
	}

	// 4. Entry 1 (DEBIT) ekle
	_, err = pgTx.Exec(ctx, `
		INSERT INTO entries (id, transaction_id, account_id, direction, amount, prev_hash, current_hash, sequence_num, created_at)
		VALUES ($1, $2, $3, 'DEBIT', 500, $4, $5, 999999, NOW())
	`, txID+"-e1", txID, acc1, audit.ZeroGenesisHash, "fake-entry-hash-1")
	if err != nil {
		fmt.Fprintf(os.Stderr, "entry 1 insert error: %v\n", err)
		os.Exit(1)
	}

	// 5. Entry 2 (CREDIT) ekle
	_, err = pgTx.Exec(ctx, `
		INSERT INTO entries (id, transaction_id, account_id, direction, amount, prev_hash, current_hash, sequence_num, created_at)
		VALUES ($1, $2, $3, 'CREDIT', 500, $4, $5, 999999, NOW())
	`, txID+"-e2", txID, acc2, audit.ZeroGenesisHash, "fake-entry-hash-2")
	if err != nil {
		fmt.Fprintf(os.Stderr, "entry 2 insert error: %v\n", err)
		os.Exit(1)
	}

	// COMMIT ÇAĞRILMADAN HEMEN ÖNCE: Ana teste hazır olduğunu bildir ve bekle!
	fmt.Println("READY_FOR_KILL")
	_ = os.Stdout.Sync()

	// Ana prosesin bizi SIGKILL ile öldürmesini bekle (asla commit çağırma!)
	time.Sleep(30 * time.Second)
	os.Exit(0)
}

// TestChaos_SIGKILL_MidTransaction, tam transaction ortasında (commit edilmeden hemen önce)
// uygulamanın işletim sistemi düzeyinde zorla sonlandırıldığı (SIGKILL / kill -9) felaket senaryosunu test eder.
//
// Doğrulanan Maddeler:
// 1. Veritabanında hiçbir zaman yarım veya yetim (orphan) kayıt kalmaz (Atomicity & Crash Consistency).
// 2. Rollback otomatik gerçekleşir; transactions ve entries tablolarında hiçbir kayıt oluşmaz.
// 3. Sistem kurtarıldıktan sonra aynı işlem baştan güvenle ve idempotent şekilde tekrar yürütülebilir.
func TestChaos_SIGKILL_MidTransaction(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresLedgerRepository(pool)
	service := usecase.NewLedgerService(repo)

	// Test hesapları oluştur
	acc1ID := fmt.Sprintf("acc-chaos-asset-%d", time.Now().UnixNano())
	acc2ID := fmt.Sprintf("acc-chaos-liab-%d", time.Now().UnixNano())

	acc1, err := domain.NewAccount(acc1ID, "Kaos Varlık Hesabı", domain.AccountTypeAsset, domain.USD)
	if err != nil {
		t.Fatalf("Hesap 1 oluşturulamadı: %v", err)
	}
	acc2, err := domain.NewAccount(acc2ID, "Kaos Borç Hesabı", domain.AccountTypeLiability, domain.USD)
	if err != nil {
		t.Fatalf("Hesap 2 oluşturulamadı: %v", err)
	}
	if err := repo.SaveAccount(ctx, acc1); err != nil {
		t.Fatalf("Hesap 1 kaydedilemedi: %v", err)
	}
	if err := repo.SaveAccount(ctx, acc2); err != nil {
		t.Fatalf("Hesap 2 kaydedilemedi: %v", err)
	}

	chaosTxID := fmt.Sprintf("tx-chaos-%d", time.Now().UnixNano())
	chaosIdempKey := fmt.Sprintf("idemp-chaos-%d", time.Now().UnixNano())

	// 1. Alt prosesi (worker) başlat
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperChaosProcess")
	cmd.Env = append(os.Environ(),
		"BE_CHAOS_WORKER=1",
		fmt.Sprintf("CHAOS_TX_ID=%s", chaosTxID),
		fmt.Sprintf("CHAOS_IDEMP_KEY=%s", chaosIdempKey),
		fmt.Sprintf("CHAOS_ACC_1=%s", acc1ID),
		fmt.Sprintf("CHAOS_ACC_2=%s", acc2ID),
	)

	cmd.Stderr = os.Stderr
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("Stdout pipe alınamadı: %v", err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("Kaos alt prosesi başlatılamadı: %v", err)
	}

	// 2. Alt prosesin tam commit öncesi satırları yazdığını ve hazır olduğunu bildirten sinyali bekle
	scanner := bufio.NewScanner(stdoutPipe)
	readyReceived := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "READY_FOR_KILL" {
			readyReceived = true
			break
		}
	}

	if !readyReceived {
		_ = cmd.Process.Kill()
		t.Fatalf("Alt prosesten READY_FOR_KILL sinyali alınamadı")
	}

	// 3. SENARYO GERÇEKLEŞTİRİLİYOR: Uygulama tam bu milisaniyede SIGKILL (kill -9) ile katlediliyor!
	t.Logf(">>> KAOS TETİKLENDİ: Alt proses (PID: %d) tam commit öncesinde SIGKILL ile öldürülüyor!", cmd.Process.Pid)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("Proses kill edilemedi: %v", err)
	}
	_ = cmd.Wait() // Prosesin kapandığını teyit et

	// PostgreSQL backend'in kopan soketi fark edip temizlemesi için çok kısa bir bekleme
	time.Sleep(100 * time.Millisecond)

	// 4. FELAKET SONRASI DOĞRULAMA (Post-Crash Verification)
	// 4.1. transactions tablosunda bu id'ye ait hiçbir kayıt OLMAMALI!
	var txCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM transactions WHERE id = $1", chaosTxID).Scan(&txCount)
	if err != nil {
		t.Fatalf("Transactions sorgulanamadı: %v", err)
	}
	if txCount != 0 {
		t.Fatalf("KRİTİK HATA: SIGKILL sonrası yarım transaction veritabanında kaldı! Sayı: %d", txCount)
	}
	t.Logf("✓ Doğrulandı: Transactions tablosunda yarım/yetim kayıt yok (count = %d).", txCount)

	// 4.2. entries tablosunda bu işleme ait hiçbir girdi OLMAMALI!
	var entryCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM entries WHERE transaction_id = $1", chaosTxID).Scan(&entryCount)
	if err != nil {
		t.Fatalf("Entries sorgulanamadı: %v", err)
	}
	if entryCount != 0 {
		t.Fatalf("KRİTİK HATA: SIGKILL sonrası yarım entry veritabanında kaldı! Sayı: %d", entryCount)
	}
	t.Logf("✓ Doğrulandı: Entries tablosunda hiçbir yetim kayıt kalmadı (count = %d).", entryCount)

	// 4.3. Idempotency tablosunda yarım kalan uncommitted kayıt olmamalı
	var idempStatus string
	err = pool.QueryRow(ctx, "SELECT status FROM idempotency_keys WHERE key = $1", chaosIdempKey).Scan(&idempStatus)
	if err != nil && err != pgx.ErrNoRows {
		t.Fatalf("Idempotency sorgulanamadı: %v", err)
	}
	if err == nil && idempStatus == "RESOLVED" {
		t.Fatalf("KRİTİK HATA: Kill edilen işlem RESOLVED olarak işaretlenmiş olamaz!")
	}
	t.Logf("✓ Doğrulandı: Idempotency durumu tutarlı (kayıt yok veya rollback oldu).")

	// 5. KURTARMA VE YENİDEN YÜRÜTME (Recovery & Re-execution)
	// Sistem ayağa kalktığında bu transfer isteği baştan normal şekilde çalıştırılabilmelidir.
	recoveryReq := usecase.CreateTransactionRequest{
		TransactionID: chaosTxID,
		Description:   "Kaos sonrası kurtarılan işlem",
		Currency:      "USD",
		Entries: []usecase.EntryDTO{
			{AccountID: acc1ID, Direction: "DEBIT", Amount: 750},
			{AccountID: acc2ID, Direction: "CREDIT", Amount: 750},
		},
	}
	reqBytes, _ := json.Marshal(recoveryReq)

	res, code, err := service.CreateTransaction(ctx, chaosIdempKey, reqBytes)
	if err != nil {
		t.Fatalf("Kaos sonrası kurtarma işlemi başarısız: %v (code: %d)", err, code)
	}
	if code != 201 || res.Status != "RESOLVED" {
		t.Fatalf("Beklenmeyen kurtarma yanıtı: code=%d, status=%s", code, res.Status)
	}
	t.Logf("✓ Doğrulandı: Sistem kurtarıldıktan sonra işlem başarıyla yürütüldü (status: %s, code: %d).", res.Status, code)

	// 6. Son bakiye doğrulaması
	bal1, err := service.GetAccountBalance(ctx, acc1ID)
	if err != nil {
		t.Fatalf("Bakiye okunamadı: %v", err)
	}
	if bal1.Amount() != 750 {
		t.Fatalf("Bakiye hatalı: beklenen 750, gelen %d", bal1.Amount())
	}
	t.Logf("✓ Doğrulandı: Hesap bakiyesi tam olarak 750 USD ve defter dengesi mükemmel.")
}

// TestChaos_AbruptConnectionDrop_MidTransaction, veritabanı bağlantısının işlem ortasında
// ağ çökmesi veya socket kapanmasıyla aniden sonlandırılması durumunda atomikliği doğrular.
func TestChaos_AbruptConnectionDrop_MidTransaction(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	ctx := context.Background()

	dropTxID := fmt.Sprintf("tx-chaos-drop-%d", time.Now().UnixNano())
	accID := fmt.Sprintf("acc-chaos-drop-%d", time.Now().UnixNano())

	repo := repository.NewPostgresLedgerRepository(pool)
	dropAcc, err := domain.NewAccount(accID, "Bağlantı Kopma Hesabı", domain.AccountTypeAsset, domain.USD)
	if err != nil {
		t.Fatalf("Hesap oluşturulamadı: %v", err)
	}
	if err := repo.SaveAccount(ctx, dropAcc); err != nil {
		t.Fatalf("Hesap kaydedilemedi: %v", err)
	}

	// Ayrı bir ham bağlantı al
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Bağlantı alınamadı: %v", err)
	}

	pgTx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		t.Fatalf("Transaction başlatılamadı: %v", err)
	}

	// Girdileri yaz
	_, err = pgTx.Exec(ctx, `
		INSERT INTO transactions (id, description, currency, prev_hash, current_hash, sequence_num, created_at)
		VALUES ($1, 'Bağlantı kopma testi', 'USD', $2, $3, 888888, NOW())
	`, dropTxID, audit.ZeroGenesisHash, "fake-hash-drop")
	if err != nil {
		conn.Release()
		t.Fatalf("Transaction insert hatası: %v", err)
	}

	_, err = pgTx.Exec(ctx, `
		INSERT INTO entries (id, transaction_id, account_id, direction, amount, prev_hash, current_hash, sequence_num, created_at)
		VALUES ($1, $2, $3, 'DEBIT', 900, $4, $5, 888888, NOW())
	`, dropTxID+"-e1", dropTxID, accID, audit.ZeroGenesisHash, "fake-entry-hash-drop")
	if err != nil {
		conn.Release()
		t.Fatalf("Entry insert hatası: %v", err)
	}

	// COMMIT YAPILMADAN: Bağlantıyı soket düzeyinde sertçe imha et
	t.Log(">>> KAOS TETİKLENDİ: PostgreSQL soket bağlantısı COMMIT öncesinde zorla kapatılıyor (Hard Close)!")
	conn.Conn().Close(context.Background())
	conn.Release()

	// Başka bir sağlıklı bağlantıyla veritabanını doğrula
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM transactions WHERE id = $1", dropTxID).Scan(&count)
	if err != nil {
		t.Fatalf("Transactions sorgulanamadı: %v", err)
	}
	if count != 0 {
		t.Fatalf("KRİTİK HATA: Soket kopmasına rağmen yarım kayıt veritabanında commit edilmiş! count=%d", count)
	}

	var entryCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM entries WHERE transaction_id = $1", dropTxID).Scan(&entryCount)
	if err != nil {
		t.Fatalf("Entries sorgulanamadı: %v", err)
	}
	if entryCount != 0 {
		t.Fatalf("KRİTİK HATA: Soket kopmasına rağmen yetim entry veritabanında kaldı! count=%d", entryCount)
	}

	t.Logf("✓ Doğrulandı: Soket kopması sonucu PostgreSQL oturumu otomatik abort etti, sıfır yetim kayıt.")
}
