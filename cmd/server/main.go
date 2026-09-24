package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/domain"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/db"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/infrastructure/repository"
	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/usecase"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	pool    *pgxpool.Pool
	service *usecase.LedgerService
	repo    *repository.PostgresLedgerRepository
}

func main() {
	ctx := context.Background()

	// 1. Veritabanı bağlantı havuzunu başlat
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
				log.Fatalf("Veritabanına bağlanılamadı: %v", err)
			}
		}
	}
	defer pool.Close()

	repo := repository.NewPostgresLedgerRepository(pool)
	service := usecase.NewLedgerService(repo)

	s := &Server{
		pool:    pool,
		service: service,
		repo:    repo,
	}

	// 2. Yük testi için tohum (seed) hesapları hazırla
	if err := s.seedAccounts(ctx); err != nil {
		log.Printf("Hesap tohumlama uyarısı: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /transactions", s.handleCreateTransaction)
	mux.HandleFunc("GET /accounts/balance", s.handleGetBalance)
	mux.HandleFunc("GET /audit", s.handleAudit)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})

	httpServer := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("Financial Ledger HTTP Sunucusu :8080 üzerinde dinlemede...")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP sunucu hatası: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Sunucu kapatılıyor...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	log.Println("Sunucu durduruldu.")
}

func (s *Server) seedAccounts(ctx context.Context) error {
	// Yük testinde kullanılacak 100 adet hesap oluştur (50 Asset, 50 Liability)
	for i := 1; i <= 100; i++ {
		id := fmt.Sprintf("acc-load-%03d", i)
		accType := domain.AccountTypeAsset
		if i > 50 {
			accType = domain.AccountTypeLiability
		}
		acc, _ := domain.NewAccount(id, fmt.Sprintf("Yük Testi Hesabı %d", i), accType, domain.USD)
		_ = s.repo.SaveAccount(ctx, acc)
	}
	return nil
}

func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	idempKey := r.Header.Get("Idempotency-Key")
	if idempKey == "" {
		idempKey = uuid.NewString()
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Gövde okunamadı", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	resp, code, err := s.service.CreateTransactionWithPessimisticLock(r.Context(), idempKey, body)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleGetBalance(w http.ResponseWriter, r *http.Request) {
	accID := r.URL.Query().Get("account_id")
	if accID == "" {
		http.Error(w, "account_id parametresi gerekli", http.StatusBadRequest)
		return
	}

	bal, err := s.service.GetAccountBalance(r.Context(), accID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"account_id": accID,
		"balance":    bal.Amount(),
		"currency":   bal.Currency().Code(),
		"formatted":  bal.String(),
	})
}

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	report, err := s.service.AuditEntireLedger(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(report)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stat := s.pool.Stat()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"total_conns":    stat.TotalConns(),
		"acquired_conns": stat.AcquiredConns(),
		"idle_conns":     stat.IdleConns(),
		"max_conns":      stat.MaxConns(),
	})
}
