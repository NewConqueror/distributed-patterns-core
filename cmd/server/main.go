package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flashsale/internal/config"
	"flashsale/internal/database"
	"flashsale/internal/handler"
	"flashsale/internal/worker"
)

func main() {
	cfg := config.Load()

	db, err := database.NewPostgresDB(cfg)
	if err != nil {
		log.Fatalf("[FATAL] Database initialization failed: %v\n", err)
	}
	defer db.Close()

	redisSvc, err := database.NewRedisService(cfg)
	if err != nil {
		log.Fatalf("[FATAL] Redis initialization failed: %v\n", err)
	}
	defer redisSvc.Client.Close()

	// Start Background Batch Consumer Worker (50ms / 100-item batches)
	batchConsumer := worker.NewBatchConsumer(cfg, db, redisSvc)
	batchConsumer.Start(context.Background())
	defer batchConsumer.Stop()

	mux := http.NewServeMux()
	h := handler.NewHandler(db, redisSvc)
	h.RegisterRoutes(mux)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown channel
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[SERVER] Flash Sale Engine listening on port :%s\n", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] Server error: %v\n", err)
		}
	}()

	<-stop
	log.Println("[SERVER] Shutting down gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("[SERVER] Server forced shutdown error: %v\n", err)
	}

	log.Println("[SERVER] Server exited cleanly.")
}
