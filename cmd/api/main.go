package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/document"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/http/handler"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/http/middleware"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/engine"
	rebacgrpc "Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/grpc"
	rebacv1 "Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/proto/rebac/v1"

	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := database.DefaultConfig()

	if host := os.Getenv("DB_HOST"); host != "" {
		cfg.Host = host
	}
	if portStr := os.Getenv("DB_PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			cfg.Port = p
		}
	}
	if user := os.Getenv("DB_USER"); user != "" {
		cfg.User = user
	}
	if pass := os.Getenv("DB_PASSWORD"); pass != "" {
		cfg.Password = pass
	}
	if db := os.Getenv("DB_NAME"); db != "" {
		cfg.Database = db
	}

	log.Printf("Connecting to PostgreSQL at %s:%d/%s (user: %s)...", cfg.Host, cfg.Port, cfg.Database, cfg.User)
	pool, err := database.NewPool(ctx, cfg)
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer pool.Close()
	log.Println("Database connection pool established successfully.")

	// 1. Initialize HTTP Layer
	docRepo := document.NewRepository()
	docHandler := handler.NewDocumentHandler(pool, docRepo)

	mux := http.NewServeMux()

	// Protected HTTP routes wrapped in TenantGuardMiddleware
	protectedMux := http.NewServeMux()
	protectedMux.HandleFunc("POST /documents", docHandler.Create)
	protectedMux.HandleFunc("GET /documents", docHandler.List)
	protectedMux.HandleFunc("GET /documents/{id}", docHandler.GetByID)

	mux.Handle("/", middleware.TenantGuardMiddleware(protectedMux))

	// Health endpoint (unprotected)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok"}`)
	})

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	httpServer := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("HTTP REST Server listening on port %s...", httpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// 2. Initialize gRPC ReBAC Check Service
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	grpcListener, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %s: %v", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	rebacCheckEngine := engine.NewSQLCheckEngine()
	rebacService := rebacgrpc.NewServer(pool, rebacCheckEngine)
	rebacv1.RegisterAuthorizationServiceServer(grpcServer, rebacService)

	go func() {
		log.Printf("gRPC ReBAC Check Server listening on port %s...", grpcPort)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// 3. Graceful Shutdown
	<-ctx.Done()
	log.Println("Shutting down servers gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	grpcServer.GracefulStop()

	log.Println("All servers gracefully stopped.")
}
