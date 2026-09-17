package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"distributed-api-gateway/internal/limiter"
	"distributed-api-gateway/internal/proxy"
	"distributed-api-gateway/internal/rule"
	"distributed-api-gateway/internal/upstream"

	"github.com/redis/go-redis/v9"
)

func main() {
	port := flag.Int("port", 8080, "Gateway listen port")
	rulesPath := flag.String("rules", "configs/rules.yaml", "Path to rules YAML configuration")
	upstreamsRaw := flag.String("upstreams", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083", "Comma-separated upstream URLs")
	limiterMode := flag.String("limiter", "memory", "Rate limiter mode: none, memory, redis")
	redisAddr := flag.String("redis-addr", "127.0.0.1:6379", "Redis server address")
	lbAlg := flag.String("lb", "round_robin", "Load balancing algorithm: round_robin, least_connections")
	flag.Parse()

	// Environment variable overrides
	if envPort := os.Getenv("PORT"); envPort != "" {
		fmt.Sscanf(envPort, "%d", port)
	}
	if envUpstreams := os.Getenv("UPSTREAMS"); envUpstreams != "" {
		*upstreamsRaw = envUpstreams
	}
	if envLimiter := os.Getenv("LIMITER_MODE"); envLimiter != "" {
		*limiterMode = envLimiter
	}
	if envRedis := os.Getenv("REDIS_ADDR"); envRedis != "" {
		*redisAddr = envRedis
	}

	upstreamList := strings.Split(*upstreamsRaw, ",")
	for i := range upstreamList {
		upstreamList[i] = strings.TrimSpace(upstreamList[i])
	}

	log.Printf("Starting Distributed API Gateway on :%d", *port)
	log.Printf("Upstreams: %v", upstreamList)
	log.Printf("Load Balancer: %s", *lbAlg)
	log.Printf("Rate Limiter: %s", *limiterMode)

	// 1. Initialize Upstream Pool with Health Checker
	pool, err := upstream.NewPool(upstream.PoolConfig{
		Targets:                 upstreamList,
		Algorithm:               upstream.Algorithm(*lbAlg),
		HealthPath:              "/health",
		HealthInterval:          5 * time.Second,
		HealthTimeout:           2 * time.Second,
		PassiveFailureThreshold: 3,
	})
	if err != nil {
		log.Fatalf("Failed to initialize upstream pool: %v", err)
	}
	defer pool.Close()

	// 2. Initialize Reverse Proxy Forwarder with Pooled Transport
	forwarder := proxy.NewForwarder(nil)

	// 3. Initialize Redis Client if needed
	var redisClient redis.UniversalClient
	if *limiterMode == "redis" {
		rdb := redis.NewClient(&redis.Options{
			Addr:        *redisAddr,
			DialTimeout: 2 * time.Second,
		})
		defer rdb.Close()
		redisClient = rdb
	}

	// 4. Initialize Rule Reloader (reads file, listens for SIGHUP & File changes)
	rulesData, err := os.ReadFile(*rulesPath)
	if err != nil {
		log.Fatalf("Failed to read rules file %q: %v", *rulesPath, err)
	}

	reloader, err := rule.NewReloader(rulesData, rule.ReloaderConfig{
		ConfigFilePath:  *rulesPath,
		WatchInterval:   2 * time.Second,
		EnableSIGHUP:    true,
		RedisClient:     redisClient,
		RedisReloadChan: "gateway:reload",
		OnReloadSuccess: func(newEngine *rule.Engine) {
			log.Printf("[HOT-RELOAD] Rules successfully reloaded and swapped atomically")
		},
		OnReloadError: func(err error) {
			log.Printf("[HOT-RELOAD] Error reloading rules: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("Failed to initialize rule reloader: %v", err)
	}
	defer reloader.Close()

	// 5. Initialize Rate Limiter
	var dynamicLimiter limiter.DynamicLimiter
	switch *limiterMode {
	case "none":
		// No rate limiting (pure proxy baseline)
		dynamicLimiter = nil
	case "redis":
		redisLim := limiter.NewRedisSlidingWindowLimiter(limiter.RedisConfig{
			Client:    redisClient,
			Limit:     1000,
			Window:    60 * time.Second,
			KeyPrefix: "gateway_rl",
		})
		memFallback := limiter.NewMemoryLimiter(limiter.DefaultMemoryConfig())
		dynamicLimiter = limiter.NewFallbackLimiter(limiter.FallbackLimiterConfig{
			Primary:           redisLim,
			Fallback:          memFallback,
			Strategy:          limiter.FailOpen,
			Cooldown:          5 * time.Second,
			ThresholdFailures: 2,
		})
	case "memory":
		fallthrough
	default:
		dynamicLimiter = limiter.NewMemoryLimiter(limiter.DefaultMemoryConfig())
	}

	// 6. Define Proxy Core Handler
	proxyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target, err := pool.Select()
		if err != nil {
			http.Error(w, "Service Unavailable: No healthy upstream targets", http.StatusServiceUnavailable)
			return
		}

		target.IncConn()
		defer target.DecConn()

		summary, err := forwarder.Forward(w, r, target.URL)
		if summary != nil {
			pool.RecordResult(target, summary.StatusCode, err)
		} else if err != nil {
			pool.RecordResult(target, http.StatusBadGateway, err)
		}
	})

	// 7. Attach Dynamic Rate Limiting Middleware
	var rootHandler http.Handler = proxyHandler
	if dynamicLimiter != nil {
		dynamicMiddleware := rule.NewDynamicMiddleware(reloader, dynamicLimiter)
		rootHandler = dynamicMiddleware.Handler(proxyHandler)
	}

	// 8. Register Routes & pprof
	mux := http.NewServeMux()

	// pprof endpoints for memory and CPU profiling
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Gateway status endpoint
	mux.HandleFunc("/gateway/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		targets := pool.Targets()
		healthyCount := 0
		for _, t := range targets {
			if t.IsAlive() {
				healthyCount++
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"UP","healthy_targets":%d,"total_targets":%d,"reloads":%d}`+"\n",
			healthyCount, len(targets), reloader.ReloadCount())))
	})

	// Main traffic handler
	mux.Handle("/", rootHandler)

	// 9. Server configuration with critical timeout boundaries (Phase 1.4)
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", *port),
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Graceful shutdown listener
	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Gateway HTTP server active on http://127.0.0.1:%d", *port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Gateway server failed: %v", err)
		}
	}()

	<-stopSig
	log.Println("Shutting down Gateway gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}
	log.Println("Gateway stopped.")
}
