package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

func main() {
	port := flag.Int("port", 8081, "Port to listen on")
	delay := flag.Duration("delay", 50*time.Millisecond, "Artificial processing delay")
	errorRate := flag.Float64("error-rate", 0.0, "Probability of random 500 error (0.0 to 1.0)")
	name := flag.String("name", "backend-1", "Instance identifier")
	flag.Parse()

	// Environment variable overrides
	if envPort := os.Getenv("PORT"); envPort != "" {
		fmt.Sscanf(envPort, "%d", port)
	}
	if envName := os.Getenv("INSTANCE_NAME"); envName != "" {
		*name = envName
	}
	if envDelay := os.Getenv("DELAY"); envDelay != "" {
		if d, err := time.ParseDuration(envDelay); err == nil {
			*delay = d
		}
	}

	var isHealthy atomic.Bool
	isHealthy.Store(true)

	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !isHealthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"UNHEALTHY"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"UP","backend":"%s"}`, *name)))
	})

	// Toggle health endpoint for testing upstream failure
	mux.HandleFunc("/health/toggle", func(w http.ResponseWriter, r *http.Request) {
		currentState := isHealthy.Load()
		newState := !currentState
		isHealthy.Store(newState)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf("backend %s healthy state set to: %v\n", *name, newState)))
	})

	// Catch-all API routes
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if *delay > 0 {
			time.Sleep(*delay)
		}

		if *errorRate > 0 && rand.Float64() < *errorRate {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"simulated internal server error"}`))
			return
		}

		resp := map[string]any{
			"backend":        *name,
			"path":           r.URL.Path,
			"query":          r.URL.RawQuery,
			"method":         r.Method,
			"x_forwarded_for": r.Header.Get("X-Forwarded-For"),
			"x_real_ip":      r.Header.Get("X-Real-IP"),
			"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Backend-Server", *name)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("[%s] Mock backend starting on %s (delay: %v, error-rate: %.2f)", *name, addr, *delay, *errorRate)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("[%s] server error: %v", *name, err)
	}
}
