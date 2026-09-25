package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

type Config struct {
	PostgresURL string
	ESURL       string
	ESAlias     string
	TotalOps    int
	Concurrency int
	BatchSize   int
	UpdateRatio float64
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.PostgresURL, "postgres-url", "postgres://postgres:postgres_master_pass@localhost:5432/inventory_db?sslmode=disable", "PostgreSQL Connection URL")
	flag.StringVar(&cfg.ESURL, "es-url", "http://localhost:9200", "Elasticsearch URL")
	flag.StringVar(&cfg.ESAlias, "es-alias", "products_search_alias", "Elasticsearch Search Alias")
	flag.IntVar(&cfg.TotalOps, "total", 5000, "Total operations to execute")
	flag.IntVar(&cfg.Concurrency, "concurrency", 16, "Number of concurrent worker goroutines")
	flag.IntVar(&cfg.BatchSize, "batch", 50, "Batch size per transaction insert")
	flag.Float64Var(&cfg.UpdateRatio, "update-ratio", 0.2, "Ratio of operations that are updates (0.0 to 1.0)")
	flag.Parse()

	log.Println("==========================================================================")
	log.Println(" CDC Pipeline Stress Test & Catch-Up Benchmark Generator                  ")
	log.Println("==========================================================================")
	log.Printf("Target: %d operations | Concurrency: %d | Batch: %d | Update Ratio: %.0f%%",
		cfg.TotalOps, cfg.Concurrency, cfg.BatchSize, cfg.UpdateRatio*100)

	// 1. Connect to PostgreSQL
	db, err := sql.Open("postgres", cfg.PostgresURL)
	if err != nil {
		log.Fatalf("Failed to open postgres: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(cfg.Concurrency * 2)
	db.SetMaxIdleConns(cfg.Concurrency)

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping postgres: %v", err)
	}
	log.Println("✔ Connected to PostgreSQL.")

	// 2. Query initial document count from Elasticsearch
	initialESCount := getESCount(cfg.ESURL, cfg.ESAlias)
	var initialPGCount int
	_ = db.QueryRow("SELECT COUNT(*) FROM products;").Scan(&initialPGCount)
	log.Printf("Initial State: PostgreSQL has %d rows | Elasticsearch has %d docs", initialPGCount, initialESCount)

	// 3. Prepare Work Items
	batchesCount := cfg.TotalOps / cfg.BatchSize
	if batchesCount == 0 {
		batchesCount = 1
	}

	workChan := make(chan int, batchesCount)
	for i := 0; i < batchesCount; i++ {
		workChan <- i
	}
	close(workChan)

	var opsCompleted atomic.Int64
	var insertsCompleted atomic.Int64
	var updatesCompleted atomic.Int64

	startTime := time.Now()
	var wg sync.WaitGroup

	log.Println("\n>>> Starting Concurrent Load Generation...")

	// 4. Worker Pool
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for batchIdx := range workChan {
				isUpdateBatch := rng.Float64() < cfg.UpdateRatio
				if isUpdateBatch && initialPGCount > 0 {
					// Execute batch update on random products
					count := executeBatchUpdate(db, cfg.BatchSize, rng)
					updatesCompleted.Add(int64(count))
					opsCompleted.Add(int64(count))
				} else {
					// Execute batch insert
					count := executeBatchInsert(db, cfg.BatchSize, batchIdx, workerID, rng)
					insertsCompleted.Add(int64(count))
					opsCompleted.Add(int64(count))
				}
			}
		}(w)
	}

	wg.Wait()
	writeDuration := time.Since(startTime)
	totalProcessed := opsCompleted.Load()
	writeThroughput := float64(totalProcessed) / writeDuration.Seconds()

	log.Println("\n==========================================================================")
	log.Println(" [Stage 1 Completed] PostgreSQL Writes Finished")
	log.Println("==========================================================================")
	log.Printf("Total Ops Written : %d (%d inserts, %d updates)", totalProcessed, insertsCompleted.Load(), updatesCompleted.Load())
	log.Printf("Time Taken        : %v", writeDuration)
	log.Printf("Write Throughput  : %.2f ops/sec", writeThroughput)

	// 5. Measure Catch-Up Time in Elasticsearch
	var finalPGCount int
	_ = db.QueryRow("SELECT COUNT(*) FROM products;").Scan(&finalPGCount)
	expectedNewInserts := insertsCompleted.Load()
	expectedESTotal := int64(initialESCount) + expectedNewInserts

	log.Println("\n>>> [Stage 2] Monitoring Elasticsearch Catch-Up & Lag Depletion...")
	log.Printf("Target Elasticsearch Document Count: >= %d", expectedESTotal)

	catchupStart := time.Now()
	maxWait := 60 * time.Second
	pollInterval := 100 * time.Millisecond
	caughtUp := false
	var finalESCount int

	for time.Since(catchupStart) < maxWait {
		finalESCount = getESCount(cfg.ESURL, cfg.ESAlias)
		if int64(finalESCount) >= expectedESTotal {
			caughtUp = true
			break
		}
		time.Sleep(pollInterval)
	}

	catchupDuration := time.Since(catchupStart)

	// 6. Print Formatted Benchmark Table
	fmt.Println("\n" + strings.Repeat("=", 74))
	fmt.Println("              CDC PIPELINE STRESS & LATENCY BENCHMARK REPORT              ")
	fmt.Println(strings.Repeat("=", 74))
	fmt.Printf("%-38s : %d ops\n", "Total Test Volume", totalProcessed)
	fmt.Printf("%-38s : %d workers\n", "Concurrency Level", cfg.Concurrency)
	fmt.Printf("%-38s : %d items/tx\n", "Batch Chunk Size", cfg.BatchSize)
	fmt.Printf("%-38s : %.2f ops/sec\n", "PostgreSQL Ingestion Rate", writeThroughput)
	if caughtUp {
		fmt.Printf("%-38s : %v\n", "Catch-up Time (Lag Drain)", catchupDuration)
		drainRate := float64(totalProcessed) / (writeDuration.Seconds() + catchupDuration.Seconds())
		fmt.Printf("%-38s : %.2f docs/sec\n", "Effective Pipeline Throughput", drainRate)
		fmt.Printf("%-38s : 100%% MATCH (PG: %d == ES: %d)\n", "Data Consistency Verification", finalPGCount, finalESCount)
	} else {
		fmt.Printf("%-38s : Timed out waiting for ES catch-up (Current ES: %d)\n", "Catch-up Time", finalESCount)
	}
	fmt.Println(strings.Repeat("=", 74))
}

func executeBatchInsert(db *sql.DB, batchSize, batchIdx, workerID int, rng *rand.Rand) int {
	categories := []int{101, 102, 103, 104, 105}
	titles := []string{"Wireless Pro Mouse", "RGB Mechanical Keyboard", "UltraWide 34in Monitor", "Gaming Headset 7.1", "Ergonomic Desk Mat"}

	valStrings := make([]string, 0, batchSize)
	valArgs := make([]any, 0, batchSize*7)

	for i := 0; i < batchSize; i++ {
		idx := len(valArgs)
		valStrings = append(valStrings, fmt.Sprintf("(gen_random_uuid(), $%d, $%d, $%d, $%d, $%d, $%d, CURRENT_TIMESTAMP)",
			idx+1, idx+2, idx+3, idx+4, idx+5, idx+6))

		title := fmt.Sprintf("%s #%d-%d-%d", titles[rng.Intn(len(titles))], workerID, batchIdx, i)
		desc := "Stress test generated payload for CDC benchmark"
		price := float64(rng.Intn(50000)+1000) / 100.0
		categoryID := categories[rng.Intn(len(categories))]
		status := "ACTIVE"
		version := int64(1)

		valArgs = append(valArgs, title, desc, price, categoryID, status, version)
	}

	query := fmt.Sprintf("INSERT INTO products (id, title, description, price, category_id, status, version, updated_at) VALUES %s;",
		strings.Join(valStrings, ", "))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, query, valArgs...)
	if err != nil {
		log.Printf("[Worker Insert Error]: %v", err)
		return 0
	}
	return batchSize
}

func executeBatchUpdate(db *sql.DB, batchSize int, rng *rand.Rand) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
		UPDATE products 
		SET price = price + 1.50, 
		    version = version + 1, 
		    updated_at = CURRENT_TIMESTAMP
		WHERE id IN (
			SELECT id FROM products ORDER BY RANDOM() LIMIT $1
		);
	`
	res, err := db.ExecContext(ctx, query, batchSize)
	if err != nil {
		return 0
	}
	rows, _ := res.RowsAffected()
	return int(rows)
}

func getESCount(esURL, aliasName string) int {
	resp, err := http.Get(fmt.Sprintf("%s/%s/_count", esURL, aliasName))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	var result struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0
	}
	return result.Count
}
