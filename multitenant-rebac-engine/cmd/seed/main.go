package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type SeedSummary struct {
	TotalTenants   int           `json:"total_tenants"`
	TotalTuples    int           `json:"total_tuples"`
	TotalDocuments int           `json:"total_documents"`
	DurationMs     int64         `json:"duration_ms"`
	SampleTenants  []TenantEntry `json:"sample_tenants"`
}

type TenantEntry struct {
	TenantID uuid.UUID `json:"tenant_id"`
	Name     string    `json:"name"`
	Admin    string    `json:"admin"`
	Member   string    `json:"member"`
	Document string    `json:"document"`
	Folder   string    `json:"folder"`
}

func main() {
	tenantsCount := flag.Int("tenants", 1000, "Number of tenants to generate")
	batchSize := flag.Int("batch-size", 5000, "Batch size for pgx.CopyFrom")
	outputSummary := flag.String("output", "test/load/seed_summary.json", "Path to save sample seed summary for k6")
	flag.Parse()

	ctx := context.Background()
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

	log.Printf("Starting bulk synthetic data generation for %d tenants...", *tenantsCount)
	startTime := time.Now()

	var tenantRows [][]any
	var tupleRows [][]any
	var docRows [][]any

	sampleTenants := make([]TenantEntry, 0, 10)
	totalTuplesCount := 0
	totalDocsCount := 0

	memberRel := "member"

	for tIdx := 1; tIdx <= *tenantsCount; tIdx++ {
		tID := uuid.New()
		tName := fmt.Sprintf("Enterprise-Tenant-%04d", tIdx)
		tenantRows = append(tenantRows, []any{tID, tName, time.Now(), time.Now()})

		wsID := fmt.Sprintf("ws-%d", tIdx)
		adminID := fmt.Sprintf("u-%d-admin", tIdx)

		// 1. Workspace tuples
		tupleRows = append(tupleRows, []any{
			tID, "workspace", wsID, "admin", "user", adminID, nil, time.Now(),
		})

		// 5 members per tenant
		for m := 1; m <= 5; m++ {
			memberID := fmt.Sprintf("u-%d-m-%d", tIdx, m)
			tupleRows = append(tupleRows, []any{
				tID, "workspace", wsID, "member", "user", memberID, nil, time.Now(),
			})
		}

		// 2. 5-Level Deep Folder Hierarchy
		// Root Folder (Level 1)
		rootFolderID := fmt.Sprintf("f-%d-l1", tIdx)
		tupleRows = append(tupleRows, []any{
			tID, "folder", rootFolderID, "parent", "workspace", wsID, nil, time.Now(),
		})
		tupleRows = append(tupleRows, []any{
			tID, "folder", rootFolderID, "viewer", "workspace", wsID, &memberRel, time.Now(),
		})

		prevFolderID := rootFolderID
		var deepestDocID string

		// Levels 2 to 5
		for lvl := 2; lvl <= 5; lvl++ {
			folderID := fmt.Sprintf("f-%d-l%d", tIdx, lvl)
			tupleRows = append(tupleRows, []any{
				tID, "folder", folderID, "parent", "folder", prevFolderID, nil, time.Now(),
			})
			tupleRows = append(tupleRows, []any{
				tID, "folder", folderID, "viewer", "folder", prevFolderID, nil, time.Now(),
			})

			// Add 20 documents inside each folder level to scale up to 500k+ tuples
			for d := 1; d <= 20; d++ {
				docID := fmt.Sprintf("doc-%d-l%d-%d", tIdx, lvl, d)
				deepestDocID = docID

				docRows = append(docRows, []any{
					tID, nil, fmt.Sprintf("Document %s", docID), "Encrypted enterprise payload", time.Now(), time.Now(),
				})

				// Document tuples: parent, owner, viewer
				tupleRows = append(tupleRows, []any{
					tID, "document", docID, "parent", "folder", folderID, nil, time.Now(),
				})
				tupleRows = append(tupleRows, []any{
					tID, "document", docID, "owner", "user", adminID, nil, time.Now(),
				})
				tupleRows = append(tupleRows, []any{
					tID, "document", docID, "viewer", "folder", folderID, nil, time.Now(),
				})
				tupleRows = append(tupleRows, []any{
					tID, "document", docID, "viewer", "workspace", wsID, &memberRel, time.Now(),
				})
				tupleRows = append(tupleRows, []any{
					tID, "document", docID, "editor", "user", adminID, nil, time.Now(),
				})
			}

			prevFolderID = folderID
		}

		if tIdx <= 20 {
			sampleTenants = append(sampleTenants, TenantEntry{
				TenantID: tID,
				Name:     tName,
				Admin:    fmt.Sprintf("user:%s", adminID),
				Member:   fmt.Sprintf("user:u-%d-m-1", tIdx),
				Document: fmt.Sprintf("document:%s", deepestDocID),
				Folder:   fmt.Sprintf("folder:%s", rootFolderID),
			})
		}
	}

	totalTuplesCount = len(tupleRows)
	totalDocsCount = len(docRows)

	log.Printf("Prepared %d tenants, %d documents, and %d relation tuples. Executing bulk copy...",
		len(tenantRows), totalDocsCount, totalTuplesCount)

	// 1. Bulk Insert Tenants
	_, err = pool.CopyFrom(
		ctx,
		pgx.Identifier{"tenants"},
		[]string{"id", "name", "created_at", "updated_at"},
		pgx.CopyFromRows(tenantRows),
	)
	if err != nil {
		log.Fatalf("Failed to copy tenants: %v", err)
	}
	log.Printf("Successfully copied %d tenants.", len(tenantRows))

	// 2. Bulk Insert Documents in batches
	log.Println("Bulk copying documents...")
	for i := 0; i < len(docRows); i += *batchSize {
		end := i + *batchSize
		if end > len(docRows) {
			end = len(docRows)
		}
		_, err = pool.CopyFrom(
			ctx,
			pgx.Identifier{"documents"},
			[]string{"tenant_id", "owner_id", "title", "content", "created_at", "updated_at"},
			pgx.CopyFromRows(docRows[i:end]),
		)
		if err != nil {
			log.Fatalf("Failed to copy documents batch %d-%d: %v", i, end, err)
		}
	}
	log.Printf("Successfully copied %d documents.", totalDocsCount)

	// 3. Bulk Insert Tuples in batches
	log.Println("Bulk copying relation tuples...")
	for i := 0; i < len(tupleRows); i += *batchSize {
		end := i + *batchSize
		if end > len(tupleRows) {
			end = len(tupleRows)
		}
		_, err = pool.CopyFrom(
			ctx,
			pgx.Identifier{"tuples"},
			[]string{"tenant_id", "entity_type", "entity_id", "relation", "subject_type", "subject_id", "subject_relation", "created_at"},
			pgx.CopyFromRows(tupleRows[i:end]),
		)
		if err != nil {
			log.Fatalf("Failed to copy tuples batch %d-%d: %v", i, end, err)
		}
	}
	log.Printf("Successfully copied %d relation tuples.", totalTuplesCount)

	elapsed := time.Since(startTime)
	log.Printf("Bulk seeding completed in %v! (%.0f tuples/sec)",
		elapsed, float64(totalTuplesCount)/elapsed.Seconds())

	// Save seed summary for k6 load tests
	if *outputSummary != "" {
		_ = os.MkdirAll(filepath.Dir(*outputSummary), 0755)
		summary := SeedSummary{
			TotalTenants:   *tenantsCount,
			TotalTuples:    totalTuplesCount,
			TotalDocuments: totalDocsCount,
			DurationMs:     elapsed.Milliseconds(),
			SampleTenants:  sampleTenants,
		}
		data, _ := json.MarshalIndent(summary, "", "  ")
		if err := os.WriteFile(*outputSummary, data, 0644); err == nil {
			log.Printf("Saved seed summary to %s", *outputSummary)
		}
	}
}
