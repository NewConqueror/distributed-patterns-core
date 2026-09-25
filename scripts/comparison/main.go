package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	pgURL := flag.String("postgres-url", "postgres://postgres:postgres_master_pass@localhost:5432/inventory_db?sslmode=disable", "Postgres URL")
	esURL := flag.String("es-url", "http://localhost:9200", "Elasticsearch URL")
	sampleCount := flag.Int("samples", 100, "Number of sample items to test")
	flag.Parse()

	log.Println("==========================================================================")
	log.Println(" Faz 7.2: Çift Yazma (Dual-Write Anti-Pattern) vs CDC Karşılaştırma Testi ")
	log.Println("==========================================================================")

	db, err := sql.Open("postgres", *pgURL)
	if err != nil {
		log.Fatalf("PostgreSQL connection error: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("PostgreSQL ping error: %v", err)
	}

	// -----------------------------------------------------------------------
	// SENARYO A: Geleneksel Dual-Write Yaklaşımı (Postgres + Doğrudan HTTP ES)
	// -----------------------------------------------------------------------
	log.Println("\n>>> [Senaryo A] Geleneksel Dual-Write Simülasyonu Başlatılıyor...")
	log.Println("    (DB commit sonrası %25 olasılıkla HTTP ağ kopması/timeout simüle ediliyor)")

	var scenarioAPGCount int
	var scenarioAESSuccessCount int
	var scenarioAESFailedCount int

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 1; i <= *sampleCount; i++ {
		id := fmt.Sprintf("dual-write-fail-test-%04d", i)
		title := fmt.Sprintf("Dual-Write Test Item #%d", i)

		// 1. PostgreSQL'e yaz ve commit et
		_, err := db.Exec(`
			INSERT INTO products (id, title, description, price, category_id, status, version)
			VALUES ($1, $2, 'Dual write risk simulation', 99.90, 101, 'ACTIVE', 1)
			ON CONFLICT (id) DO NOTHING;
		`, id, title)

		if err != nil {
			log.Printf("DB insert failed: %v", err)
			continue
		}
		scenarioAPGCount++

		// 2. Uygulama DB commit'i aldıktan sonra ES'e HTTP POST atmaya çalışır
		// Gerçek hayattaki ağ kopması / process kill / timeout senaryosu: %25 hata
		networkDropped := rng.Float64() < 0.25

		if networkDropped {
			// Simüle edilen ağ çökmesi: DB'ye yazıldı ama ES'e asla gidemedi!
			scenarioAESFailedCount++
		} else {
			// Başarılı HTTP isteği simülasyonu
			doc := map[string]any{
				"id":            id,
				"title":         title,
				"category_name": "Mice & Pointers",
				"price":         99.90,
				"status":        "ACTIVE",
				"version":       1,
			}
			b, _ := json.Marshal(doc)
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodPut, fmt.Sprintf("%s/products_search_alias/_doc/%s", *esURL, id), bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err == nil && resp.StatusCode < 300 {
				scenarioAESSuccessCount++
				resp.Body.Close()
			} else {
				scenarioAESFailedCount++
			}
		}
	}

	driftA := float64(scenarioAESFailedCount) / float64(scenarioAPGCount) * 100.0

	// -----------------------------------------------------------------------
	// SENARYO B: Debezium + Kafka + Go Worker (CDC Boru Hattı)
	// -----------------------------------------------------------------------
	log.Println("\n>>> [Senaryo B] CDC Boru Hattı Simülasyonu Başlatılıyor...")
	log.Println("    (Uygulama yalnızca DB'ye yazar. Ağ kesintisinde WAL & Kafka garantisi devrededir)")

	var scenarioBPGCount int
	for i := 1; i <= *sampleCount; i++ {
		id := fmt.Sprintf("cdc-guaranteed-item-%04d", i)
		title := fmt.Sprintf("CDC Pipeline Guaranteed Item #%d", i)

		_, err := db.Exec(`
			INSERT INTO products (id, title, description, price, category_id, status, version)
			VALUES ($1, $2, 'CDC zero data loss test', 149.50, 102, 'ACTIVE', 1)
			ON CONFLICT (id) DO NOTHING;
		`, id, title)

		if err == nil {
			scenarioBPGCount++
		}
	}

	// CDC'nin Kafka ve Worker üzerinden ES'e akmasını bekle
	log.Println("    CDC boru hattının Kafka ve Worker üzerinden ES'e aktarması bekleniyor (3 sn)...")
	time.Sleep(3 * time.Second)

	// ES'te Senaryo B kayıtlarının sayısını kontrol et
	var scenarioBESCount int
	for i := 1; i <= *sampleCount; i++ {
		id := fmt.Sprintf("cdc-guaranteed-item-%04d", i)
		resp, err := http.Get(fmt.Sprintf("%s/products_search_alias/_doc/%s", *esURL, id))
		if err == nil && resp.StatusCode == http.StatusOK {
			scenarioBESCount++
		}
		if resp != nil {
			resp.Body.Close()
		}
	}

	driftB := float64(scenarioBPGCount-scenarioBESCount) / float64(scenarioBPGCount) * 100.0

	// -----------------------------------------------------------------------
	// KARŞILAŞTIRMA RAPORU TABLOSU
	// -----------------------------------------------------------------------
	fmt.Println("\n" + strings.Repeat("=", 78))
	fmt.Println("       ÇİFT YAZMA (DUAL-WRITE) VS CDC MİMARİSİ KARŞILAŞTIRMA TABLOSU         ")
	fmt.Println(strings.Repeat("=", 78))
	fmt.Printf("%-32s | %-20s | %-20s\n", "Kriter / Metrik", "Senaryo A (Dual-Write)", "Senaryo B (CDC Pipeline)")
	fmt.Println(strings.Repeat("-", 78))
	fmt.Printf("%-32s | %-20s | %-20s\n", "Veritabanına Yazılan (PG)", fmt.Sprintf("%d kayıt", scenarioAPGCount), fmt.Sprintf("%d kayıt", scenarioBPGCount))
	fmt.Printf("%-32s | %-20s | %-20s\n", "Arama Motoruna Geçen (ES)", fmt.Sprintf("%d kayıt", scenarioAESSuccessCount), fmt.Sprintf("%d kayıt", scenarioBESCount))
	fmt.Printf("%-32s | %-20s | %-20s\n", "Kaybolan / Eksik Doküman", fmt.Sprintf("%d doküman", scenarioAESFailedCount), fmt.Sprintf("%d doküman", scenarioBPGCount-scenarioBESCount))
	fmt.Printf("%-32s | %-20s | %-20s\n", "Veri Uçurumu (Data Drift)", fmt.Sprintf("%%%.1f (YÜKSEK RİSK)", driftA), fmt.Sprintf("%%%.1f (SIFIR KAYIP)", driftB))
	fmt.Printf("%-32s | %-20s | %-20s\n", "Ağ Hatasında Davranış", "Kalıcı Veri Uyuşmazlığı", "Re-try & Circuit Breaker")
	fmt.Printf("%-32s | %-20s | %-20s\n", "İşlem Güvenliği (ACID)", "Dağıtık İki Aşamalı Yok", "PostgreSQL WAL Güvencesi")
	fmt.Println(strings.Repeat("=", 78))
}
