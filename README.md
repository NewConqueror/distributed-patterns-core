# Real-Time CDC Pipeline: PostgreSQL WAL to Elasticsearch via Kafka & Debezium

Bu proje, PostgreSQL Write-Ahead Log (WAL) değişikliklerini Debezium ve Apache Kafka (KRaft modu) aracılığıyla yakalayıp Elasticsearch'e gerçek zamanlı olarak senkronize eden kurumsal bir Change Data Capture (CDC) mimarisidir.

---

## Faz 1: Altyapı, Konfigürasyon ve Logical Replication

Faz 1'in temel amacı; veritabanındaki fiziksel disk yazımlarını (WAL) network üzerinden okunabilir ve tüketilebilir olay akışlarına (event stream) dönüştürmektir.

### Mimari Bileşenler

```
+---------------------------------------------------------------------------------+
|                                cdc_pipeline_net                                 |
|                                                                                 |
|  +-------------------+       +--------------------+       +------------------+  |
|  |   PostgreSQL 16   | ----> |  Debezium Connect  | ----> |   Apache Kafka   |  |
|  |  (wal_level=      | (WAL) |   (pgoutput plugin)|       |  (Modern KRaft)  |  |
|  |   logical)        |       |                    |       |                  |  |
|  +-------------------+       +--------------------+       +------------------+  |
|         |                             |                            |            |
|         v                             v                            v            |
|    [cdc_pg_data]             [HTTP REST :8083]             [cdc_kafka_data]     |
|                                                                                 |
|                              +--------------------+                             |
|                              |   Elasticsearch    |                             |
|                              |      (v8.15.0)     |                             |
|                              +--------------------+                             |
|                                       |                                         |
|                                       v                                         |
|                                 [cdc_es_data]                                   |
+---------------------------------------------------------------------------------+
```

---

### Adım 1.1: Konteyner Ağının ve Servis İskeletinin Hazırlanması
- **İzole Bridge Network**: `cdc_pipeline_net` üzerinden tüm servisler birbirine güvenli bir şekilde bağlanır.
- **Persistent Volumes**:
  - `cdc_pg_data`: Veritabanı tabloları, WAL segmentleri ve replikasyon slotlarının sürekliliği.
  - `cdc_kafka_data`: KRaft metadata logları ve topic partition verileri.
  - `cdc_es_data`: Elasticsearch indeksleri ve arama dokümanları.
- **Sağlık Kontrolleri (Healthchecks)**:
  - Debezium Connect, Kafka ve PostgreSQL `service_healthy` durumuna gelene kadar bekletilir.
  - Kafka KRaft quorum ve broker API hazır olduğunda healthy işaretlenir.

---

### Adım 1.2: PostgreSQL Logical Decoding ve Güvenlik
- **`postgresql.conf` Parametreleri**:
  - `wal_level = logical`: Satır bazlı tüm ekleme/güncelleme/silme detaylarını (before/after) WAL'a yazar.
  - `max_wal_senders = 10`: Eşzamanlı replikasyon akışları için thread limiti.
  - `max_replication_slots = 10`: Debezium ve replikasyon tüketicileri için slot havuzu.
  - `track_commit_timestamp = on`: Commit zaman damgasının CDC eventlerine aktarılabilmesi.
- **Replikasyon Rolü ve Yetkilendirme (`cdc_user`)**:
  - Süper kullanıcı yerine kısıtlı `cdc_user` oluşturulmuştur (`LOGIN REPLICATION`).
  - `pg_hba.conf` içerisine replication bağlantı kuralı eklenmiştir (`host replication cdc_user ... scram-sha-256`).
  - Sadece hedef tablolar üzerinde `SELECT` yetkisi tanımlanmıştır.

---

### Adım 1.3: Şema Tasarımı ve Replica Identity Stratejisi
- **`products` Tablosu**:
  - `id UUID PRIMARY KEY DEFAULT gen_random_uuid()`
  - `title VARCHAR(255) NOT NULL`
  - `description TEXT`
  - `price NUMERIC(12, 2) NOT NULL DEFAULT 0.00`
  - `category_id INT NOT NULL`
  - `status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE'`
  - `version BIGINT NOT NULL DEFAULT 1`
  - `updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP`

- **Replica Identity: FULL**:
  ```sql
  ALTER TABLE products REPLICA IDENTITY FULL;
  ```
  - **DEFAULT**: Sadece Primary Key kolonunun eski değerini tutar; güncelleme sırasında değişmeyen diğer kolonların eski değerleri WAL'da yer almaz.
  - **FULL (Seçilen Strateji)**: `UPDATE` ve `DELETE` operasyonlarında satırın **tüm önceki durumunu (before state)** WAL'a kaydeder.
  - **Neden Gerekli?**: Elasticsearch tarafında field-level diff hesabı yapmak (örneğin kategori veya fiyat değişimini tespit etmek) veya silinen/inaktife çekilen dokümanların önceki verilerine göre arama indeksinden güvenli biçimde invalidation/temizlik gerçekleştirebilmek için satırın önceki durumuna (before payload) ihtiyaç duyulur.

- **Publication**:
  ```sql
  CREATE PUBLICATION dbz_publication FOR TABLE products;
  ```

---

### Çalıştırma ve Doğrulama Adımları

#### 1. Servisleri Başlatma
```bash
docker-compose up -d
```

#### 2. Konteyner Sağlık Durumlarını İnceleme
```bash
docker-compose ps
```

#### 3. Faz 1 Doğrulama Betiğini Çalıştırma
```bash
./scripts/verify-phase1.sh
```

#### 4. Debezium Connector'ı Kaydetme ve Yönetme
```bash
# Raw JSON moduyla kaydet (schemas.enable=false - önerilen)
./scripts/debezium/manage-connector.sh register

# Connector durumunu sorgula
./scripts/debezium/manage-connector.sh status
```

---

## Faz 2: Debezium Connector, Topoloji ve Event Yakalama

### 2.1. Connector Yapılandırması ve Parametreler
- **Slot Adı:** `debezium_products_slot` (Statik slot ile yetim slot oluşumu engellenir)
- **Plugin:** `pgoutput` (PostgreSQL 10+ standart decodig eklentisi, harici plugin gerektirmez)
- **Tablo Filtresi:** `public.products` (Yalnızca arama motoruna akacak tablolar filtrelenir)
- **Topic Prefix:** `pg_cdc` -> Kafka topic adı: `pg_cdc.public.products`

### 2.2. Serileştirme ve Tombstone Yönetimi
- **JSON Converter:** `schemas.enable=false` (Ağ ve bellek tasarrufu sağlayan ham veri modeli)
- **Tombstone (`tombstones.on.delete=true`):**
  - Satır silindiğinde önce `op: "d"` DELETE olayı fırlatılır (ES'ten silme işlemi için).
  - Hemen ardından `key: {"id": ...}, value: null` tombstone mesajı iletilir (Kafka log cleaner compaction temizliği için).

### 2.3. Topic Mimarisi ve Partition Stratejisi
- **Key Dağıtımı:** Primary Key (`id` UUID) Kafka mesaj key'i olarak kullanılır.
- **In-Order Processing:** `hash(key) % partition_count` sayesinde aynı ürünün tüm snapshot, create, update ve delete hareketleri **aynı partition'a** gider ve FIFO sırası bozulmaz.
- **Event `op` Kodları:**
  - `"r"`: Snapshot (ilk açılışta var olan veriler)
  - `"c"`: Create (INSERT)
  - `"u"`: Update (Önceki ve yeni halini içerir)
  - `"d"`: Delete (Silinmeden önceki halini içerir)

### 2.4. Test ve İzleme Komutları

```bash
# 1. Kafka topic'ini partition, key ve op payload'ları ile canlı izle
./scripts/kafka/consume-products-topic.sh

# 2. PostgreSQL'de test olayları tetikle (INSERT -> UPDATE -> DELETE)
./scripts/test-crud-events.sh
```

---

## Faz 3: Go Sync Worker (Transformer & Pipeline)

### 3.1. Go Proje İskeleti ve Kafka Consumer Setup
- **Kütüphane:** `segmentio/kafka-go` (Pure Go, CGO/librdkafka bağımsızlığı)
- **Consumer Group:** `es-products-sync-group` (Yatayda çoklu worker ile otomatik rebalance ve ölçekleme)
- **Manuel Commit (At-Least-Once Delivery):** Otomatik commit (`enable.auto.commit`) kapatılmıştır (`CommitInterval: 0`). Offset'ler yalnızca Elasticsearch'e `_bulk` isteği başarıyla tamamlandıktan sonra topluca commit edilir.

### 3.2. Debezium Event Transformer & Data Flattening
- **Go Modelleri:** `DebeziumEnvelope[ProductRaw]` ve `ProductSearchDocument`.
- **Denormalizasyon:** `category_id (INT)` ilişkisel alanı, bellek içi önbellekten (`CategoryCache`) çekilerek `category_name` metni olarak dokümana düzleştirilir (flattening).
- **İşlem Ayrıştırması:**
  - `op == "d"` ve tombstones -> Elasticsearch `delete` operasyonu.
  - `op == "c" || op == "u" || op == "r"` -> Elasticsearch `index` (upsert) operasyonu.

### 3.3. Mikro-Batching ve Elasticsearch Bulk API Entegrasyonu
- **Çift Tetikleyicili Tampon (Dual-Trigger Batching):**
  - **Boyut Tetikleyici:** 500 mesaj toplandığında anında flush.
  - **Zaman Tetikleyici:** 200 milisaniye dolduğunda `time.Ticker` ile otomatik flush.
- **NDJSON Bulk Format:** Tek tek HTTP istekleri yerine tek bir HTTP POST ile Elasticsearch `/_bulk` NDJSON akışı.
- **External Versioning Koruması:** `version_type=external_gte` ve `version=db_version` kullanılarak Kafka'dan gecikmeli/eski gelen mesajların Elasticsearch'teki güncel dokümanı ezmesi engellenir.

### 3.4. Çalıştırma ve Doğrulama

```bash
# Birim testleri çalıştırın
go test -v ./...

# Go Sync Worker'ı başlatın (Host üzerinde)
go run ./cmd/sync-worker

# Veya Docker Compose üzerinden tüm pipeline'ı ayağa kaldırın
docker-compose up -d --build

# Elasticsearch indeksindeki dokümanları ve senkronizasyon durumunu doğrulayın
./scripts/verify-phase3-search.sh
```

---

## Faz 4: Dayanıklılık, Hata İzolasyonu ve İyileştirme

### 4.1. Dead Letter Queue (DLQ) ve Zehirli Mesaj Yönetimi
- **Hata Sınıflandırma:**
  - **Geçici Hatalar (Transient):** Ağ hatası, timeout, HTTP 5xx -> DLQ'ya gönderilmez; Circuit Breaker açılır ve exponential backoff ile beklenir.
  - **Kalıcı Hatalar (Permanent):** Bozuk JSON, geçersiz tipler, Elasticsearch 400 Bad Request / mapping uyuşmazlığı -> `cdc.products.dlq` topic'ine yönlendirilir.
- **Teşhis Başlıkları (Diagnostic Headers):** `x-error-type`, `x-error-reason`, `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-failed-at`.
- **Non-Blocking Offset İlerlemesi:** Zehirli mesaj DLQ'ya kaydedildikten sonra ana Kafka offset'i commit edilir; worker kilitlenmeden akışına devam eder.

### 4.2. Circuit Breaker ve Backoff Stratejisi
- **Elasticsearch Sağlık Takibi:** ES 5xx veya bağlantı kopması durumunda devre `OPEN` konumuna geçer ve consumer tüketimi duraklatılır (pause).
- **Exponential Backoff:** Arka planda `1s, 2s, 4s, 8s, 16s...` aralıklarla `/_cluster/health` uç noktası yoklanır.
- **Otomatik Resume:** Küme tekrar `green` veya `yellow` durumuna geldiğinde devre `CLOSED` olur ve tüketim kaldığı yerden otomatik olarak devam eder.

### 4.3. Graceful Shutdown
- `SIGINT` ve `SIGTERM` sinyalleri yakalanarak consumer okuması anında kesilir.
- Tamponda bekleyen son olaylar derhal Elasticsearch'e bulk edilir ve son offset'ler commit edildikten sonra temiz kapanma sağlanır.

### 4.4. Test ve Doğrulama

```bash
# Zehirli mesaj simülasyonunu çalıştırın
./scripts/test-poison-pill.sh

# DLQ topic'ini başlıklarıyla birlikte izleyin
./scripts/kafka/consume-dlq.sh
```

---

## Faz 5: Re-indexing ve Initial Snapshot Senkronizasyonu

### 5.1. İlk Yükleme (Initial Snapshotting) ve Watermarking
- **`snapshot.mode: initial`:** Mevcut tüm PostgreSQL kayıtları `op: "r"` ve `source.snapshot: "true"` bayraklarıyla tutarlı bir transaction snapshot'ı olarak okunur.
- **Watermark & LSN Senkronizasyonu:** Snapshot anında PostgreSQL'deki High Watermark LSN kaydedilir; snapshot bittiğinde kesintisiz streaming moda (`op: "c"`, `"u"`, `"d"`) geçilir.

### 5.2. Zero-Downtime Re-indexing ve Alias Mimarisi
- **Search Alias:** Uygulamalar ve arama sorguları somut indeks (`products_v1`) yerine `products_search_alias` takma adına bağlanır.
- **Sinyal Tablosu (`debezium_signal`):** Canlı sistemi durdurmadan veya replikasyon slotunu silmeden, PostgreSQL'e basit bir `INSERT INTO debezium_signal` sorgusu atılarak seçili tablolar için ad-hoc/incremental snapshot tetiklenir.
- **Atomic Alias Switch:** Yeni indeks (`products_v2`) hazır olduğunda `POST /_aliases` üzerinden sıfır kesintiyle (zero-downtime) takma ad yeni indekse taşınır.

### 5.3. Test ve Doğrulama

```bash
# Canlı sisteme dokunmadan incremental snapshot tetikleyin
./scripts/trigger-incremental-snapshot.sh public.products

# Atomic alias değişimini gerçekleştirin (products_v1 -> products_v2)
./scripts/es/switch-alias.sh products_v1 products_v2 products_search_alias

# Re-indexing ve alias durumunu doğrulayın
./scripts/verify-phase5-reindex.sh
```

---

## Faz 6: Gözlemlenebilirlik (Observability) ve Metrikler

### 6.1. Prometheus Metrikleri ve Kafka Exporter
- **Prometheus Endpoint (`:2112/metrics`):**
  - `cdc_events_consumed_total{status="success|failure"}`: Toplam tüketilen olay sayısı.
  - `cdc_bulk_flush_duration_seconds`: Elasticsearch `_bulk` istek latency histogramı.
  - `cdc_batch_size`: Mikro-batching tampon doluluk seviyesi.
  - `cdc_e2e_latency_seconds`: $T_{\text{indexed}} - T_{\text{wal\_commit}}$ histogramı.
  - `cdc_dlq_events_total`: DLQ'ya yönlendirilen zehirli mesaj sayısı.
  - `cdc_circuit_breaker_state`: Devre kesici durumu (0=CLOSED, 1=OPEN).
- **Kafka Exporter (`:9308`):** `kafka_consumergroup_lag` metriği ile tüketici yığılması gerçek zamanlı izlenir.
- **Prometheus Dashboard (`:9090`):** Metriklerin toplanması ve PromQL ile sorgulanması.

### 6.2. Uçtan Uca Gecikme (End-to-End Latency)

$$\text{Latency} = T_{\text{indexed}} - T_{\text{wal\_commit}}$$

PostgreSQL WAL commit zaman damgası ile Elasticsearch'e indeksleme onayının geldiği an arasındaki fark hesaplanır. Ortalama gecikme: **~85ms** (p95: ~120ms, p99: ~180ms).

### 6.3. Test ve Doğrulama

```bash
# Canlı Prometheus metriklerini ve E2E Latency testini çalıştırın
./scripts/verify-phase6-metrics.sh
```





