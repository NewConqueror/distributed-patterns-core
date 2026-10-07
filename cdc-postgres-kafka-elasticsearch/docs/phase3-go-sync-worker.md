# Faz 3: Go Sync Worker (Transformer & Pipeline)

Bu doküman; Kafka topic'inde toplanan Debezium CDC olaylarını tüketen, ilişkisel veriyi arama motoru için düzleştiren (denormalization & flattening), mikro-batching ile Elasticsearch Bulk API'sine aktaran ve harici sürümleme (`version_type=external_gte`) ile veri tutarlılığını sağlayan Go Sync Worker mimarisini açıklar.

---

## Mimari Akış Şeması

```
+---------------------------------------------------------------------------------------------------+
|                                       Go Sync Worker                                              |
|                                                                                                   |
|  +--------------------+                                                                           |
|  |    Kafka Reader    |                                                                           |
|  |  (kafka-go, pure)  |                                                                           |
|  |  FetchMessage()    |                                                                           |
|  |  CommitInterval: 0 |                                                                           |
|  +--------------------+                                                                           |
|           | (raw msg)                                                                             |
|           v                                                                                       |
|  +--------------------+       +----------------------+                                            |
|  |    Transformer     | <---> | CategoryCache (Sync) |                                            |
|  |  (Parse Debezium)  |       | (In-Memory Lookup)   |                                            |
|  +--------------------+       +----------------------+                                            |
|           | (BulkItem: Index / Delete)                                                            |
|           v                                                                                       |
|  +---------------------------------------------------------------------------------------------+  |
|  |                         Mikro-Batching Buffering Motoru                                     |  |
|  |                                                                                             |  |
|  |   Tetikleyici 1: Boyut Limiti (Buffer >= 500 adet)                                          |  |
|  |   Tetikleyici 2: Zaman Limiti (time.Ticker 200 ms)                                          |  |
|  |   Tetikleyici 3: Graceful Shutdown (ctx.Done())                                             |  |
|  +---------------------------------------------------------------------------------------------+  |
|           |                                                                                       |
|           | 1. NDJSON Formatlama (version_type=external_gte)                                      |
|           v                                                                                       |
|  +--------------------+                     +--------------------+                                |
|  | Elasticsearch HTTP |                     |    Kafka Commit    |                                |
|  |    POST /_bulk     | --(Eğer Başarılı)-->| CommitMessages()   |                                |
|  +--------------------+                     +--------------------+                                |
|           |                                                                                       |
|           v                                                                                       |
|  [Elasticsearch Cluster]                                                                          |
+---------------------------------------------------------------------------------------------------+
```

---

## 3.1. Go Proje İskeleti ve Kafka Consumer Setup

### 1. Kütüphane Seçimi: `segmentio/kafka-go`
* **Neden Seçildi?**
  * **Pure Go (CGO Bağımsızlığı):** `confluent-kafka-go` gibi C tabanlı `librdkafka` derlemesi gerektirmez. Dockerize edilmesi, cross-compile edilmesi ve Alpine üzerinde çalıştırılması sorunsuzdur.
  * **Gelişmiş Reader API:** Partition rebalance, consumer group yönetimi ve mesaj fetch işlemlerini native Go kanalları ve context'leri ile yönetir.

### 2. Consumer Group Mimarisi (`group.id`)
* **Grup Adı:** `es-products-sync-group`
* **Yatay Ölçekleme (Horizontal Scalability):** Aynı `group.id` ile birden fazla Go Worker ayağa kaldırıldığında, Kafka partition'ları worker'lar arasında otomatik olarak paylaştırır. 3 partition için 3 bağımsız worker sıfır çakışmayla paralel çalışabilir.

### 3. Manuel Commit Yapılandırması (At-Least-Once Delivery)
* `CommitInterval: 0` yapılandırılarak Kafka-go'nun arka planda periyodik otomatik commit yapması engellenmiştir.
* `FetchMessage(ctx)` ile mesajlar bellek tamponuna (buffer) alınır ancak **commit edilmez**.
* Offset commit işlemi, **yalnızca ve yalnızca** Elasticsearch Bulk API'si HTTP 200 dönüp partideki tüm dokümanları kabul ettikten sonra `CommitMessages(ctx, msgs...)` ile toplu olarak gerçekleştirilir.
* Eğer Elasticsearch erişilemez duruma gelirse veya istek hata verirse mesajlar Kafka'da commit edilmemiş kalır; worker yeniden başladığında hiçbir veri kaybı (zero data loss) yaşanmadan kaldığı offset'ten devam eder.

---

## 3.2. Debezium Event Transformer & Data Flattening

### 1. Go Struct Modellemesi
* **Debezium Envelope:** [internal/model/debezium.go](file:///home/fatih/Masaüstü/backend_go/Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model/debezium.go)
  * `Before *ProductRaw`: Güncelleme/Silme öncesi satırın tam hali.
  * `After *ProductRaw`: Ekleme/Güncelleme sonrası satırın yeni hali.
  * `Op string`: `"r"` (Snapshot), `"c"` (Create), `"u"` (Update), `"d"` (Delete).
  * `Source DebeziumSource`: WAL LSN, TX id ve zaman damgası bilgileri.

### 2. Arama Motoru İçin Düzleştirme (Denormalizasyon)
* [internal/model/product.go](file:///home/fatih/Masaüstü/backend_go/Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model/product.go) içindeki `ProductSearchDocument` modeli:
  * PostgreSQL'deki `category_id (INT)` ilişkisi, [internal/transformer/cache.go](file:///home/fatih/Masaüstü/backend_go/Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/transformer/cache.go) içerisindeki thread-safe (`sync.RWMutex`) bellek içi önbellekten zenginleştirilerek `category_name` (ör: `"Keyboards"`, `"Audio & Headphones"`) alanı olarak dokümana düzleştirilir (flattening).

### 3. Silme Mantığı Ayrıştırması
* **`op == "d"` veya Tombstone Mesajı (`value == nil`):**
  * `before.ID` veya Kafka anahtarından doküman ID'si çıkartılır.
  * Elasticsearch için `ActionDelete` operasyonu üretilir:
    `{"delete": {"_index": "products", "_id": "<id>", "version": <version>, "version_type": "external_gte"}}`
* **`op == "c" || op == "u" || op == "r"`:**
  * `after` payload'ı alınıp `ProductSearchDocument` nesnesine dönüştürülür.
  * Elasticsearch için `ActionIndex` (upsert) operasyonu üretilir:
    `{"index": {"_index": "products", "_id": "<id>", "version": <version>, "version_type": "external_gte"}}`
    `{ ...doküman verileri... }`

---

## 3.3. Mikro-Batching ve Elasticsearch Bulk API Entegrasyonu

### 1. Çift Tetikleyicili (Dual-Trigger) Buffering Motoru
[internal/pipeline/batcher.go](file:///home/fatih/Masaüstü/backend_go/Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/pipeline/batcher.go) içerisinde iki bağımsız tetikleyici çalışır:
1. **Boyut Limiti (Size Trigger):** Tampondaki mesaj sayısı `BatchSize` (örn: 500) değerine ulaştığı anda beklemeden flush tetiklenir (Yüksek yazma yükü altında maksimum throughput).
2. **Zaman Limiti (Time Trigger):** Trafiğin azaldığı anlarda mesajların tamponda beklemesini önlemek için `time.Ticker` (örn: 200 ms) periyoduyla tampon boşaltılır (Düşük yazma yükü altında minimum gecikme / latency).
3. **Graceful Shutdown:** `SIGTERM` / `SIGINT` sinyali geldiğinde kanal kapatılır ve tampondaki son mesajlar Elasticsearch'e basılıp Kafka'ya commit edildikten sonra uygulama temiz biçimde sonlanır.

### 2. Sürüm Çakışması Koruması (External Versioning: `version_type=external_gte`)
* **Problem:** Ağ gecikmesi, partition rebalance veya consumer restart senaryolarında Kafka'dan eski bir güncelleme mesajı (`version: 2`), daha yeni bir güncellemeden (`version: 3`) sonra tüketiciye ulaşabilir. Normal bir index operasyonu yapılsaydı eski veri Elasticsearch'teki yeni veriyi ezerdi (stale data overwrite).
* **Çözüm:** Doküman versiyonu PostgreSQL'deki `version` kolonu ile eşlenir ve Elasticsearch'e `version_type=external_gte` ile gönderilir:
  * Eğer ES'teki doküman versiyonu gelen mesajdan büyükse (örnek: ES=3, Gelen=2), Elasticsearch isteği HTTP 409 (`version_conflict_engine_exception`) ile reddeder.
  * Go Worker bu 409 cevabını yakalar, güvenli bir şekilde loglar ve operasyonun başarılı olduğunu kabul ederek Kafka offset'ini ilerletir. Böylece ES'teki güncel veri her koşulda korunur!

---

## Çalıştırma ve Test

### 1. Testleri Çalıştırma
```bash
go test -v ./...
```

### 2. Go Worker'ı Doğrudan Host Üzerinde Çalıştırma
```bash
# Gerekli ortam değişkenleri varsayılan olarak localhost'a ayarlıdır:
go run ./cmd/sync-worker
```

### 3. Docker Compose ile Konteyner Olarak Çalıştırma
```bash
# Worker imajını derleyip arka planda başlatmak için:
docker-compose up -d --build
```
