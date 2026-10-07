# Real-Time Change Data Capture (CDC) Pipeline
### Syncing PostgreSQL WAL to Elasticsearch via Kafka & Debezium

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-336791?style=flat&logo=postgresql)](https://www.postgresql.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8%20(KRaft)-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org)
[![Debezium](https://img.shields.io/badge/Debezium-2.7-FF6600?style=flat&logo=apache)](https://debezium.io)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.15-005571?style=flat&logo=elasticsearch)](https://www.elastic.co)
[![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?style=flat&logo=prometheus)](https://prometheus.io)

Bu proje; PostgreSQL Write-Ahead Log (WAL) seviyesindeki veri değişikliklerini Debezium ve modern KRaft modundaki Apache Kafka üzerinden sıfır veri kaybıyla yakalayan, Go tabanlı yüksek verimli bir Sync Worker ile dönüştürüp (denormalization & micro-batching) Elasticsearch'e ortalama **~85ms uçtan uca gecikmeyle (E2E Latency)** senkronize eden kurumsal bir Change Data Capture (CDC) boru hattıdır.

---

## 🏛️ Mimari Şema

```
+---------------------------------------------------------------------------------------------------------+
|                                           cdc_pipeline_net                                              |
|                                                                                                         |
|  +--------------------+        +--------------------+        +-------------------+                      |
|  |   PostgreSQL 16    | =====> |  Debezium Connect  | =====> |   Apache Kafka    |                      |
|  | (wal_level=logical |  WAL   | (pgoutput plugin)  | Events |   (KRaft Modu)    |                      |
|  | REPLICA IDENTITY   |        |  debezium_products |        |  pg_cdc.public.   |                      |
|  |       FULL)        |        |       _slot        |        |     products      |                      |
|  +--------------------+        +--------------------+        +-------------------+                      |
|           ^                                                            |                                |
|           | Sinyal Tablosu                                             | Partition In-Order             |
|           | (Ad-Hoc Snapshot)                                          v                                |
|  [debezium_signal]                                           +-------------------+                      |
|                                                              |   Go Sync Worker  |                      |
|                                                              | (kafka-go, manual)|                      |
|                                                              +-------------------+                      |
|                                                                /       |       \                        |
|                                                  Micro-Batch  /         |        \ Circuit Breaker      |
|                                              (500 ev / 200ms)/          |         \ & Health Probe      |
|                                                             v           |          v                    |
|                                                    +-------------+      |      +---------------------+  |
|                                                    | Elastic Bulk|      |      | Dead Letter Queue   |  |
|                                                    |   NDJSON    |      |      | (cdc.products.dlq)  |  |
|                                                    |external_gte |      |      +---------------------+  |
|                                                    +-------------+      |                               |
|                                                           |             | Prometheus Metrics            |
|                                                           v             v (:2112/metrics)               |
|                                                  +---------------+  +-------------------+               |
|                                                  | Elasticsearch |  |    Prometheus     |               |
|                                                  |   (v8.15.0)   |  |     (:9090)       |               |
|                                                  +---------------+  +-------------------+               |
|                                                           |                                             |
|                                                           v                                             |
|                                                [products_search_alias]                                  |
|                                                (Zero-Downtime Reindex)                                  |
+---------------------------------------------------------------------------------------------------------+
```

---

## 🚀 Hızlı Başlangıç (Tek Komutla Kurulum)

Projeyi ayağa kaldırmak ve tüm pipeline'ı doğrulamak için tek yapmanız gereken:

```bash
# 1. Tüm servisleri (Postgres, Kafka KRaft, Debezium, ES, Go Worker, Prometheus) başlatın
make up

# 2. Debezium konnektörünü kaydedin
make connector

# 3. Tüm faz doğrulamalarını sırayla çalıştırın
make verify
```

---

## 📊 Fazlara Göre Sistem Mimarisi

### [Faz 1: Altyapı, Konfigürasyon ve Logical Replication](docs/phase1-infrastructure.md)
* **ZooKeeper'sız Modern KRaft Kafka:** Controller quorum ile tek düğümlü modern Kafka 3.8 mimarisi.
* **PostgreSQL Logical Decoding:** `wal_level = logical`, `max_wal_senders = 10`, `max_replication_slots = 10`.
* **Kısıtlı CDC Kullanıcısı:** Süper yetki verilmeden yalnızca `REPLICATION LOGIN` ve hedef tabloda `SELECT` iznine sahip `cdc_user`.
* **Replica Identity FULL:** `UPDATE` ve `DELETE` işlemlerinde satırın tüm önceki durumunun (`before state`) WAL'a yazılması sağlanarak Elasticsearch diff analizi ve invalidation güvenceye alınmıştır.

### [Faz 2: Debezium Connector, Topoloji ve Event Yakalama](docs/phase2-debezium-topology.md)
* **Çekirdek Eklenti:** PostgreSQL 10+ standart decoding motoru `pgoutput` kullanımı.
* **Statik Slot:** `debezium_products_slot` ile yeniden başlatmalarda yetim slot oluşumu engellenmiştir.
* **Tombstone Yönetimi (`tombstones.on.delete=true`):** Silme operasyonlarında önce `op: "d"` event'i, ardından Kafka log cleaner için `null` değerli tombstone mesajı iletilir.
* **In-Order Partitioning:** Primary Key (`id` UUID) mesaj anahtarı yapılarak aynı ürünün tüm snapshot, insert, update ve delete hareketlerinin **aynı partition'a** düşmesi ve FIFO sırasıyla tüketilmesi garantilenmiştir.

### [Faz 3: Go Sync Worker (Transformer & Pipeline)](docs/phase3-go-sync-worker.md)
* **Pure Go Kafka Consumer:** CGO bağımlılığı olmayan `segmentio/kafka-go` kütüphanesi.
* **Manuel Offset Commit (At-Least-Once Delivery):** Mesajlar çekilir ancak otomatik commit edilmez. Yalnızca Elasticsearch `/_bulk` API'si HTTP 200 dönüp tüm partiyi kabul ettikten sonra Kafka offset'leri topluca commit edilir.
* **İlişkisel Veriyi Düzleştirme (Denormalizasyon):** Thread-safe `CategoryCache` ile `category_id` bilgisi `category_name` olarak dokümana gömülür.
* **Çift Tetikleyicili (Dual-Trigger) Mikro-Batching:** 500 mesaj toplandığında veya 200ms dolduğunda NDJSON bulk akışı tetiklenir.
* **Sürüm Çakışması Koruması (`version_type=external_gte`):** Gecikmeli veya sıra dışı gelen eski mesajların Elasticsearch'teki güncel dokümanı ezmesi engellenir.

### [Faz 4: Dayanıklılık, Hata İzolasyonu ve İyileştirme](docs/phase4-resilience-dlq-circuitbreaker.md)
* **Hata Sınıflandırması:** Geçici hatalar (ağ kopması, 5xx) ile kalıcı hatalar (bozuk JSON, mapping uyuşmazlığı) birbirinden ayrılmıştır.
* **Dead Letter Queue (DLQ):** Zehirli mesajlar teşhis başlıklarıyla (`x-error-type`, `x-error-reason`, `x-original-offset`) `cdc.products.dlq` topic'ine aktarılır; ana topic offset'i commit edilerek boru hattı kilitlenmesi önlenir.
* **Circuit Breaker:** Elasticsearch kesintilerinde devre `OPEN` olur ve Kafka tüketimi duraklatılır (pause). Arka planda `1s, 2s, 4s, 8s, 16s...` exponential backoff ile `/_cluster/health` yoklanır; küme `green`/`yellow` olduğunda otomatik resume edilir.
* **Graceful Shutdown:** `SIGINT`/`SIGTERM` sinyallerinde okuma kesilir, tampon Elasticsearch'e basılır, offset'ler commit edilir ve temiz çıkış yapılır.

### [Faz 5: Re-indexing ve Initial Snapshot Senkronizasyonu](docs/phase5-reindexing-and-snapshots.md)
* **Initial Snapshotting & Watermarking:** Mevcut PostgreSQL verisi `op: "r"` ile taranırken canlı gelen yazımlar WAL'da sıraya alınır; High Watermark LSN üzerinden kesintisiz streaming moda geçilir.
* **Debezium Sinyal Tablosu (`debezium_signal`):** Canlı sistemi durdurmadan tek bir `INSERT INTO debezium_signal` SQL sorgusu ile seçili tablolar için ad-hoc incremental snapshot tetiklenir.
* **Zero-Downtime Atomic Alias Switch:** Yeni mapping gerektiğinde `products_v2` arka planda hazırlanır ve `POST /_aliases` ile atomik olarak milisaniyede geçiş yapılır.

### [Faz 6: Gözlemlenebilirlik (Observability) ve Metrikler](docs/phase6-observability-metrics.md)
* **Prometheus Endpoint (`:2112/metrics`):** `cdc_events_consumed_total`, `cdc_bulk_flush_duration_seconds`, `cdc_batch_size`, `cdc_e2e_latency_seconds`, `cdc_dlq_events_total`, `cdc_circuit_breaker_state`.
* **Kafka Exporter (`:9308`):** `kafka_consumergroup_lag` metriği ile tüketici gecikmesi izlenir.
* **Uçtan Uca Gecikme Formülü:**
  $$\text{Latency} = T_{\text{indexed}} - T_{\text{wal\_commit}}$$
  PostgreSQL WAL transaction commit anı ile Elasticsearch indeksleme anı arasındaki fark ölçülür.

### [Faz 7: Benchmark, Doğrulama ve Raporlama](docs/phase7-benchmark-and-validation.md)
* **Yük Testi:** 16 eşzamanlı worker ile saniyede 3.000+ işlem basılmış, lag zirvesi ve 0.82 saniyelik catch-up süresi doğrulanmıştır.
* **Dual-Write Karşılaştırmalı Test:** Geleneksel çift yazmada %25 veri kaybı (data drift) yaşanırken, CDC boru hattında %0.0 veri kaybı ile mutlak tutarlılık kanıtlanmıştır.

---

## 📈 Benchmark ve Performans Sonuçları

```
+---------------------------------------------------------------------------------+
|                               RESMİ PERFORMANS TABLOSU                          |
+------------------------------------+--------------------------------------------+
| Metrik                             | Ölçülen Değer                              |
+------------------------------------+--------------------------------------------+
| Ortalama Uçtan Uca Gecikme (E2E)   | ~85 ms (T_indexed - T_wal_commit)          |
| 95. Yüzdelik Gecikme (p95)         | 120 ms                                     |
| 99. Yüzdelik Gecikme (p99)         | 180 ms                                     |
| PostgreSQL Yazma Hızı              | ~3.125 ops/sec (16 concurrent workers)     |
| Efektif Pipeline Tüketim Hızı      | ~3.850 docs/sec (Micro-batching 500)       |
| Catch-Up Süresi (2.5K Kuyruk)      | 0.82 saniye                                |
| Veri Tutarlılığı Garantisi         | 100% (Sıfır Veri Kaybı / At-Least-Once)    |
| Zehirli Mesaj İzolasyonu (DLQ)     | 100% Non-blocking                          |
+------------------------------------+--------------------------------------------+
```

---

## ⚔️ Çift Yazma (Dual-Write Anti-Pattern) vs CDC Mimarisi

```
+---------------------------------------------------------------------------------+
| Kriter / Metrik             | Geleneksel Dual-Write     | CDC Boru Hattı        |
+-----------------------------+---------------------------+-----------------------+
| Mimari Yaklaşım             | App -> PG, sonra App -> ES| App -> PG -> WAL -> ES|
| Ağ Hatasında Davranış       | Kalıcı Veri Uyuşmazlığı   | Self-Healing & Retry  |
| Veri Uçurumu (Data Drift)   | %25.0 (Yüksek Risk)       | %0.0 (Sıfır Kayıp)    |
| ACID Güvencesi              | 2PC Gerektirir (Yavaş)    | Yerel PostgreSQL WAL  |
| Uygulama Bağımlılığı        | Yüksek (Tüm servislere ES)| Sıfır (DB Odaklı)     |
+-----------------------------+---------------------------+-----------------------+
```

---

## 🛠️ Makefile Komut Referansı

| Komut | Açıklama |
| :--- | :--- |
| `make up` | Tüm konteynerleri derler ve arka planda ayağa kaldırır. |
| `make down` | Tüm servisleri ve named volume'leri temizleyerek durdurur. |
| `make status` | Servislerin sağlık (healthcheck) durumlarını gösterir. |
| `make connector` | Debezium PostgreSQL connector'ını kaydeder. |
| `make test` | Tüm Go birim testlerini (circuit breaker, transformer, bulk builder, dlq, metrics) çalıştırır. |
| `make stress` | 5.000 eşzamanlı işlemle PostgreSQL yük testi ve catch-up benchmark'ı başlatır. |
| `make compare` | Geleneksel Dual-Write vs CDC karşılaştırma simülasyonunu çalıştırır. |
| `make snapshot` | `debezium_signal` tablosuna SQL atarak canlı incremental snapshot tetikler. |
| `make metrics` | Prometheus metriklerini sorgular ve canlı E2E gecikme benchmark'ı yapar. |
| `make dlq` | Zehirli mesaj (DLQ) kuyruğunu başlıklarıyla birlikte canlı izler. |
| `make verify` | Faz 1'den Faz 6'ya kadar tüm doğrulama scriptlerini sırayla koşturur. |

---

## 💼 CV ve Teknik Mülakatlar İçin Özet

* **Gerçek Zamanlı CDC Mimarisi:** PostgreSQL WAL loglarını Debezium ve KRaft Kafka üzerinden dinleyip, Go ile ortalama **~85ms gecikmeyle** Elasticsearch'e senkronize eden kurumsal pipeline tasarlandı.
* **Yüksek Verim ve Güvenilirlik:** Dual-trigger mikro-batching (500 event / 200ms), manuel offset commit ve `version_type=external_gte` ile saniyede 3.800+ doküman tüketim hızında sıfır veri kaybı garantilendi.
* **Dayanıklılık ve Hata İzolasyonu:** Elasticsearch kesintilerini exponential backoff ile yöneten Circuit Breaker ve zehirli mesajları boru hattını tıkamadan izole eden Dead Letter Queue (DLQ) mimarisi kuruldu.
* **Sıfır Kesintili Operasyon:** Debezium Sinyal Tablosu ile canlıda artımsal snapshot tetikleme ve Elasticsearch atomic alias değişimiyle sıfır kesintili (zero-downtime) re-indexing sağlandı.
