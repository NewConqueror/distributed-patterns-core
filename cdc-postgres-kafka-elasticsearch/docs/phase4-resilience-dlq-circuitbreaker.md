# Faz 4: Dayanıklılık, Hata İzolasyonu ve İyileştirme

Bu doküman; ağ kopması, Elasticsearch kümesinin çökmesi veya bozuk veri (poison pill) gibi gerçek dünya senaryolarında Change Data Capture (CDC) hattının kilitlenmesini önleyen ve sıfır veri kaybı sağlayan dayanıklılık mimarisini açıklar.

---

## 4.1. Dead Letter Queue (DLQ) ve Zehirli Mesaj (Poison Pill) Yönetimi

### 1. Hata Sınıflandırması (Error Classification)

| Hata Kategorisi | Örnek Durumlar | Boru Hattı Davranışı |
| :--- | :--- | :--- |
| **Geçici Hatalar (Transient)** | • Elasticsearch bağlantı kopması (`connection refused`)<br>• HTTP 5xx Server Errors (500, 502, 503, 504)<br>• HTTP 429 Too Many Requests / Circuit Breaker trip | **DLQ'ya GÖNDERİLMEZ.** Mesaj tamponda tutulur, Circuit Breaker açılır ve küme iyileşene kadar exponential backoff ile beklenir. |
| **Kalıcı Hatalar (Permanent)** | • Geçersiz / bozuk JSON (Parse error)<br>• Veri tipi / mapping uyuşmazlığı (Elasticsearch 400 Bad Request, mapper_parsing_exception)<br>• Eksik zorunlu alanlar | **DLQ'ya YÖNLENDİRİLİR.** Mesaj `cdc.products.dlq` topic'ine yazılır ve ana topic offset'i commit edilerek pipeline'ın kilitlenmesi önlenir. |
| **Önemsiz Durumlar (Harmless)** | • HTTP 409 Version Conflict (`version_type=external_gte`)<br>• Zaten silinmiş doküman için HTTP 404 | Normal karşılanır. Loglanır ve offset ilerletilir. |

### 2. DLQ Producer ve Teşhis Başlıkları (Diagnostic Headers)
[internal/dlq/dlq.go](file:///home/fatih/Masaüstü/backend_go/Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/dlq/dlq.go) üzerinden `cdc.products.dlq` topic'ine yönlendirilen her zehirli mesaja aşağıdaki metaveriler başlık (header) olarak eklenir:

```text
x-error-type         : MALFORMED_JSON | MAPPING_CONFLICT | TRANSFORM_FAILED
x-error-reason       : invalid character 'M' looking for beginning of value
x-original-topic     : pg_cdc.public.products
x-original-partition : 1
x-original-offset    : 10452
x-failed-at          : 2026-09-25T12:50:00Z
```

### 3. Offset İlerlemesi (Non-Blocking Pipeline)
* Zehirli mesaj `RequiredAcks: RequireAll` garantisiyle DLQ topic'ine basıldıktan hemen sonra `reader.CommitMessages(ctx, msg)` çağrılarak ana topic'teki offset'i commit edilir.
* Bu sayede Go worker sonsuz çöküş döngüsüne (crash loop backoff) girmez ve arkadan gelen binlerce sağlıklı CDC olayını gecikmesiz işlemeye devam eder.

---

## 4.2. Circuit Breaker ve Backoff Stratejisi

```
             +-------------------------------------------------------+
             |                                                       |
             v                                                       |
       [State: CLOSED]                                               |
     (Normal CDC Akışı)                                              |
             |                                                       |
             | ES 5xx veya Bağlantı Hatası (Transient Error)         | ES Sağlıklı
             v                                                       | (status == green || yellow)
        [State: OPEN] -----------------------------------------------+
    (Consumer PAUSED)
             |
             +---> Arka Plan Health Probe (Exponential Backoff: 1s, 2s, 4s, 8s, 16s...)
                   GET /_cluster/health
```

### 1. Elasticsearch Sağlık Takibi ve Tüketim Duraklatma (Pause)
* [internal/circuitbreaker/circuitbreaker.go](file:///home/fatih/Masaüstü/backend_go/Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/circuitbreaker/circuitbreaker.go):
* Bulk isteği sırasında bağlantı reddedildiğinde veya ES 5xx döndüğünde `cb.Trip(err)` tetiklenir ve devre `OPEN` konumuna geçer.
* Tüketici döngüsü `cb.WaitIfOpen(ctx)` noktasında bloke edilerek Kafka'dan yeni mesaj çekimi duraklatılır (pause). Bu sayede bellekte işlenemeyecek milyonlarca mesajın birikmesi (OOM riski) engellenir.

### 2. Exponential Backoff ile Sağlık Yoklaması
* Devre açıldığında arka planda `/_cluster/health` endpoint'i yoklanmaya başlar:
  * 1. Deneme: 1 saniye sonra
  * 2. Deneme: 2 saniye sonra
  * 3. Deneme: 4 saniye sonra
  * 4. Deneme: 8 saniye sonra
  * 5. Deneme: 16 saniye sonra (ve tavan limitinde sabitlenir)

### 3. Otomatik Devam Etme (Auto-Resume)
* Elasticsearch ayağa kalkıp küme durumu `"green"` veya `"yellow"` olduğunda:
  * Devre tekrar `CLOSED` konumuna geçer.
  * `cb.WaitIfOpen(ctx)` kanalı serbest bırakılır.
  * Tüketici partition okumalarına kaldığı offset'ten otomatik olarak devam eder.

---

## 4.3. Graceful Shutdown (Temiz Kapanma)

[cmd/sync-worker/main.go](file:///home/fatih/Masaüstü/backend_go/Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/cmd/sync-worker/main.go) içinde işletim sistemi seviyesinde temiz kapanma mekanizması uygulanmıştır:

1. **Sinyal Yakalama:** `SIGINT` (Ctrl+C) veya `SIGTERM` (Docker stop / Kubernetes termination) sinyalleri yakalanır.
2. **Okumayı Durdurma:** Ana `context` iptal edilerek (`cancel()`) Kafka'dan yeni mesaj çekimi anında kesilir.
3. **Tampon Boşaltma (Final Buffer Flush):** Bellekte (`Batcher`) bekleyen son olaylar derhal NDJSON'a çevrilip Elasticsearch `/_bulk` API'sine basılır.
4. **Son Offset Commit:** Başarıyla yazılan son mesajların offset'leri Kafka'ya commit edilir.
5. **Kaynakların Kapatılması:** Kafka Reader, DLQ Writer ve HTTP bağlantıları güvenli biçimde kapatılır.

---

## Test ve Doğrulama Komutları

### 1. Birim Testleri Çalıştırma (State Machine, Error Classification & DLQ Headers)
```bash
go test -v ./...
```

### 2. Zehirli Mesaj Simülasyonu
```bash
./scripts/test-poison-pill.sh
```

### 3. DLQ Topic'ini Başlıklarıyla İzleme
```bash
./scripts/kafka/consume-dlq.sh
```
