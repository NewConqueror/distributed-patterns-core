# Faz 5: Re-indexing ve Initial Snapshot Senkronizasyonu

Bu doküman; mevcut veritabanındaki milyonlarca satırın ilk yüklenmesi (Initial Snapshotting), su seviyesi (watermark) LSN senkronizasyonu, Debezium Sinyal Tablosu (Signaling Table) ile canlıda artımsal snapshot tetikleme ve Elasticsearch Alias Atomic Switch ile sıfır kesintili (zero-downtime) yeniden indeksleme süreçlerini açıklar.

---

## 5.1. İlk Yükleme (Initial Snapshotting) ve Watermarking

```
1. Initial Snapshot Başlar:
   [Postgres: products] ======(SELECT * FOR SNAPSHOT)======> [Debezium] ======> [Kafka: op="r", snapshot="true"]
                                                                   |
2. Canlı DML Gelir:                                                v
   [INSERT / UPDATE] ======(WAL Logunda Sıraya Alınır)=====> [LSN Watermark: 24891000]
                                                                   |
3. Snapshot Biter, Streaming Başlar:                               v
   [WAL Stream] =========(LSN 24891000'den itibaren)========> [Kafka: op="c"/"u"/"d", snapshot="false"]
```

### 1. Snapshot Modu: `snapshot.mode: initial`
* Konnektör ilk kez ayağa kalktığında ve kayıtlı bir replikasyon slot offset'i bulunmadığında çalışır.
* Hedef tablodaki (`public.products`) mevcut tüm veriler tutarlı bir transaction snapshot'ı ile taranır.
* Üretilen Kafka mesajlarının başlık ve payload'ında `op = "r"` (Read) ve `source.snapshot = "true"` yer alır.
* Bu esnada veritabanına gelen canlı `INSERT`, `UPDATE` veya `DELETE` işlemleri PostgreSQL Write-Ahead Log (WAL) üzerinde sıraya alınır; veritabanı kilitlenmez.

### 2. Watermarking ve Offset Senkronizasyonu
* Snapshot başlangıcında Debezium, PostgreSQL üzerindeki geçerli LSN (Log Sequence Number) değerini "High Watermark" olarak işaretler.
* Snapshot tamamlandığı anda konnektör bu LSN değerini Kafka Connect offset deposuna kaydeder ve streaming moda geçer.
* Downstream Go worker tarafında `version_type=external_gte` kullanıldığı için, snapshot okunurken aynı satır güncellenmiş olsa dahi daha yüksek sürüme sahip olan canlı kayıt korunur; veri tutarlılığı matematiksel olarak garantilenir.

---

## 5.2. Zero-Downtime Re-indexing ve Alias Mimarisi

Arama motoru şemasında (mapping) veya analyzer ayarlarında radikal bir değişiklik gerektiğinde (örneğin yeni bir dil analyzer'ı, ngram autocomplete veya alan tipi değişikliği), canlı sistemi durdurmadan indeks değişimi 4 adımda gerçekleştirilir:

```
[İstemciler / Arama API]
           |
           v
  [products_search_alias]
      /             \
(Adım 1: Aktif)   (Adım 4: Atomic Switch)
    /                 \
   v                   v
[products_v1]       [products_v2] (Yeni Mapping & Incremental Snapshot ile Dolduruldu)
```

### 1. Index Alias Mimarisi
* Arama yapan API'ler somut indeks adına (`products_v1`) değil, mantıksal arama takma adına (`products_search_alias`) bağlanır:
  ```http
  GET /products_search_alias/_search
  ```

### 2. Arka Planda Yeni İndeksin Açılması (`products_v2`)
* Yeni mapping ve ayarlarla `products_v2` indeksi oluşturulur.
* Bu esnada `products_search_alias` kesintisiz bir biçimde `products_v1`'e hizmet vermeye devam eder.

### 3. Debezium Sinyal Tablosu ile Canlı Snapshot Tetikleme
* Konnektörü durdurmadan, replikasyon slotunu silmeden veya PostgreSQL'i yeniden başlatmadan, PostgreSQL `debezium_signal` tablosuna tek bir SQL sorgusu atılır:

```sql
INSERT INTO debezium_signal (id, type, data)
VALUES (
    'ad-hoc-snapshot-' || gen_random_uuid(),
    'execute-snapshot',
    '{"data-collections": ["public.products"], "type": "INCREMENTAL"}'
);
```

* **Debezium Sinyal Mekanizması:**
  1. Debezium WAL üzerinden bu sinyali yakalar.
  2. Tablodaki Primary Key aralıklarını (chunk/window) su seviyesi (watermark) ile böler.
  3. Canlı veri akışını kesmeden, ürünleri arka planda Kafka topic'ine tekrar basar.
  4. Go worker bu kayıtları `products_v2` indeksine aktarır.

### 4. Alias Atomic Switch (Sıfır Kesinti)
* `products_v2` indeksi güncel veriye ulaştığında tek bir atomik Elasticsearch isteği ile takma ad taşınır:

```bash
POST /_aliases
{
  "actions": [
    { "remove": { "index": "products_v1", "alias": "products_search_alias" } },
    { "add":    { "index": "products_v2", "alias": "products_search_alias" } }
  ]
}
```

* **Kazanım:** Bu işlem tek bir transaction içinde mikro-saniye mertebesinde gerçekleşir. İstemciler tek bir istek bile kaybetmeden (zero downtime) yeni indeksten arama yapmaya başlar.

---

## Pratik Komutlar ve Test Akışı

### 1. Sinyal Tablosu ile Snapshot Tetikleme
```bash
./scripts/trigger-incremental-snapshot.sh public.products
```

### 2. Atomic Alias Değişimi
```bash
./scripts/es/switch-alias.sh products_v1 products_v2 products_search_alias
```

### 3. Faz 5 Doğrulama
```bash
./scripts/verify-phase5-reindex.sh
```
