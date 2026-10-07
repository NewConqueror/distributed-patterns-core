# Faz 2: Debezium Connector, Topoloji ve Event Yakalama

Bu doküman, PostgreSQL WAL loglarının Debezium aracılığıyla dinlenmesi, şema doğrulaması, serileştirme modelleri, Kafka topic mimarisi ve event payload yapısının detaylarını içerir.

---

## 2.1. Debezium Connector JSON Tanımı ve Dağıtımı

Debezium REST API endpoint'ine (`POST /connectors`) gönderilen yapılandırma payload'ı:

```json
{
  "name": "inventory-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "cdc_user",
    "database.password": "cdc_secure_pass_2026",
    "database.dbname": "inventory_db",
    "topic.prefix": "pg_cdc",
    "table.include.list": "public.products",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_products_slot",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "filtered",
    "decimal.handling.mode": "double",
    "time.precision.mode": "connect",
    "tombstones.on.delete": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

### Parametre Analizi
| Parametre | Amaç ve Kritik Karar |
| :--- | :--- |
| `connector.class` | PostgreSQL için resmi Debezium konnektörü (`io.debezium.connector.postgresql.PostgresConnector`). |
| `plugin.name` | **`pgoutput`**: PostgreSQL 10+'da çekirdeğe dahil (built-in) standart logical decoding eklentisidir. `decoderbufs` veya `wal2json` gibi üçüncü parti eklenti derleme ihtiyacını ortadan kaldırır. |
| `publication.autocreate.mode` | **`filtered`**: Sadece `table.include.list` ile belirtilen tabloları içeren publication'ı kullanır veya oluşturur. Kısıtlı `cdc_user` yetkileri ile tam uyumludur. |
| `slot.name` | **`debezium_products_slot`**: Replikasyon yuvasına statik, belirgin bir isim verilir. Connector yeniden başladığında veya konteyner çöktüğünde yetim (orphan) slotlar oluşmasını engeller ve aynı slot üzerinden LSN takibini sürdürür. |
| `table.include.list` | **`public.products`**: Veritabanındaki diğer sistem veya operasyonel tablolar yerine yalnızca arama motoruna (Elasticsearch) gidecek tabloları filtreler. |
| `topic.prefix` | **`pg_cdc`**: Topic adlandırma şablonunun ön ekidir. Hedef topic: `pg_cdc.public.products`. |

---

## 2.2. Serileştirme ve Payload Formatı

### 1. Raw JSON Modu (`schemas.enable=false`) - Tavsiye Edilen
Ağ tüketimini ve JSON parsing CPU maliyetini minimize eder. Go consumer tarafında doğrudan struct'lara eşlenir.

```json
{
  "before": null,
  "after": {
    "id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "title": "Logitech MX Master 3S",
    "description": "Ergonomic wireless mouse",
    "price": 99.99,
    "category_id": 101,
    "status": "ACTIVE",
    "version": 1,
    "updated_at": 1727264100000
  },
  "source": {
    "version": "2.7.3.Final",
    "connector": "postgresql",
    "name": "pg_cdc",
    "ts_ms": 1727264100123,
    "snapshot": "false",
    "db": "inventory_db",
    "schema": "public",
    "table": "products",
    "txId": 650,
    "lsn": 24890128
  },
  "op": "c",
  "ts_ms": 1727264100150
}
```

### 2. Şema Metadatalı Mod (`schemas.enable=true`)
Payload ile birlikte her kolonun tipini, zorunluluğunu ve şema sürümünü taşır. Şema evrimi (schema evolution) doğrulamasında faydalıdır ancak mesaj boyutunu 5-10 kat artırır.

### 3. Tombstone Yönetimi (`tombstones.on.delete=true`)
Veritabanında bir satır silindiğinde (`DELETE FROM products WHERE id = '...'`):
1. **Adım 1 - DELETE Event Mesajı:**
   - **Key:** `{"id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}`
   - **Value:** `op = "d"`, `before` dolu (REPLICA IDENTITY FULL sayesinde), `after = null`.
   - **Tüketici Aksiyonu:** Go Consumer bu mesajı okur ve Elasticsearch'ten dokümanı siler (`DELETE /products/_doc/{id}`).
2. **Adım 2 - Tombstone (Boş) Mesajı:**
   - **Key:** `{"id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}`
   - **Value:** `null`
   - **Kafka Aksiyonu:** Kafka Log Cleaner (log compaction), bu key'e ait eski tüm partition log segmentlerini temizler (purge eder).

---

## 2.3. Topic Mimarisi ve Partition Stratejisi

### 1. Key Dağıtımı ve Sıralı Okuma (In-Order Processing)
```
[PostgreSQL Table: products (PK: id)]
                  |
                  v  (Debezium extracts PK as Kafka message key)
Key = {"id": "a0eebc99-..."}
                  |
                  v  (Kafka Murmur2 Partition Hash)
Partition = hash(Key) % 3  ---> Hep Partition-1'e düşer!
```
- Bir ürünün başlangıç snapshot kaydı (`r`), eklenmesi (`c`), güncellemeleri (`u`) ve silinmesi (`d`) **istisnasız aynı partition'a** yazılır.
- Kafka partition seviyesinde FIFO (First-In, First-Out) sıralama garantisi sunduğundan, downstream Go consumer olayları mutlak kronolojik sırayla işler. Out-of-order veri tutarsızlığı yaşanmaz.

---

## 2.4. Event Payload Analizi ve `op` Bayrakları

| `op` Kodu | Açıklama | `before` Durumu | `after` Durumu | Tipik Tüketici Aksiyonu |
| :---: | :--- | :---: | :---: | :--- |
| **`r`** | **Read (Snapshot)** | `null` | Mevcut satır verisi | Elasticsearch'e ilk indeksleme (`PUT /products/_doc/{id}`) |
| **`c`** | **Create (Insert)** | `null` | Yeni eklenen satır | Elasticsearch'e yeni kayıt (`PUT /products/_doc/{id}`) |
| **`u`** | **Update** | **Önceki veriler (FULL)** | **Yeni güncellenen veriler** | Diff kontrolü, kısmi veya tam indeks güncellemesi |
| **`d`** | **Delete** | **Silinmeden önceki veriler** | `null` | Elasticsearch'ten doküman silme (`DELETE /products/_doc/{id}`) |
| **-** | **Tombstone** | `null` | `null` | Tüketici görmezden gelir (Kafka compaction için) |
