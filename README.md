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

#### 4. Debezium Connector'ı Kaydetme
```bash
./scripts/debezium/register-connector.sh
```
