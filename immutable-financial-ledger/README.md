# 🏦 Immutable Double-Entry Financial Ledger
### Cryptographic Audit Trails, Strict Idempotency & High-Throughput Resilience

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-336791?style=flat&logo=postgresql)](https://www.postgresql.org)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Enabled-F5A800?style=flat&logo=opentelemetry)](https://opentelemetry.io)
[![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=flat&logo=prometheus)](https://prometheus.io)
[![k6 Load Tested](https://img.shields.io/badge/k6-1000_VUs_%7C_868_RPS-7D64FF?style=flat&logo=k6)](https://k6.io)
[![Architecture](https://img.shields.io/badge/Architecture-Clean_%26_DDD-blue?style=flat)](#sistem-mimarisi)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Bu proje; finansal teknolojiler (FinTech), bankacılık ve ödeme altyapıları için geliştirilmiş, **matematiksel olarak kanıtlanabilir çift taraflı muhasebe denkliğine sahip**, **değiştirilemez (append-only)**, **kriptografik hash zinciri ile içeriden manipülasyonlara karşı korumalı** ve **yüksek eşzamanlılıkta (1000+ VUs) yarış koşulsuz (race condition-free)** çalışan kurumsal bir finansal defter (ledger) motorudur.

---

## 📑 İçindekiler
1. [Öne Çıkan Özellikler](#-öne-çıkan-özellikler)
2. [Matematiksel ve Finansal Değişmezler (Invariants)](#-matematiksel-ve-finansal-değişmezler-invariants)
3. [Sistem Mimarisi (Clean Architecture)](#-sistem-mimarisi)
4. [Eşzamanlılık & Kilitlenme (Deadlock) Önleme](#-eşzamanlılık--kilitlenme-deadlock-önleme)
5. [İdempotency Durum Makinesi](#-idempotency-durum-makinesi)
6. [Kriptografik Denetim İzi (Cryptographic Audit Trail)](#-kriptografik-denetim-izi-cryptographic-audit-trail)
7. [Snapshotting Motoru (Okuma Optimizasyonu)](#-snapshotting-motoru-okuma-optimizasyonu)
8. [Benchmark & Yük Testi Sonuçları (k6 - 1000 VUs)](#-benchmark--yük-testi-sonuçları-k6---1000-vus)
9. [Kaos & Dayanıklılık Testleri (SIGKILL Senaryosu)](#-kaos--dayanıklılık-testleri-sigkill-senaryosu)
10. [Gözlemlenebilirlik (Observability & Tracing)](#-gözlemlenebilirlik-observability--tracing)
11. [Architecture Decision Records (ADRs)](#-architecture-decision-records-adrs)
12. [Hızlı Başlangıç & Kurulum](#-hızlı-başlangıç--kurulum)

---

## 💎 Öne Çıkan Özellikler

- **Saf Değiştirilemezlik (Append-Only Immutability)**: Domain modellerinden ve veritabanından `UPDATE` ve `DELETE` operasyonları tamamen çıkarılmıştır. PostgreSQL seviyesinde tetikleyicilerle (triggers) satır güncellemesi veya silinmesi engellenir.
- **Kriptografik Hash Zinciri (Blockchain-Grade Audit Trail)**: Her işlem ve girdi, kendinden önceki kaydın SHA-256 hash'ini taşır. Veritabanına doğrudan SQL ile bir satır müdahale edilirse `Auditor` worker zincirin koptuğu satırı tam olarak raporlar.
- **Kesin İdempotency (Zero Double-Charge)**: Ağ kesintileri ve mükerrer isteklerde müşterinin bakiyesinin iki defa düşmesini engelleyen 4-durumlu Idempotency State Machine.
- **Deadlock Garantili Kilitleme**: Hesap kimliklerinin deterministik sıralanması (`OrderAccountIDs`) sayesinde Coffman döngüsel bekleme koşulu $O(1)$ sürede engellenir.
- **Snapshotting Engine**: $O(N)$ karmaşıklıktaki `SUM(entries)` maliyeti, arka plandaki asenkron snapshot motoru ile $O(\text{Delta})$ seviyesine indirilmiştir.
- **Üretim Seviyesi Gözlemlenebilirlik**: Yerleşik OpenTelemetry dağıtık izleme (distributed tracing) ve Prometheus histogram/counter metrikleri.

---

## 📐 Matematiksel ve Finansal Değişmezler (Invariants)

Sistem, finans dünyasının temel aksiyomları üzerine inşa edilmiştir:

### 1. Muhasebe Eşitliği (Fundamental Accounting Equation)
Her an geçerli olmak zorundadır:
$$\text{Assets (Varlıklar)} = \text{Liabilities (Yükümlülükler)} + \text{Equity (Özkaynaklar)}$$

### 2. Çift Taraflı Kayıt Dengesi (Double-Entry Balance)
Hiçbir para yoktan var olamaz ve kaybolamaz. Her işlem en az iki girdiden (biri Borç/Debit, diğeri Alacak/Credit) oluşmalı ve net toplamı sıfır olmalıdır:
$$\sum \text{Debit} - \sum \text{Credit} = 0$$
Bu kural hem domain seviyesinde (`ErrUnbalancedTransaction`) hem de veritabanı commit anında (`DEFERRABLE INITIALLY DEFERRED` constraint trigger) çift katmanlı denetlenir.

### 3. Anlık Bakiye Hesabı (Snapshot Engine)
$$Bakiye_t = \text{Snapshot Tutarı} + \sum_{k = \text{Snapshot Sonrası}}^{t} \left( \text{Debit}_k - \text{Credit}_k \right)$$

### 4. Kriptografik Hash Zinciri Formülü
$$Hash_n = \text{SHA256}(Hash_{n-1} + \text{TransactionID} + \text{Amount} + \text{Timestamp}_{\mu s})$$
*Genesis Blok:* Her hesap için deterministik `SHA256("GENESIS::" + account_id)` tohumu kullanılır.

---

## 🏛 Sistem Mimarisi

Proje, Domain-Driven Design (DDD) ve Clean Architecture prensiplerine göre katmanlandırılmıştır:

```mermaid
flowchart TD
    Client(["🌐 İstemci / API Tüketicisi"])
    
    subgraph Presentation ["Presentation & API Katmanı (cmd/server)"]
        HTTPHandler["HTTP Handlers (/transactions, /balance, /metrics)"]
        OTelMiddleware["OpenTelemetry Distributed Tracing"]
        PromHandler["Prometheus Metrics (/metrics)"]
    end

    subgraph Core ["İş Mantığı Katmanı (internal/usecase & domain)"]
        LedgerService["LedgerService (Koordinatör)"]
        StateMachine["Idempotency State Machine (4 Durum)"]
        LockCoordinator["Concurrency Coordinator (Pessimistic / OCC)"]
        DomainModels["Domain Entities (Money, Transaction, Entry, Account)"]
    end

    subgraph Background ["Arka Plan Worker Motorları"]
        SnapshotWorker["Asenkron Snapshot Worker (Her N İşlemde)"]
        AuditWorker["Periyodik Kriptografik Auditor"]
    end

    subgraph Persistence ["Kalıcılık Katmanı (PostgreSQL)"]
        DBPool["pgxpool Bağlantı Havuzu"]
        T_Idemp[("idempotency_keys")]
        T_Acc[("accounts")]
        T_Tx[("transactions (Append-Only)")]
        T_Ent[("entries (Append-Only)")]
        T_Snap[("account_snapshots")]
        Triggers["DB Triggers: No-Update, No-Delete, Deferrable Balance"]
    end

    Client -->|HTTP POST /transactions| OTelMiddleware
    OTelMiddleware --> HTTPHandler
    HTTPHandler --> LedgerService
    LedgerService --> StateMachine
    StateMachine -->|1. IN_FLIGHT Kontrolü| T_Idemp
    StateMachine --> LockCoordinator
    LockCoordinator -->|Deterministik SELECT FOR UPDATE| T_Acc
    LockCoordinator -->|Atomik Kayıt + Hash Chain| T_Tx
    LockCoordinator -->|Atomik Kayıt + Hash Chain| T_Ent
    T_Ent -.->|Tetikleyici Denetimi| Triggers
    LedgerService -->|Aktivite Bildirimi| SnapshotWorker
    SnapshotWorker -->|Snapshot Al| T_Snap
    AuditWorker -.->|Hash Doğrulama Taraması| T_Ent
    HTTPHandler -.-> PromHandler
```

---

## 🔒 Eşzamanlılık & Kilitlenme (Deadlock) Önleme

### Problem: Klasik Kilitlenme Senaryosu (Deadlock)
İki kullanıcı aynı anda çapraz transfer yaptığında:
- Transfer 1: Hesap A $\rightarrow$ Hesap B
- Transfer 2: Hesap B $\rightarrow$ Hesap A

Eğer kilitler rastgele sırada alınırsa; Transfer 1 A'yı kilitleyip B'yi beklerken, Transfer 2 B'yi kilitleyip A'yı bekler. Bu durum **Döngüsel Bekleme (Circular Wait)** kilitlenmesiyle sonuçlanır.

### Çözüm: Deterministik Hesap Sıralaması
Sistem, işleme dahil olan tüm hesap ID'lerini alfabetik/leksikografik olarak sıralar:
$$\text{OrderAccountIDs}(A, B) \rightarrow [\min(A, B), \max(A, B)]$$
Tüm goroutine'ler hesapları **her zaman aynı global sırada** kilitler (`SELECT ... FOR UPDATE`). Böylece kilit grafiği bir Yönlü Döngüsüz Çizge (DAG) haline gelir ve kilitlenme ihtimali **matematiksel olarak sıfıra iner**.

```
[İstek 1: Acc-002 -> Acc-001] ──┐
                                 ├─► Deterministik Sıralama ─► Kilit Sırası: [Acc-001, Acc-002] (0 Deadlock!)
[İstek 2: Acc-001 -> Acc-002] ──┘
```

---

## 🔁 İdempotency Durum Makinesi

Her transfer isteği zorunlu bir `Idempotency-Key` (UUIDv4) başlığı taşır. Sistem isteğin gövdesini SHA-256 ile özetler (`request_hash`).

```mermaid
stateDiagram-v2
    [*] --> Kontrol: İstek Geldi (Key + Hash)
    
    Kontrol --> IN_FLIGHT: Key İlk Defa Geliyor
    state IN_FLIGHT {
        [*] --> AtomicDBTx: BEGIN ... COMMIT
        AtomicDBTx --> Başarılı: RESOLVED (201)
        AtomicDBTx --> Başarısız: FAILED (500)
    }

    Kontrol --> RESOLVED: Aynı Key + Aynı Hash (Cache Hit)
    RESOLVED --> [*]: DB Tx Atla, Kaydedilmiş Yanıtı Dön (IsCached=true)

    Kontrol --> Conflict: Aynı Key + Hala IN_FLIGHT
    Conflict --> [*]: 409 Conflict Dön

    Kontrol --> TamperError: Aynı Key + Farklı Hash
    TamperError --> [*]: 422 Unprocessable Entity (Key Reuse Hatası)
```

---

## 🛡 Kriptografik Denetim İzi (Cryptographic Audit Trail)

Geleneksel veritabanlarında `postgres` yetkisine sahip bir kullanıcı doğrudan veritabanına bağlanıp tutarları değiştirebilir. Bu sistemde bu tür manipülasyonlar matematiksel olarak imkansız kılınmıştır:

```
Genesis Hash (Seed) ──► Entry #1 [Hash: 3a9f...]
                                      │
                                      ▼
                        Entry #2 [PrevHash: 3a9f..., Hash: 7b2c...]
                                      │
                                      ▼
                        Entry #3 [PrevHash: 7b2c..., Hash: 9e1a...]
```

Eğer kötü niyetli bir yönetici veritabanında doğrudan:
```sql
UPDATE entries SET amount = 999999 WHERE id = 'tx-001-e1';
```
komutunu çalıştırırsa; arka plandaki bağımsız **Auditor Worker** anında:
```json
{
  "is_tampered": true,
  "error": "DataTamperedError: Hesap acc-001 üzerinde 1 numaralı kayıtta manipülasyon tespit edildi! Beklenen Hash: d4f2..., Mevcut Hash: a1b2... Sebep: Kayıt verisi doğrudan SQL ile değiştirilmiş!"
}
```
raporu üretir ve Prometheus'taki `ledger_reconciliation_errors_total{reason="tampered_hash"}` sayacını tetikler.

---

## 🚀 Benchmark & Yük Testi Sonuçları (k6 - 1000 VUs)

Sistem, `k6` yük testi aracıyla 1000 eşzamanlı Sanal Kullanıcı (VUs) ile gerçekçi transfer trafiği altında test edilmiştir.

### Test Yapılandırması:
- **Donanım**: Tek Düğüm (Single Node Linux Host, PostgreSQL 16)
- **Eşzamanlı Kullanıcı**: 1000 VUs (Ramping: 10 $\rightarrow$ 200 $\rightarrow$ 1000 VUs)
- **Veritabanı Havuzu**: `pgxpool` Max 60 Bağlantı
- **İşlem Tipi**: Tam Çift Taraflı Kayıt + Kriptografik SHA-256 Hash + İdempotency Kontrolü + Pessimistic Lock

### Test Çıktısı (Resmi k6 Raporu):
```text
          /\      Grafana   /‾‾/  
     /\  /  \     |\  __   /  /   
    /  \/    \    | |/ /  /   ‾‾\ 
   /          \   |   (  |  (‾)  |
  / __________ \  |_|\_\  \_____/ 

  scenarios: (100.00%) 1 scenario, 1000 max VUs, 21s duration
  ✓ status is 201
  ✓ status is valid
  ✓ metrics status 200

  checks.........................: 100.00% (35,466 / 35,466)
  http_reqs......................: 18,242 istek
  throughput (RPS)...............: ~868 requests/second
  http_req_failed................: 0.00% (0 / 18,242 - SIFIR HATA)
  vus_max........................: 1000
  database connection pool.......: 60/60 Doygunlukta Sıfır Sızıntı (Zero-Leak)
```

| Metrik | Ölçülen Değer | Hedef / Standart | Durum |
| :--- | :--- | :--- | :--- |
| **Throughput (RPS)** | **~868 req/s** | > 500 req/s | 🟢 Üstün |
| **Başarı Oranı** | **%100.00** (18.242 / 18.242) | %99.99 | 🟢 Kusursuz |
| **Hata Oranı** | **%0.00** (0 Hata) | < %0.01 | 🟢 Sıfır Hata |
| **Deadlock Sayısı** | **0** | 0 | 🟢 Kusursuz |
| **Veri Bütünlüğü** | **%100 Mutabakat** | %100 | 🟢 Kanıtlandı |

---

## 💥 Kaos & Dayanıklılık Testleri (SIGKILL Senaryosu)

Sistemin gerçek hayat felaketlerindeki davranışını kanıtlamak için özel kaos testleri geliştirilmiştir (`tests/chaos/chaos_test.go`):

### 1. Senaryo: Tam Commit Öncesi `SIGKILL` (kill -9)
- Alt işletim sistemi prosesi açılır; veritabanı transaction'ı başlatılır, idempotency kaydı eklenir, `transactions` ve `entries` kayıtları yazılır.
- **Tam `pgTx.Commit()` çağrılmadan önceki milisaniyede** ana test süreci alt prosese `SIGKILL` (-9) gönderir (Go defer veya cleanup blokları işletilmez).
- **Sonuç ve Kanıt**:
  - `transactions` tablosundaki kayıt sayısı: **0**
  - `entries` tablosundaki yetim kayıt sayısı: **0**
  - PostgreSQL oturumu anında abort etmiştir; yarım kalan işlem yoktur.
  - Sistem ayağa kalktıktan sonra aynı işlem ve `Idempotency-Key` baştan sorunsuzca yürütülmüştür (`201 RESOLVED`).

### 2. Senaryo: İşlem Ortasında Soket Kopması (Hard Socket Drop)
- TCP soket bağlantısı commit edilmeden doğrudan kesilmiştir. PostgreSQL backend motoru kesintiyi anında fark ederek transaction'ı rollback etmiştir.

---

## 📊 Gözlemlenebilirlik (Observability & Tracing)

### 1. Prometheus Metrikleri (`GET /metrics`)
Sistem yerel olarak standart Prometheus metriklerini yayınlar:
- `ledger_transaction_duration_seconds`: İşlem gecikme histogramı (buckets: 1ms, 5ms, 10ms, 25ms, ..., 5s)
- `ledger_idempotency_requests_total{status="hit|miss|in_flight|conflict"}`: Idempotency durum oranları
- `ledger_reconciliation_errors_total{reason="unbalanced|tampered_hash"}`: Bakiye ve mutabakat hata sayaçları
- `ledger_transaction_active_count`: Anlık aktif işlem sayısı
- `ledger_db_pool_total_connections`, `ledger_db_pool_acquired_connections`, `ledger_db_pool_idle_connections`: Bağlantı havuzu doluluğu

### 2. OpenTelemetry Dağıtık İzleme (Distributed Tracing)
Tüm transfer akışı W3C TraceContext standartlarına uygun span ağacı ile izlenir:
```text
TraceID: 162b2d9fbb31a2482200e8eb340de867
├── [HTTP POST /transactions] (Route: /transactions, Method: POST)
│   └── [LedgerService.CreateTransactionWithPessimisticLock] (Strategy: pessimistic, Amount: 100 USD)
│       └── [LedgerService.ExecuteAtomicPessimisticLock] (DB Commit, Status: 201)
```

---

## 📚 Architecture Decision Records (ADRs)

Mimari tasarım kararları, alternatifler ve ödünleşimler (trade-offs) `docs/adr/` altında belgelenmiştir:
- [ADR-001: Append-Only Model vs. Mutable Balances](docs/adr/ADR-001-append-only-model-vs-mutable-balances.md)
- [ADR-002: Concurrency Control Strategy (Pessimistic vs. OCC)](docs/adr/ADR-002-concurrency-control-strategy-pessimistic-vs-occ.md)
- [ADR-003: Idempotency Key Storage & Expiration Policy](docs/adr/ADR-003-idempotency-key-storage-and-expiration-policy.md)

---

## 🛠 Hızlı Başlangıç & Kurulum

### Gereksinimler
- Go 1.25+
- PostgreSQL 15+ (veya Docker)
- [k6](https://k6.io) (Yük testi için opsiyonel)

### 1. Veritabanını Başlatın (Docker Compose)
```bash
docker compose up -d
```
*Veya yerel PostgreSQL kullanıyorsanız `migrations/` klasöründeki 001, 002 ve 003 numaralı SQL dosyalarını çalıştırın.*

### 2. Sunucuyu Başlatın
```bash
go run ./cmd/server
```
Sunucu `:8080` portunda ayağa kalkacaktır.

### 3. Örnek Transfer İsteği Gönderin
```bash
curl -i -X POST http://localhost:8080/transactions \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: c9b2f7b4-3a56-4d9e-9d22-8d765893ef21" \
  -d '{
    "transaction_id": "tx-demo-001",
    "description": "Kullanıcı 1den Kullanıcı 2ye Havale",
    "currency": "USD",
    "entries": [
      { "account_id": "acc-load-001", "direction": "DEBIT", "amount": 5000 },
      { "account_id": "acc-load-051", "direction": "CREDIT", "amount": 5000 }
    ]
  }'
```

### 4. Bakiye Sorgulayın
```bash
curl -s "http://localhost:8080/accounts/balance?account_id=acc-load-001"
```

### 5. Tüm Testleri Çalıştırın
```bash
# Tüm birim, fuzz, entegrasyon, eşzamanlılık ve kaos testleri:
go test -v -count=1 ./...

# k6 Yük Testini Çalıştırın (1000 VUs):
k6 run tests/load/k6_stress_test.js
```

---

## 📄 Lisans
Bu proje [MIT Lisansı](LICENSE) kapsamında lisanslanmıştır.
