# Distributed Fault-Tolerant Task Orchestrator

Production-grade, distributed, fault-tolerant asynchronous task orchestration engine written in Go.

---

## Mimarisi ve Faz Özeti

| Faz | Kapsam | Durum |
| --- | --- | --- |
| **Faz 1** | Çekirdek Concurrency, Sabit Boyutlu Worker Pool, State Machine & Panic Recovery | Tamamlandı |
| **Faz 2** | Dağıtık Hata Toleransı, Redis Streams (PEL), Lease Heartbeat, Reaper & Delayed Retries | Tamamlandı |
| **Faz 3** | Observability, Prometheus Metrikleri, OpenTelemetry Dağıtık Tracing & Healthcheck | Tamamlandı |
| **Faz 4** | Chaos Testing (SIGKILL), Poison Pill İzolasyonu, Docker Compose & Benchmarklar | Tamamlandı |

---

## Faz 1: Çekirdek Concurrency ve Yerel Worker Pool

Tek bir binary içinde harici ağ bağımlılığı olmadan bellek ve thread yönetimini kurar.

```
+-------------------------------------------------------------+
|                        Worker Pool                          |
|                                                             |
|  [Job Queue Chan] ---> (Worker 1) ---> panic recover()     |
|                   ---> (Worker 2) ---> ctx.Done() timeout   |
|                   ---> (Worker N) ---> sync.WaitGroup       |
+-------------------------------------------------------------+
```

- **State Machine**: `PENDING -> RUNNING -> COMPLETED / FAILED -> DLQ` sıkı durum geçiş kontrolü (`ValidateTransition`).
- **Worker Pool**: `runtime.NumCPU()` adet worker goroutine, buffered kanal ve backpressure koruması.
- **Panic Recovery**: Worker başına bağımsız `defer / recover()`, handler panic atsa dahi worker goroutine canlı kalır.
- **Error Classification**: `TemporaryError` (yeniden denenebilir) ve `FatalError` (doğrudan DLQ) ayrımı.
- **Graceful Shutdown**: `signal.NotifyContext` ile OS sinyallerini yakalama ve `sync.WaitGroup` ile kademeli tahliye.

---

## Faz 2: Dağıtık Hata Toleransı, Heartbeat ve Kuyruk (Redis Streams)

Sistemi çok düğümlü (multi-node) hale getirir; iş kaybını, düğüm çökmelerini ve ağ gecikmelerini dağıtık olarak yönetir.

```
[Producer] ---> (Redis Stream: tasks:stream)
                     │
         XREADGROUP (Pending List - PEL)
                     │
                     ▼
              [Worker Node] ──(Heartbeat Loop)──> [Redis Lock: job:lock:{id}]
                     │ (Crash / Timeout)
                     ▼
          [Claimer/Reaper Goroutine] (XCLAIM)
```

- **Redis Streams & Consumer Groups**: `XGROUP CREATE`, `XADD`, `XREADGROUP` ve `XACK` ile Pending Entries List (PEL) üzerinden hatasız teslimat garantisi.
- **Heartbeat & Lease**: `SET NX EX` dağıtık kilit edinme ve Lua script ile periyodik TTL yenileme / güvenli serbest bırakma.
- **Orphan Job Reaper**: Askıda kalan veya kilidi düşen yetim işlerin `XCLAIM` ile canlı düğümlere devredilmesi ve poison pill koruması.
- **Exponential Backoff & Delayed Retries**: $WaitTime = Base \times 2^{RetryCount} + RandomJitter$ formülü, Redis ZSET (`tasks:delayed`) ve atomik Lua re-enqueue poller.

---

## Faz 3: Observability ve İzlenebilirlik

Sistemin iç durumunu görünür kılan ve performans darboğazlarını ortaya çıkaran izleme ve alarm katmanı.

```
[Worker App] ──(/metrics)──────────────> [Prometheus Server] ---> [Grafana Dashboard]
     │
     └──(OpenTelemetry Traces)────────> [Jaeger / Tempo]
```

- **Prometheus Metrikleri**:
  - `task_orchestrator_jobs_total{type, status}`: Counter
  - `task_orchestrator_job_duration_seconds{type}`: Histogram (0.005s - 10s)
  - `task_orchestrator_active_workers`: Gauge
  - `task_orchestrator_queue_depth`: Gauge
- **OpenTelemetry Tracing**: W3C `traceparent` context enjeksiyonu (`InjectTraceContext`), worker context extraction (`StartJobSpan`), span attributeları ve hata kaydı.
- **Health & Readiness**: `/healthz` (liveness) ve `/ready` (readiness - Redis ping denetimi).
- **Alerting Kuralları**: `deployments/prometheus/alerts.yml` (`QueueBacklogTooHigh`, `DLQSpike`).

---

## Faz 4: Dayanıklılık Doğrulaması ve Production-Ready Paketleme

### 1. Benchmark & Throughput (TPS) Ölçümleri

Intel(R) Core(TM) i3-1005G1 CPU @ 1.20GHz üzerinde yürütülen resmi `go test -bench` sonuçları:

| Benchmark Senaryosu | İterasyon (Ops) | Süre / Operasyon | Throughput (Kapasite) | Bellek Tahsisi |
| --- | --- | --- | --- | --- |
| **LocalWorkerPool** (Bellek içi kuyruk & havuz) | **202,949,578** | **12.14 ns/op** | **~82,300,000 TPS** | **0 B/op (0 allocs)** |
| **DistributedStreamEnqueue** (Redis Streams XADD) | **29,732** | **79.7 µs/op** | **~12,500 TPS** | 3,416 B/op (75 allocs) |

```bash
# Benchmarkları çalıştırmak için:
go test -bench=. -benchmem -run=^$ ./tests
```

### 2. Chaos / Resilience Testi (SIGKILL & Failover)

Yük altındayken worker process'lerine aniden `SIGKILL` (`kill -9`) gönderildiğinde kilit düşer, Reaper askıda kalan işleri `XCLAIM` ile yeni canlı düğüme devreder.

- **Kayıp İş Oranı:** %0 (Zero Lost Jobs)
- **Mükerrer Çalışma:** %0 (Idempotency doğrulanmıştır)

```bash
# Otomatik Go Chaos Testi:
go test -v -race ./tests -run TestChaos_WorkerCrashAndReaperRecovery

# Bash Tabanlı Canlı Chaos Testi:
./scripts/chaos_test.sh 10
```

### 3. Poison Pill Testi (Bozuk Veri ve Panic Koruması)

Bozuk JSON formatı veya nil pointer dereference fırlatan zehirli mesajlar worker havuzunu düşürmez; anında izole edilerek `tasks:dlq` akışına taşınır ve normal iş akışı kesintisiz devam eder.

```bash
go test -v -race ./tests -run TestPoisonPill_ResilienceAndDLQIsolation
```

### 4. Docker Compose ile Tüm Servisleri Ayağa Kaldırma

Tek komutla tüm dağıtık ekosistemi (Redis, 2x Worker Düğümü, Prometheus, Grafana, Jaeger) ayağa kaldırabilirsiniz:

```bash
docker compose up -d --build
```

**Erişilebilir Servisler:**
- **Grafana Dashboard:** `http://localhost:3000` (Kullanıcı: `admin` / Şifre: `admin`)
- **Prometheus UI:** `http://localhost:9090`
- **Jaeger Tracing UI:** `http://localhost:16686`
- **Worker 1 Metrikleri & Health:** `http://localhost:9091/metrics` | `http://localhost:9091/healthz`
- **Worker 2 Metrikleri & Health:** `http://localhost:9092/metrics` | `http://localhost:9092/healthz`
- **Redis Server:** `localhost:6379`

Durdurmak için:
```bash
docker compose down
```

---

### Tüm Testleri Çalıştırma

Tüm birim, entegrasyon ve dayanıklılık testlerini race condition detector (`-race`) ile doğrulamak için:

```bash
go test -count=1 -v -race ./...
```
