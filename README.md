# Distributed Fault-Tolerant Task Orchestrator

Production-grade, distributed, fault-tolerant asynchronous task orchestration engine written in Go.

---

## Faz 1: Çekirdek Concurrency ve Yerel Worker Pool

Tek bir binary içinde harici bağımlılık olmadan bellek ve thread yönetimini kurar.

```
+-------------------------------------------------------------+
|                        Worker Pool                          |
|                                                             |
|  [Job Queue Chan] ---> (Worker 1) ---> panic recover()     |
|                   ---> (Worker 2) ---> ctx.Done() timeout   |
|                   ---> (Worker N) ---> sync.WaitGroup       |
+-------------------------------------------------------------+
```

- **State Machine**: `PENDING -> RUNNING -> COMPLETED / FAILED -> DLQ` durum geçiş kontrolü (`ValidateTransition`).
- **Worker Pool**: `runtime.NumCPU()` adet worker goroutine, buffered kanal ve backpressure koruması.
- **Panic Recovery**: Worker başına bağımsız `defer / recover()`, handler çökse dahi worker goroutine canlı kalır.
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

### Proje Mimarisi

- `internal/task/`:
  - `job.go`: `Job` modeli, durum kuralları, `Metadata` alanı ve OpenTelemetry `MapCarrier` adaptörü.
  - `errors.go`: `TemporaryError`, `FatalError`, `IsTemporary`, `IsFatal`.
- `internal/pool/`:
  - `pool.go`: Sabit boyutlu worker havuzu, panic koruması, Prometheus metrikleri ve OpenTelemetry trace enstrümantasyonu.
- `internal/distributed/`:
  - `queue.go`: Redis Streams entegrasyonu, `XGroupCreateMkStream`, `XADD`, `XACK`, `EnqueueDLQ`.
  - `heartbeat.go`: Dağıtık kilit edinme (`SET NX EX`), Lua tabanlı güvenli `ReleaseLock` ve periyodik `StartHeartbeat`.
  - `retry.go`: Exponential backoff + jitter hesaplama, Redis ZSET gecikmeli kuyruk ve atomik Lua poller.
  - `reaper.go`: PEL (`XPENDING`) taraması ve yetim işlerin `XCLAIM` ile devralınması.
  - `consumer.go`: `WorkerNode` ile stream tüketimi, heartbeat, pool yürütmesi ve ACK/Retry/DLQ yönlendirmesi.
- `internal/observability/`:
  - `metrics.go`: Prometheus metrikleri (`task_orchestrator_jobs_total`, `task_orchestrator_job_duration_seconds`, `task_orchestrator_active_workers`, `task_orchestrator_queue_depth`).
  - `tracing.go`: OpenTelemetry W3C distributed trace context enjeksiyonu (`InjectTraceContext`), çıkarılması (`StartJobSpan`), span attributeları ve hata kaydı.
  - `server.go`: Bağımsız HTTP portu (`:9090`) üzerinden `/metrics`, `/healthz` (liveness) ve `/ready` (readiness) servisleri.
- `deployments/prometheus/alerts.yml`:
  - `QueueBacklogTooHigh`: Bekleyen iş sayısı 10 dakika boyunca eşiği aştığında tetiklenen kritik alarm.
  - `DLQSpike`: Son 5 dakikada DLQ'ya düşen iş oranı toplam işlerin %5'ini aştığında tetiklenen kritik alarm.

---

### Testleri Çalıştırma

Tüm paketlerin birim ve entegrasyon testlerini race condition detector (`-race`) ile çalıştırmak için:

```bash
go test -count=1 -v -race ./...
```

### Sunucuyu Başlatma ve Endpointler

```bash
# Yerel In-Memory Modda:
go run ./cmd/server/main.go

# Dağıtık Redis Modunda (Redis açıkken):
REDIS_ADDR="127.0.0.1:6379" METRICS_PORT="9090" go run ./cmd/server/main.go
```

**Erişilebilir Endpoint'ler:**
- **Prometheus Metrikleri:** `http://localhost:9090/metrics`
- **Liveness Probu:** `http://localhost:9090/healthz`
- **Readiness Probu:** `http://localhost:9090/ready`
