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

### Proje Mimarisi

- `internal/task/`:
  - `job.go`: `Job` modeli, durum kuralları ve `Handler` arayüzü.
  - `errors.go`: `TemporaryError`, `FatalError`, `IsTemporary`, `IsFatal`.
- `internal/pool/`:
  - `pool.go`: Sabit boyutlu worker havuzu, panic koruması, graceful shutdown.
- `internal/distributed/`:
  - `queue.go`: Redis Streams entegrasyonu, `XGroupCreateMkStream`, `XADD`, `XACK`, `EnqueueDLQ`.
  - `heartbeat.go`: `LeaseManager` ile dağıtık kilit edinme (`SET NX EX`), Lua tabanlı güvenli `ReleaseLock` ve periyodik `StartHeartbeat` TTL yenileme döngüsü.
  - `retry.go`: `RetryManager` ile $WaitTime = Base \times 2^{RetryCount} + RandomJitter$ formülü, gecikmeli işlerin Redis Sorted Set (`tasks:delayed`) üzerinde tutulması ve Lua script ile atomik olarak ana akışa yeniden enjekte eden scheduler poller.
  - `reaper.go`: `Reaper` goroutine ile PEL (`XPENDING`) taraması, kilit düşmesi veya idle time aşımı durumunda yetim (orphan) işlerin `XCLAIM` ile canlı düğümlere devredilmesi.
  - `consumer.go`: `WorkerNode` ile Redis Stream (`XREADGROUP`), yerel Worker Pool, Lease/Heartbeat ve Ack/Retry/DLQ orkestrasyonu.
- `cmd/server/main.go`:
  - Hibrit çalışma modu: Redis varsa tam dağıtık çok düğümlü modda, yoksa izole yerel worker modunda çalışır.

---

### Testleri Çalıştırma

Tüm birim ve entegrasyon testlerini race condition detector (`-race`) ile çalıştırmak için:

```bash
go test -count=1 -v -race ./...
```

### Sunucuyu Başlatma

```bash
# Yerel In-Memory Modda:
go run ./cmd/server/main.go

# Dağıtık Redis Modunda (Redis açıkken):
REDIS_ADDR="127.0.0.1:6379" go run ./cmd/server/main.go
```
Durdurmak için `Ctrl+C` basarak Graceful Shutdown sürecini tetikleyebilirsiniz.
