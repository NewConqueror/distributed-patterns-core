# Distributed Fault-Tolerant Task Orchestrator

## Faz 1: Çekirdek Concurrency ve Yerel Worker Pool

Bu fazın amacı, hiçbir dış ağ bağımlılığı (Redis, DB vb.) olmadan, tek bir binary içinde bellek ve goroutine yönetimini hatasız, güvenli ve dayanıklı bir şekilde kurmaktır.

```
+-------------------------------------------------------------+
|                        Worker Pool                          |
|                                                             |
|  [Job Queue Chan] ---> (Worker 1) ---> panic recover()     |
|                   ---> (Worker 2) ---> ctx.Done() timeout   |
|                   ---> (Worker N) ---> sync.WaitGroup       |
+-------------------------------------------------------------+
```

### Proje Mimarisi

- `internal/task/job.go`: 
  - `Job` veri yapısı (`ID`, `Type`, `Payload`, `CurrentState`, `MaxRetries`, `RetryCount`, `CreatedAt`, `UpdatedAt`).
  - `State` Enum: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `DLQ`.
  - State Machine Doğrulaması: `ValidateTransition` & `TransitionTo` kuralları.
  - `Handler` arayüzü ve `HandlerFunc` adaptörü.
- `internal/task/errors.go`:
  - `TemporaryError`: Yeniden denenebilir (retryable) geçici hatalar.
  - `FatalError`: Yeniden denenemez, doğrudan DLQ'ya gönderilen kritik veri hataları.
  - `IsTemporary` ve `IsFatal` tip sorgulama fonksiyonları.
- `internal/pool/pool.go`:
  - Sabit boyutlu worker goroutine yönetimi (`runtime.NumCPU()`).
  - Buffered kanal ile iş kuyruğu (`chan *task.Job`).
  - Kanal tıkanma koruması (Backpressure & `ErrQueueFull`).
  - Her iş için bağımsız `defer / recover()` ile Panic Recovery (worker'lar çökmez).
  - Hata sınıflandırmasına göre otomatik yeniden deneme (`MaxRetries`) veya DLQ'ya yönlendirme.
  - Kademeli Graceful Shutdown (`ShutdownTimeout`, `sync.WaitGroup`, `context.DeadlineExceeded`).
- `cmd/server/main.go`:
  - `signal.NotifyContext` ile `SIGINT` (Ctrl+C) ve `SIGTERM` sinyallerini yakalama.
  - Gerçek zamanlı örnek görev üreticisi ve simülasyonu.

### Testleri Çalıştırma

Tüm testleri race condition detector (`-race`) ile çalıştırmak için:

```bash
go test -v -race ./...
```

### Sunucuyu Başlatma

```bash
go run ./cmd/server/main.go
```
Durdurmak için `Ctrl+C` basarak Graceful Shutdown sürecini tetikleyebilirsiniz.
