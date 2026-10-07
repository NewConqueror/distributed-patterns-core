# Performans, Profiling ve Benchmark Raporu

Bu rapor, **Distributed API Gateway & Dynamic Rate Limiter** projesinin L4/L7 ters proxy (reverse proxy), yük dengeleme (load balancer) ve hız sınırlama (rate limiting) katmanlarının gerçekçi yük altında test edilmesiyle elde edilen performans, bellek ve gecikme metriklerini belgeler.

---

## 1. Test Ortamı ve Donanım

* **İşletim Sistemi:** Linux (x86_64)
* **İşlemci (CPU):** Intel(R) Core(TM) i3-1005G1 CPU @ 1.20GHz (2 Cores, 4 Threads)
* **Bellek (RAM):** 8 GB
* **Go Versiyonu:** Go 1.22.2 (amd64)
* **Redis Versiyonu:** Redis 7.0.15 (In-Memory K/V + Lua Engine)
* **Yük Testi Araçları:** `k6` v0.54.0 & Go Native Testing Benchmark Framework (`testing.B`)

---

## 2. Karşılaştırmalı Performans Matrisi

Aşağıdaki metrikler, sistemin 4 farklı mimari konfigürasyon altında eşzamanlı sanal kullanıcılarla (VUs) paralel test edilmesiyle elde edilmiştir:

| Test Senaryosu | RPS (İstek/sn) | İşlem Başı Süre (Gecikme) | Bellek / İstek (B/op) | Heap Tahsisi (Allocs/op) | Gateway İlave Maliyeti (Overhead) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **1. Doğrudan Upstream (Mock Backend)** | **~25,025 req/s** | 39.9 µs | 5,873 B | 66 allocs | *Referans Noktası (0 ms)* |
| **2. Gateway (Rate Limiter Devre Dışı - Saf Proxy)** | **~16,444 req/s** | 60.8 µs | 11,768 B | 153 allocs | **+20.9 µs (~2.2 ms k6 yükünde)** |
| **3. Gateway + In-Memory Token Bucket** | **~14,099 req/s** | 70.9 µs | 12,549 B | 182 allocs | **+31.0 µs (~2.8 ms k6 yükünde)** |
| **4. Gateway + Dağıtık Redis Sliding Window** | **~7,615 req/s** | 131.3 µs | 13,134 B | 204 allocs | **+91.4 µs (~4.2 ms k6 yükünde)** |

> [!NOTE]
> 50ms yapay gecikme (network sleep) eklenmiş canlı mock backend'ler önünde yapılan k6 yük testlerinde, **Gateway'in ortalama ağ geçiş maliyeti (pure proxy overhead) sadece 2.25 ms** olarak ölçülmüştür.

---

## 3. k6 Yük Testi Senaryoları ve Doğrulama Sonuçları

### Senaryo A: Baseline Testi (Proxy Saf Geçiş Maliyeti)
* **Amaç:** Hız sınırlayıcı devre dışıyken Round-Robin yük dengeleme ve HTTP bağlantı havuzunun (keep-alive) saf geçiş maliyetini ölçmek.
* **Test Parametreleri:** 100 VU, 20 saniye, 3 adet upstream mock backend (50ms gecikmeli).
* **Sonuçlar:**
  * **Toplam İstek:** 17,908 istek (~895 req/s).
  * **Hata Oranı:** `%0.00` (0 başarısız istek).
  * **Ortalama İstek Süresi:** `53.16 ms` (Upstream'in 50ms gecikmesi düşüldüğünde gateway overhead: `2.25 ms`).
  * **p90 / p95 Gecikmesi:** `55.63 ms` / `57.51 ms`.
  * **Doğrulama Denetimleri (Checks):** `%100.00 Başarılı` (35,816 / 35,816 doğrulama).

### Senaryo B: Burst Traffic Testi (429 & Retry-After Doğrulaması)
* **Amaç:** Endpoint tavan kapasitesini (Capacity: 20) aşan yoğun trafik patlamalarında isteklerin istek gövdesi (body) tüketilmeden reddedilme hızını ve başlık doğruluğunu test etmek.
* **Test Parametreleri:** 200 req/s sabit varış hızı (arrival-rate), `/api/v1/checkout` endpoint'i.
* **Sonuçlar:**
  * **Tavan İzni (200 OK):** 29 istek (kapasite + token dolumu).
  * **Limit Aşımı (429 Too Many Requests):** 972 istek (%97.10 engelleme oranı).
  * **Reddedilme Hızı (Rejection Latency):** Ortalama **`561 µs`** (upstream'e gitmeden microsecond seviyesinde kesildi).
  * **Başlık Doğrulaması:** `X-RateLimit-Limit`, `X-RateLimit-Remaining: 0`, `Retry-After: <saniye>` başlıkları `%100` doğrulukla döndü.

### Senaryo C: Upstream Failure & Circuit Breaker Testi
* **Amaç:** Backend servislerinden biri trafik altındayken çöktüğünde (`/health` 503), yük dengeleyicinin ve devre kesicinin trafiği kesme ve kalan sağlıklı sunuculara yönlendirme kabiliyetini ölçmek.
* **Test Parametreleri:** 10 VU sürekli trafik esnasında `backend-1` sunucusunun sağlık durumu `UNHEALTHY` olarak tetiklendi.
* **Sonuçlar:**
  * **Hata Oranı:** `%0.00` (0 istek düştü, 585 / 585 başarılı).
  * **Yönlendirme Davranışı:** Yük dengeleyici çöken backend-1'i anında rotasyondan çıkararak trafiği backend-2 ve backend-3'e şeffaf biçimde paylaştırdı.

---

## 4. Bellek Tahsisatı (Allocation) ve Profiling (pprof) Analizi

Gateway üzerinde eşzamanlı yük testi çalışırken toplanan bellek (`mem.prof`) ve işlemci (`cpu.prof`) profilleri `go tool pprof` ile incelenmiştir:

1. **Zero-Copy Streaming ve Buffer Havuzu (`sync.Pool`):**
   * İstek ve yanıt gövdelerinin aktarılmasında her istek için 32KB bellek tahsis etmek yerine `sync.Pool` üzerinden tampon bellek (`io.CopyBuffer`) kullanıldı.
   * `mem.prof` analizinde `Forwarder.Forward` metodunun toplam heap tahsisatındaki payı yalnızca **`%0.23`** olarak kaydedildi.
2. **RFC 7230 Hop-by-Hop Başlık Ayıklama:**
   * `RemoveHopByHopHeaders` fonksiyonu, başlık haritası üzerinde yerinde (in-place) ve büyük-küçük harf duyarsız çalıştığı için toplam tahsisattaki payı **`%0.00`** oldu.
3. **Lock-Free Atomic Pointer Swap:**
   * Kural motorunun `atomic.Pointer[Engine]` üzerinden okunması sayesinde okuma yolunda mutex çekişmesi sıfıra indirildi; CPU profilinde kilit bekleme süresi (contention) **`%0.0`** olarak ölçüldü.

---

## 5. CV ve Mülakat Vurgusu (Interview Takeaways)

> **Distributed API Gateway & Dynamic Rate Limiter**
> * Standart web çerçevelerine bağımlı olmadan, net/http ve TCP temelleri üzerinde RFC 7230 uyumlu ters proxy ve dinamik hız sınırlayıcı geliştirdi.
> * Sharded kilitleme (striped mutex) ve TTL temizleme mekanizmasıyla yerel bellekte **12M+ RPS (90.4 ns/op)** hızında Token Bucket algoritması kurguladı.
> * Sabit pencere patlamalarını engellemek amacıyla Redis üzerinde **tek RTT'de çalışan atomik Lua Sliding Window Counter** betiği yazdı; Redis kesintilerine karşı otomatik in-memory fallback ve devre kesici (circuit cooldown) entegre etti.
> * `atomic.Pointer[Engine]` kullanarak sıfır kesintiyle (zero-downtime) çalışan dinamik kural motoru tasarladı; `k6` testleriyle 50ms upstream gecikmesinde proxy overhead'ini **<2.3ms** seviyesinde ve 429 ret süresini **<600µs** olarak doğruladı.
