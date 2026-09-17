# Proje Özeti: Distributed API Gateway & Dynamic Rate Limiter

## 1. Proje Vizyonu ve Amacı
Bu proje, standart web çerçeveleri (framework) ve hazır gateway paketleri yerine, ağ protokolleri, eşzamanlılık (concurrency), bellek yönetimi ve dağıtık durum senkronizasyonunu sıfırdan ele alan L4/L7 seviyesinde hafif ve yüksek performanslı bir ters proxy (reverse proxy) ve hız sınırlayıcı (rate limiter) sistemidir.

Projenin temel amacı; ağ gecikmesini (overhead) minimumda tutarak sınır aşım patlamalarını (boundary bursts) engelleyen, çoklu instance mimarilerinde atomik durum tutarlılığı sağlayan ve kesintisiz (zero-downtime) kural güncelleyebilen production-ready bir tooling bileşeni geliştirmektir.

---

## 2. Çözülen Problem & Teknik Kapsam
* **Ağ Seviyesinde Yönlendirme ve Dayanıklılık:** İstemci bağlantılarını dinamik upstream havuzuna yük dengelemeli (Round Robin / Least Connections) iletmek, çöken servisleri aktif/pasif health-check ile trafikten anında izole etmek.
* **Trafik Patlaması ve Sınır Aşımı (Rate Limiting):** Sabit pencere (Fixed Window) algoritmalarındaki pencere geçişi yük açıklarını ortadan kaldırmak için **Sliding Window Counter** mekanizmasını uygulamak.
* **Dağıtık Tutarlılık & Race Condition:** Çoklu gateway instance'larında paylaşılan durumu atomik tutmak için **Redis Lua scriptleri** ile RTT (Round Trip Time) maliyetini tek adıma indirmek; Redis kesintilerinde yerel in-memory fallback mekanizması kurgulamak.
* **Zero-Downtime Konfigürasyon:** Gateway sürecini (process) yeniden başlatmadan kuralları çalışma anında atomik işaretçi değişimi (atomic pointer swap) ile hot-reload yapabilmek.

---

## 3. Modüler Yol Haritası (Fazlar ve Alt Parçalar)

### Faz 1: Çekirdek Proxy Katmanı & Network Temelleri
* **1.1. Minimal HTTP/TCP Forwarding:** RFC 7230 hop-by-hop başlık ayıklama, `X-Forwarded-For` ve `X-Real-IP` enjeksiyonu.
* **1.2. Upstream Havuzu & Load Balancer:** Round Robin ve Least Connections algoritmalarıyla dinamik yönlendirme.
* **1.3. Aktif & Pasif Health-Check:** Periyodik arka plan yoklaması (active polling) ve ardışık 5xx/timeout durumunda devre kesici (circuit breaker) davranışı.
* **1.4. Connection Pool & Timeout Yönetimi:** Keep-alive TCP havuzu, `ReadTimeout`, `WriteTimeout` ve `IdleTimeout` yapılandırmaları.

### Faz 2: Rate Limiter Çekirdeği (Algoritmik Derinlik)
* **2.1. In-Memory Limiter (Tekil Düğüm):** Mutex çekişmesini azaltan ince taneli kilit (fine-grained locking) veya lock-free sayaçlar, periyodik TTL temizleyici.
* **2.2. Dağıtık Sliding Window (Redis + Lua):** Atomik pencere ağırlıklandırma formülü ve tekil ağ paketinde get-check-increment operasyonu.
* **2.3. Hata Toleransı & Fallback:** Redis bağlantı kopmalarında sistem kilitlenmesini önleyen Fail-Open/Fail-Closed ve In-Memory fallback stratejisi.
* **2.4. Standart HTTP Başlıkları:** `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` başlıkları ve limit aşımında `HTTP 429` dönüşü.

### Faz 3: Dinamik Kural Motoru & Hot-Reload
* **3.1. Çok Katmanlı Kural Matrisi:** Global, endpoint bazlı (örn. `/api/v1/checkout`) ve istemci/API-Key bazlı hiyerarşik eşleme.
* **3.2. Konfigürasyon Doğrulama:** JSON/YAML şema denetimi ve hatalı kural girişlerinin runtime öncesi engellenmesi.
* **3.3. Zero-Downtime Hot-Reload:** `SIGHUP` sinyali veya Redis Pub/Sub üzerinden lock-free bellek içi atomik işaretçi değişimi (pointer swap).

### Faz 4: Metrik, Profiling ve Benchmarking
* **4.1. Mock Upstream Test Ortamı:** Docker Compose ile gecikme ve hata üreten izole arka uç servisleri.
* **4.2. Yük Testi (Load Testing):** `k6` / `wrk` ile Baseline, Burst Traffic ve Upstream Failure senaryoları.
* **4.3. Profiling & Optimizasyon:** pprof ile bellek tahsisatı (heap allocation) tespiti, zero-copy buffer yaklaşımları.
* **4.4. Kanıt ve Karşılaştırma Raporu:** p50, p95 ve p99 gecikme sürelerini RPS ve bellek tüketimiyle belgeleyen doğrulama tablosu.

---

## 4. CV ve Mülakat Vurgusu
* **CV Madde Formatı:**
  > **Distributed API Gateway & Dynamic Rate Limiter**
  > * RFC 7230 uyumlu reverse proxy motorunu sıfırdan geliştirerek dinamik upstream routing, least-connections yük dengeleme ve periyodik health-check altyapısı kurdu.
  > * Fixed-window sınır patlamalarını engellemek amacıyla Sliding Window algoritmasını uyguladı; çoklu instance tutarlılığı için atomik Redis Lua scriptleri ve in-memory fallback mekanizması entegre etti.
  > * Zero-downtime kural güncellemeleri için bellek içi atomik pointer swap mimarisi tasarladı; `wrk` yük testleriyle saniyede 15k+ RPS altında p99 gecikmesini <18ms seviyesinde doğruladı.