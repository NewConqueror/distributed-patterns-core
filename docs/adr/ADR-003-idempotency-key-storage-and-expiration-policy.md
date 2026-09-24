# ADR-003: Idempotency Key Storage & Expiration Policy

## Durum
**Kabul Edildi (Accepted)**

## Tarih
2026-09-24

## Karar Vericiler
Baş Sistem Mimarı (Principal Systems Architect), Ödeme Altyapıları Güvenlik Ekibi

---

## Bağlam ve Problem Tanımı (Context & Problem Statement)
Dağıtık ödeme ve bankacılık sistemlerinde ağ kararsızlıkları (network timeouts, socket drops, client retries) kaçınılmazdır. İstemci bir transfer isteği gönderdiğinde sunucu işlemi başarıyla işleyebilir ancak istemciye dönen HTTP 201 yanıtı ağda kaybolabilir. İstemci aynı transferi tekrar denediğinde (retry) kullanıcının hesabından iki kez para çekilmemesi (**Double Spending / Double Charge önleme**) hayati önem taşır.

Çözülmesi gereken temel problemler:
1. **İki Aşamalı Yazma Problemi (Dual-Write Problem)**: İdempotency durumu ile finansal kayıtların ayrı sistemlerde (örn. Redis + PostgreSQL) tutulması durumunda biri başarılı diğeri başarısız olursa sistem tutarsızlığa düşer.
2. **Key Reuse Saldırıları**: Bir istemcinin daha önce başarılı olmuş bir Idempotency anahtarını farklı bir tutar veya alıcıyla yeniden kullanmaya çalışması.
3. **Bellek / Depolama Şişmesi**: Milyonlarca idempotency kaydının sonsuza kadar saklanmasının depolama maliyeti.

---

## Değerlendirilen Seçenekler (Considered Options)

### Seçenek 1: Bellek İçi Önbellek (Redis / Key-Value Store)
- İdempotency anahtarları Redis'te `SET NX EX` ile saklanır.
- **Dezavantajları**:
  - Redis çökerse veya bağlantı koparsa idempotency garantisi yok olur.
  - PostgreSQL transaction'ı ile Redis komutu atomik tek bir transaction oluşturamaz (Dağıtık Transaction / 2PC zorunluluğu). Redis'e yazılıp DB'de hata alınırsa veya tam tersi olursa "hayalet kilit" (ghost lock) kalır.

### Seçenek 2: PostgreSQL İlişkisel Tablosu ve Tek Atomik Transaction (Seçilen)
- `idempotency_keys` tablosu finansal veritabanında yer alır:
  ```sql
  CREATE TABLE idempotency_keys (
      key VARCHAR(255) PRIMARY KEY,
      request_hash VARCHAR(64) NOT NULL,
      status idempotency_status NOT NULL, -- IN_FLIGHT, RESOLVED, FAILED
      response_code INT,
      response_body JSONB,
      locked_at TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
  ```
- Finansal kayıtlar (`transactions` + `entries`) ile idempotency durumunun `RESOLVED` yapılması **tek bir `BEGIN ... COMMIT` bloğu** içinde çalıştırılır.

---

## Karar (Decision Outcome)
**Seçenek 2 (PostgreSQL İlişkisel Tablosu ve Atomik State Machine) seçilmiştir.**

### 4 Temel Senaryo ve Durum Makinesi (State Machine):
1. **Senaryo 1 (Yeni Anahtar)**:
   - Anahtar ilk defa geliyorsa `IN_FLIGHT` durumuyla kaydedilir.
   - İşlem başarıyla sonuçlandığında aynı DB transaction'ı içinde `status = 'RESOLVED'`, HTTP status code ve JSON response kaydedilir.
2. **Senaryo 2 (Mükerrer İstek - Cache Hit)**:
   - Aynı anahtar, aynı istek gövdesi SHA-256 hash'i ile ve durumu `RESOLVED` ise:
   - **Veritabanı transaction'ına hiç girmeden**, kaydedilmiş yanıt gövdesi doğrudan önbellekten (`IsCached = true`) istemciye döndürülür.
3. **Senaryo 3 (Payload Değişikliği / Key Reuse İhlali)**:
   - Aynı anahtar fakat farklı bir istek gövdesi (farklı SHA-256 hash) gelirse:
   - İşlem anında reddedilir: `422 Unprocessable Entity` (`ErrIdempotencyKeyReuseWithDifferentPayload`).
4. **Senaryo 4 (İşlem Sürerken Tekrar İstek - Concurrent In-Flight)**:
   - Aynı anahtar hala `IN_FLIGHT` durumundayken ikinci bir istek gelirse:
   - `409 Conflict` (`ErrIdempotencyKeyInFlight`) döndürülür.

---

## Yaşam Döngüsü ve Temizleme Politikası (Expiration Policy)

1. **İdempotency Penceresi (Retention TTL)**:
   Finansal işlemlerde standart yeniden deneme (retry) penceresi 24 saat ile 7 gün arasındadır. `idempotency_keys` tablosu üzerinde `created_at` indekslenir.
2. **Zaman Aşımı Temizliği (Janitor Worker)**:
   Periyodik olarak çalışan temizleme sorgusu:
   ```sql
   DELETE FROM idempotency_keys 
   WHERE created_at < NOW() - INTERVAL '7 days';
   ```
   Bu silme işlemi `transactions` ve `entries` tablolarına hiçbir şekilde dokunmaz; defterin kriptografik hash zinciri ve muhasebe kayıtları sonsuza kadar değişmez (immutable) kalır.
3. **Askıda Kalan Kilitler (Stale IN_FLIGHT Expiry)**:
   Eğer bir worker düğümü işletim sistemi çökmesi (SIGKILL / Out-Of-Memory) nedeniyle `IN_FLIGHT` durumundaki kaydı çözemeden ölürse:
   - `locked_at < NOW() - INTERVAL '2 minutes'` olan kayıtlar zaman aşımına uğramış (stale lock) kabul edilir ve güvenle yeniden işletilebilir.
