# ADR-002: Concurrency Control Strategy (Pessimistic Locking vs. Optimistic Concurrency Control)

## Durum
**Kabul Edildi (Accepted)**

## Tarih
2026-09-24

## Karar Vericiler
Baş Sistem Mimarı (Principal Systems Architect), Dağıtık Sistemler Çalışma Grubu

---

## Bağlam ve Problem Tanımı (Context & Problem Statement)
Finansal sistemlerde "sıcak hesaplar" (hot accounts - örneğin merkezi bir havuz hesabı, borsa operasyon cüzdanı veya popüler bir satıcı hesabı) aynı saniye içinde yüzlerce hatta binlerce eşzamanlı para çekme ve yatırma isteğine maruz kalır.

Yüksek eşzamanlılık (high concurrency) altında iki temel risk mevcuttur:
1. **Yarış Koşulları ve Negatif Bakiye**: Bakiye kontrolü ile paranın düşülmesi arasına başka bir işlemin girmesi (Check-Then-Act race condition).
2. **Kilitlenme (Deadlock)**: İşlem A $Hesap_1 \rightarrow Hesap_2$ transferi yaparken İşlem B $Hesap_2 \rightarrow Hesap_1$ transferi yaparsa, iki işlem birbirinin kilidini bekleyerek kilitlenir (Deadlock).

---

## Değerlendirilen Seçenekler (Considered Options)

### Seçenek 1: Optimistic Concurrency Control (OCC) + Exponential Backoff
- `accounts` tablosunda `version BIGINT` sütunu tutulur.
- İşlem başında hesap versiyonu okunur. Kayıt anında:
  ```sql
  UPDATE accounts SET version = version + 1 
  WHERE id = $1 AND version = $expected_version;
  ```
  Eğer güncellenen satır sayısı 0 ise çakışma (`ErrOptimisticLockConflict`) oluşmuştur.
- Üst katmanda Jitter eklenmiş Üstel Geri Çekilme (Exponential Backoff) ile işlem tekrar denenir.
- **Değerlendirme**:
  - Düşük çakışmalı senaryolarda sıfır kilit maliyeti sunar.
  - Ancak aynı hesaba saniyede 1000 sanal kullanıcının (VUs) yüklendiği yüksek çakışmalı "sıcak hesap" ortamlarında **Yeniden Deneme Fırtınası (Retry Storm)** ve CPU doyumuna yol açar; p99 gecikmeleri katlanarak artar.

### Seçenek 2: Deterministik Sıralı Pessimistic Locking (SELECT ... FOR UPDATE)
- Transferde yer alan tüm hesap kimlikleri işlem başlamadan önce leksikografik/alfabetik olarak sıralanır (`OrderAccountIDs`):
  $$ID_1 < ID_2 < \dots < ID_k$$
- Sıralı olarak satır bazlı kilit alınır:
  ```sql
  SELECT id FROM accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE;
  ```
- **Değerlendirme**:
  - Coffman kilitlenme koşullarından "Döngüsel Bekleme" (Circular Wait) matematiksel olarak imkansız hale gelir ($O(1)$ Deadlock garantisi).
  - İstekler veritabanı motoru tarafından adil (fair) bir FIFO kuyruğunda sıraya alınır; boşa dönen retry CPU yükü oluşmaz.

---

## Karar (Decision Outcome)
**Birincil Üretim Stratejisi olarak Seçenek 2 (Deterministik Sıralı Pessimistic Locking) kabul edilmiştir.** Sistem mimarisi aynı zamanda Seçenek 1'i (OCC) destekleyecek şekilde çift-motorlu (pluggable) tasarlanmıştır.

### Teknik Gerekçeler ve Matematiksel Kanıt:
1. **Döngüsel Kilitlenme İmkansızlığı**:
   Tüm goroutine'ler $A$ ve $B$ hesaplarını kilitlerken her zaman $\min(A, B)$ hesabını önce, $\max(A, B)$ hesabını sonra kilitler. Bir $T_1$ işleminin $A$'yı tutup $B$'yi beklemesi, aynı anda $T_2$'nin $B$'yi tutup $A$'yı beklemesi grafikte bir yönlü döngü (cycle) gerektirir. Deterministik sıralama bu grafiğin bir Yönlü Döngüsüz Çizge (DAG - Directed Acyclic Graph) olmasını garanti eder.
2. **Yük Testi Kanıtı (Faz 5.2 - k6 Stres Testi)**:
   1000 VU altında yapılan yük testinde deterministik pessimistic lock sayesinde **18.242 işlemde 0 Deadlock ve %0 hata oranı** ile ~868 RPS throughput elde edilmiştir.

---

## Sonuçlar ve Performans Metrikleri (Consequences)

| Kriter | Deterministik Pessimistic Lock (Seçilen) | Saf OCC (Alternatif) |
| :--- | :--- | :--- |
| **Deadlock Riski** | Matematiksel olarak %0 (DAG Garantisi) | Yok |
| **Yüksek Çakışmada CPU Yükü** | Düşük (Bekleme durumunda thread uyur) | Çok Yüksek (Agresif Retry Loop) |
| **Sıcak Hesap Gecikmesi (p99)** | Öngörülebilir ve stabil | Değişken (Exponential Backoff'a bağlı) |
| **İmplementasyon Karmaşıklığı** | Kolay (`OrderAccountIDs` + `FOR UPDATE`) | Orta (Retry + Jitter + State Rollback) |
