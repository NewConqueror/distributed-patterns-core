# ADR-001: Append-Only Model vs. Mutable Balances

## Durum
**Kabul Edildi (Accepted)**

## Tarih
2026-09-24

## Karar Vericiler
Baş Sistem Mimarı (Principal Systems Architect), Finansal Yazılım Mühendisliği Ekibi

---

## Bağlam ve Problem Tanımı (Context & Problem Statement)
Geleneksel veritabanı tasarımlarında kullanıcı hesap bakiyeleri çoğunlukla `accounts` tablosunda `balance` sütunu olarak saklanır ve her para transferinde doğrudan `UPDATE accounts SET balance = balance + :amount WHERE id = :id` sorgusuyla güncellenir (Mutable Balance Model). 

Ancak bir finansal defterde (financial ledger):
1. **İç Denetim ve Adli İnceleme (Forensics)**: Geçmişe dönük bakiyenin anlık durumunu yeniden oluşturabilmek, herhangi bir para kayıbı veya iç hile (fraud) iddiasını kanıtlayabilmek için tam bir denetim izine (audit trail) ihtiyaç vardır.
2. **Düzenleyici Kurum Gereksinimleri (Regulatory Compliance)**: SOX, PCI-DSS, Basel III gibi standartlar muhasebe kayıtlarının asla silinemez ve değiştirilemez (WORM - Write Once, Read Many) olmasını şart koşar.
3. **Kayıp Güncelleme (Lost Update) & Yarış Koşulları**: Doğrudan bakiye sütununu mutate etmek, yüksek eşzamanlılıkta veri tutarsızlıklarına ve ara durumların kalıcı olarak yitirilmesine yol açar.

---

## Değerlendirilen Seçenekler (Considered Options)

### Seçenek 1: Değiştirilebilir Bakiye Modeli (Mutable Balances)
- `accounts` tablosunda `balance` sütunu güncellenir (`UPDATE`).
- Opsiyonel olarak `audit_logs` tablosuna arka planda log düşülür.
- **Dezavantajları**:
  - `UPDATE` sorgusu bir önceki değeri yok eder. Ara durumlar kaybolur.
  - Veritabanı yöneticisi veya DBA haklarına sahip bir saldırgan doğrudan SQL çalıştırarak bakiyeyi değiştirebilir.
  - İki tablonun (accounts ve audit_logs) asenkron güncellenmesi durumunda mutabakat (reconciliation) imkansız hale gelir.

### Seçenek 2: Salt Ekleme Yapılan Çift Taraflı Muhasebe Defteri (Append-Only Double-Entry Model)
- `entries` ve `transactions` tabloları sadece `INSERT` kabul eder.
- `UPDATE` ve `DELETE` operasyonları hem domain seviyesinde metotlardan çıkarılmış hem de veritabanı seviyesinde tetikleyicilerle (PostgreSQL triggers) bloke edilmiştir (`BEFORE UPDATE OR DELETE ON entries RAISE EXCEPTION`).
- Düzeltmeler yalnızca ters kayıt (`CreateReversalTransaction`) açılarak yapılabilir.
- Bakiye saklanan bir sayı değil, tüm girdilerin cebirsel toplamı ya da `Snapshot + Delta` türetilmiş değeridir.

---

## Karar (Decision Outcome)
**Seçenek 2 (Append-Only Double-Entry Model) seçilmiştir.**

### Teknik Gerekçeler:
1. **Matematiksel Kanıtlanabilirlik**:
   Muhasebe denkliği ($\sum \text{Debit} - \sum \text{Credit} = 0$) her işlemde zorunludur. Defterdeki anlık bakiye formülü:
   $$\text{Bakiye}_t = \sum_{i=1}^{t} \text{Debit}_i - \sum_{i=1}^{t} \text{Credit}_i$$
   şeklinde belirlenir. Bu modelde hiçbir bakiye gökten inmez; her kuruşun kaynağı ve varış noktası mevcuttur.
2. **Kriptografik Bütünlük (Hash Chain)**:
   Her `entry` kendisinden bir önceki satırın SHA-256 hash'ini taşır (`prev_hash`). Satır silinemediği ve güncellenemediği için hash zinciri deterministik olarak doğrulanabilir.
3. **Ters İşlem (Reversal) Disiplini**:
   Hatalı işlemler silinmez veya güncellenmez. Muhasebe standartlarına uygun olarak orijinal işlemin tam tersi (Debit $\leftrightarrow$ Credit) yeni bir kayıt üretilir ve `reversal_of_id` ile orijinal işleme bağlanır.

---

## Sonuçlar ve Ödünleşimler (Consequences & Trade-offs)

### Olumlu Sonuçlar (Pros):
- **%100 Denetlenebilirlik**: Sistem kurulduğu andan itibaren gerçekleşen her transferin geçmişi adli tıp hassasiyetinde mevcuttur.
- **Güvenlik**: Veritabanı seviyesinde `UPDATE`/`DELETE` yasaklandığı için SQL enjeksiyonu veya içeriden sabotaj durumlarında veri tahrif edilemez.
- **Determinizm**: Testlerde ve kaos senaryolarında her zaman aynı bakiye üretilir.

### Yönetilen Zorluklar ve Çözümleri (Mitigations):
- **Okuma Maliyeti ($O(N)$ SUM Problemi)**:
  - *Risk*: Yıllar içinde milyonlarca satır biriktiğinde `SELECT SUM(amount)` sorgusu yavaşlar.
  - *Çözüm*: **Snapshotting Engine (ADR-002)** devreye alınmıştır. Her $N$ işlemde bir hesap özeti (`account_snapshots`) asenkron olarak alınır. Bakiye hesaplama karmaşıklığı $O(N)$'den $O(\text{Delta})$'ya indirilmiştir.
- **Disk Büyümesi**:
  - *Risk*: Sürekli `INSERT` yapılması disk kullanımını artırır.
  - *Çözüm*: PostgreSQL partition pruning ve soğuk veriler için tablespace arşivleme stratejileriyle disk maliyeti optimize edilebilir. Finansal doğruluk disk maliyetinden her zaman önce gelir.
