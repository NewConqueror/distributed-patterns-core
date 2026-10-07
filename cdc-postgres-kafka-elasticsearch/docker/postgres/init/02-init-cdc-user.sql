-- ========================================================================
-- CDC Kullanıcısı ve Kısıtlı Yetkilendirme
-- ========================================================================
-- Süper kullanıcı (postgres) yerine, sadece replikasyon ve okuma
-- yetkilerine sahip kısıtlı cdc_user oluşturulur.
-- ========================================================================

DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'cdc_user') THEN

      CREATE ROLE cdc_user WITH REPLICATION LOGIN ENCRYPTED PASSWORD 'cdc_secure_pass_2026';
   END IF;
END
$do$;

-- Veritabanı ve Şema İzinleri
GRANT CONNECT ON DATABASE inventory_db TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;

-- CDC İlk Snapshot ve Olay Akışı için Tablolara SELECT Hakkı
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- Sequence izinleri (Varsa snapshot sırasında id tutarlılığı için)
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO cdc_user;
