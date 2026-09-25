#!/usr/bin/env bash
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=====================================================${NC}"
echo -e "${BLUE}   Faz 1: Altyapı, Konfigürasyon ve CDC Doğrulama    ${NC}"
echo -e "${BLUE}=====================================================${NC}\n"

# 1. PostgreSQL Konfigürasyon ve Logical Replication Kontrolü
echo -e "${YELLOW}[1/4] PostgreSQL Logical Decoding Parametreleri Kontrol Ediliyor...${NC}"
docker exec -i cdc_postgres psql -U postgres -d inventory_db -c "
SELECT 
    name, 
    setting, 
    source 
FROM pg_settings 
WHERE name IN ('wal_level', 'max_wal_senders', 'max_replication_slots', 'track_commit_timestamp');
"

# 2. cdc_user ve Yetkilerin Doğrulanması
echo -e "\n${YELLOW}[2/4] cdc_user ve REPLICATION İzni Kontrol Ediliyor...${NC}"
docker exec -i cdc_postgres psql -U postgres -d inventory_db -c "
SELECT rolname, rolsuper, rolreplication, rolcanlogin 
FROM pg_roles 
WHERE rolname = 'cdc_user';
"

# 3. products Tablosu ve Replica Identity FULL Kontrolü
echo -e "\n${YELLOW}[3/4] 'products' Tablosu Replica Identity Kontrol Ediliyor...${NC}"
docker exec -i cdc_postgres psql -U postgres -d inventory_db -c "
SELECT 
    relname AS table_name,
    CASE relreplident
        WHEN 'd' THEN 'DEFAULT (Sadece PK)'
        WHEN 'n' THEN 'NOTHING'
        WHEN 'f' THEN 'FULL (Tüm kolonlar - before state aktif)'
        WHEN 'i' THEN 'INDEX'
    END AS replica_identity
FROM pg_class
WHERE relname = 'products';
"

# 4. Publication Kontrolü
echo -e "\n${YELLOW}[4/4] Logical Replication Publication Kontrol Ediliyor...${NC}"
docker exec -i cdc_postgres psql -U postgres -d inventory_db -c "
SELECT pubname, puballtables FROM pg_publication;
"

echo -e "\n${GREEN}✔ Faz 1 Veritabanı ve Logical Replication kontrolleri başarıyla tamamlandı!${NC}"
