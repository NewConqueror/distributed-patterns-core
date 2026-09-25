#!/bin/bash
set -e

echo ">>> [CDC Init 01] Updating pg_hba.conf for CDC logical replication..."

# pg_hba.conf içine cdc_user için replication ve normal connection izinlerini ekle
cat <<EOF >> "$PGDATA/pg_hba.conf"

# CDC Logical Replication Entries
host replication ${CDC_USER:-cdc_user} 0.0.0.0/0 scram-sha-256
host replication ${CDC_USER:-cdc_user} ::/0 scram-sha-256
host all ${CDC_USER:-cdc_user} 0.0.0.0/0 scram-sha-256
host all ${CDC_USER:-cdc_user} ::/0 scram-sha-256
EOF

echo ">>> [CDC Init 01] pg_hba.conf updated successfully."
