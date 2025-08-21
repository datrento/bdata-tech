#!/bin/bash
set -e

# Enable logical replication
echo "Configuring PostgreSQL for CDC (Change Data Capture)..."
cat >> "${PGDATA}/postgresql.conf" << EOF
# CDC Configuration
wal_level = logical
max_wal_senders = 10
max_replication_slots = 5
EOF

# Allow replication connections
cat >> "${PGDATA}/pg_hba.conf" << EOF
# Allow replication connections
host replication postgres all md5
EOF

# Grant replication permissions to postgres user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" << EOF
ALTER USER postgres WITH REPLICATION;
EOF

echo "PostgreSQL CDC configuration complete!"
