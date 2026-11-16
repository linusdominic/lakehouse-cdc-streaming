#!/bin/sh
# Least-privilege role for Debezium: replication + read, nothing else.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE ${CDC_USER} WITH LOGIN REPLICATION PASSWORD '${CDC_PASSWORD}';
GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${CDC_USER};
GRANT USAGE ON SCHEMA public TO ${CDC_USER};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${CDC_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${CDC_USER};
SQL

# The stock pg_hba.conf only allows replication connections from localhost.
echo "host replication ${CDC_USER} all scram-sha-256" >> "$PGDATA/pg_hba.conf"
