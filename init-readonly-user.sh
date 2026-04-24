#!/usr/bin/env bash
set -e

: "${READONLY_DB_USER:=readonly_user}"
: "${READONLY_DB_PASSWORD:?READONLY_DB_PASSWORD is required for readonly DB bootstrap}"

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v postgres_db="$POSTGRES_DB" \
  -v readonly_user="$READONLY_DB_USER" \
  -v readonly_password="$READONLY_DB_PASSWORD" <<'SQL'
SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L',
    :'readonly_user',
    :'readonly_password'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_roles
    WHERE rolname = :'readonly_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN PASSWORD %L',
    :'readonly_user',
    :'readonly_password'
)
WHERE EXISTS (
    SELECT 1
    FROM pg_catalog.pg_roles
    WHERE rolname = :'readonly_user'
)
\gexec

GRANT CONNECT ON DATABASE :"postgres_db" TO :"readonly_user";
GRANT USAGE ON SCHEMA public TO :"readonly_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"readonly_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :"readonly_user";
ALTER ROLE :"readonly_user" SET statement_timeout = '15000';
SQL
