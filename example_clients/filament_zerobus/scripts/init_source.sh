#!/bin/sh
# Runs once, when the PostgreSQL data volume is first created (mounted into docker-entrypoint-initdb.d).
# It creates the source database "shop" next to Filament's own "filament" database and loads the sample tables.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "CREATE DATABASE shop"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname shop \
  -v orders_table="${TABLE_PREFIX:-}orders" \
  -v payments_table="${TABLE_PREFIX:-}payments" \
  -f /seed/seed_source.sql
