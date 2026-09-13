#!/usr/bin/env sh
set -eu

docker compose up -d --wait

for migration in migrations/*.sql; do
  docker compose exec -T postgres sh -c \
    'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < "$migration"
done
