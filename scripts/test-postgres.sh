#!/usr/bin/env sh
set -eu

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
else
  set -a
  . ./.env.example
  set +a
fi

scripts/migrate.sh
DATABASE_URL="$DATABASE_URL" cargo test --test postgres_task_store -- --ignored --test-threads=1
