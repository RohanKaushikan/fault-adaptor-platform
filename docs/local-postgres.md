# Local PostgreSQL

The development environment runs one PostgreSQL database named `platform` and uses one schema named `platform`. It publishes on port `55432` by default so it does not conflict with a local PostgreSQL installation. The example connection URL uses `127.0.0.1` so it reaches Docker directly.

Copy the example configuration, then start the database:

```sh
cp .env.example .env
docker compose up -d --wait
```

The database is ready when the Compose health check passes. The initial migration runs automatically when the volume is first created. Later migrations can be applied with `scripts/migrate.sh`.

Run the PostgreSQL integration tests with:

```sh
scripts/test-postgres.sh
```

Stop the database without removing data:

```sh
docker compose down
```

To remove the local database volume and start fresh:

```sh
docker compose down -v
```
