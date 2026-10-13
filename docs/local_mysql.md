# Local MySQL (Windows CMD)

Requires Docker Desktop. From the repository root:

```cmd
copy .env.example .env
```

Edit `.env` and set both passwords locally. Then:

```cmd
docker compose --env-file .env -f infrastructure/local/compose.yml up -d
docker compose --env-file .env -f infrastructure/local/compose.yml ps
```

Workbench: host `127.0.0.1`, port `3307`, user `taxi`, your local MYSQL_PASSWORD, database
`taxi_reporting`. For control-schema verification, connect as root with MYSQL_ROOT_PASSWORD.
The bootstrap script runs only on the first creation of the data volume. Later schema changes
require explicit migrations. Do not remove the volume to apply changes without a backup.

Stop compute with `docker compose --env-file .env -f infrastructure/local/compose.yml stop`.
Data remains in the named volume. MySQL 8.4 is the local development version, not a claim about
future Azure server-version compatibility; test migrations against the selected Azure version.

This instance listens only on localhost. ADF and Databricks cannot automatically reach it.
Use it for local tests; design the eventual cloud connection separately. No credentials should be
pasted into chat or committed to Git. Cloud MySQL deployment has not succeeded.
