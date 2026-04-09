#!/usr/bin/env bash
# Применяет все sql/bootstrap/*.sql по порядку (как initdb.d), но к уже работающему кластеру.
# Идемпотентно вместе с обновлённым 001_create_databases.sql — безопасно после каждого `docker compose up`.
set -euo pipefail

export PGHOST="${PGHOST:-postgres}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

# Избегаем старта до готовности сервера (редкие гонки в CI/Compose: healthcheck ок, приём соединений ещё нет).
for _i in $(seq 1 90); do
  if pg_isready -h "$PGHOST" -U "$PGUSER" -d postgres -q; then
    break
  fi
  if [[ "$_i" -eq 90 ]]; then
    echo "ERROR: Postgres at ${PGHOST} not ready after 90s (pg_isready)" >&2
    exit 1
  fi
  sleep 1
done

if ! psql -h "$PGHOST" -U "$PGUSER" -d postgres -v ON_ERROR_STOP=1 -c "SELECT 1 AS bootstrap_ping;" >/dev/null; then
  echo "ERROR: cannot run test query on postgres at ${PGHOST}" >&2
  exit 1
fi

mapfile -t files < <(find /bootstrap -maxdepth 1 -type f -name '*.sql' | sort)

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No SQL files in /bootstrap" >&2
  exit 1
fi

for f in "${files[@]}"; do
  echo "==> Applying $(basename "$f")"
  psql -h "$PGHOST" -U "$PGUSER" -v ON_ERROR_STOP=1 -d postgres -f "$f"
done

echo "Bootstrap apply finished OK (${#files[@]} files)."
