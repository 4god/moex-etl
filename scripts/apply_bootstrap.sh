#!/usr/bin/env bash
# Применяет все sql/bootstrap/*.sql по порядку (как initdb.d), но к уже работающему кластеру.
# Идемпотентно вместе с обновлённым 001_create_databases.sql — безопасно после каждого `docker compose up`.
set -euo pipefail

export PGHOST="${PGHOST:-postgres}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

mapfile -t files < <(find /bootstrap -maxdepth 1 -type f -name '*.sql' | sort)

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No SQL files in /bootstrap" >&2
  exit 1
fi

for f in "${files[@]}"; do
  echo "==> Applying $(basename "$f")"
  psql -v ON_ERROR_STOP=1 -d postgres -f "$f"
done

echo "Bootstrap apply finished OK (${#files[@]} files)."
