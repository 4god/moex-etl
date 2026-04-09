#!/usr/bin/env bash
# Применяет все sql/bootstrap/*.sql по порядку (как initdb.d), но к уже работающему кластеру.
# Идемпотентно вместе с обновлённым 001_create_databases.sql — безопасно после каждого `docker compose up`.
set -euo pipefail

export PGHOST="${PGHOST:-postgres}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-10}"
# Идемпотентные CREATE … IF NOT EXISTS дают NOTICE «already exists» — убираем шум в логах (ERROR/WARNING видны).
export PGOPTIONS="-c client_min_messages=WARNING ${PGOPTIONS:-}"

echo "==> bootstrap-apply: PGHOST=${PGHOST} PGUSER=${PGUSER}"
echo "==> bootstrap-apply: contents of /bootstrap"
ls -la /bootstrap 2>&1 || {
  echo "ERROR: cannot list /bootstrap (volume ./sql/bootstrap не смонтирован или compose запущен не из корня репозитория)" >&2
  exit 1
}

if [[ ! -d /bootstrap ]]; then
  echo "ERROR: /bootstrap is not a directory" >&2
  exit 1
fi

# Ожидание встроенного DNS Docker и самого Postgres. Только pg_isready недостаточно: при «Temporary failure in name
# resolution» он так же падает, зато отдельный ping-psql после «успешного» pg_isready давал бы гонку.
# Повторяем реальный SELECT 1 — каждая попытка снова резолвит имя сервиса (обычно postgres).
echo "==> Waiting for Postgres at ${PGHOST}:5432 (DNS + TCP)..."
for _i in $(seq 1 120); do
  if psql -h "$PGHOST" -U "$PGUSER" -d postgres -c "SELECT 1" >/dev/null 2>&1; then
    echo "==> Postgres reachable (attempt ${_i})"
    break
  fi
  if [[ "$_i" -eq 120 ]]; then
    echo "ERROR: cannot connect to ${PGHOST}:5432 after 120s." >&2
    echo "  Частая причина: имя хоста не резолвится внутри контейнера (Docker DNS ещё не готов или другая сеть)." >&2
    echo "  Убедитесь: docker compose up из корня проекта; сервисы в одном project/сети; не network_mode: host у одного из сервисов." >&2
    if command -v getent >/dev/null 2>&1; then
      echo "  getent hosts ${PGHOST}:" >&2
      getent hosts "${PGHOST}" 2>&1 || true
    fi
    echo "  Last psql error:" >&2
    psql -h "$PGHOST" -U "$PGUSER" -d postgres -c "SELECT 1" 2>&1 || true
    exit 1
  fi
  sleep 1
done

# Список файлов без find в process substitution (при ошибке find + set -e скрипт мог выходить с 1 без сообщения).
shopt -s nullglob
_candidates=(/bootstrap/*.sql)
files=()
if ((${#_candidates[@]} > 0)); then
  readarray -t files < <(printf '%s\n' "${_candidates[@]}" | LC_ALL=C sort)
fi
shopt -u nullglob

if [[ ${#files[@]} -eq 0 ]]; then
  echo "ERROR: No *.sql in /bootstrap — на хосте пусто или неверный путь: запускайте docker compose из корня клона (рядом с папкой sql/)." >&2
  exit 1
fi

for f in "${files[@]}"; do
  echo "==> Applying $(basename "$f")"
  psql -h "$PGHOST" -U "$PGUSER" -v ON_ERROR_STOP=1 -d postgres -f "$f"
done

echo "Bootstrap apply finished OK (${#files[@]} files)."
