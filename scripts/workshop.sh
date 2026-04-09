#!/usr/bin/env bash
# Обертка над docker compose для хостов без GNU make (типично минимальная VM).
# Использование из корня репозитория: ./scripts/workshop.sh <команда>
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Как в Makefile: BI по умолчанию. Минимальный стек: STACK_PROFILES= ./scripts/workshop.sh up
STACK_PROFILES="${STACK_PROFILES:---profile bi}"

usage() {
  cat <<'EOF'
Использование: ./scripts/workshop.sh <команда>

  up                 docker compose up -d --build (STACK_PROFILES по умолчанию: --profile bi)
  down               docker compose down
  reset-db           ./scripts/reset_stack.sh
  apply-bootstrap    повторный sql/bootstrap через контейнер bootstrap-apply
  provision-students учётки Airflow + Metabase (Metabase — если в профиле bi)

Переменные окружения:
  STACK_PROFILES   например "" или "--profile bi" (по умолчанию --profile bi)

Примеры без BI:
  STACK_PROFILES= ./scripts/workshop.sh up
  STACK_PROFILES= ./scripts/workshop.sh provision-students   # только Airflow
EOF
}

case "${1:-}" in
  up)
    # shellcheck disable=SC2086
    docker compose ${STACK_PROFILES} up -d --build
    ;;
  down)
    docker compose down
    ;;
  reset-db)
    ./scripts/reset_stack.sh
    ;;
  apply-bootstrap)
    docker compose run --rm bootstrap-apply
    ;;
  provision-students)
    docker compose run --rm workshop-provision-airflow
    # shellcheck disable=SC2086
    docker compose ${STACK_PROFILES} run --rm workshop-provision-metabase
    ;;
  ""|-h|--help)
    usage
    exit 0
    ;;
  *)
    echo "Неизвестная команда: $1" >&2
    usage >&2
    exit 1
    ;;
esac
