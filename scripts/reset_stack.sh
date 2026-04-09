#!/usr/bin/env bash
# Полный сброс томов Postgres/Kafka и пересборка стека. После первого старта initdb + bootstrap-apply создают схему заново.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PROFILES=(--profile bi)
if [[ "${1:-}" == "--no-bi" ]]; then
  PROFILES=()
fi

docker compose down -v
docker compose "${PROFILES[@]}" up -d --build
echo "Готово. Airflow: http://localhost:8080 (admin/admin). Bootstrap применён сервисом bootstrap-apply."
