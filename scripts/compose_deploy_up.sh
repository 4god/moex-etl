#!/usr/bin/env bash
# Устойчивый подъём стека для VM/CI: перед up — down (чистая сеть), COMPOSE_PARALLEL_LIMIT=1,
# затем два шага без лишних перезапусков: (1) kafka+postgres healthy, (2) один docker compose up — Compose
# сам соблюдает depends_on, bootstrap-apply не дёргается заново на каждом сервисе.
#
# Из корня репозитория: bash scripts/compose_deploy_up.sh
# Переменные: COMPOSE_DEPLOY_ATTEMPTS (по умолчанию 3),
#             COMPOSE_DEPLOY_SERIAL (1 = два шага kafka/postgres + полный up; 0 = сразу один up без ожидания),
#             STACK_PROFILES (по умолчанию --profile bi), COMPOSE_PARALLEL_LIMIT (по умолчанию 1).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export COMPOSE_PARALLEL_LIMIT="${COMPOSE_PARALLEL_LIMIT:-1}"

STACK_PROFILES="${STACK_PROFILES:---profile bi}"
MAX_ATTEMPTS="${COMPOSE_DEPLOY_ATTEMPTS:-3}"
SERIAL="${COMPOSE_DEPLOY_SERIAL:-1}"

compose_down() {
  docker compose --profile bi --profile dbt down --remove-orphans || true
  sleep 2
}

wait_kafka_postgres() {
  docker compose up -d kafka postgres
  local i k p
  for i in $(seq 1 50); do
    k=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}starting{{end}}' workshop-kafka 2>/dev/null || echo missing)
    p=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}starting{{end}}' workshop-postgres 2>/dev/null || echo missing)
    if [[ "$k" == "healthy" && "$p" == "healthy" ]]; then
      return 0
    fi
    if [[ "$i" -eq 50 ]]; then
      echo "Timeout: kafka=$k postgres=$p" >&2
      docker compose ps -a >&2 || true
      return 1
    fi
    sleep 2
  done
}

compose_up_two_phase() {
  wait_kafka_postgres
  echo "=== docker compose ${STACK_PROFILES} up -d --remove-orphans (один проход) ==="
  # shellcheck disable=SC2086
  docker compose ${STACK_PROFILES} up -d --remove-orphans
}

wait_bootstrap_apply_ok() {
  local i status code
  for i in $(seq 1 90); do
    status=$(docker inspect --format='{{.State.Status}}' workshop-bootstrap-apply 2>/dev/null || echo pending)
    if [[ "$status" == "exited" ]]; then
      code=$(docker inspect --format='{{.State.ExitCode}}' workshop-bootstrap-apply)
      echo "bootstrap-apply exit code: $code"
      if [[ "$code" == "0" ]]; then
        return 0
      fi
      docker logs workshop-bootstrap-apply 2>&1 | tail -n 120 >&2 || true
      return 1
    fi
    sleep 2
  done
  echo "bootstrap-apply: timeout" >&2
  docker logs workshop-bootstrap-apply 2>&1 | tail -n 120 >&2 || true
  return 1
}

one_attempt() {
  if [[ "$SERIAL" == "1" ]]; then
    compose_up_two_phase
  else
    echo "=== docker compose ${STACK_PROFILES} up -d --remove-orphans (без отдельного ожидания kafka/postgres) ==="
    # shellcheck disable=SC2086
    docker compose ${STACK_PROFILES} up -d --remove-orphans
  fi
  wait_bootstrap_apply_ok
}

log_failure() {
  echo "=== docker compose ps -a ===" >&2
  docker compose ps -a >&2 || true
  echo "=== workshop-metabase (tail) ===" >&2
  docker logs workshop-metabase 2>&1 | tail -n 80 >&2 || true
}

main() {
  local attempt
  compose_down
  docker compose build

  for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
    echo "=== compose_deploy_up: attempt $attempt/$MAX_ATTEMPTS (SERIAL=$SERIAL) ==="
    if one_attempt; then
      docker compose exec -T airflow-scheduler airflow dags reserialize || true
      echo "=== compose_deploy_up: OK ==="
      exit 0
    fi
    log_failure
    if [[ "$attempt" -eq "$MAX_ATTEMPTS" ]]; then
      exit 1
    fi
    echo "=== retry after down (attempt $((attempt + 1))) ==="
    compose_down
    if [[ "${COMPOSE_DEPLOY_REBUILD_ON_RETRY:-0}" == "1" ]]; then
      docker compose build
    fi
  done
  exit 1
}

main "$@"
