#!/usr/bin/env bash
# Устанавливает на **хост** (не в контейнер) утилиты для воркшопа: make, psql.
# Запуск на Debian/Ubuntu: sudo ./scripts/vm_install_host_tools.sh
# Из workflow деплоя вызывается с sudo (нужен NOPASSWD для пользователя deploy или интерактивный sudo).
set -euo pipefail

if command -v make >/dev/null 2>&1 && command -v psql >/dev/null 2>&1; then
  echo "make и psql уже доступны — пропуск установки."
  exit 0
fi

need_sudo() {
  if [[ "$(id -u)" -eq 0 ]]; then
    return 1
  fi
  return 0
}

SUDO=()
if need_sudo; then
  if ! command -v sudo >/dev/null 2>&1; then
    echo "Нужны права root: установите sudo или запустите скрипт от root." >&2
    exit 1
  fi
  SUDO=(sudo)
fi

export DEBIAN_FRONTEND=noninteractive

if command -v apt-get >/dev/null 2>&1; then
  "${SUDO[@]}" apt-get update -qq
  "${SUDO[@]}" apt-get install -y -qq make postgresql-client
  echo "OK: установлены make и postgresql-client (команда psql)."
elif command -v dnf >/dev/null 2>&1; then
  "${SUDO[@]}" dnf install -y make postgresql
  echo "OK: установлены make и postgresql (psql)."
elif command -v apk >/dev/null 2>&1; then
  "${SUDO[@]}" apk add --no-cache make postgresql-client
  echo "OK: установлены make и postgresql-client (psql)."
else
  echo "Не найден apt-get, dnf или apk — установите make и клиент psql вручную." >&2
  exit 1
fi
