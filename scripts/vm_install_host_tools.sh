#!/usr/bin/env bash
# Устанавливает на **хост** (не в контейнер) утилиты для воркшопа: make, psql.
# Локально: sudo ./scripts/vm_install_host_tools.sh
# Из GitHub Actions (SSH без TTY): VM_INSTALL_NONINTERACTIVE=1 bash scripts/vm_install_host_tools.sh
#   — использует только sudo -n; если NOPASSWD не настроен, пишет WARN и выходит 0 (деплой не ломается).
set -euo pipefail

if command -v make >/dev/null 2>&1 && command -v psql >/dev/null 2>&1; then
  echo "make и psql уже доступны — пропуск установки."
  exit 0
fi

noninteractive=false
if [[ "${VM_INSTALL_NONINTERACTIVE:-}" == "1" ]] || [[ "${CI:-}" == "true" ]]; then
  noninteractive=true
fi

if [[ "$(id -u)" -ne 0 ]] && ! command -v sudo >/dev/null 2>&1; then
  echo "Нужны права root: установите sudo или запустите скрипт от root." >&2
  if $noninteractive; then
    exit 0
  fi
  exit 1
fi

# Неинтерактивный режим (CI по SSH): только sudo -n, иначе запрос пароля невозможен.
if [[ "$(id -u)" -ne 0 ]] && $noninteractive; then
  if ! sudo -n true 2>/dev/null; then
    echo "WARN: sudo без пароля (NOPASSWD) не настроен — пропуск установки make/psql на хосте." >&2
    echo "      Выполните на VM один раз: sudo ./scripts/vm_install_host_tools.sh" >&2
    echo "      Или добавьте в sudoers для пользователя деплоя: NOPASSWD для apt-get/dnf/apk (см. README)." >&2
    exit 0
  fi
fi

run_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  elif $noninteractive; then
    sudo -n "$@"
  else
    sudo "$@"
  fi
}

export DEBIAN_FRONTEND=noninteractive

if command -v apt-get >/dev/null 2>&1; then
  run_root apt-get update -qq
  run_root apt-get install -y -qq make postgresql-client
  echo "OK: установлены make и postgresql-client (команда psql)."
elif command -v dnf >/dev/null 2>&1; then
  run_root dnf install -y make postgresql
  echo "OK: установлены make и postgresql (psql)."
elif command -v apk >/dev/null 2>&1; then
  run_root apk add --no-cache make postgresql-client
  echo "OK: установлены make и postgresql-client (psql)."
else
  echo "Не найден apt-get, dnf или apk — установите make и клиент psql вручную." >&2
  if $noninteractive; then
    exit 0
  fi
  exit 1
fi
