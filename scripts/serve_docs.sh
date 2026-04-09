#!/bin/sh
# Локальный HTTP для docs/ (services-map.html). Запуск: из корня репозитория ./scripts/serve_docs.sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/docs" || exit 1
PORT="${PORT:-8765}"
exec python3 -m http.server "$PORT" --bind 0.0.0.0
