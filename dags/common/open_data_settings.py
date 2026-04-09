"""Загрузка конфигурации открытых источников (YAML) из /opt/airflow/config."""

from __future__ import annotations

import os
from typing import Any

import yaml

CONFIG_DIR = os.getenv("OPEN_DATA_CONFIG_DIR", "/opt/airflow/config")
PRIMARY = "open_data_sources.yaml"
FALLBACK = "open_data_sources.example.yaml"


def load_open_data_config() -> dict[str, Any]:
    for name in (PRIMARY, FALLBACK):
        path = os.path.join(CONFIG_DIR, name)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def get_source_config(source_key: str) -> dict[str, Any]:
    cfg = load_open_data_config()
    sources = cfg.get("sources") or {}
    if source_key not in sources:
        raise KeyError(f"Unknown open-data source_key: {source_key}")
    return sources[source_key]


def get_credentials(profile: str | None) -> dict[str, str]:
    if not profile:
        return {}
    cfg = load_open_data_config()
    creds = cfg.get("credentials") or {}
    block = creds.get(profile)
    if isinstance(block, dict):
        return {k: str(v) for k, v in block.items()}
    return {}
