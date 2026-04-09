"""Загрузка конфигурации открытых источников (YAML) из /opt/airflow/config."""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any

import yaml

CONFIG_DIR = os.getenv("OPEN_DATA_CONFIG_DIR", "/opt/airflow/config")
PRIMARY = "open_data_sources.yaml"
FALLBACK = "open_data_sources.example.yaml"

DEFAULT_ODS_BACKFILL_FLOOR = date(2025, 1, 1)


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


def resolve_open_data_url(spec: dict[str, Any]) -> str:
    """Собрать URL: либо `url`, либо `base_url`.format(**path_params)."""
    if spec.get("url"):
        return str(spec["url"])
    return str(spec["base_url"]).format(**spec["path_params"])


def open_data_query_params(spec: dict[str, Any]) -> dict[str, Any]:
    return dict(spec.get("query_params") or {})


def _ods_backfill_flag_from_env() -> bool | None:
    """True/False из env; None если ни одна переменная не задана."""
    for key in ("WORKSHOP_ODS_BACKFILL", "AIRFLOW_VAR_WORKSHOP_ODS_BACKFILL"):
        raw = (os.environ.get(key) or "").strip().lower()
        if raw in ("1", "true", "yes", "on"):
            return True
        if raw in ("0", "false", "no", "off"):
            return False
    return None


def is_ods_backfill_enabled() -> bool:
    """По умолчанию включено (true), кроме явного выключения в env/Variable."""
    env = _ods_backfill_flag_from_env()
    if env is not None:
        return env
    try:
        from airflow.models import Variable

        raw = Variable.get("workshop_ods_backfill", default_var="true")
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return True


def get_backfill_floor_date() -> date:
    """Нижняя граница окна backfill (по умолчанию 2025-01-01)."""
    for key in (
        "WORKSHOP_ODS_BACKFILL_FROM",
        "AIRFLOW_VAR_WORKSHOP_ODS_BACKFILL_FROM",
        "WORKSHOP_ODS_FROM_DATE",
        "AIRFLOW_VAR_WORKSHOP_ODS_FROM_DATE",
    ):
        raw = (os.environ.get(key) or "").strip()
        if raw:
            return date.fromisoformat(raw[:10])
    try:
        from airflow.models import Variable

        for vn in ("workshop_ods_backfill_from", "workshop_ods_from_date"):
            r = str(Variable.get(vn, default_var="")).strip()
            if r:
                return date.fromisoformat(r[:10])
    except Exception:
        pass
    return DEFAULT_ODS_BACKFILL_FLOOR


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def resolve_ods_ingest_mode(spec: dict[str, Any], params: dict[str, Any]) -> str:
    """
    gap_fill — в запросе есть временная ось (date / lastTimePeriod): догрузка пробелов по raw.
    snapshot_replace — снимок без поосевой догрузки: перед загрузкой raw для этого source очищается.
    Явно: ods_ingest_mode в YAML; иначе по наличию date / lastTimePeriod в query_params.
    """
    explicit = spec.get("ods_ingest_mode")
    if explicit in ("gap_fill", "snapshot_replace"):
        return str(explicit)
    if "date" in params or "lastTimePeriod" in params:
        return "gap_fill"
    return "snapshot_replace"


def get_credentials(profile: str | None) -> dict[str, str]:
    if not profile:
        return {}
    cfg = load_open_data_config()
    creds = cfg.get("credentials") or {}
    block = creds.get(profile)
    if isinstance(block, dict):
        return {k: str(v) for k, v in block.items()}
    return {}
