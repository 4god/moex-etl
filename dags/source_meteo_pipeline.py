from __future__ import annotations

import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task

from common.open_data_settings import (
    get_backfill_floor_date,
    is_ods_backfill_enabled,
    utc_today,
)
from common.pipeline_utils import (
    DS_STG_METEO_READY,
    TOPIC_OPEN_METEO_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
    run_pipeline_sql,
)

_LOG = logging.getLogger(__name__)

OPEN_METEO_BASE = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=55.7558&longitude=37.6173"
    "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
    "&timezone=Europe%2FMoscow"
)


def _open_meteo_url() -> str:
    """При включённом backfill — расширяем окно через past_days (ограничение API ~92 дня)."""
    if not is_ods_backfill_enabled():
        return OPEN_METEO_BASE + "&forecast_days=7"
    floor = get_backfill_floor_date()
    today = utc_today()
    ndays = min(max((today - floor).days + 1, 1), 92)
    return OPEN_METEO_BASE + f"&forecast_days=7&past_days={ndays}"


@dag(
    dag_id="src_meteo_ingestion",
    description="Open-Meteo source DAG: API -> Kafka -> raw -> stg",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["workshop", "source", "meteo"],
)
def src_meteo_ingestion() -> None:
    """Load Open-Meteo source independently from other sources."""

    @task
    def extract_open_meteo_raw() -> None:
        url = _open_meteo_url()
        _LOG.info("Open-Meteo GET %s", url)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        _LOG.info("Open-Meteo response status=%s url=%s", response.status_code, response.url)
        publish_to_kafka(
            TOPIC_OPEN_METEO_RAW,
            {
                "source": "OPEN_METEO",
                "endpoint": response.url,
                "payload": response.json(),
            },
        )

    @task
    def ingest_open_meteo_raw_from_kafka() -> int:
        return consume_raw_to_postgres(
            topic=TOPIC_OPEN_METEO_RAW,
            table="open_meteo_payloads",
            group_id="workshop-raw-open-meteo-loader",
        )

    @task(outlets=[DS_STG_METEO_READY])
    def build_meteo_stg_layer() -> None:
        run_pipeline_sql("SRC-130_meteo_stg_refresh")

    extract_open_meteo_raw() >> ingest_open_meteo_raw_from_kafka() >> build_meteo_stg_layer()


dag = src_meteo_ingestion()
