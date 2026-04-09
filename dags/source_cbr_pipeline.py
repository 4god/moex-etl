from __future__ import annotations

import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.cbr_backfill import (
    CBR_SOURCE_LABEL,
    cbr_archive_url,
    cbr_missing_dates,
)
from common.open_data_settings import (
    get_backfill_floor_date,
    is_ods_backfill_enabled,
    utc_today,
)
from common.pipeline_utils import (
    DS_STG_CBR_READY,
    TOPIC_CBR_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
    run_pipeline_sql,
)

_LOG = logging.getLogger(__name__)

CBR_URL_CURRENT = "https://www.cbr-xml-daily.ru/daily_json.js"


@dag(
    dag_id="src_cbr_ingestion",
    description="CBR source DAG: API -> Kafka -> raw -> stg (архив по датам при workshop_ods_backfill).",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["workshop", "source", "cbr"],
)
def src_cbr_ingestion() -> None:
    """Load CBR source independently from other sources."""

    @task
    def extract_cbr_raw() -> None:
        if is_ods_backfill_enabled():
            hook = PostgresHook(postgres_conn_id="dwh")
            floor = get_backfill_floor_date()
            ceiling = utc_today()
            if floor > ceiling:
                floor, ceiling = ceiling, floor
            missing = cbr_missing_dates(hook, floor, ceiling)
            if not missing:
                _LOG.info(
                    "CBR: окно %s..%s уже полностью в raw, новых GET нет",
                    floor,
                    ceiling,
                )
                return
            for d in missing:
                url = cbr_archive_url(d)
                _LOG.info("CBR archive GET %s", url)
                r = requests.get(url, timeout=30)
                if r.status_code == 404:
                    _LOG.warning("CBR archive 404, пропуск даты %s", d)
                    continue
                r.raise_for_status()
                _LOG.info("CBR archive status=%s final_url=%s", r.status_code, r.url)
                publish_to_kafka(
                    TOPIC_CBR_RAW,
                    {
                        "source": CBR_SOURCE_LABEL,
                        "endpoint": r.url,
                        "payload": r.json(),
                    },
                )
            return

        _LOG.info("CBR GET %s", CBR_URL_CURRENT)
        response = requests.get(CBR_URL_CURRENT, timeout=30)
        response.raise_for_status()
        _LOG.info("CBR response status=%s final_url=%s", response.status_code, response.url)
        publish_to_kafka(
            TOPIC_CBR_RAW,
            {
                "source": CBR_SOURCE_LABEL,
                "endpoint": CBR_URL_CURRENT,
                "payload": response.json(),
            },
        )

    @task
    def ingest_cbr_raw_from_kafka() -> int:
        return consume_raw_to_postgres(
            topic=TOPIC_CBR_RAW,
            table="cbr_daily_payloads",
            group_id="workshop-raw-cbr-loader",
        )

    @task(outlets=[DS_STG_CBR_READY])
    def build_cbr_stg_layer() -> None:
        run_pipeline_sql("SRC-120_cbr_stg_refresh")

    extract_cbr_raw() >> ingest_cbr_raw_from_kafka() >> build_cbr_stg_layer()


dag = src_cbr_ingestion()
