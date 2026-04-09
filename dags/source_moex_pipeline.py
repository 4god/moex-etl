from __future__ import annotations

import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task

from common.pipeline_utils import (
    DS_STG_MOEX_READY,
    TOPIC_MOEX_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
    run_pipeline_sql,
)

_LOG = logging.getLogger(__name__)

MOEX_URL = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/securities.json"
    "?iss.meta=off&iss.only=securities,marketdata&limit=100"
)


@dag(
    dag_id="src_moex_ingestion",
    description="MOEX source DAG: API -> Kafka -> raw -> stg",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["workshop", "source", "moex"],
)
def src_moex_ingestion() -> None:
    """Load MOEX source independently from other sources."""

    @task
    def extract_moex_raw() -> None:
        _LOG.info("MOEX GET %s", MOEX_URL)
        response = requests.get(MOEX_URL, timeout=30)
        response.raise_for_status()
        _LOG.info("MOEX response status=%s final_url=%s", response.status_code, response.url)
        publish_to_kafka(
            TOPIC_MOEX_RAW,
            {
                "source": "MOEX_ISS",
                "endpoint": MOEX_URL,
                "payload": response.json(),
            },
        )

    @task
    def ingest_moex_raw_from_kafka() -> int:
        return consume_raw_to_postgres(
            topic=TOPIC_MOEX_RAW,
            table="moex_iss_payloads",
            group_id="workshop-raw-moex-loader",
        )

    @task(outlets=[DS_STG_MOEX_READY])
    def build_moex_stg_layer() -> None:
        run_pipeline_sql("SRC-110_moex_stg_refresh")

    extract_moex_raw() >> ingest_moex_raw_from_kafka() >> build_moex_stg_layer()


dag = src_moex_ingestion()
