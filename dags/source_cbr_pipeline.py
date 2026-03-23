from __future__ import annotations

from datetime import datetime

import requests
from airflow.decorators import dag, task

from common.pipeline_utils import (
    DS_STG_CBR_READY,
    TOPIC_CBR_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
    run_pipeline_sql,
)

CBR_URL = "https://www.cbr-xml-daily.ru/daily_json.js"


@dag(
    dag_id="src_cbr_ingestion",
    description="CBR source DAG: API -> Kafka -> raw -> stg",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["workshop", "source", "cbr"],
)
def src_cbr_ingestion() -> None:
    """Load CBR source independently from other sources."""

    @task
    def extract_cbr_raw() -> None:
        response = requests.get(CBR_URL, timeout=30)
        response.raise_for_status()
        publish_to_kafka(
            TOPIC_CBR_RAW,
            {
                "source": "CBR_DAILY",
                "endpoint": CBR_URL,
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
