from __future__ import annotations

from datetime import datetime

import requests
from airflow.decorators import dag, task

from common.open_data_settings import get_source_config
from common.pipeline_utils import (
    TOPIC_ODS_PUBLIC_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
)

SOURCE_KEY = "nasa_apod"
SOURCE_LABEL = "ODS_NASA_APOD"


@dag(
    dag_id="ods_nasa_apod_fetch",
    description="NASA APOD: REST → Kafka → raw.ods_public_api_payloads.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["workshop", "ods", "open-data", "kafka", "nasa"],
)
def ods_nasa_apod_fetch() -> None:
    @task
    def extract_to_kafka() -> None:
        spec = get_source_config(SOURCE_KEY)
        url = spec.get("url") or spec["base_url"].format(**spec["path_params"])
        params = dict(spec.get("query_params") or {})
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        publish_to_kafka(
            TOPIC_ODS_PUBLIC_RAW,
            {
                "source": SOURCE_LABEL,
                "endpoint": r.url,
                "payload": r.json(),
            },
        )

    @task
    def ingest_kafka_to_raw() -> int:
        return consume_raw_to_postgres(
            topic=TOPIC_ODS_PUBLIC_RAW,
            table="ods_public_api_payloads",
            group_id="workshop-raw-ods-public-loader",
        )

    extract_to_kafka() >> ingest_kafka_to_raw()


dag = ods_nasa_apod_fetch()
