from __future__ import annotations

from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from common.open_data_settings import get_source_config
from common.pipeline_utils import (
    TOPIC_ODS_ECONOMIC_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
)

SOURCE_KEY = "eurostat_sdg_sample"
SOURCE_LABEL = "ODS_EUROSTAT_SDG_SAMPLE"


@dag(
    dag_id="ods_eurostat_sdg_fetch",
    description="Eurostat (JSON API): TaskGroup → Kafka → raw.ods_economic_payloads.",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["workshop", "ods", "open-data", "kafka", "eurostat", "task-group"],
)
def ods_eurostat_sdg_fetch() -> None:
    with TaskGroup(
        group_id="eurostat_ods",
        tooltip="Один запрос Eurostat → Kafka → сырой слой",
    ):
        @task(task_id="extract_to_kafka")
        def extract_to_kafka() -> None:
            spec = get_source_config(SOURCE_KEY)
            url = spec.get("url") or spec["base_url"].format(**spec["path_params"])
            params = dict(spec.get("query_params") or {})
            r = requests.get(url, params=params, timeout=180)
            r.raise_for_status()
            publish_to_kafka(
                TOPIC_ODS_ECONOMIC_RAW,
                {
                    "source": SOURCE_LABEL,
                    "endpoint": r.url,
                    "payload": r.json(),
                },
            )

        @task(task_id="ingest_kafka_to_raw")
        def ingest_kafka_to_raw() -> int:
            return consume_raw_to_postgres(
                topic=TOPIC_ODS_ECONOMIC_RAW,
                table="ods_economic_payloads",
                group_id="workshop-raw-ods-economic-loader",
            )

        extract_to_kafka() >> ingest_kafka_to_raw()


dag = ods_eurostat_sdg_fetch()
