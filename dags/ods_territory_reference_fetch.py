from __future__ import annotations

from datetime import datetime

import requests
from airflow.decorators import dag, task

from common.open_data_settings import get_source_config
from common.pipeline_utils import (
    TOPIC_ODS_TERRITORY_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
)

SOURCE_KEY = "territory_catalog"
SOURCE_LABEL = "ODS_TERRITORY_CATALOG"


@dag(
    dag_id="ods_territory_reference_fetch",
    description="Справочник территорий: REST → Kafka → raw.ods_territory_payloads.",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["workshop", "ods", "open-data", "kafka"],
)
def ods_territory_reference_fetch() -> None:
    @task
    def extract_to_kafka() -> None:
        spec = get_source_config(SOURCE_KEY)
        url = spec["url"]
        params = dict(spec.get("query_params") or {})
        r = requests.get(url, params=params, timeout=120)
        r.raise_for_status()
        publish_to_kafka(
            TOPIC_ODS_TERRITORY_RAW,
            {
                "source": SOURCE_LABEL,
                "endpoint": r.url,
                "payload": r.json(),
            },
        )

    @task
    def ingest_kafka_to_raw() -> int:
        return consume_raw_to_postgres(
            topic=TOPIC_ODS_TERRITORY_RAW,
            table="ods_territory_payloads",
            group_id="workshop-raw-ods-territory-loader",
        )

    extract_to_kafka() >> ingest_kafka_to_raw()


dag = ods_territory_reference_fetch()
