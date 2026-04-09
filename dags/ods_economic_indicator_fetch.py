from __future__ import annotations

from datetime import datetime

import requests
from airflow.decorators import dag, task

from common.open_data_settings import get_source_config
from common.pipeline_utils import (
    TOPIC_ODS_ECONOMIC_RAW,
    consume_raw_to_postgres,
    publish_to_kafka,
)

SOURCE_KEY = "national_accounts_gdp"
SOURCE_LABEL = "ODS_ECONOMIC_INDICATOR"


@dag(
    dag_id="ods_economic_indicator_fetch",
    description="Публичные макроданные: REST → Kafka → raw.ods_economic_payloads (как MOEX/CBR).",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["workshop", "ods", "open-data", "kafka"],
)
def ods_economic_indicator_fetch() -> None:
    @task
    def extract_to_kafka() -> None:
        spec = get_source_config(SOURCE_KEY)
        base = spec["base_url"].format(**spec["path_params"])
        params = dict(spec.get("query_params") or {})
        r = requests.get(base, params=params, timeout=120)
        r.raise_for_status()
        publish_to_kafka(
            TOPIC_ODS_ECONOMIC_RAW,
            {
                "source": SOURCE_LABEL,
                "endpoint": r.url,
                "payload": r.json(),
            },
        )

    @task
    def ingest_kafka_to_raw() -> int:
        return consume_raw_to_postgres(
            topic=TOPIC_ODS_ECONOMIC_RAW,
            table="ods_economic_payloads",
            group_id="workshop-raw-ods-economic-loader",
        )

    extract_to_kafka() >> ingest_kafka_to_raw()


dag = ods_economic_indicator_fetch()
