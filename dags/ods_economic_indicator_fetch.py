from __future__ import annotations

from airflow.decorators import dag

from common.dag_defaults import TAGS_ODS_OPEN_DATA, WORKSHOP_START_DATE
from common.operators.workshop_kafka import (
    WorkshopJsonToKafkaOperator,
    WorkshopKafkaConsumeToRawOperator,
)
from common.pipeline_utils import TOPIC_ODS_ECONOMIC_RAW

SOURCE_KEY = "national_accounts_gdp"
SOURCE_LABEL = "ODS_ECONOMIC_INDICATOR"


@dag(
    dag_id="ods_economic_indicator_fetch",
    description="Публичные макроданные: REST → Kafka → raw.ods_economic_payloads (как MOEX/CBR).",
    start_date=WORKSHOP_START_DATE,
    schedule="@daily",
    catchup=False,
    tags=TAGS_ODS_OPEN_DATA,
)
def ods_economic_indicator_fetch() -> None:
    extract = WorkshopJsonToKafkaOperator(
        task_id="extract_to_kafka",
        source_key=SOURCE_KEY,
        source_label=SOURCE_LABEL,
        topic=TOPIC_ODS_ECONOMIC_RAW,
        raw_table="ods_economic_payloads",
        timeout=120,
    )
    ingest = WorkshopKafkaConsumeToRawOperator(
        task_id="ingest_kafka_to_raw",
        topic=TOPIC_ODS_ECONOMIC_RAW,
        table="ods_economic_payloads",
        group_id="workshop-raw-ods-economic-loader",
    )
    extract >> ingest


dag = ods_economic_indicator_fetch()
