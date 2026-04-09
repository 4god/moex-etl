from __future__ import annotations

from airflow.decorators import dag

from common.dag_defaults import TAGS_ODS_OPEN_DATA, WORKSHOP_START_DATE
from common.operators.workshop_kafka import (
    WorkshopJsonToKafkaOperator,
    WorkshopKafkaConsumeToRawOperator,
)
from common.pipeline_utils import TOPIC_ODS_TERRITORY_RAW

SOURCE_KEY = "territory_catalog"
SOURCE_LABEL = "ODS_TERRITORY_CATALOG"


@dag(
    dag_id="ods_territory_reference_fetch",
    description="Справочник территорий: REST → Kafka → raw.ods_territory_payloads.",
    start_date=WORKSHOP_START_DATE,
    schedule="@weekly",
    catchup=False,
    tags=TAGS_ODS_OPEN_DATA,
)
def ods_territory_reference_fetch() -> None:
    extract = WorkshopJsonToKafkaOperator(
        task_id="extract_to_kafka",
        source_key=SOURCE_KEY,
        source_label=SOURCE_LABEL,
        topic=TOPIC_ODS_TERRITORY_RAW,
        raw_table="ods_territory_payloads",
        timeout=120,
    )
    ingest = WorkshopKafkaConsumeToRawOperator(
        task_id="ingest_kafka_to_raw",
        topic=TOPIC_ODS_TERRITORY_RAW,
        table="ods_territory_payloads",
        group_id="workshop-raw-ods-territory-loader",
    )
    extract >> ingest


dag = ods_territory_reference_fetch()
