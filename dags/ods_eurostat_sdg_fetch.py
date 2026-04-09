from __future__ import annotations

from airflow.decorators import dag

from common.dag_defaults import TAGS_ODS_OPEN_DATA, WORKSHOP_START_DATE
from common.operators.workshop_kafka import (
    WorkshopJsonToKafkaOperator,
    WorkshopKafkaConsumeToRawOperator,
)
from common.pipeline_utils import TOPIC_ODS_ECONOMIC_RAW

SOURCE_KEY = "eurostat_sdg_sample"
SOURCE_LABEL = "ODS_EUROSTAT_SDG_SAMPLE"


@dag(
    dag_id="ods_eurostat_sdg_fetch",
    description="Eurostat (JSON API): REST → Kafka → raw.ods_economic_payloads.",
    start_date=WORKSHOP_START_DATE,
    schedule="@weekly",
    catchup=False,
    tags=[*TAGS_ODS_OPEN_DATA, "eurostat"],
)
def ods_eurostat_sdg_fetch() -> None:
    extract = WorkshopJsonToKafkaOperator(
        task_id="extract_to_kafka",
        source_key=SOURCE_KEY,
        source_label=SOURCE_LABEL,
        topic=TOPIC_ODS_ECONOMIC_RAW,
        timeout=180,
    )
    ingest = WorkshopKafkaConsumeToRawOperator(
        task_id="ingest_kafka_to_raw",
        topic=TOPIC_ODS_ECONOMIC_RAW,
        table="ods_economic_payloads",
        group_id="workshop-raw-ods-economic-loader",
    )
    extract >> ingest


dag = ods_eurostat_sdg_fetch()
