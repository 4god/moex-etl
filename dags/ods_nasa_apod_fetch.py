from __future__ import annotations

from airflow.decorators import dag

from common.dag_defaults import TAGS_ODS_OPEN_DATA, WORKSHOP_START_DATE
from common.operators.workshop_kafka import (
    WorkshopJsonToKafkaOperator,
    WorkshopKafkaConsumeToRawOperator,
)
from common.pipeline_utils import TOPIC_ODS_PUBLIC_RAW

SOURCE_KEY = "nasa_apod"
SOURCE_LABEL = "ODS_NASA_APOD"


@dag(
    dag_id="ods_nasa_apod_fetch",
    description="NASA APOD: REST → Kafka → raw.ods_public_api_payloads.",
    start_date=WORKSHOP_START_DATE,
    schedule="@daily",
    catchup=False,
    tags=[*TAGS_ODS_OPEN_DATA, "nasa"],
)
def ods_nasa_apod_fetch() -> None:
    extract = WorkshopJsonToKafkaOperator(
        task_id="extract_to_kafka",
        source_key=SOURCE_KEY,
        source_label=SOURCE_LABEL,
        topic=TOPIC_ODS_PUBLIC_RAW,
        raw_table="ods_public_api_payloads",
        timeout=60,
    )
    ingest = WorkshopKafkaConsumeToRawOperator(
        task_id="ingest_kafka_to_raw",
        topic=TOPIC_ODS_PUBLIC_RAW,
        table="ods_public_api_payloads",
        group_id="workshop-raw-ods-public-loader",
    )
    extract >> ingest


dag = ods_nasa_apod_fetch()
