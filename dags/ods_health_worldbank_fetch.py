"""World Bank: здоровье и среда (ожидаемая продолжительность жизни, PM2.5, избыток веса у детей, урбанизация).

Данные в Metabase: джойн по стране/году из JSON (см. README). Индикаторы — в open_data_sources.example.yaml.
"""

from __future__ import annotations

from airflow.decorators import dag

from common.dag_defaults import TAGS_ODS_OPEN_DATA, WORKSHOP_START_DATE
from common.operators.workshop_kafka import (
    WorkshopJsonToKafkaOperator,
    WorkshopKafkaConsumeToRawOperator,
)
from common.pipeline_utils import TOPIC_ODS_HEALTH_RAW

# Ключи источников в open_data_sources.yaml (см. example).
_KEYS = (
    ("health_life_expectancy_wb", "ODS_HEALTH_WB_LIFE_EXPECTANCY"),
    ("health_air_pm25_wb", "ODS_HEALTH_WB_PM25"),
    ("health_overweight_u5_wb", "ODS_HEALTH_WB_OVERWEIGHT_U5"),
    ("health_urbanization_wb", "ODS_HEALTH_WB_URBAN_POP"),
)


@dag(
    dag_id="ods_health_worldbank_fetch",
    description="World Bank: LE, PM2.5, избыток веса (дети), урбанизация → Kafka → raw.ods_health_payloads.",
    start_date=WORKSHOP_START_DATE,
    schedule="@weekly",
    catchup=False,
    tags=TAGS_ODS_OPEN_DATA + ["health", "worldbank"],
)
def ods_health_worldbank_fetch() -> None:
    prev = None
    for source_key, label in _KEYS:
        t = WorkshopJsonToKafkaOperator(
            task_id=f"extract_{source_key}",
            source_key=source_key,
            source_label=label,
            topic=TOPIC_ODS_HEALTH_RAW,
            raw_table="ods_health_payloads",
            timeout=180,
        )
        if prev is not None:
            prev >> t
        prev = t

    ingest = WorkshopKafkaConsumeToRawOperator(
        task_id="ingest_kafka_to_raw",
        topic=TOPIC_ODS_HEALTH_RAW,
        table="ods_health_payloads",
        group_id="workshop-raw-ods-health-loader",
    )
    prev >> ingest


dag = ods_health_worldbank_fetch()
