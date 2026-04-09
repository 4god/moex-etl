"""Открытая статистика для «маркетингового» дашборда: опросы (Eurostat) + макро/цифра (World Bank).

Eurostat ei_bsco_m — ежемесячные результаты опросов потребительской уверенности (индикаторы BS-*).
World Bank — охват интернета, доля домашнего потребления в ВВП, ВВП на душу (контекст платёжеспособности).
См. README: Metabase, backfill.
"""

from __future__ import annotations

from airflow.decorators import dag

from common.dag_defaults import TAGS_ODS_OPEN_DATA, WORKSHOP_START_DATE
from common.operators.workshop_kafka import (
    WorkshopJsonToKafkaOperator,
    WorkshopKafkaConsumeToRawOperator,
)
from common.pipeline_utils import TOPIC_ODS_MARKET_RAW

_KEYS = (
    ("market_eurostat_consumer_confidence_ea20", "ODS_MARKET_EUROSTAT_CONSUMER_CONF"),
    ("market_wb_internet_users", "ODS_MARKET_WB_INTERNET_USERS_PCT"),
    ("market_wb_hh_consumption_pctgdp", "ODS_MARKET_WB_HH_CONSUMPTION_PCT_GDP"),
    ("market_wb_gdp_per_capita", "ODS_MARKET_WB_GDP_PER_CAPITA"),
)


@dag(
    dag_id="ods_marketing_insight_fetch",
    description="Eurostat (опросы потребителей) + WB (интернет, потребление, ВВП/душу) → raw.ods_marketing_payloads.",
    start_date=WORKSHOP_START_DATE,
    schedule="@weekly",
    catchup=False,
    tags=TAGS_ODS_OPEN_DATA + ["marketing", "eurostat", "worldbank"],
)
def ods_marketing_insight_fetch() -> None:
    prev = None
    for source_key, label in _KEYS:
        t = WorkshopJsonToKafkaOperator(
            task_id=f"extract_{source_key}",
            source_key=source_key,
            source_label=label,
            topic=TOPIC_ODS_MARKET_RAW,
            raw_table="ods_marketing_payloads",
            timeout=240,
        )
        if prev is not None:
            prev >> t
        prev = t

    ingest = WorkshopKafkaConsumeToRawOperator(
        task_id="ingest_kafka_to_raw",
        topic=TOPIC_ODS_MARKET_RAW,
        table="ods_marketing_payloads",
        group_id="workshop-raw-ods-marketing-loader",
    )
    prev >> ingest


dag = ods_marketing_insight_fetch()
