from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

from airflow.datasets import Dataset
from airflow.providers.postgres.hooks.postgres import PostgresHook

if TYPE_CHECKING:
    from kafka import KafkaConsumer, KafkaProducer

_LOG = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Kafka topics per source.
TOPIC_MOEX_RAW = "raw.moex.payloads"
TOPIC_CBR_RAW = "raw.cbr.payloads"
TOPIC_OPEN_METEO_RAW = "raw.open_meteo.payloads"
# Открытые REST (ODS DAG-и): тот же контракт сообщения, что у MOEX/CBR (source, endpoint, payload).
TOPIC_ODS_ECONOMIC_RAW = "raw.ods.economic.payloads"
TOPIC_ODS_TERRITORY_RAW = "raw.ods.territory.payloads"
TOPIC_ODS_PUBLIC_RAW = "raw.ods.public.payloads"
# World Bank: здоровье и среда (ожирение, загрязнение воздуха, диабет, урбанизация) — см. ods_health_worldbank_fetch.
TOPIC_ODS_HEALTH_RAW = "raw.ods.health.payloads"
# Eurostat (опросы/уверенность) + World Bank (цифровой охват, потребление) — ods_marketing_insight_fetch.
TOPIC_ODS_MARKET_RAW = "raw.ods.marketing.payloads"

# Dataset contracts between pipelines (Airflow Assets).
DS_STG_MOEX_READY = Dataset("dataset://stg/moex")
DS_STG_CBR_READY = Dataset("dataset://stg/cbr")
DS_STG_METEO_READY = Dataset("dataset://stg/meteo")
DS_VAULT_READY = Dataset("dataset://vault/loaded")
DS_DATAMART_READY = Dataset("dataset://datamart/published")
# История режимов погоды в analytics (тип 2 по Кимбаллу — см. DV-320 SQL и dbt).
DS_WEATHER_ANALYTICS_DIM_READY = Dataset("dataset://analytics/weather_dimension")


def _ensure_raw_kafka_metadata_columns(table: str) -> None:
    """Ensure Kafka metadata columns exist in raw table for inserts."""
    hook = PostgresHook(postgres_conn_id="dwh")
    hook.run(
        f"""
        ALTER TABLE raw.{table}
            ADD COLUMN IF NOT EXISTS kafka_topic TEXT,
            ADD COLUMN IF NOT EXISTS kafka_partition INTEGER,
            ADD COLUMN IF NOT EXISTS kafka_offset BIGINT
        """
    )


def publish_to_kafka(topic: str, message: dict) -> None:
    """Publish one JSON message into Kafka."""
    from kafka import KafkaProducer

    endpoint = (message.get("endpoint") or "")[:800]
    _LOG.info(
        "kafka_publish topic=%s source=%s endpoint=%s",
        topic,
        message.get("source"),
        endpoint,
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    try:
        producer.send(topic, message).get(timeout=30)
        producer.flush(timeout=10)
    finally:
        producer.close()


def consume_raw_to_postgres(topic: str, table: str, group_id: str) -> int:
    """Consume source topic and persist events into raw schema.

    Повторная вставка с тем же (source, endpoint) подавляется уникальным индексом
    на (source, endpoint_hash); см. sql/bootstrap/063_raw_payloads_endpoint_dedupe.sql.
    """
    from kafka import KafkaConsumer

    _ensure_raw_kafka_metadata_columns(table)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=5000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    hook = PostgresHook(postgres_conn_id="dwh")
    inserted = 0
    skipped_dup = 0
    try:
        conn = hook.get_conn()
        try:
            for msg in consumer:
                payload = msg.value
                source = payload.get("source")
                endpoint = payload.get("endpoint")
                params = (
                    source,
                    endpoint,
                    json.dumps(payload.get("payload")),
                    msg.topic,
                    msg.partition,
                    msg.offset,
                )
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO raw.{table}
                            (source, endpoint, payload, kafka_topic, kafka_partition, kafka_offset)
                        VALUES (%s, %s, %s::jsonb, %s, %s, %s)
                        ON CONFLICT (source, endpoint_hash) DO NOTHING
                        """,
                        params,
                    )
                    rc = cur.rowcount
                conn.commit()
                if rc:
                    inserted += 1
                    _LOG.info(
                        "raw_insert table=%s topic=%s source=%s partition=%s offset=%s endpoint=%s",
                        table,
                        topic,
                        source,
                        msg.partition,
                        msg.offset,
                        (endpoint or "")[:800],
                    )
                else:
                    skipped_dup += 1
                    _LOG.info(
                        "raw_skip_duplicate table=%s topic=%s source=%s partition=%s offset=%s endpoint=%s",
                        table,
                        topic,
                        source,
                        msg.partition,
                        msg.offset,
                        (endpoint or "")[:800],
                    )
        finally:
            conn.close()
    finally:
        consumer.close()
    if skipped_dup:
        _LOG.info(
            "consume_raw_to_postgres summary table=%s topic=%s inserted=%s skipped_duplicate=%s",
            table,
            topic,
            inserted,
            skipped_dup,
        )
    return inserted


def run_pipeline_sql(pipeline_folder: str) -> None:
    """Execute SQL scripts from one task folder under sql/tasks (root + nested)."""
    hook = PostgresHook(postgres_conn_id="dwh")
    base_path = f"/opt/airflow/sql/tasks/{pipeline_folder}"

    if not os.path.exists(base_path):
        return

    ordered_paths: list[str] = []
    root_scripts = [f"{base_path}/ddl.sql", f"{base_path}/dml.sql"]
    for root_script in root_scripts:
        if os.path.exists(root_script):
            ordered_paths.append(root_script)

    nested_paths: list[str] = []
    for current_root, dir_names, file_names in os.walk(base_path):
        dir_names.sort()
        for file_name in sorted(file_names):
            if not file_name.endswith(".sql"):
                continue
            full_path = os.path.join(current_root, file_name)
            if full_path in root_scripts:
                continue
            nested_paths.append(full_path)

    ordered_paths.extend(nested_paths)

    for full_path in ordered_paths:
        with open(full_path, "r", encoding="utf-8") as file:
            sql_text = file.read()

        executable_lines = [
            line for line in sql_text.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        if not executable_lines:
            continue

        hook.run(sql_text)
