from __future__ import annotations

from datetime import datetime
import json

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

MOEX_URL = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/securities.json"
    "?iss.meta=off&iss.only=securities,marketdata&limit=100"
)


def extract_moex_to_raw() -> None:
    response = requests.get(MOEX_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()

    hook = PostgresHook(postgres_conn_id="dwh")
    hook.run(
        """
        INSERT INTO raw.moex_iss_payloads (source, endpoint, payload)
        VALUES (%s, %s, %s::jsonb)
        """,
        parameters=("MOEX_ISS", MOEX_URL, json.dumps(payload)),
    )


def run_sql_file(path: str) -> None:
    with open(path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    hook = PostgresHook(postgres_conn_id="dwh")
    hook.run(sql_text)


with DAG(
    dag_id="moex_etl_workshop",
    description="Workshop ETL: MOEX -> raw -> stg -> datamart",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["workshop", "etl", "moex"],
) as dag:
    extract_raw = PythonOperator(
        task_id="extract_moex_raw",
        python_callable=extract_moex_to_raw,
    )

    build_stg = PythonOperator(
        task_id="build_stg_layer",
        python_callable=run_sql_file,
        op_kwargs={"path": "/opt/airflow/sql/02_transform_stg.sql"},
    )

    build_datamart = PythonOperator(
        task_id="build_datamart_layer",
        python_callable=run_sql_file,
        op_kwargs={"path": "/opt/airflow/sql/03_build_datamarts.sql"},
    )

    extract_raw >> build_stg >> build_datamart
