"""
Плагин Airflow: регистрирует операторы воркшопа (видны в UI → Admin → Plugins).
Код операторов в dags/common/operators — нужен PYTHONPATH=/opt/airflow/dags (см. Dockerfile).
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from common.operators.workshop_kafka import (
    WorkshopJsonToKafkaOperator,
    WorkshopKafkaConsumeToRawOperator,
)


class WorkshopAirflowPlugin(AirflowPlugin):
    name = "workshop"
    operators = [WorkshopJsonToKafkaOperator, WorkshopKafkaConsumeToRawOperator]
