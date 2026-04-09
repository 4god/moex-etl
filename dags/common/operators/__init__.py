"""Кастомные операторы воркшопа (регистрируются плагином airflow/plugins/workshop_plugin.py)."""

from common.operators.workshop_kafka import (
    WorkshopJsonToKafkaOperator,
    WorkshopKafkaConsumeToRawOperator,
)

__all__ = [
    "WorkshopJsonToKafkaOperator",
    "WorkshopKafkaConsumeToRawOperator",
]
