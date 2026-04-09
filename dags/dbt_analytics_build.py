"""Интеграция dbt в Airflow: вызов `dbt run` и `dbt test` после публикации datamart.

Почему не «без dbt run»: вся модель dbt (Jinja, `ref`, pre/post-hook, тесты, циклы в шаблонах)
выполняется **только** рантаймом dbt. Альтернатива — `dbt compile` и ручной `psql` по артефактам,
но тогда тесты, хуки и оркестрация зависимостей моделей нужно воспроизводить отдельно; для
воркшопа оркестрация через Airflow + те же CLI-команды — нормальная практика.

Опциональные переменные dbt: экспортируйте перед запуском или добавьте в команду флаг
`--vars '{"ключ": "значение"}'` (см. документацию dbt).
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from common.pipeline_utils import DS_DATAMART_READY


@dag(
    dag_id="dbt_analytics_build",
    description="dbt run → dbt test после Dataset datamart/published.",
    start_date=datetime(2024, 1, 1),
    schedule=[DS_DATAMART_READY],
    catchup=False,
    tags=["workshop", "dbt"],
)
def dbt_analytics_build() -> None:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
set -euo pipefail
cd /opt/airflow/dbt
exec dbt run --project-dir . --profiles-dir . --target dev
""",
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt"},
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
set -euo pipefail
cd /opt/airflow/dbt
exec dbt test --project-dir . --profiles-dir . --target dev
""",
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt"},
    )

    dbt_run >> dbt_test


dag = dbt_analytics_build()
