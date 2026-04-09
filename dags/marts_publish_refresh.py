from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from common.pipeline_utils import DS_DATAMART_READY, DS_VAULT_READY, run_pipeline_sql


@dag(
    dag_id="marts_publish_refresh",
    description="Публикация витрин datamart после успешной загрузки vault",
    start_date=datetime(2024, 1, 1),
    schedule=[DS_VAULT_READY],
    catchup=False,
    tags=["workshop", "datamart"],
)
def marts_publish_refresh() -> None:
    @task(outlets=[DS_DATAMART_READY])
    def run_datamart_sql() -> None:
        run_pipeline_sql("DV-310_datamart_publish")

    run_datamart_sql()


dag = marts_publish_refresh()
