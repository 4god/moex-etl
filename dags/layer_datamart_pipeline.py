from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from common.pipeline_utils import DS_DATAMART_READY, DS_VAULT_READY, run_pipeline_sql


@dag(
    dag_id="layer_datamart_publish",
    description="Datamart layer DAG triggered by vault dataset",
    start_date=datetime(2024, 1, 1),
    schedule=[DS_VAULT_READY],
    catchup=False,
    tags=["workshop", "layer", "datamart"],
)
def layer_datamart_publish() -> None:
    """Publish data marts after Vault load is complete."""

    @task(outlets=[DS_DATAMART_READY])
    def build_datamart_layer() -> None:
        run_pipeline_sql("DV-310_datamart_publish")

    build_datamart_layer()


dag = layer_datamart_publish()
