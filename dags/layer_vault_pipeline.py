from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from common.pipeline_utils import DS_STG_CBR_READY, DS_STG_MOEX_READY, DS_VAULT_READY, run_pipeline_sql


@dag(
    dag_id="layer_vault_load",
    description="Vault layer DAG triggered by STG source datasets",
    start_date=datetime(2024, 1, 1),
    schedule=(DS_STG_MOEX_READY & DS_STG_CBR_READY),
    catchup=False,
    tags=["workshop", "layer", "vault"],
)
def layer_vault_load() -> None:
    """Load Data Vault from source STG layers."""

    @task(outlets=[DS_VAULT_READY])
    def build_vault_layer() -> None:
        run_pipeline_sql("DV-210_vault_load")

    build_vault_layer()


dag = layer_vault_load()
