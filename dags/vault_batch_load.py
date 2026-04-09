from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from common.pipeline_utils import DS_STG_CBR_READY, DS_STG_MOEX_READY, DS_VAULT_READY, run_pipeline_sql


@dag(
    dag_id="vault_batch_load",
    description="Загрузка Data Vault (hub/link/satellite) после готовности STG по MOEX и CBR",
    start_date=datetime(2024, 1, 1),
    schedule=(DS_STG_MOEX_READY & DS_STG_CBR_READY),
    catchup=False,
    tags=["workshop", "vault"],
)
def vault_batch_load() -> None:
    @task(outlets=[DS_VAULT_READY])
    def run_vault_sql() -> None:
        run_pipeline_sql("DV-210_vault_load")

    run_vault_sql()


dag = vault_batch_load()
