# SQL tasks

Здесь лежат SQL-скрипты, сгруппированные по папкам:

- **Регулярные ETL / DV** — подпапки вида `SRC-*`, `DV-*`; Airflow DAG-и выполняют их через `run_pipeline_sql("<имя_папки>")` (см. `dags/common/pipeline_utils.py`).
- **Ad-hoc / one-off** — папки вида `DE-1234_*`: разовые запросы, backfill, правки по тикетам; в DAG-и не подключены.
