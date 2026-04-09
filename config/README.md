# Конфигурация вне кода

- **`open_data_sources.example.yaml`** — шаблон URL и параметров для DAG `ods_*` (экономика, справочники). Скопируйте в `open_data_sources.yaml` для переопределений; `open_data_sources.yaml` в `.gitignore`.
- Секреты (API keys): задаются в профиле `credentials` и при необходимости читаются в коде через `get_credentials` в `dags/common/open_data_settings.py`.

В контейнере Airflow каталог монтируется в `/opt/airflow/config`.
