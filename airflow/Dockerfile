FROM apache/airflow:2.10.4-python3.11

# Официальные пины для Python 3.11 + Airflow 2.10.4 (избегает pip ResolutionTooDeep).
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"

USER airflow
COPY airflow_local_settings.py /opt/airflow/airflow_local_settings.py
COPY plugins/ /opt/airflow/plugins/
COPY requirements.txt /requirements.txt

# Операторы в /opt/airflow/dags/common — плагин импортирует их при старте веб-сервера/воркеров.
ENV PYTHONPATH=/opt/airflow/dags
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /requirements.txt -c "${AIRFLOW_CONSTRAINTS_URL}" \
    && pip install --no-cache-dir "dbt-postgres==1.8.2"
