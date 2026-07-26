ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt /

USER airflow

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# dbt runs in its OWN virtualenv so its large dependency tree can never clash
# with Airflow's pinned packages. Airflow tasks shell out to this venv's `dbt`
# binary via dags/datawarehouse/dbt_runner.py (DBT_BIN points here).
COPY dbt/requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt