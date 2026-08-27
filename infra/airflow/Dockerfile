FROM apache/airflow:3.3.1-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY infra/airflow/requirements.txt /tmp/airflow-assignment-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-assignment-requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/opt/airflow/project
WORKDIR /opt/airflow/project

