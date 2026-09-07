FROM apache/airflow:3.3.1-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY infra/airflow/requirements.txt /tmp/airflow-assignment-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-assignment-requirements.txt

ARG SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7
RUN python -c "from pyspark.sql import SparkSession; session = SparkSession.builder.master('local[1]').config('spark.jars.packages', '${SPARK_KAFKA_PACKAGE}').getOrCreate(); session.stop()"
RUN chmod -R g+rwX /home/airflow/.ivy2

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/opt/airflow/project
WORKDIR /opt/airflow/project
