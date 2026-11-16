FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu

ARG ICEBERG_VERSION=1.10.1
ARG SPARK_VERSION=3.5.6
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Iceberg runtime + AWS bundle (S3FileIO), and the Kafka source with its
# runtime dependencies. Baked into the image so the job starts without
# resolving packages from the network.
RUN set -eux; cd /opt/spark/jars; \
    for url in \
      "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
    ; do curl -fsSL --retry 3 -O "$url"; done

RUN pip3 install --no-cache-dir pytest==8.3.4

RUN mkdir -p /checkpoints /opt/app && chown -R spark:spark /checkpoints /opt/app

ENV PYTHONPATH=/opt/app/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3

USER spark
WORKDIR /opt/app
