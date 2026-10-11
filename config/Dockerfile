# One image used by master, worker1 and worker2: Hadoop 3.3.6 + Spark 3.5.1 + Hive 3.1.3 + Java 11
# Build context is the repo root (see docker-compose.yml): docker compose build
FROM ubuntu:22.04

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.1
ARG HIVE_VERSION=3.1.3
ARG JAVA8_URL=https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u504-b01/OpenJDK8U-jre_x64_linux_hotspot_8u504b01.tar.gz
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-11-jdk-headless python3 python3-pip curl ca-certificates netcat-openbsd \
      procps iputils-ping less nano git \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-home \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir pandas pyarrow pymongo numpy

# ---- Hadoop ----
RUN curl -fL --retry 5 --retry-delay 5 -o /tmp/hadoop.tgz \
      https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
 && tar -xzf /tmp/hadoop.tgz -C /opt \
 && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm -rf /tmp/hadoop.tgz /opt/hadoop/share/doc

# ---- Spark (prebuilt for Hadoop 3) ----
RUN curl -fL --retry 5 --retry-delay 5 -o /tmp/spark.tgz \
      https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
 && rm -f /tmp/spark.tgz

# ---- Hive (replace old guava with the one Hadoop 3.3 ships, otherwise Hive fails to start) ----
RUN curl -fL --retry 5 --retry-delay 5 -o /tmp/hive.tgz \
      https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
 && tar -xzf /tmp/hive.tgz -C /opt \
 && mv /opt/apache-hive-${HIVE_VERSION}-bin /opt/hive \
 && rm -f /tmp/hive.tgz /opt/hive/lib/guava-*.jar \
 && cp /opt/hadoop/share/hadoop/common/lib/guava-*.jar /opt/hive/lib/

# Hive 3.1.3 requires Java 8; Hadoop and Spark continue to use Java 11.
RUN curl -fL --retry 5 --retry-delay 5 -o /tmp/java8.tgz "${JAVA8_URL}" \
 && mkdir -p /opt/java8 \
 && tar -xzf /tmp/java8.tgz --strip-components=1 -C /opt/java8 \
 && rm -f /tmp/java8.tgz

ENV JAVA_HOME=/usr/lib/jvm/java-home \
    HIVE_JAVA_HOME=/opt/java8 \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_COMMON_HOME=/opt/hadoop \
    HADOOP_HDFS_HOME=/opt/hadoop \
    HADOOP_YARN_HOME=/opt/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_HOME=/opt/spark \
    HIVE_HOME=/opt/hive \
    HIVE_CONF_DIR=/opt/hive/conf \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/hive/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# ---- Configuration and scripts (last, so changing them rebuilds in seconds) ----
COPY config/hadoop/ /opt/hadoop/etc/hadoop/
COPY config/spark/  /opt/spark/conf/
COPY config/hive/   /opt/hive/conf/
COPY scripts/       /opt/scripts/

RUN sed -i 's/\r$//' /opt/scripts/*.sh /opt/hadoop/etc/hadoop/hadoop-env.sh \
      /opt/spark/conf/spark-env.sh /opt/hive/conf/hive-env.sh \
 && chmod +x /opt/scripts/*.sh \
 && printf '#!/bin/bash\nexec /opt/hive/bin/beeline -n root "$@"\n' > /usr/local/bin/beeline \
 && chmod +x /usr/local/bin/beeline \
 && git config --global --add safe.directory '*'

WORKDIR /opt/project
ENTRYPOINT ["/opt/scripts/entrypoint.sh"]
