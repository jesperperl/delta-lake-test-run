FROM openjdk:11-jdk-slim

RUN apt-get update && apt-get install -y wget python3 python3-pip

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.3.0-bin-hadoop3.tgz && \
    mv spark-3.3.0-bin-hadoop3 /opt/spark && \
    rm spark-3.3.0-bin-hadoop3.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install PySpark, Delta Lake, and pandas
RUN pip3 install pyspark==3.3.0 delta-spark==2.2.0 pandas

WORKDIR /app
