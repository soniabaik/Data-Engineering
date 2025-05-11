FROM bitnami/spark:latest

# Switch to root user for installing packages and creating directories
USER root

# Ensure apt lists directory exists
RUN mkdir -p /var/lib/apt/lists/partial

# Clean apt cache and update apt-get
RUN apt-get clean && apt-get update -o Dir::Cache::apt::Archives=/var/cache/apt/archives

# Install necessary packages
RUN apt-get install -y krb5-user curl

# Create necessary directories and adjust permissions
RUN mkdir -p /opt/bitnami/spark/jobs
RUN chmod -R 777 /opt/bitnami/spark/jobs

# Download Spark SQL Kafka JAR files
RUN curl -sL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/spark-sql-kafka-0-10_2.12-3.1.2.jar -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar
RUN curl -sL https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.1.2/spark-streaming-kafka-0-10_2.12-3.1.2.jar -o /opt/bitnami/spark/jars/spark-streaming-kafka-0-10_2.12-3.1.2.jar

# Download Kafka client libraries
RUN curl -sL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar -o /opt/bitnami/spark/jars/kafka-clients-2.8.1.jar
RUN curl -sL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar
RUN curl -sL https://repo1.maven.org/maven2/com/google/code/gson/gson/2.8.9/gson-2.8.9.jar -o /opt/bitnami/spark/jars/gson-2.8.9.jar

# Copy your stream analysis script
COPY ./analysis_stream.py /opt/bitnami/spark/jobs/analysis_stream.py

# Return to non-root user for security
USER 1001
