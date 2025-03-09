FROM apache/airflow:latest

# Switch to root to install system dependencies
USER root

# Set working directory
WORKDIR /opt/airflow

# Install Java & system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    wget \
    tar \
    gcc \
    g++ \
    make \
    build-essential \
    python3-dev \
    liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Apache Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME="/opt/spark"

# Download and extract Spark with verbose output
RUN mkdir -p ${SPARK_HOME}
RUN cd /tmp \
    && echo "Downloading Spark..." \
    && wget -v "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && echo "Extracting Spark..." \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && echo "Copying Spark files..." \
    && cp -r spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* ${SPARK_HOME}/ \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && echo "Setting permissions..." \
    && chmod -R 755 ${SPARK_HOME} 

# Set Spark environment variables
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Verify installation with detailed outputs
RUN echo "==== INSTALLATION VERIFICATION ====" \
    && echo "Java version:" \
    && java -version \
    && echo "Spark home directory contents:" \
    && ls -la ${SPARK_HOME} \
    && echo "Spark bin directory contents:" \
    && ls -la ${SPARK_HOME}/bin \
    && echo "Spark-submit version:" \
    && ${SPARK_HOME}/bin/spark-submit --version \
    && echo "==== END VERIFICATION ===="

# Change ownership of /opt/airflow to the existing airflow user
RUN chown -R airflow:root ${SPARK_HOME}

# Switch to airflow user before installing Python packages
USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to root to copy and fix script permissions
USER root
COPY airflow-init.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN chown airflow /entrypoint.sh

# Switch back to airflow user before running the container
USER airflow

# Entrypoint
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]