FROM apache/airflow:latest

# Switch to root to install system dependencies
USER root

# Set working directory
WORKDIR /opt/airflow

# Install required system packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    build-essential \
    python3-dev \
    liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

# Change ownership of /opt/airflow to the existing airflow user
RUN chown -R airflow /opt/airflow

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
