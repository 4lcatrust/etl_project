#!/bin/bash
set -e

echo "🔄 Initializing Airflow database..."
airflow db init
airflow db upgrade

# Ensure Airflow CLI is available
export PATH="$HOME/.local/bin:$PATH"

# Function to safely add connections
add_connection_if_not_exists() {
    local conn_id=$1
    shift
    if ! airflow connections list 2>/dev/null | grep -q "^$conn_id "; then
        echo "🔗 Creating connection: $conn_id"
        airflow connections add "$conn_id" "$@" || echo "⚠️ Failed to create connection $conn_id"
    else
        echo "🔗 Connection '$conn_id' already exists, skipping"
    fi
}

# Admin credentials come from the environment (.env); fall back to demo defaults.
AIRFLOW_ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}"
AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-admin}"

# Check if user already exists to avoid errors on restart
if ! airflow users list 2>/dev/null | grep -q "^${AIRFLOW_ADMIN_USER} \|${AIRFLOW_ADMIN_USER}$"; then
    echo "👤 Creating admin user..."
    airflow users create \
      --username "${AIRFLOW_ADMIN_USER}" \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@example.com \
      --password "${AIRFLOW_ADMIN_PASSWORD}"
else
    echo "👤 Admin user already exists, skipping creation"
fi

echo "⏳ Waiting for services to be ready..."

# Wait for PostgreSQL
echo "🔍 Checking PostgreSQL..."
until pg_isready -h postgres_db -p 5432 -U "${POSTGRES_USER:-postgres}" 2>/dev/null; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

# Wait for ClickHouse
echo "🔍 Checking ClickHouse..."
until curl -s http://clickhouse_db:8123/ping > /dev/null 2>&1; do
    echo "Waiting for ClickHouse..."
    sleep 2
done

# Wait for MinIO
echo "🔍 Checking MinIO..."
until curl -s http://minio:9000/minio/health/live > /dev/null 2>&1; do
    echo "Waiting for MinIO..."
    sleep 2
done

# Wait for Spark Master
echo "🔍 Checking Spark Master..."
until curl -s http://spark-master:8080 > /dev/null 2>&1; do
    echo "Waiting for Spark Master..."
    sleep 2
done

echo "🔗 Creating connections..."

# Spark connection (standalone master, client deploy mode)
add_connection_if_not_exists 'spark' \
  --conn-type 'spark' \
  --conn-host 'spark://spark-master' \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode": "client", "spark-binary": "spark-submit"}'

# Spark connection (standalone master, cluster deploy mode) — Phase B2. Targets the REST
# submission server (port 6066) so the driver runs on spark-worker-1 and SparkSubmitOperator
# can poll its status. Kept separate so the client-mode 'spark' connection stays untouched.
add_connection_if_not_exists 'spark_cluster' \
  --conn-type 'spark' \
  --conn-host 'spark://spark-master' \
  --conn-port 6066 \
  --conn-extra '{"deploy-mode": "cluster", "spark-binary": "spark-submit"}'

# PostgreSQL connection (credentials from .env)
add_connection_if_not_exists 'postgres' \
  --conn-type 'postgres' \
  --conn-host 'postgres_db' \
  --conn-port 5432 \
  --conn-login "${POSTGRES_USER}" \
  --conn-password "${POSTGRES_PASSWORD}" \
  --conn-schema 'public'

# ClickHouse connection (credentials from .env)
add_connection_if_not_exists 'clickhouse' \
  --conn-type 'http' \
  --conn-host 'clickhouse_db' \
  --conn-port 8123 \
  --conn-login "${CLICKHOUSE_USER}" \
  --conn-password "${CLICKHOUSE_PASSWORD}"

# MinIO connection (credentials from .env)
add_connection_if_not_exists 'minio' \
  --conn-type 'aws' \
  --conn-host 'http://minio:9000' \
  --conn-login "${MINIO_ROOT_USER}" \
  --conn-password "${MINIO_ROOT_PASSWORD}" \
  --conn-extra '{"endpoint_url": "http://minio:9000"}'

# Spark pool: cap concurrent client-mode Spark drivers (each ~2g JVM in airflow_worker)
# regardless of worker/DAG concurrency, so many queued Spark tasks can't OOM the host.
# `pools set` is idempotent (upsert).
echo "🔧 Creating spark pool (2 slots)..."
airflow pools set spark 2 "Concurrent client-mode Spark drivers (memory cap)" || echo "⚠️ Failed to set spark pool"

echo "📊 Importing variables..."
if [[ -f /opt/airflow/variables.json ]]; then
    if airflow variables import /opt/airflow/variables.json; then
        echo "✅ Variables imported successfully"
    else
        echo "⚠️ Failed to import variables, but continuing..."
    fi
else
    echo "📊 No variables.json file found, skipping variable import"
fi

echo "🎉 Airflow initialization completed successfully!"
echo "🌐 Web UI will be available at http://localhost:8080 (user: ${AIRFLOW_ADMIN_USER})"