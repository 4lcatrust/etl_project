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

# Required secrets: refuse to boot rather than silently falling back to a well-known
# admin/admin login. Checked here (not just in compose) because this script is the first
# thing every `docker compose up` runs, regardless of which services are involved.
if [[ -z "${AIRFLOW_ADMIN_PASSWORD:-}" ]]; then
    echo "❌ AIRFLOW_ADMIN_PASSWORD is not set in .env -- refusing to start with a default admin login." >&2
    exit 1
fi
if [[ -z "${GRAFANA_ADMIN_PASSWORD:-}" ]]; then
    # Grafana itself is a separate --profile monitoring container this script never touches,
    # but its own undocumented internal default is also "admin" -- this is still the earliest,
    # most visible place to catch a missing .env secret before anything boots weak.
    echo "❌ GRAFANA_ADMIN_PASSWORD is not set in .env -- refusing to start with a default admin login." >&2
    exit 1
fi
if [[ -z "${REDIS_PASSWORD:-}" ]]; then
    echo "❌ REDIS_PASSWORD is not set in .env -- refusing to start with an unauthenticated Celery broker." >&2
    exit 1
fi

# Admin username isn't a secret; the password now always comes from .env (validated above).
AIRFLOW_ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}"

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

# docling pool: 1 slot so the heavy PDF parse (torch models) never runs more than once at a
# time and stays within the worker mem_limit alongside the Spark drivers.
echo "🔧 Creating docling pool (1 slot)..."
airflow pools set docling 1 "Concurrent docling PDF parses (memory cap)" || echo "⚠️ Failed to set docling pool"

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