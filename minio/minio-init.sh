#!/bin/bash
set -e

# Start MinIO in the background
/usr/bin/minio server /data --console-address ":9001" &
MINIO_PID=$!

echo "⏳ Waiting for MinIO to be live on port 9000..."
until curl -s http://localhost:9000/minio/health/live >/dev/null; do
  echo "🔁 Waiting for live check..."
  sleep 2
done

echo "✅ MinIO live. Waiting for mc to connect..."

until mc alias set minio http://localhost:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  echo "🔁 Retrying mc alias set..."
  sleep 2
done

echo "✅ mc alias set successful. Creating buckets..."
mc mb minio/staging || true            # API connector landing (NDJSON)
mc mb minio/iceberg-warehouse || true  # Iceberg lake (bronze/silver/gold via Spark + ClickHouse)

echo "✅ Buckets created. Passing control to MinIO process."

wait $MINIO_PID
