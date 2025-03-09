#!/bin/bash
set -e

/bin/bash /install-spark.sh

echo "🔄 Initializing Airflow database..."
airflow db check
airflow db init

echo "👤 Creating Airflow Admin User..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com

if [[ -f /opt/airflow/variables.json ]]; then
    airflow variables import /opt/airflow/variables.json
fi


echo "✅ Airflow DB and User Setup Complete!"

echo "🚀 Starting Airflow Webserver..."
exec airflow webserver