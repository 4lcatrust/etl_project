#!/bin/bash

echo "🔄 Initializing Airflow database..."
airflow db check || airflow db init

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

echo "🚀 Starting Airflow Scheduler..."
exec airflow scheduler

echo "🚀 Starting Airflow Webserver..."
exec airflow webserver