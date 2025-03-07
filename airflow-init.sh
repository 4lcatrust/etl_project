#!/bin/bash

echo "⏳ Waiting for database to be ready..."
sleep 10

echo "🔄 Initializing Airflow database..."
airflow db init

echo "👤 Creating Airflow Admin User..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com

echo "✅ Airflow DB and User Setup Complete!"
echo "🚀 Starting Airflow Webserver..."
exec airflow webserver