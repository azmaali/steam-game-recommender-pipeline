#!/bin/bash
set -e

# Clean up broken symlinks
rm -rf /opt/airflow/logs/latest

# Init DB if not already initialized
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init
fi

# Always run migrations (safe even if db is initialized)
airflow db migrate

# Create admin user if not exists
airflow users create \
  --username admin \
  --firstname Azma \
  --lastname Ali \
  --role Admin \
  --password admin \
  --email admin@example.com || true

# Start the webserver and scheduler
airflow webserver & airflow scheduler
