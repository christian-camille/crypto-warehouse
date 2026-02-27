#!/bin/bash
set -e

echo "Waiting for PostgreSQL..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -q; do
  sleep 1
done
echo "PostgreSQL is ready."

echo "Running database setup..."
python src/setup_db.py

echo "Starting API server..."
exec uvicorn api:app --app-dir src --host 0.0.0.0 --port 8000
