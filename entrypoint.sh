#! /bin/bash

# Set environment variables from .env file
set -e

>&2 echo "Running migrations"
alembic -c alembic.ini upgrade head

# Run the application
uvicorn trucost.main:app --host 0.0.0.0 --port 8000 --reload