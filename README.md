# Trucost Backend

A FastAPI-based backend application designed to help manage and optimize AWS cloud costs.

## Description

Trucost is a cloud cost management solution that provides a robust backend API for tracking, analyzing, and optimizing cloud infrastructure costs. The application is built using FastAPI and PostgreSQL, offering a scalable and efficient way to manage cloud spending.

## Features

- FastAPI-based REST API
- PostgreSQL database integration
- Authentication service
- CORS support
- Environment-based configuration
- YAML-based metadata management

## Prerequisites

- Python 3.13 or higher
- PostgreSQL database
- pip (Python package manager)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/trucost-backend.git
cd trucost-backend
```

2. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -e .
```

## Configuration

1. Create a `meta.yaml` file in the root directory with the following structure:

```yaml
name: Trucost
version: 0.1.0
description: Save cloud costs
author: Your Name
settings:
  cors_origins:
    - http://localhost:9090
  db_host: localhost
  db_port: 5432
  db_user: postgres
  db_password: postgres
  db_name: postgres
  db_schema: public
```

2. Set up environment variables (optional):

```bash
export METADATA_PATH=meta.yaml
export CORS_ORIGINS=localhost:9090
export DB_HOST=localhost:5432
export DB_USER=postgres
export DB_PASSWORD=postgres
export DB_NAME=postgres
export DB_SCHEMA=public
```

```bash
set METADATA_PATH=meta.yaml
set CORS_ORIGINS=localhost:9090
set DB_HOST=localhost:5432
set DB_USER=postgres
set DB_PASSWORD=postgres
set DB_NAME=postgres
set DB_SCHEMA=public
```

## Running the Application

1. Start the application:

```bash
python main.py
```

The API will be available at `http://localhost:8000`

## API Documentation

Once the application is running, you can access the interactive API documentation at:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Project Structure

```
trucost-backend/
├── core/
│   ├── app.py
│   ├── metadata.py
│   ├── settings.py
│   └── services/
│       ├── auth.py
│       ├── base.py
│       └── db.py
├── main.py
├── meta.yaml
├── pyproject.toml
└── README.md
```

## License

[Add your license information here]

## Author

Vivek Baraiya
