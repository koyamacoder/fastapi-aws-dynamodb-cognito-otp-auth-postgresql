FROM python:3.11-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.6.9 /uv /uvx /bin/

# Install system dependencies for psycopg
RUN apt-get update && apt-get install -y \
libpq-dev \
gcc \
python3-dev \
&& rm -rf /var/lib/apt/lists/*

COPY README.md .
COPY pyproject.toml .

RUN uv venv /opt/venv

ENV PATH=/opt/venv/bin:$PATH
ENV VIRTUAL_ENV=/opt/venv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv

# RUN uv python install
RUN uv sync --no-install-project

# Copy the rest of the application code
COPY . .

ENTRYPOINT ["/app/entrypoint.sh"]