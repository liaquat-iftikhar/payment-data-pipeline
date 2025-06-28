# Use official Python base image
FROM python:3.11-slim

# Set workdir inside container
WORKDIR /app

# Install system dependencies for airflow (if needed) and your project
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY pipeline/ ./pipeline/
COPY dags/ ./dags/
COPY config/ ./config/

# Environment variables for Dynaconf to pick configs in /app/config
ENV DYNACONF_SETTINGS_PATH="/app/config/settings.toml"

# Set PYTHONPATH so airflow and dags can import your pipeline modules
ENV PYTHONPATH="/app/pipeline"

# Entrypoint can be airflow scheduler or airflow webserver,
# but here we just set default CMD for airflow scheduler example
CMD ["airflow", "scheduler"]
