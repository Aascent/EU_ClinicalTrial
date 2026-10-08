# ==============================================================================
# EU CTIS Data Pipeline - Production Dockerfile with Cron Scheduler
# ==============================================================================
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output for Docker logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies: cron and ca-certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency specifications first for Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code and configuration files
COPY ctis_etl/ ./ctis_etl/
COPY docs/ ./docs/
COPY crontab /etc/cron.d/ctis-cron
COPY entrypoint.sh .

# Configure permissions for cron and entrypoint (ensuring Unix LF line endings)
RUN chmod 0644 /etc/cron.d/ctis-cron && \
    chmod +x entrypoint.sh && \
    mkdir -p data logs quarantine

# Define volumes for persistent local data & logs (if used)
VOLUME ["/app/data", "/app/logs", "/app/quarantine"]

# Run entrypoint script
ENTRYPOINT ["/app/entrypoint.sh"]
