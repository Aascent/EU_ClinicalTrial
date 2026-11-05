# ==============================================================================
# ANZCTR Data Pipeline - Production Dockerfile with Cron Scheduler
# ==============================================================================
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output for Docker logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies: cron, ca-certificates, tzdata, curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    ca-certificates \
    tzdata \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency specifications first for Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code and configuration files
COPY anz_etl/ ./anz_etl/
COPY docs/ ./docs/
COPY test_raw_apis.py .
COPY crontab /etc/cron.d/anz-cron
COPY entrypoint.sh .

# Configure permissions for cron and entrypoint (ensuring Unix LF line endings)
RUN chmod 0644 /etc/cron.d/anz-cron && \
    chmod +x entrypoint.sh && \
    mkdir -p data logs quarantine

# Expose Web Dashboard Port (default 8081)
EXPOSE 8081

# Run entrypoint script
ENTRYPOINT ["/app/entrypoint.sh"]

