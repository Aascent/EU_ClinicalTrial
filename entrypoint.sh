#!/bin/bash
set -e

# Export all current environment variables safely quoted for cron subshell execution
python3 -c "import os, shlex; f = open('/app/env.sh', 'w'); f.write('export PATH=' + shlex.quote(os.environ.get('PATH', '/usr/local/bin:/usr/bin:/bin')) + '\n'); [f.write('export ' + k + '=' + shlex.quote(v) + '\n') for k, v in os.environ.items() if 'no_proxy' not in k and k != 'PATH']; f.close()"
chmod +x /app/env.sh

# If a custom CRON_SCHEDULE is supplied via environment variable, update crontab
if [ -n "$CRON_SCHEDULE" ]; then
    echo "Updating cron schedule to: $CRON_SCHEDULE"
    echo "PATH=/usr/local/bin:/usr/bin:/bin" > /etc/cron.d/ctis-cron
    echo "$CRON_SCHEDULE root /bin/bash -c \". /app/env.sh && cd /app && python -m ctis_etl.main --mode incremental >> /app/logs/cron.log 2>&1\"" >> /etc/cron.d/ctis-cron
    echo "" >> /etc/cron.d/ctis-cron
    chmod 0644 /etc/cron.d/ctis-cron
fi

# Ensure log files exist and redirect cron output to container stdout
touch /app/logs/cron.log
touch /app/logs/ctis_etl.log

echo "=========================================================="
echo "EU CTIS Data Pipeline Container Started"
echo "Active Storage Backend : ${STORAGE_BACKEND:-s3}"
echo "Active State Backend   : ${STATE_BACKEND:-dynamodb}"
echo "Cron Schedule          : ${CRON_SCHEDULE:-0 2 * * *}"
echo "=========================================================="

echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Starting cron daemon in background (for scheduled new trials & update checks)..."
cron

# Publish initial status report immediately so dashboard is available from minute 1
echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Publishing initial status dashboard..."
python -m ctis_etl.main --mode report || true

# Launch initial historical backfill (extract all historical data by default, or as configured)
if [ "${RUN_ON_STARTUP:-true}" = "true" ]; then
    STARTUP_MODE="${STARTUP_MODE:-historical}"
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Running initial sync on container startup with mode: ${STARTUP_MODE}..."
    python -m ctis_etl.main --mode "${STARTUP_MODE}"
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Initial startup sync completed."
fi

# Stream application and cron logs to Docker standard output
echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Following pipeline logs..."
tail -F /app/logs/ctis_etl.log /app/logs/cron.log
