#!/bin/bash
set -e

# Export all current environment variables safely quoted for cron subshell execution
python3 -c "import os, shlex; f = open('/app/env.sh', 'w'); f.write('export PATH=' + shlex.quote(os.environ.get('PATH', '/usr/local/bin:/usr/bin:/bin')) + '\n'); [f.write('export ' + k + '=' + shlex.quote(v) + '\n') for k, v in os.environ.items() if 'no_proxy' not in k and k != 'PATH']; f.close()"
chmod +x /app/env.sh

# If a custom CRON_SCHEDULE is supplied via environment variable, update crontab
if [ -n "$CRON_SCHEDULE" ]; then
    echo "Updating cron schedule to: $CRON_SCHEDULE"
    echo "PATH=/usr/local/bin:/usr/bin:/bin" > /etc/cron.d/anz-cron
    echo "$CRON_SCHEDULE root /bin/bash -c \". /app/env.sh && cd /app && python -m anz_etl.main --mode incremental >> /app/logs/cron.log 2>&1\"" >> /etc/cron.d/anz-cron
    echo "" >> /etc/cron.d/anz-cron
    chmod 0644 /etc/cron.d/anz-cron
fi

# Ensure log files exist and redirect cron output to container stdout
touch /app/logs/cron.log
touch /app/logs/anz_etl.log

PORT="${PORT:-8081}"

echo "=========================================================="
echo "ANZCTR Data Pipeline Container Started"
echo "Active Storage Backend : ${STORAGE_BACKEND:-local}"
echo "Active State Backend   : ${STATE_BACKEND:-sqlite}"
echo "Cron Schedule          : ${CRON_SCHEDULE:-0 3 * * *}"
echo "Dashboard Port         : ${PORT}"
echo "=========================================================="

echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Starting cron daemon in background..."
cron

# Start background Web Dashboard HTTP server on configured port
echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Starting Web Dashboard HTTP server on http://0.0.0.0:${PORT}..."
python -m http.server "${PORT}" --directory /app/data > /app/logs/webserver.log 2>&1 &

# Generate initial status report immediately
python -m anz_etl.main --mode report || true

# Launch startup sync if configured
if [ "${RUN_ON_STARTUP:-true}" = "true" ]; then
    STARTUP_MODE="${STARTUP_MODE:-status}"
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Running startup task with mode: ${STARTUP_MODE}..."
    python -m anz_etl.main --mode "${STARTUP_MODE}" || true
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Startup task finished."
fi

# Print status prominently
python -m anz_etl.main --mode status || true

echo "=========================================================="
echo " ANZCTR PIPELINE & DASHBOARD ARE READY AND RUNNING!"
echo " Web Dashboard : http://<server-ip>:${PORT}/"
echo "=========================================================="

# Stream application and cron logs to Docker standard output
echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Following pipeline logs..."
tail -F /app/logs/anz_etl.log /app/logs/cron.log

