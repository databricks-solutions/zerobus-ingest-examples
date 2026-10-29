# The connector: Sparkplug B (MQTT) to Databricks through Zerobus. Python only (the web page has no build step);
# protobuf runs its fast upb implementation.
#
#   docker build -t sparkplug-zerobus --build-arg PIP_INDEX_URL=https://pypi.org/simple .
#   docker run -d --stop-timeout 30 -e DATABRICKS_CLIENT_SECRET -e SPARKPLUG_MQTT_PASSWORD -p 127.0.0.1:8001:8001 \
#     --mount type=bind,source="$PWD/sparkplug_connector/config/config.yaml",target=/app/sparkplug_connector/config/config.yaml \
#     --mount type=volume,source=connector-wal,target=/app/wal \
#     sparkplug-zerobus
#
# sparkplug_connector/config/config.yaml, edited (source enabled, table set), is mounted over the image's copy so
# the web page's saves land in it (--mount stops if the file is missing; -v would mount an empty directory).
# The two secrets come from the shell's environment (-e NAME; or --env-file connector.env: .env.example). The
# write-ahead log (zerobus.wal) is in /app/wal. A stop can take up to ~20 s (OFFLINE first, then the queued rows
# written): --stop-timeout 30, as Docker's default is 10 s.
# The container runs as uid 1000: on Linux the mounted config file must be writable by it for the web page's saves.
FROM python:3.12-slim
ARG PIP_INDEX_URL=https://pypi.org/simple
# CONNECTOR_LOG_FILES=false: logs to the console only (docker logs), no log files inside the container
ENV PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 CONNECTOR_LOG_FILES=false
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --index-url "$PIP_INDEX_URL" -r requirements.txt
COPY sparkplug_connector ./sparkplug_connector
# /app/wal and the uploaded certificates' directory exist in the image, so a volume mounted there is the connector's
RUN mkdir -p /app/wal && useradd --create-home connector && chown -R connector /app \
    && install -d -o connector -m 700 /home/connector/.sparkplug_connector /home/connector/.sparkplug_connector/certs
USER connector
EXPOSE 8001
# Health: GET /health on the web UI's port (web_ui.port in the config; CONNECTOR_HEALTH_PORT if it isn't 8001)
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/health' % os.environ.get('CONNECTOR_HEALTH_PORT', '8001'), timeout=4)"]
ENTRYPOINT ["python", "-m", "sparkplug_connector"]
