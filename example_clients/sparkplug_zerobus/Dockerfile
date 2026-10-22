# The connector: Sparkplug B (MQTT) to Databricks through Zerobus (its web UI is prebuilt; protobuf runs its fast
# upb implementation).
#
#   docker build -t sparkplug-zerobus --build-arg PIP_INDEX_URL=https://pypi.org/simple .
#   docker run -d --env-file connector.env -p 127.0.0.1:8001:8001 \
#     --mount type=bind,source="$PWD/my-config.yaml",target=/app/sparkplug_connector/config/config.yaml \
#     --mount type=volume,source=connector-wal,target=/app/wal \
#     sparkplug-zerobus --config /app/sparkplug_connector/config/config.yaml
#
# my-config.yaml: your copy of sparkplug_connector/config/config.yaml (source enabled, table set), mounted over
# the example (--mount stops if the file is missing; -v would mount an empty directory). connector.env holds
# the variables the config reads (README.md lists them). The write-ahead log (zerobus.wal) is in /app/wal.
FROM python:3.12-slim
ARG PIP_INDEX_URL=https://pypi.org/simple
ENV PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --index-url "$PIP_INDEX_URL" -r requirements.txt
COPY sparkplug_connector ./sparkplug_connector
RUN mkdir -p /app/wal && useradd --create-home connector && chown -R connector /app
USER connector
EXPOSE 8001
# Health: GET /health on the web UI's port (web_ui.port in the config; CONNECTOR_HEALTH_PORT if it isn't 8001)
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/health' % os.environ.get('CONNECTOR_HEALTH_PORT', '8001'), timeout=4)"]
ENTRYPOINT ["python", "-m", "sparkplug_connector"]
