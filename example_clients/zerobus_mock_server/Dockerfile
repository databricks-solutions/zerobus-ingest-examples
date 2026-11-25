# Zerobus mock server image.
#
# Build (the box's own Python is too old, so everything runs in Docker):
#   docker build --network host --build-arg PIP_INDEX_URL=https://pypi-proxy.cloud.databricks.com/simple -t zerobus-mock-server .
#   docker build --network host --target test ... -t zerobus-mock-server:test .   # test image
FROM python:3.12-slim AS base
LABEL org.opencontainers.image.title="unofficial-zerobus-mock-server" \
      org.opencontainers.image.description="UNOFFICIAL Zerobus Ingest REST and gRPC mock server for integration tests. Not affiliated with Databricks." \
      org.opencontainers.image.source="https://github.com/databricks-solutions/zerobus-ingest-examples" \
      org.opencontainers.image.version="0.1.0"
ARG PIP_INDEX_URL=https://pypi.org/simple
ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --index-url "$PIP_INDEX_URL" -r requirements.txt
COPY src ./src
RUN chmod -R a+rX /app
ENV PYTHONPATH=/app/src
EXPOSE 8443 50051
ENTRYPOINT ["python", "-m", "zerobus_mock_server"]
CMD ["--config", "/config/config.yaml"]

FROM base AS test
ARG PIP_INDEX_URL=https://pypi.org/simple
COPY requirements-test.txt ./
RUN pip install --index-url "$PIP_INDEX_URL" -r requirements-test.txt
COPY tests ./tests
COPY pytest.ini ./
RUN chmod -R a+rX /app
ENTRYPOINT []
CMD ["python", "-m", "pytest", "-v", "tests"]
