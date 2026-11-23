# SDK matrix client: Python. Pinned SDK version (matches the repo's python SDK release used for this matrix).
FROM python:3.12-slim
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --no-cache-dir --index-url "$PIP_INDEX_URL" "databricks-zerobus-ingest-sdk[arrow,avro]==1.11.0" pyarrow==21.0.0
WORKDIR /client
COPY client.py /client/client.py
COPY run.sh /client/run.sh
RUN chmod +x /client/run.sh
ENTRYPOINT ["/client/run.sh"]
