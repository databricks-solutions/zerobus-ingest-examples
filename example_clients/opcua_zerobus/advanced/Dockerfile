FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid appuser --create-home appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY opcua_client.py forwarding_agent.py ./

RUN mkdir -p /app/config /app/data /app/certs && \
    chown -R appuser:appuser /app

USER appuser
