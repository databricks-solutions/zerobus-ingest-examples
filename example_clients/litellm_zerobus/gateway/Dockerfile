# LiteLLM Gateway (the LiteLLM proxy server). The zerobus callback ships in LiteLLM 1.105.0 and above.
FROM python:3.12-slim
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG LITELLM_VERSION=1.109.1
RUN pip install --no-cache-dir --index-url "$PIP_INDEX_URL" "litellm[proxy]==${LITELLM_VERSION}"
WORKDIR /app
COPY config.yaml /app/config.yaml
EXPOSE 4000
CMD ["litellm", "--config", "/app/config.yaml", "--port", "4000"]
