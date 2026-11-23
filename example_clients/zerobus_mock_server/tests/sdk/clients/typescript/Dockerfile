# SDK matrix client: TypeScript. @databricks/zerobus-ingest-sdk 1.5.0 (repo version), from the npm registry.
FROM node:22-bookworm-slim
ARG NPM_CONFIG_REGISTRY=https://registry.npmjs.org
ENV NPM_CONFIG_REGISTRY=${NPM_CONFIG_REGISTRY}
WORKDIR /client
COPY package.json /client/package.json
RUN npm install --no-audit --no-fund --omit=dev @databricks/zerobus-ingest-sdk@1.5.0 apache-arrow@18.1.0 avsc@5.7.9
COPY client.js /client/client.js
COPY run.sh /client/run.sh
RUN chmod +x /client/run.sh
ENTRYPOINT ["/client/run.sh"]
