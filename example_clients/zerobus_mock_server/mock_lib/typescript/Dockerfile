# TypeScript unit tests for the Zerobus mock lib. Build context is the mock_lib directory.
#   docker build --network host -f typescript/Dockerfile -t zbmock-typescript:test .
#   docker run --rm --network host zbmock-typescript:test
FROM node:22.21-bookworm-slim
ARG NPM_CONFIG_REGISTRY=https://registry.npmjs.org/
ENV NPM_CONFIG_REGISTRY=${NPM_CONFIG_REGISTRY}
WORKDIR /zbmock/typescript
COPY typescript/package.json typescript/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY typescript/tsconfig.json ./
COPY typescript/src ./src
COPY typescript/test ./test
# The tests read ../../scenarios.json, which is /zbmock/scenarios.json here.
COPY scenarios.json /zbmock/scenarios.json
RUN npm run --silent typecheck && echo "typecheck: ok"
CMD ["npm", "test", "--silent"]
