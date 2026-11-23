# Go unit tests for the Zerobus mock lib (standard library only). Build context is the mock_lib directory.
#   docker build --network host -f go/Dockerfile -t zbmock-go:test .
#   docker run --rm --network host zbmock-go:test
FROM golang:1.25.4-bookworm
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /zbmock/go
COPY go/go.mod ./
COPY go/zerobusmock.go go/zerobusmock_test.go ./
# The tests read ../scenarios.json, which is /zbmock/scenarios.json here.
COPY scenarios.json /zbmock/scenarios.json
RUN gofmt -l . | (! grep .) && go vet ./...
CMD ["go", "test", "-count=1", "-v", "./..."]
