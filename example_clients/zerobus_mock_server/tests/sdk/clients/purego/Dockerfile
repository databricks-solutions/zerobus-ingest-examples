# SDK matrix client: pure Go (no cgo, no Rust core). Build step (build.sh) compiles it against the monorepo's purego module.
FROM golang:1.25-bookworm
ARG GOPROXY=https://proxy.golang.org
ENV GOPROXY=${GOPROXY}
WORKDIR /client
COPY main.go go.mod build.sh run.sh /client/
RUN chmod +x /client/build.sh /client/run.sh
ENTRYPOINT ["/client/run.sh"]
