# SDK matrix client: Go with cgo. The Go toolchain is copied from the official golang image into the
# rust:1.94-bookworm image, which provides cargo and gcc for the Rust core behind the cgo binding.
FROM golang:1.25-bookworm AS gosrc

FROM rust:1.94-bookworm
COPY --from=gosrc /usr/local/go /usr/local/go
ENV PATH=/usr/local/go/bin:$PATH GOPATH=/go
WORKDIR /client
COPY main.go build.sh run.sh /client/
RUN chmod +x /client/build.sh /client/run.sh
ENTRYPOINT ["/client/run.sh"]
